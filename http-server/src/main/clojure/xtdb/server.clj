(ns xtdb.server
  (:require [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [cognitect.transit :as transit]
            [muuntaja.core :as m]
            [muuntaja.format.core :as mf]
            [reitit.coercion :as r.coercion]
            [reitit.coercion.spec :as rc.spec]
            [reitit.core :as r]
            [reitit.http :as http]
            [reitit.http.coercion :as rh.coercion]
            [reitit.http.interceptors.exception :as ri.exception]
            [reitit.http.interceptors.muuntaja :as ri.muuntaja]
            [reitit.http.interceptors.parameters :as ri.parameters]
            [reitit.interceptor.sieppari :as r.sieppari]
            [reitit.ring :as r.ring]
            [ring.adapter.jetty9 :as j]
            [spec-tools.core :as st]
            [xtdb.api :as xt]
            [xtdb.error :as err]
            [xtdb.node :as xtn]
            [xtdb.protocols :as xtp]
            [xtdb.serde :as serde])
  (:import (java.io OutputStream)
           (java.time Duration ZoneId)
           (java.util Map)
           [java.util.function Consumer]
           [java.util.stream Stream]
           org.eclipse.jetty.server.Server
           (xtdb.api HttpServer$Factory IXtdb TransactionKey Xtdb$Config)
           xtdb.api.module.XtdbModule
           (xtdb.api.query Basis IKeyFn Query)
           (xtdb.api.tx TxOptions)))

(def ^:private muuntaja-opts
  (-> m/default-options
      (m/select-formats #{"application/transit+json", "application/json"})
      (assoc :default-format "application/json")
      (assoc-in [:formats "application/transit+json" :decoder-opts :handlers]
                serde/transit-read-handlers)
      (assoc-in [:formats "application/transit+json" :encoder-opts :handlers]
                serde/transit-write-handlers)
      (assoc-in [:http :encode-response-body?] (constantly true))))

(defmulti ^:private route-handler :name, :default ::default)

(s/def ::tx-ops seqable?)

(s/def ::key-fn (s/nilable #(instance? IKeyFn %)))

(s/def ::default-tz #(instance? ZoneId %))
(s/def ::explain? boolean?)

(s/def ::opts (s/nilable #(instance? TxOptions %)))

(defmethod route-handler :status [_]
  {:muuntaja (m/create muuntaja-opts)

   :get (fn [{:keys [node] :as _req}]
          {:status 200, :body (xtp/status node)})})

(defmethod route-handler :tx [_]
  {:muuntaja (m/create muuntaja-opts)

   :post {:handler (fn [{:keys [^IXtdb node] :as req}]
                     (let [{:keys [tx-ops opts await-tx?]} (get-in req [:parameters :body])]
                       {:status 200
                        :body (if await-tx?
                                (xtp/execute-tx node tx-ops opts)
                                (xtp/submit-tx node tx-ops opts))}))

          ;; TODO spec-tools doesn't handle multi-spec with a vector,
          ;; so we just check for vector and then conform later.
          :parameters {:body (s/keys :req-un [::tx-ops]
                                     :opt-un [::opts ::await-tx?])}}})

(defn- throwable->ex-info [^Throwable t]
  (ex-info (.getMessage t) {::err/error-type :unknown-runtime-error
                            :class (.getName (.getClass t))
                            :stringified (.toString t)}))

(defn- ->tj-resultset-encoder [opts]
  (reify
    mf/EncodeToBytes
    ;; we're required to be a sub-type of ETB but don't need to implement its fn.

    mf/EncodeToOutputStream
    (encode-to-output-stream [_ res _]
      (fn [^OutputStream out]
        (if-not (ex-data res)
          (with-open [^Stream res res
                      out out]
            (let [writer (transit/writer out :json opts)]
              (try
                (.forEach res
                          (reify Consumer
                            (accept [_ el]
                              (transit/write writer el))))
                (catch xtdb.RuntimeException e
                  (transit/write writer e))
                (catch Throwable t
                  (transit/write writer (throwable->ex-info t))))))
          (with-open [out out]
            (let [writer (transit/writer out :json opts)]
              (transit/write writer res))))))))

(def ^:private ascii-newline (int \newline))

(s/def ::current-time inst?)
(s/def ::at-tx (s/nilable #(instance? TransactionKey %)))
(s/def ::after-tx (s/nilable #(instance? TransactionKey %)))
(s/def ::basis (s/nilable (s/or :class #(instance? Basis %)
                                :map (s/keys :opt-un [::current-time ::at-tx]))))

(s/def ::tx-timeout
  (st/spec (s/nilable #(instance? Duration %))
           {:decode/string (fn [_ s] (some-> s Duration/parse))}))

(s/def ::query (some-fn string? seq? #(instance? Query %)))

(s/def ::args (s/nilable #(instance? Map %)))

(s/def ::query-body
  (s/keys :req-un [::query],
          :opt-un [::after-tx ::basis ::tx-timeout ::args ::default-tz ::key-fn ::explain?]))

(defmethod route-handler :query [_]
  {:muuntaja (m/create (-> muuntaja-opts
                           (assoc :return :output-stream)

                           (assoc-in [:formats "application/transit+json" :encoder]
                                     [->tj-resultset-encoder {:handlers serde/transit-write-handlers}])))

   :post {:handler (fn [{:keys [node parameters]}]
                     (let [{{:keys [query] :as query-opts} :body} parameters]
                       {:status 200
                        :body (cond
                                (string? query) (xtp/open-sql-query node query
                                                                    (xt/->QueryOptions (into {:key-fn :snake-case-string}
                                                                                             (dissoc query-opts :query))))

                                (seq? query) (xtp/open-xtql-query node query
                                                                  (xt/->QueryOptions (into {:key-fn :snake-case-string}
                                                                                           (dissoc query-opts :query))))

                                :else (throw (err/illegal-arg :unknown-query-type {:query query, :type (type query)})))}))

          :parameters {:body ::query-body}}})

(defn- handle-ex-info [ex _req]
  {:status 400, :body ex})

(defn- handle-request-coercion-error [ex _req]
  {:status 400
   :body (err/illegal-arg :malformed-request
                          (merge (r.coercion/encode-error (ex-data ex))
                                 {::err/message (str "Malformed request: " (ex-message ex))}))})

(defn- unroll-xt-iae [ex]
  (if (instance? xtdb.IllegalArgumentException ex)
    ex
    (when-let [ex (ex-cause ex)]
      (recur ex))))

(defn- handle-muuntaja-decode-error [ex _req]
  (if-let [xt-iae (unroll-xt-iae ex)]
    {:status 400
     :body xt-iae}
    {:status 400
     :body (err/illegal-arg :malformed-request
                            (merge {::err/message (str "Malformed " (-> ex ex-data :format pr-str) " request.")}
                                   #_(ex-data ex)))}))

(defn- default-handler
  [^Exception e _]
  {:status 500 :body (throwable->ex-info e)})

(def router
  (http/router xtp/http-routes
               {:expand (fn [{route-name :name, :as route} opts]
                          (r/expand (cond-> route
                                      route-name (merge (route-handler route)))
                                    opts))

                :data {:muuntaja (m/create muuntaja-opts)
                       :coercion rc.spec/coercion
                       :interceptors [[ri.parameters/parameters-interceptor]
                                      [ri.muuntaja/format-negotiate-interceptor]

                                      [ri.muuntaja/format-response-interceptor]

                                      [ri.exception/exception-interceptor
                                       (merge ri.exception/default-handlers
                                              {::ri.exception/default default-handler
                                               xtdb.IllegalArgumentException handle-ex-info
                                               xtdb.RuntimeException handle-ex-info
                                               ::r.coercion/request-coercion handle-request-coercion-error
                                               :muuntaja/decode handle-muuntaja-decode-error
                                               ::ri.exception/wrap (fn [handler e req]
                                                                     (log/debug e (format "response error (%s): '%s'" (class e) (ex-message e)))
                                                                     (handler e req))})]

                                      [ri.muuntaja/format-request-interceptor]
                                      [rh.coercion/coerce-request-interceptor]]}}))

(defn- with-opts [opts]
  {:enter (fn [ctx]
            (update ctx :request merge opts))})

(defn handler [node]
  (http/ring-handler router
                     (r.ring/create-default-handler)
                     {:executor r.sieppari/executor
                      :interceptors [[with-opts {:node node}]]}))


#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn open-server [node ^HttpServer$Factory module]
  (let [port (.getPort module)
        ^Server server (j/run-jetty (handler node)
                                    (merge {:port port, :h2c? true, :h2? true}
                                           #_jetty-opts
                                           {:async? true, :join? false}))]
    (log/info "HTTP server started on port: " port)
    (reify XtdbModule
      (close [_]
        (.stop server)
        (log/info "HTTP server stopped.")))))

(defmethod xtn/apply-config! :xtdb/server [^Xtdb$Config config, _ {:keys [port]}]
  (.module config (cond-> (HttpServer$Factory.)
                    (some? port) (.port port))))
