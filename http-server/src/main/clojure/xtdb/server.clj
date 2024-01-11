(ns xtdb.server
  (:require [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [cognitect.transit :as transit]
            [juxt.clojars-mirrors.integrant.core :as ig]
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
            [reitit.swagger :as r.swagger]
            [ring.adapter.jetty9 :as j]
            [ring.util.response :as ring-response]
            [spec-tools.core :as st]
            xtdb.api
            [xtdb.error :as err]
            [xtdb.jackson :as jackson]
            [xtdb.protocols :as xtp]
            [xtdb.serde :as serde]
            [xtdb.util :as util])
  (:import java.io.OutputStream
           (java.time Duration ZoneId)
           [java.util.function Consumer]
           [java.util.stream Stream]
           org.eclipse.jetty.server.Server
           (xtdb.api TransactionKey)
           (xtdb.api.query Basis IKeyFn Query)
           (xtdb.api.tx TxOptions TxRequest)))

(def ^:private muuntaja-opts
  (-> m/default-options
      (m/select-formats #{"application/json" "application/transit+json"})
      (assoc-in [:formats "application/json" :encoder-opts :mapper] jackson/json-ld-mapper)
      (assoc-in [:formats "application/json" :decoder-opts :mapper] jackson/json-ld-mapper)
      (assoc-in [:formats "application/transit+json" :decoder-opts :handlers]
                serde/transit-read-handlers)
      (assoc-in [:formats "application/transit+json" :encoder-opts :handlers]
                serde/transit-write-handlers)
      (assoc-in [:http :encode-response-body?] (constantly true))))

(defmulti ^:private route-handler :name, :default ::default)

(s/def ::tx-ops seqable?)

(s/def ::key-fn (s/nilable #(instance? IKeyFn %)))

(s/def ::default-all-valid-time? boolean?)
(s/def ::default-tz #(instance? ZoneId %))
(s/def ::explain? boolean?)

(s/def ::opts (s/nilable #(instance? TxOptions %)))

(defmethod route-handler :status [_]
  {:get (fn [{:keys [node] :as _req}]
          {:status 200, :body (xtp/status node)})})

(defn json-tx-decoder []
  (reify
    mf/Decode
    (decode [_ data _]
      (with-open [rdr (io/reader data)]
        (let [^TxRequest tx (jackson/read-tx-req rdr)]
          {:tx-ops (.getTxOps tx) :opts (.getOpts tx)})))))

(defmethod route-handler :tx [_]
  {:muuntaja (m/create (-> muuntaja-opts
                           (assoc-in [:formats "application/json" :decoder] (json-tx-decoder))))

   :post {:handler (fn [{:keys [node] :as req}]
                     (let [{:keys [tx-ops opts]} (get-in req [:parameters :body])]
                       (-> (xtp/submit-tx& node tx-ops opts)
                           (util/then-apply (fn [tx]
                                              {:status 200, :body tx})))))

          ;; TODO spec-tools doesn't handle multi-spec with a vector,
          ;; so we just check for vector and then conform later.
          :parameters {:body (s/keys :req-un [::tx-ops]
                                     :opt-un [::opts])}}})


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

(defn- ->jsonl-resultset-encoder [_opts]
  (reify
    mf/EncodeToBytes

    mf/EncodeToOutputStream
    (encode-to-output-stream [_ res _]
      (fn [^OutputStream out]
        (if-not (ex-data res)
          (with-open [^Stream res res
                      seq-wtr (-> (.writer jackson/json-ld-mapper)
                                  (.writeValues out))]
            (try
              (.forEach res
                        (reify Consumer
                          (accept [_ el]
                            (.write seq-wtr el)
                            (.write out ^byte ascii-newline))))
              (catch Throwable t
                (.write seq-wtr t)
                (.write out ^byte ascii-newline))
              (finally
                (util/close res)
                (util/close out))))

          (try
            (.writeValue jackson/json-ld-mapper out res)
            (finally
              (util/close out))))))))

(s/def ::current-time inst?)
(s/def ::at-tx (s/nilable #(instance? TransactionKey %)))
(s/def ::after-tx (s/nilable #(instance? TransactionKey %)))
(s/def ::basis (s/nilable (s/or :class #(instance? Basis %)
                                :map (s/keys :opt-un [::current-time ::at-tx]))))

(s/def ::tx-timeout
  (st/spec (s/nilable #(instance? Duration %))
           {:decode/string (fn [_ s] (some-> s Duration/parse))}))

(s/def ::query (some-fn string? seq? #(instance? Query %)))

(s/def ::args (s/nilable (s/coll-of any?)))

(s/def ::query-body
  (s/keys :req-un [::query],
          :opt-un [::after-tx ::basis ::tx-timeout ::args ::default-all-valid-time? ::default-tz ::key-fn ::explain?]))

(defn json-query-decoder []
  (reify
    mf/Decode
    (decode [_ data _]
      (with-open [rdr (io/reader data)]
        (let [query-request (jackson/read-query-req rdr)]
          (-> (into {} (.queryOpts query-request))
              (assoc :query (.query query-request))))))))

(defmethod route-handler :query [_]
  {:muuntaja (m/create (-> muuntaja-opts
                           (assoc :return :output-stream)

                           (assoc-in [:formats "application/transit+json" :encoder]
                                     [->tj-resultset-encoder {:handlers serde/transit-write-handlers}])

                           (assoc-in [:formats "application/jsonl" :encoder]
                                     [->jsonl-resultset-encoder {}])

                           (assoc-in [:formats "application/json" :decoder] (json-query-decoder))))

   :post {:handler (fn [{:keys [node parameters]}]
                     (let [{{:keys [query] :as query-opts} :body} parameters]
                       (-> (xtp/open-query& node query (dissoc query-opts :query))
                           (util/then-apply (fn [res]
                                              {:status 200, :body res})))))

          :parameters {:body ::query-body}}})

(defmethod route-handler :openapi [_]
  {:get {:handler (fn [_req]
                    (-> (ring-response/resource-response "openapi.yaml")
                        (assoc "Access-Control-Allow-Origin" "*")))
         :muuntaja (m/create m/default-options)}})

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
                       :interceptors [r.swagger/swagger-feature
                                      [ri.parameters/parameters-interceptor]
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
                                                                     (log/debug (format "response error (%s): '%s'" (class e) (ex-message e)))
                                                                     (let [response-format (:raw-format (:muuntaja/response req))]
                                                                       (cond-> (handler e req)
                                                                         (#{"application/jsonl"} response-format)
                                                                         (assoc :muuntaja/content-type "application/json"))))})]

                                      [ri.muuntaja/format-request-interceptor]
                                      [rh.coercion/coerce-request-interceptor]]}}))

(defn- with-opts [opts]
  {:enter (fn [ctx]
            (update ctx :request merge opts))})

(defmethod ig/prep-key :xtdb/server [_ opts]
  (merge {:node (ig/ref :xtdb/node)
          :read-only? true
          :port 3000}
         opts))

(defmethod ig/init-key :xtdb/server [_ {:keys [port jetty-opts] :as opts}]
  (let [server (j/run-jetty (http/ring-handler router
                                               (r.ring/create-default-handler)
                                               {:executor r.sieppari/executor
                                                :interceptors [[with-opts (select-keys opts [:node :read-only?])]]})

                            (merge {:port port, :h2c? true, :h2? true}
                                   jetty-opts
                                   {:async? true, :join? false}))]
    (log/info "HTTP server started on port: " port)
    server))

(defmethod ig/halt-key! :xtdb/server [_ ^Server server]
  (.stop server)
  (log/info "HTTP server stopped."))
