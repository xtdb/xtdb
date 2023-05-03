(ns xtdb.server
  (:require [clojure.instant :as inst]
            [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [cognitect.transit :as transit]
            [xtdb.api.impl :as xt.impl]
            [xtdb.core.datalog :as d]
            [xtdb.error :as err]
            [xtdb.node :as node]
            [xtdb.transit :as xt.transit]
            [xtdb.util :as util]
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
            [spec-tools.core :as st])
  (:import xtdb.api.TransactionInstant
           xtdb.IResultSet
           java.io.OutputStream
           (java.time Duration ZoneId)
           org.eclipse.jetty.server.Server))

(def ^:private muuntaja-opts
  (-> m/default-options
      (m/select-formats #{"application/transit+json"})
      (assoc-in [:formats "application/transit+json" :decoder-opts :handlers]
                xt.transit/tj-read-handlers)
      (assoc-in [:formats "application/transit+json" :encoder-opts :handlers]
                xt.transit/tj-write-handlers)
      (assoc-in [:http :encode-response-body?] (constantly true))))

(defmulti ^:private route-handler :name, :default ::default)

(s/def ::tx-ops vector?)

(s/def ::tx-id int?)

(s/def ::system-time
  (st/spec inst?
           {:decode/string (fn [_ s]
                             (cond
                               (inst? s) s
                               (string? s) (inst/read-instant-date s)))}))

(s/def ::default-all-valid-time? boolean?)
(s/def ::default-tz #(instance? ZoneId %))

(s/def ::opts (s/keys :opt-un [::system-time ::default-all-valid-time? ::default-tz]))

(defmethod route-handler :status [_]
  {:get (fn [{:keys [node] :as _req}]
          {:status 200, :body (xt.impl/status node)})})

(defmethod route-handler :tx [_]
  {:post {:handler (fn [{:keys [node] :as req}]
                     (let [{:keys [tx-ops opts]} (get-in req [:parameters :body])]
                       (-> (xt.impl/submit-tx& node tx-ops opts)
                           (util/then-apply (fn [tx]
                                              {:status 200, :body tx})))))

          ;; TODO spec-tools doesn't handle multi-spec with a vector,
          ;; so we just check for vector and then conform later.
          :parameters {:body (s/keys :req-un [::tx-ops]
                                     :opt-un [::opts])}}})

(defn- ->tj-resultset-encoder [opts]
  (reify
    mf/EncodeToBytes
    ;; we're required to be a sub-type of ETB but don't need to implement its fn.

    mf/EncodeToOutputStream
    (encode-to-output-stream [_ res _]
      (fn [^OutputStream out]
        (with-open [^IResultSet res res
                    out out]
          (let [writer (transit/writer out :json opts)]
            (doseq [el (iterator-seq res)]
              (transit/write writer el))))))))

(s/def ::current-time inst?)
(s/def ::tx (s/nilable #(instance? TransactionInstant %)))
(s/def ::after-tx (s/nilable #(instance? TransactionInstant %)))
(s/def ::basis (s/keys :opt-un [::current-time ::tx ::after-tx]))

(s/def ::basis-timeout
  (st/spec (s/nilable #(instance? Duration %))
           {:decode/string (fn [_ s] (some-> s Duration/parse))}))

(s/def ::params (s/nilable (s/coll-of any? :kind vector?)))

(s/def :xtdb.server.datalog/query (s/merge ::d/query (s/keys :opt-un [::basis ::basis-timeout ::default-tz])))

(s/def :xtdb.server.datalog/query-body
  (s/keys :req-un [:xtdb.server.datalog/query], :opt-un [::params]))

(defmethod route-handler :datalog-query [_]
  {:muuntaja (m/create (-> muuntaja-opts
                           (assoc :return :output-stream)

                           (assoc-in [:formats "application/transit+json" :encoder]
                                     [->tj-resultset-encoder {:handlers xt.transit/tj-write-handlers}])))

   :post {:handler (fn [{:keys [node parameters]}]
                     (let [{{:keys [query params]} :body} parameters]
                       (-> (xt.impl/open-datalog& node query params)
                           (util/then-apply (fn [res]
                                              {:status 200, :body res})))))

          :parameters {:body :xtdb.server.datalog/query-body}}})

(s/def :xtdb.server.sql/query string?)

(s/def ::args (s/nilable (s/coll-of any? :kind vector?)))

(s/def :xtdb.server.sql/query-body
  (s/keys :req-un [:xtdb.server.sql/query],
          :opt-un [::basis ::basis-timeout ::args ::default-all-valid-time? ::default-tz]))

(defmethod route-handler :sql-query [_]
  {:muuntaja (m/create (-> muuntaja-opts
                           (assoc :return :output-stream)

                           (assoc-in [:formats "application/transit+json" :encoder]
                                     [->tj-resultset-encoder {:handlers xt.transit/tj-write-handlers}])))

   :post {:handler (fn [{:keys [node parameters]}]
                     (let [{{:keys [query args] :as query-opts} :body} parameters]
                       (-> (xt.impl/open-sql& node (into [query] args) (dissoc query-opts :query :args))
                           (util/then-apply (fn [res]
                                              {:status 200, :body res})))))

          :parameters {:body :xtdb.server.sql/query-body}}})

(defn- handle-ex-info [ex _req]
  {:status 400, :body ex})

(defn- handle-request-coercion-error [ex _req]
  {:status 400
   :body (err/illegal-arg :malformed-request
                          (merge (r.coercion/encode-error (ex-data ex))
                                 {::err/message "Malformed request."}))})

(defn- handle-muuntaja-decode-error [ex _req]
  {:status 400
   :body (err/illegal-arg :malformed-request
                          {::err/message (str "Malformed " (-> ex ex-data :format pr-str) " request.")})})

(def router
  (http/router xt.impl/http-routes
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
                                       (-> (merge ri.exception/default-handlers
                                                  {xtdb.IllegalArgumentException handle-ex-info
                                                   xtdb.RuntimeException handle-ex-info
                                                   ::r.coercion/request-coercion handle-request-coercion-error
                                                   :muuntaja/decode handle-muuntaja-decode-error
                                                   ::ri.exception/wrap (fn [handler e req]
                                                                         (log/warn e "response error")
                                                                         (handler e req))}))]

                                      [ri.muuntaja/format-request-interceptor]
                                      [rh.coercion/coerce-request-interceptor]]}}))

(defn- with-opts [opts]
  {:enter (fn [ctx]
            (update ctx :request merge opts))})

(defmethod ig/prep-key :xtdb/server [_ opts]
  (merge {:node (ig/ref ::node/node)
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
