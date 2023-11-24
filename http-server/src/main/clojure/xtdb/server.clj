(ns xtdb.server
  (:require [clojure.instant :as inst]
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
            [xtdb.protocols :as xtp]
            [xtdb.serde :as serde]
            [xtdb.util :as util])
  (:import java.io.OutputStream
           (java.time Duration ZoneId)
           org.eclipse.jetty.server.Server
           xtdb.api.TransactionKey
           xtdb.IResultSet))

(def ^:private muuntaja-opts
  (-> m/default-options
      (m/select-formats #{"application/transit+json"})
      (assoc-in [:formats "application/transit+json" :decoder-opts :handlers]
                serde/transit-read-handlers)
      (assoc-in [:formats "application/transit+json" :encoder-opts :handlers]
                serde/transit-write-handlers)
      (assoc-in [:http :encode-response-body?] (constantly true))))

(defmulti ^:private route-handler :name, :default ::default)

(s/def ::tx-ops vector?)

(s/def ::tx-id int?)

(s/def ::key-fn keyword?)

(s/def ::system-time
  (st/spec {:decode/string (fn [_ s]
                             (cond
                               (inst? s) s
                               (string? s) (inst/read-instant-date s)))}))

(s/def ::default-all-valid-time? boolean?)
(s/def ::default-tz #(instance? ZoneId %))

(s/def ::opts (s/keys :opt-un [::system-time ::default-all-valid-time? ::default-tz]))

(defmethod route-handler :status [_]
  {:get (fn [{:keys [node] :as _req}]
          {:status 200, :body (xtp/status node)})})

(defmethod route-handler :tx [_]
  {:post {:handler (fn [{:keys [node] :as req}]
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
          (with-open [^IResultSet res res
                      out out]
            (let [writer (transit/writer out :json opts)]
              (try
                (doseq [el (iterator-seq res)]
                  (transit/write writer el))
                (catch xtdb.RuntimeException e
                  (transit/write writer e))
                (catch Throwable t
                  (transit/write writer (throwable->ex-info t))))))
          (with-open [out out]
            (let [writer (transit/writer out :json opts)]
              (transit/write writer res))))))))

(s/def ::current-time inst?)
(s/def ::tx (s/nilable #(instance? TransactionKey %)))
(s/def ::after-tx (s/nilable #(instance? TransactionKey %)))
(s/def ::basis (s/keys :opt-un [::current-time ::tx ::after-tx]))

(s/def ::basis-timeout
  (st/spec (s/nilable #(instance? Duration %))
           {:decode/string (fn [_ s] (some-> s Duration/parse))}))

(s/def ::query (some-fn string? map? list?))

(s/def ::args (s/nilable (s/coll-of any?)))

(s/def ::query-body
  (s/keys :req-un [::query],
          :opt-un [::basis ::basis-timeout ::args ::default-all-valid-time? ::default-tz ::key-fn]))

(defmethod route-handler :query [_]
  {:muuntaja (m/create (-> muuntaja-opts
                           (assoc :return :output-stream)

                           (assoc-in [:formats "application/transit+json" :encoder]
                                     [->tj-resultset-encoder {:handlers serde/transit-write-handlers}])))

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
                                 {::err/message "Malformed request."}))})

(defn- handle-muuntaja-decode-error [ex _req]
  {:status 400
   :body (err/illegal-arg :malformed-request
                          {::err/message (str "Malformed " (-> ex ex-data :format pr-str) " request.")})})

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
                                                                     (log/warn e "response error")
                                                                     (handler e req))})]

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
