(ns core2.server
  (:require [clojure.instant :as inst]
            [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [cognitect.transit :as transit]
            [core2.api :as c2]
            [core2.datalog :as d]
            [core2.error :as err]
            [core2.local-node :as node]
            [core2.transit :as c2.transit]
            [core2.util :as util]
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
            [spec-tools.core :as st]
            [clojure.java.io :as io])
  (:import core2.api.TransactionInstant
           core2.IResultSet
           java.io.OutputStream
           java.time.Duration
           org.eclipse.jetty.server.Server))

(def ^:private muuntaja-opts
  (-> m/default-options
      (m/select-formats #{"application/transit+json" "application/edn"})
      (assoc-in [:formats "application/transit+json" :decoder-opts :handlers]
                c2.transit/tj-read-handlers)
      (assoc-in [:formats "application/transit+json" :encoder-opts :handlers]
                c2.transit/tj-write-handlers)
      (assoc-in [:http :encode-response-body?] (constantly true))))

(defmulti ^:private route-handler :name, :default ::default)

(s/def ::tx-ops vector?)

(defmethod route-handler :status [_]
  {:get (fn [{:keys [node] :as _req}]
          {:status 200, :body (c2/status node)})})

(defmethod route-handler :tx [_]
  {:post {:handler (fn [{:keys [node] :as req}]
                     (-> (c2/submit-tx node (get-in req [:parameters :body :tx-ops]))
                         (util/then-apply (fn [tx]
                                            {:status 200, :body tx}))))

          ;; TODO spec-tools doesn't handle multi-spec with a vector,
          ;; so we just check for vector and then conform later.
          :parameters {:body (s/keys :req-un [::tx-ops])}}})

(defn- ->edn-resultset-encoder [_]
  (reify
    mf/EncodeToBytes
    ;; we're required to be a sub-type of ETB but don't need to implement its fn.

    mf/EncodeToOutputStream
    (encode-to-output-stream [_ res _]
      (fn [^OutputStream out]
        (with-open [^IResultSet res res
                    writer (io/writer out)]
          (doseq [el (iterator-seq res)]
            (print-method el writer)))))))

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

(s/def ::tx-id int?)

(s/def ::tx-time
  (st/spec inst?
           {:decode/string (fn [_ s]
                             (cond
                               (inst? s) s
                               (string? s) (inst/read-instant-date s)))}))

(s/def ::default-valid-time inst?)
(s/def ::tx #(instance? TransactionInstant %))
(s/def ::basis (s/keys :opt-un [::default-valid-time ::tx]))

(s/def ::basis-timeout
  (st/spec (s/nilable #(instance? Duration %))
           {:decode/string (fn [_ s] (some-> s Duration/parse))}))

(s/def ::query (s/merge ::d/query (s/keys :opt-un [::basis ::basis-timeout])))
(s/def ::params (s/nilable (s/coll-of any? :kind vector?)))

(s/def ::query-body
  (s/keys :req-un [::query], :opt-un [::params]))

(defmethod route-handler :query [_]
  {:muuntaja (m/create (-> muuntaja-opts
                           (assoc :return :output-stream)

                           (assoc-in [:formats "application/transit+json" :encoder]
                                     [->tj-resultset-encoder {:handlers c2.transit/tj-write-handlers}])
                           (assoc-in [:formats "application/edn" :encoder]
                                     [->edn-resultset-encoder])))

   :post {:handler (fn [{:keys [node parameters]}]
                     (let [{{:keys [query params]} :body} parameters]
                       (-> (apply c2/open-query-async node query params)
                           (util/then-apply (fn [res]
                                              {:status 200, :body res})))))

          :parameters {:body ::query-body}}})

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
  (http/router c2/http-routes
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
                                                  {core2.IllegalArgumentException handle-ex-info
                                                   ::r.coercion/request-coercion handle-request-coercion-error
                                                   :muuntaja/decode handle-muuntaja-decode-error
                                                   ::ri.exception/wrap (fn [handler e req]
                                                                         #_(log/warn e "response error")
                                                                         (handler e req))}))]

                                      [ri.muuntaja/format-request-interceptor]
                                      [rh.coercion/coerce-request-interceptor]]}}))

(defn- with-opts [opts]
  {:enter (fn [ctx]
            (update ctx :request merge opts))})

(defmethod ig/prep-key :core2/server [_ opts]
  (merge {:node (ig/ref ::node/node)
          :read-only? true
          :port 3000}
         opts))

(defmethod ig/init-key :core2/server [_ {:keys [port jetty-opts] :as opts}]
  (let [server (j/run-jetty (http/ring-handler router
                                               (r.ring/create-default-handler)
                                               {:executor r.sieppari/executor
                                                :interceptors [[with-opts (select-keys opts [:node :read-only?])]]})

                            (merge {:port port, :h2c? true, :h2? true}
                                   jetty-opts
                                   {:async? true, :join? false}))]
    (log/info "HTTP server started on port: " port)
    server))

(defmethod ig/halt-key! :core2/server [_ ^Server server]
  (.stop server)
  (log/info "HTTP server stopped."))
