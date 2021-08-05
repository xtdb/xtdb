(ns core2.server
  (:require [clojure.instant :as inst]
            [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [core2.api :as c2]
            [core2.datalog :as d]
            [core2.local-node :as node]
            [core2.transit :as c2.transit]
            [core2.util :as util]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [muuntaja.core :as m]
            [reitit.coercion.spec :as rc.spec]
            [reitit.http :as http]
            [reitit.http.coercion :as r.coercion]
            [reitit.http.interceptors.exception :as ri.exception]
            [reitit.http.interceptors.muuntaja :as ri.muuntaja]
            [reitit.http.interceptors.parameters :as ri.parameters]
            [reitit.interceptor.sieppari :as r.sieppari]
            [reitit.swagger :as r.swagger]
            [ring.adapter.jetty9 :as j]
            [ring.util.response :as resp]
            [spec-tools.core :as st]
            [reitit.core :as r])
  (:import core2.api.TransactionInstant
           java.time.Duration
           org.eclipse.jetty.server.Server))

(defn- get-status [_req]
  ;; TODO currently empty to pass healthcheck, we'll want something more later.
  {:status 200, :body {}})

(defn handle-tx [{:keys [node] :as req}]
  (-> (c2/submit-tx node (get-in req [:parameters :body]))
      (util/then-apply (fn [tx]
                         {:status 200, :body tx}))))

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

(defn handle-query [{:keys [node parameters]}]
  (let [{{:keys [query params]} :body} parameters]
    {:status 200
     :body (into [] (apply c2/plan-query node query params))}))

(defn- handle-ex-info [ex _req]
  {:status 400
   :body (ex-data ex)})

(defn- handle-muuntaja-decode-error [ex _req]
  {:status 400
   :body {:error (str "Malformed " (-> ex ex-data :format pr-str) " request.")}})

(def ^:private muuntaja
  (m/create (-> m/default-options
                (m/select-formats #{"application/transit+json" "application/edn"})
                (assoc-in [:formats "application/transit+json" :decoder-opts :handlers]
                          c2.transit/tj-read-handlers)
                (assoc-in [:formats "application/transit+json" :encoder-opts :handlers]
                          c2.transit/tj-write-handlers))))

(def handlers
  {:status {:get #(get-status %)}

   :tx {:post {:handler #(handle-tx %)
               ;; TODO spec-tools doesn't handle multi-spec with a vector,
               ;; so we just check for vector and then conform later.
               :parameters {:body vector?}}}

   :query {:post {:handler #(handle-query %)
                  :parameters {:body ::query-body}}}

   :latest-completed-tx {:get {:handler (fn [{:keys [node]}]
                                          (resp/response (into {} (c2/latest-completed-tx node))))}}})

(def router
  (http/router c2/http-routes
               {:expand (fn [{route-name :name, :as route} opts]
                          (r/expand (cond-> route
                                      route-name (merge (get handlers route-name)))
                                    opts))

                :data {:muuntaja muuntaja
                       :coercion rc.spec/coercion
                       :interceptors [r.swagger/swagger-feature
                                      (ri.parameters/parameters-interceptor)
                                      (ri.muuntaja/format-interceptor)
                                      (ri.exception/exception-interceptor
                                       (merge ri.exception/default-handlers
                                              {core2.IllegalArgumentException handle-ex-info
                                               :muuntaja/decode handle-muuntaja-decode-error}))

                                      (r.coercion/coerce-request-interceptor)
                                      (r.coercion/coerce-response-interceptor)
                                      (r.coercion/coerce-exceptions-interceptor)]}}))

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
