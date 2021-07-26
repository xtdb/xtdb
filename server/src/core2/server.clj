(ns core2.server
  (:require [juxt.clojars-mirrors.integrant.core :as ig]
            [ring.adapter.jetty9 :as j]
            [clojure.tools.logging :as log]
            [ring.util.response :as resp]
            [reitit.http :as http]
            [reitit.swagger :as r.swagger]
            [reitit.http.coercion :as r.coercion]
            [reitit.coercion.spec :as rc.spec]
            [reitit.http.interceptors.parameters :as ri.parameters]
            [reitit.http.interceptors.muuntaja :as ri.muuntaja]
            [reitit.http.interceptors.exception :as ri.exception]
            [reitit.interceptor.sieppari :as r.sieppari]
            [muuntaja.core :as m]
            [core2.api :as c2]
            [core2.util :as util])
  (:import org.eclipse.jetty.server.Server))

(defn- get-status [_req]
  ;; TODO currently empty to pass healthcheck, we'll want something more later.
  {:status 200, :body {}})

(defn handle-tx [{:keys [node] :as req}]
  (-> (c2/submit-tx node (get-in req [:parameters :body]))
      (util/then-apply (fn [tx]
                         {:status 200, :body (into {} tx)}))))

(defn- handle-ex-info [ex _req]
  {:status 400
   :body (ex-data ex)})

(defn- handle-muuntaja-decode-error [ex _req]
  {:status 400
   :body {:error (str "Malformed " (-> ex ex-data :format pr-str) " request.")}})

(def router
  (http/router
   [["/status" {:summary "Status"
                :description "Get status information from the node"
                :get get-status}]

    ["/tx" {:name :tx
            :summary "Transaction"
            :description "Submits a transaction to the cluster"
            :post #(handle-tx %)
            ;; TODO spec-tools doesn't handle multi-spec with a vector,
            ;; so we just check for vector and then conform later.
            :parameters {:body vector?}}]

    ["/latest-completed-tx" {:get (fn [{:keys [node]}]
                                    (resp/response (into {} (c2/latest-completed-tx node))))
                             :summary "Latest completed transaction"
                             :description "Get the latest transaction to have been indexed by this node"}]

    ["/swagger.json"
     {:get {:no-doc true
            :swagger {:info {:title "Core2 API"}}
            :handler (r.swagger/create-swagger-handler)}}]]

   {:data {:muuntaja m/instance
           :coercion rc.spec/coercion
           :interceptors [r.swagger/swagger-feature
                          (ri.parameters/parameters-interceptor)
                          (ri.muuntaja/format-interceptor)
                          (ri.exception/exception-interceptor
                           (merge ri.exception/default-handlers
                                  {core2.IllegalArgumentException handle-ex-info
                                   :muuntaja/decode handle-muuntaja-decode-error
                                   ::ri.exception/wrap ri.exception/wrap-log-to-console}))

                          (r.coercion/coerce-request-interceptor)
                          (r.coercion/coerce-response-interceptor)
                          (r.coercion/coerce-exceptions-interceptor)]}}))

(defn- with-opts [opts]
  {:enter (fn [ctx]
            (update ctx :request merge opts))})

(defmethod ig/prep-key :core2/server [_ opts]
  (merge {:node (ig/ref :core2/node)
          :read-only? true
          :port 3000}
         opts))

(defmethod ig/init-key :core2/server [_ {:keys [port jetty-opts] :as opts}]
  (let [server (j/run-jetty (http/ring-handler router
                                               {:executor r.sieppari/executor
                                                :interceptors [[with-opts (select-keys opts [:node :read-only?])]]})

                            (merge {:port port
                                    :h2c? true
                                    :h2? true}
                                   jetty-opts
                                   {:async? true
                                    :join? false}))]
    (log/info "HTTP server started on port: " port)
    server))

(defmethod ig/halt-key! :core2/server [_ ^Server server]
  (.stop server)
  (log/info "HTTP server stopped."))
