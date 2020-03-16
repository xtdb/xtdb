(ns crux.console
  (:require [aleph.http :as http]
            [clojure.tools.logging :as log]
            [clojure.pprint :as pp]
            [clojure.string :as string]
            [crux-ui-server.main :as ui]
            [crux-ui-server.pages :as pages]
            [crux.http-server :as server]
            [crux.node :as n]
            [crux.io :as cio]
            [ring.adapter.jetty :as j]
            [ring.middleware.cors :refer [wrap-cors]]
            [ring.middleware.params :as p]))

(defn- response
  ([status headers body]
   {:status status
    :headers headers
    :body body}))

(defn- exception-response [status ^Exception e]
  (response status
            {"Content-Type" "application/edn"}
            (with-out-str
              (pp/pprint (Throwable->map e)))))

(defn- wrap-exception-handling [handler]
  (fn [request]
    (try
      (try
        (handler request)
        (catch Exception e
          (if (and (.getMessage e)
                   (string/starts-with? (.getMessage e) "Spec assertion failed"))
            (exception-response 400 e) ;; Valid edn, invalid content
            (do (log/error e "Exception while handling request:" (cio/pr-edn-str request))
                (exception-response 500 e))))) ;; Valid content; something internal failed, or content validity is not properly checked
      (catch Exception e
        (exception-response 400 e)))))

(def cors-access-control
  [:access-control-allow-origin [#".*"]
   :access-control-allow-headers
   ["X-Requested-With" "X-Custom-Header" "Content-Type" "Cache-Control"
    "Origin" "Accept" "Authorization"]
   :access-control-allow-methods [:get :options :head :post]])

(def module
  (merge n/base-topology
         {::node-server
          {:start-fn (fn [{:keys [crux.node/node]} {::keys [crux-port] :as options}]
                       (let [server (j/run-jetty (-> (partial server/handler node)
                                                     (p/wrap-params)
                                                     (#(apply wrap-cors (cons % cors-access-control)))
                                                     (wrap-exception-handling))
                                                 {:port crux-port
                                                  :join? false})]
                         (log/info "Crux HTTP server started on port: " crux-port)
                         (server/->HTTPServer server options)))
           :deps #{:crux.node/node}
           :args {::crux-port {:crux.config/type :crux.config/nat-int
                               :doc "Port to start the Crux HTTP server on"
                               :default 8080}}}
          ::console-server
          {:start-fn (fn [{{:keys [options]} ::node-server} {::keys [console-port routes-prefix]}]
                       (reset! ui/routes (ui/calc-routes routes-prefix))
                       (reset! ui/config
                               {:console/frontend-port console-port
                                :console/embed-crux true
                                :console/routes-prefix routes-prefix
                                :console/crux-node-url-base "localhost:8080"
                                :console/hide-features #{:features/attribute-history}
                                :console/crux-http-port (::crux-port options)})
                       (reset! pages/routes-prefix routes-prefix)
                       (http/start-server ui/handler {:port console-port})
                       (log/info "Crux Console started on port: " console-port))
           :deps #{::node-server}
           :args {::console-port {:crux.config/type :crux.config/nat-int
                                  :doc "Port to start the console frontend on"
                                  :default 5000}
                  ::routes-prefix {:crux.config/type :crux.config/string
                                   :doc "Console route prefix"
                                   :default "/console"}}}}))
