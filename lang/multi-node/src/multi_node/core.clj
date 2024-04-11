(ns multi-node.core
  (:require
   [integrant.core :as ig]
   [reitit.ring :as ring]
   [ring.adapter.jetty :refer [run-jetty]]))

(defonce system
  {::server {:join false
             :port 3300}})

(def router
  (ring/router [["/"
                 ["hello-world" {:get {:handler (fn [_]
                                                  {:status 200
                                                   :body "hello world"})}}]]]))

(defmethod ig/init-key ::server [_ {:keys [port join] :or {port 3300}}]
  (let [server (run-jetty (ring/ring-handler
                           router
                           (ring/routes
                            (ring/create-default-handler)))
                          {:port port :join? join})]
    server))

(defmethod ig/halt-key! ::server [_ server]
  (.stop server))

(defn -main [& _args]
  (ig/init system)
  @(delay))
