(ns http-proxy.core
  (:require
   [clojure.string :as str]
   [juxt.clojars-mirrors.integrant.core :as ig]
   [reitit.ring :as ring]
   [ring.adapter.jetty9 :refer [run-jetty]]
   [xtdb.server :as xts]
   [xtdb.node :as xtn]))

(defonce system
  {::server {:join false
             :port 3300}})

;; TODO: Make this a component & close all the nodes
(defonce token->handler (atom {}))

(defn default-handler [request]
  (let [[_ token & rest-uri] (str/split (:uri request) #"\/")
        query (str "/" (str/join "/" rest-uri))
        handler (:handler (get @token->handler token))]
    (if handler
      (handler (assoc request :uri query))
      {:status 404})))

(defn setup-handler [{token :query-string}]
  (let [token (or token (str (gensym)))
        node (xtn/start-node {})
        handler (xts/handler node)]
    (swap! token->handler assoc token {:node node
                                       :handler handler})
    {:status 200
     :body token}))

(defn tear-down-handler [{token :query-string}]
  (if-let [{:keys [node]} (get @token->handler token)]
    (do
      (.close node)
      (swap! token->handler dissoc token)
      {:status 200
       :body (pr-str @token->handler)})
    {:status 404}))

(def router
  (ring/router [["/setup" {:get {:handler #'setup-handler}}]
                ["/teardown" {:get {:handler #'tear-down-handler}}]]))

(defmethod ig/init-key ::server [_ {:keys [port join] :or {port 3300}}]
  (let [server (run-jetty (ring/ring-handler
                           router
                           #'default-handler)
                          {:port port :join? join})]
    server))

(defmethod ig/halt-key! ::server [_ server]
  (.stop server))

(defn -main [& _args]
  (ig/init system)
  @(delay))


(comment
                                        ; start
  (def sys (ig/init system))
                                        ; stop
  (ig/halt! sys))
