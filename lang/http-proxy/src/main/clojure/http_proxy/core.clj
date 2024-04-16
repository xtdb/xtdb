(ns http-proxy.core
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [reitit.ring :as ring]
            [ring.adapter.jetty9 :refer [run-jetty]]
            [xtdb.server :as xts]
            [xtdb.node :as xtn]))


;; >> Node store
;; Nodes are stored in a map of:
;; {<token>: {:node <node>, :handler <handler>}}
;; Where:
;; - <token> is a string
;; - <node> is an xtdb node
;; - <handler> is a ring handler associated with the given node

(defn new-node! [node-store token]
  (if-not (get @node-store token)
    (let [node (xtn/start-node {})
          handler (xts/handler node)]
      (swap! node-store assoc token {:node node
                                     :handler handler})
      true)
    false))

(defn remove-node! [node-store token]
  (if-let [{:keys [^xtdb.api.IXtdb node]} (get @node-store token)]
    (do
      (.close node)
      (swap! node-store dissoc token)
      true)
    false))

(defn cleanup! [node-store]
  (run! (comp (partial remove-node! node-store) key) @node-store))

(defmethod ig/init-key ::node-store [_ _]
  (atom {}))

(defmethod ig/halt-key! ::node-store [_ node-store]
  (cleanup! node-store))



;; >> Handlers

(def calls (atom []))

(comment
  @calls)

(defn setup-handler
  "Setup a new node and return its token.
  A token may be provided in the query-string."
  [{:keys [node-store] token :query-string}]
  (swap! calls conj :setup)
  (let [token (or token (str (gensym)))]
    (if (new-node! node-store token)
      {:status 200
       :body token}
      {:status 400
       :body "Node with token already exists"})))

(defn tear-down-handler
  "Delete a node given its token in the query-string."
  [{:keys [node-store] token :query-string}]
  (swap! calls conj :teardown)
  (if (remove-node! node-store token)
    {:status 200
     :body (pr-str (keys @node-store))}
    {:status 404}))

(defn extract-token [uri]
  (when-let [[_ token & rest-url] (re-find #"/([^/]+)/(.*)" uri)]
    {:token token
     :uri (str "/" (str/join "/" rest-url))}))

(defn default-handler
  "Pass through requests to the appropriate node handler."
  [{:keys [node-store] :as request}]
  (swap! calls conj :default)
  (let [{:keys [token uri]} (extract-token (:uri request))
        {:keys [handler]} (get @node-store token)]
    (if handler
      (handler (assoc request :uri uri))
      {:status 404})))



;; >> App

(def router
  (ring/router [["/setup" {:get {:handler #'setup-handler}}]
                ["/teardown" {:get {:handler #'tear-down-handler}}]
                ["/healthcheck" {:get {:handler (fn [_] {:status 200})}}]]))

(defn wrap-node-store [handler node-store]
  (fn
    ([request]
     (handler (assoc request :node-store node-store)))
    ([request respond raise]
     (handler (assoc request :node-store node-store) respond raise))))

(defmethod ig/init-key ::server [_ {:keys [port join node-store] :or {port 3300}}]
  (log/info "HTTP server started on port: " port)
  (let [server (run-jetty (-> (ring/ring-handler router #'default-handler)
                              (wrap-node-store node-store))
                          {:port port :join? join})]
    server))

(defmethod ig/halt-key! ::server [_ server]
  (.stop server)
  (log/info "HTTP server stopped"))

(defonce system
  {::node-store {}
   ::server {:join false
             :port 3300
             :node-store (ig/ref ::node-store)}})

(defn -main [& _args]
  (ig/init system)
  @(delay))


(comment
  ;; start
  (def sys (ig/init system))
  ;; stop
  (ig/halt! sys))
