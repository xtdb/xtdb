(ns webservice.main
  (:require [integrant.core :as ig]
            [webservice.graphql :as gql]
            [webservice.util]
            [webservice.ds9]
            [aleph.http :as http]
            [crux.api :as crux]
            [reitit.ring :as r.ring]
            [clojure.java.io :as io]
            [reitit.ring.middleware.parameters :as params]
            [reitit.ring.middleware.muuntaja :as muuntaja]
            [ring.util.response :as resp]
            [crux.api :as c])
  (:import [java.io Closeable]))

(defmethod ig/init-key :webservice/crux-node [_ config]
  (crux/start-node config))

(defmethod ig/halt-key! :webservice/crux-node [_ ^Closeable node]
  (.close node))

(defn with-crux-node [handler crux-node]
  (fn [req]
    (handler (assoc req :webservice/crux-node crux-node))))

(defn handle-graphiql [_]
  (resp/file-response "src/webservice/index.html"))

(defn ->handler
  [{:keys [webservice/crux-node]}]

  (r.ring/ring-handler (r.ring/router [["/" {:name ::root,
                                             :get {:handler #'handle-graphiql}}]
                                       ["/graphql" {:post {:handler (gql/->graphql-handler crux-node)}}]]
                                      {:data {:middleware [#(with-crux-node % crux-node)
                                                           params/parameters-middleware
                                                           muuntaja/format-middleware]}})))

(defmethod ig/init-key :webservice/server [_ {:keys [port webservice/crux-node]}]
  (http/start-server (->handler {:webservice/crux-node crux-node}) {:port port}))

(defmethod ig/halt-key! :webservice/server [_ server]
  (.close server))

(def ig-config
  {:webservice/crux-node (case (System/getenv "WEBSERVICE_ENV")
                     "DOCKER"
                     {:crux.node/topology :crux.kafka/topology
                      :crux.kafka/bootstrap-servers "broker:29092"
                      :crux.kv/db-dir "crux-node/db"}
                     {:crux.node/topology :crux.standalone/topology
                      :crux.node/kv-store "crux.kv.rocksdb/kv"
                      :crux.kv/db-dir "test-data/webservicedemo"
                      :crux.standalone/event-log-dir "test-data/webservicedemo"
                      :crux.standalone/event-log-kv-store "crux.kv.memdb/kv"})
   :webservice.ds9/populate {:webservice/crux-node (ig/ref :webservice/crux-node)
                       :webservice.ds9/dataset "smalldata.edn"}
   :webservice/server {:port 3000
                 :webservice/crux-node (ig/ref :webservice/crux-node)}})

(defonce !ig-system
  (atom nil))

(defn start-system [sys] (or sys (ig/init ig-config)))
(defn stop-system [sys] (when sys (ig/halt! sys)))

(defn -main [& args]
  ;; stop is a no-op if there's not a started system
  (println "Starting system...")
  (swap! !ig-system (comp start-system stop-system))
  (println "System started."))

#_(c/q (c/db (:webservice/crux-node @!ig-system)) '{:find [e]
                                              :where [[e :crux.db/id]]})
