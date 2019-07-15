(ns example-standalone-integrant-node
  (:require [crux.api :as api]
            [integrant.core :as ig]))

(def config {:crux/standalone {:kv-backend "crux.kv.memdb.MemKv"
                               :db-dir "data/db-dir-1"
                               :event-log-dir "data/eventlog-1"}})

(defmethod ig/init-key :crux/standalone [_ opts]
  (api/start-standalone-node opts))

(defmethod ig/halt-key! :crux/standalone [_ ^java.io.Closeable closeable]
  (.close closeable))

(def node nil)

(defn start-node []
  (alter-var-root #'node (fn [_] (ig/init config)))
  :started)

(defn stop-node []
  (when (and (bound? #'node)
             (not (nil? node)))
    (alter-var-root #'node (fn [node] (ig/halt! node)))
    :stopped))
