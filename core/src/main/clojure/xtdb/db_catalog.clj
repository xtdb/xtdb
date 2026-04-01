(ns xtdb.db-catalog
  (:require [integrant.core :as ig]
            [xtdb.util :as util])
  (:import xtdb.NodeBase
           [xtdb.database Database$Catalog DatabaseCatalog]
           [xtdb.database.proto DatabaseConfig DatabaseConfig$LogCase DatabaseConfig$StorageCase DatabaseMode]))

(defmethod ig/expand-key :xtdb/db-catalog [k _]
  {k {:base (ig/ref :xtdb/base)}})

(defmethod ig/init-key :xtdb/db-catalog [_ {:keys [^NodeBase base]}]
  (DatabaseCatalog/open base))

(defmethod ig/halt-key! :xtdb/db-catalog [_ db-cat]
  (util/close db-cat))

(defn single-writer? []
  (some-> (System/getenv "XTDB_SINGLE_WRITER") Boolean/parseBoolean))

(defn <-node ^xtdb.database.Database$Catalog [node]
  (:db-cat node))

(defn primary-db ^xtdb.database.Database [node]
  ;; HACK a temporary util to just pull the primary DB out of the node
  ;; chances are the non-test callers of this will need to know which database they're interested in.
  (.getPrimary (<-node node)))

(defn <-DatabaseConfig [^DatabaseConfig conf]
  {:log (condp = (.getLogCase conf)
          DatabaseConfig$LogCase/IN_MEMORY_LOG :memory

          DatabaseConfig$LogCase/LOCAL_LOG
          [:local {:path (.hasPath (.getLocalLog conf))}])

   :storage (condp = (.getStorageCase conf)
              DatabaseConfig$StorageCase/IN_MEMORY_STORAGE
              [:memory {:epoch (.getEpoch (.getInMemoryStorage conf))}]

              DatabaseConfig$StorageCase/LOCAL_STORAGE
              (let [ls (.getLocalStorage conf)]
                [:local {:path (.hasPath ls), :epoch (.getEpoch ls)}]))

   :mode (condp = (.getMode conf)
           DatabaseMode/READ_WRITE :read-write
           DatabaseMode/READ_ONLY :read-only
           :read-write)})
