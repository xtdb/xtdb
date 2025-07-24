(ns xtdb.garbage-collector
  (:require [integrant.core :as ig]
            [xtdb.node :as xtn]
            [xtdb.util :as util])
  (:import [xtdb.api Xtdb$Config GarbageCollectorConfig]
           xtdb.database.DatabaseCatalog
           [xtdb.garbage_collector GarbageCollector]))

(defmethod xtn/apply-config! :xtdb/garbage-collector [^Xtdb$Config config _
                                                      {:keys [enabled? blocks-to-keep garbage-lifetime approx-run-interval]}]
  (.garbageCollector config
                     (cond-> (GarbageCollectorConfig.)
                       (some? enabled?) (.enabled enabled?)
                       blocks-to-keep (.blocksToKeep blocks-to-keep)
                       garbage-lifetime (.garbageLifetime garbage-lifetime)
                       approx-run-interval (.approxRunInterval approx-run-interval))))

(defmethod ig/prep-key :xtdb/garbage-collector [_ ^GarbageCollectorConfig config]
  {:db-cat (ig/ref :xtdb/db-catalog)
   :enabled? (.getEnabled config)
   :blocks-to-keep (.getBlocksToKeep config)
   :garbage-lifetime (.getGarbageLifetime config)
   :approx-run-interval (.getApproxRunInterval config)})

(defmethod ig/init-key :xtdb/garbage-collector [_ {:keys [^DatabaseCatalog db-cat, enabled? blocks-to-keep garbage-lifetime approx-run-interval]}]
  ;; TODO multi-db
  (cond-> (GarbageCollector. (.getPrimary db-cat) blocks-to-keep garbage-lifetime approx-run-interval)
    enabled? (.start)))

(defmethod ig/halt-key! :xtdb/garbage-collector [_ ^GarbageCollector gc]
  (when gc
    (.close gc)))

(defn garbage-collector ^xtdb.garbage_collector.GarbageCollector [node]
  (util/component node :xtdb/garbage-collector))
