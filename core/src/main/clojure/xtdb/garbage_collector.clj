(ns xtdb.garbage-collector
  (:require [integrant.core :as ig]
            [xtdb.node :as xtn]
            [xtdb.util :as util])
  (:import [xtdb.api Xtdb$Config GarbageCollectorConfig]
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
  {:block-catalog (ig/ref :xtdb/block-catalog)
   :trie-catalog (ig/ref :xtdb/trie-catalog)
   :enabled? (.getEnabled config)
   :blocks-to-keep (.getBlocksToKeep config)
   :garbage-lifetime (.getGarbageLifetime config)
   :approx-run-interval (.getApproxRunInterval config)
   :buffer-pool (ig/ref :xtdb/buffer-pool)})

(defmethod ig/init-key :xtdb/garbage-collector [_ {:keys [block-catalog trie-catalog enabled? blocks-to-keep
                                                          garbage-lifetime approx-run-interval
                                                          buffer-pool]}]

  (cond-> (GarbageCollector. block-catalog blocks-to-keep trie-catalog garbage-lifetime approx-run-interval buffer-pool)
    enabled? (.start)))

(defmethod ig/halt-key! :xtdb/garbage-collector [_ ^GarbageCollector gc]
  (when gc
    (.close gc)))

(defn garbage-collector ^xtdb.garbage_collector.GarbageCollector [node]
  (util/component node :xtdb/garbage-collector))
