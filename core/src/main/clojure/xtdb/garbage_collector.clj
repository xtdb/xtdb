(ns xtdb.garbage-collector
  (:require [integrant.core :as ig]
            [xtdb.node :as xtn])
  (:import [xtdb.api Xtdb$Config GarbageCollectorConfig]
           [xtdb.garbage_collector GarbageCollector]))

(defmethod xtn/apply-config! :xtdb/garbage-collector [^Xtdb$Config config _ {:keys [enabled? blocks-to-keep
                                                                                    grace-period approx-run-interval]}]
  (.garbageCollector config
                     (cond-> (GarbageCollectorConfig.)
                       enabled? (.enabled enabled?)
                       blocks-to-keep (.blocksToKeep blocks-to-keep)
                       grace-period (.gracePeriod grace-period)
                       approx-run-interval (.approxRunInterval approx-run-interval))))

(defmethod ig/prep-key :xtdb/garbage-collector [_ ^GarbageCollectorConfig config]
  {:block-catalog (ig/ref :xtdb/block-catalog)
   :enabled? (.getEnabled config)
   :blocks-to-keep (.getBlocksToKeep config)
   :grace-period (.getGracePeriod config)
   :approx-run-interval (.getApproxRunInterval config)})

(defmethod ig/init-key :xtdb/garbage-collector [_ {:keys [block-catalog enabled? blocks-to-keep grace-period approx-run-interval]}]
  (when enabled?
    (GarbageCollector. block-catalog blocks-to-keep grace-period approx-run-interval)))

(defmethod ig/halt-key! :xtdb/garbage-collector [_ ^GarbageCollector gc]
  (when gc
    (.close gc)))
