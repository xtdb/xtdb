(ns xtdb.garbage-collector
  (:require [integrant.core :as ig]
            [xtdb.node :as xtn])
  (:import [xtdb.api Xtdb$Config GarbageCollectorConfig]
           [xtdb.garbage_collector GarbageCollector GarbageCollector$Impl GarbageCollector$Driver]))

(defmethod xtn/apply-config! :xtdb/garbage-collector [^Xtdb$Config config _
                                                      {:keys [enabled? blocks-to-keep garbage-lifetime approx-run-interval]}]
  (.garbageCollector config
                     (cond-> (GarbageCollectorConfig.)
                       (some? enabled?) (.enabled enabled?)
                       blocks-to-keep (.blocksToKeep blocks-to-keep)
                       garbage-lifetime (.garbageLifetime garbage-lifetime)
                       approx-run-interval (.approxRunInterval approx-run-interval))))

(defmethod ig/expand-key :xtdb/garbage-collector [k ^GarbageCollectorConfig config]
  {k {:enabled? (.getEnabled config)
      :blocks-to-keep (.getBlocksToKeep config)
      :garbage-lifetime (.getGarbageLifetime config)
      :approx-run-interval (.getApproxRunInterval config)}})

(defmethod ig/init-key :xtdb/garbage-collector [_ {:keys [enabled? blocks-to-keep garbage-lifetime approx-run-interval]}]
  (GarbageCollector$Impl. (GarbageCollector$Driver/real)
                          blocks-to-keep garbage-lifetime approx-run-interval
                          (boolean enabled?)))

(defmethod ig/halt-key! :xtdb/garbage-collector [_ ^GarbageCollector gc]
  (when gc
    (.close gc)))
