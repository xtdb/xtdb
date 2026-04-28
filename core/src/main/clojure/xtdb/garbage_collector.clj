(ns xtdb.garbage-collector
  (:require [xtdb.node :as xtn])
  (:import [xtdb.api Xtdb$Config GarbageCollectorConfig]))

(defmethod xtn/apply-config! :xtdb/garbage-collector [^Xtdb$Config config _
                                                      {:keys [enabled? blocks-to-keep garbage-lifetime]}]
  (.garbageCollector config
                     (cond-> (GarbageCollectorConfig.)
                       (some? enabled?) (.enabled enabled?)
                       blocks-to-keep (.blocksToKeep blocks-to-keep)
                       garbage-lifetime (.garbageLifetime garbage-lifetime))))
