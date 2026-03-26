(ns xtdb.cache
  (:require [xtdb.node :as xtn]
            [xtdb.util :as util])
  (:import xtdb.api.Xtdb$Config
           (xtdb.cache DiskCache$Factory MemoryCache$Factory)))

(defmethod xtn/apply-config! ::memory [^Xtdb$Config config _ {:keys [max-cache-bytes max-cache-ratio]}]
  (cond-> (.getMemoryCache config)
    max-cache-bytes (.maxSizeBytes max-cache-bytes)
    max-cache-ratio (.maxSizeRatio max-cache-ratio)))

(defmethod xtn/apply-config! ::disk [^Xtdb$Config config _ {:keys [path max-cache-bytes max-cache-ratio]}]
  (.diskCache config
              (cond-> (DiskCache$Factory. (util/->path path))
                max-cache-bytes (.maxSizeBytes max-cache-bytes)
                max-cache-ratio (.maxSizeRatio max-cache-ratio))))
