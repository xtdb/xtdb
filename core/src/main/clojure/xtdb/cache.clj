(ns xtdb.cache
  (:require [integrant.core :as ig]
            [xtdb.node :as xtn]
            [xtdb.util :as util])
  (:import xtdb.api.Xtdb$Config
           (xtdb.cache DiskCache$Factory MemoryCache$Factory)))

(defmethod xtn/apply-config! ::memory [^Xtdb$Config config _ {:keys [max-cache-bytes max-cache-ratio]}]
  (cond-> (.getMemoryCache config)
    max-cache-bytes (.maxSizeBytes max-cache-bytes)
    max-cache-ratio (.maxSizeRatio max-cache-ratio)))

(defmethod ig/prep-key ::memory [_ factory]
  {:allocator (ig/ref :xtdb/allocator)
   :factory factory
   :metrics-registry (ig/ref :xtdb.metrics/registry)})

(defmethod ig/init-key ::memory [_ {:keys [allocator ^MemoryCache$Factory factory metrics-registry]}]
  (.open factory allocator metrics-registry))

(defmethod ig/halt-key! ::memory [_ ^MemoryCache$Factory memory-cache]
  (util/close memory-cache))

(defmethod xtn/apply-config! ::disk [^Xtdb$Config config _ {:keys [path max-cache-bytes max-cache-ratio]}]
  (.diskCache config
              (cond-> (DiskCache$Factory. (util/->path path))
                max-cache-bytes (.maxSizeBytes max-cache-bytes)
                max-cache-ratio (.maxSizeRatio max-cache-ratio))))

(defmethod ig/prep-key ::disk [_ factory]
  {:factory factory
   :metrics-registry (ig/ref :xtdb.metrics/registry)})

(defmethod ig/init-key ::disk [_ {:keys [^DiskCache$Factory factory metrics-registry]}]
  (some-> factory (.build metrics-registry)))

(defmethod ig/halt-key! ::disk [_ ^DiskCache$Factory _disk-cache])
