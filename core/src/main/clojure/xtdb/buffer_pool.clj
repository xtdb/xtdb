(ns xtdb.buffer-pool
  (:require [clojure.tools.logging :as log]
            [integrant.core :as ig]
            [xtdb.metrics :as metrics]
            [xtdb.node :as xtn]
            [xtdb.util :as util])
  (:import (com.github.benmanes.caffeine.cache Cache Caffeine)
           [io.micrometer.core.instrument MeterRegistry]
           [io.micrometer.core.instrument.simple SimpleMeterRegistry]
           [java.nio.file Files Path]
           [java.util TreeMap]
           [java.util.concurrent ConcurrentSkipListMap]
           [org.apache.arrow.memory BufferAllocator]
           (org.apache.arrow.vector VectorSchemaRoot)
           (xtdb IBufferPool)
           (xtdb.api.log FileLog FileLog$Subscriber)
           (xtdb.api.storage Storage Storage$Factory Storage$LocalStorageFactory Storage$RemoteStorageFactory)
           xtdb.api.Xtdb$Config
           (xtdb.buffer_pool LocalBufferPool MemoryBufferPool RemoteBufferPool)
           (xtdb.cache DiskCache MemoryCache)))

(set! *unchecked-math* :warn-on-boxed)

;; bump this if the storage format changes in a backwards-incompatible way
(def version (let [version 5] (str "v" (util/->lex-hex-string version))))

(def ^java.nio.file.Path storage-root (util/->path version))

(defn ->buffer-pool-child-allocator [^BufferAllocator allocator, ^MeterRegistry metrics-registry]
  (let [child-allocator (.newChildAllocator allocator "buffer-pool" 0 Long/MAX_VALUE)]
    (when metrics-registry
      (metrics/add-allocator-gauge metrics-registry "buffer-pool.allocator.allocated_memory" child-allocator))
    child-allocator))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn open-in-memory-storage ^xtdb.IBufferPool [^BufferAllocator allocator, ^MeterRegistry metrics-registry]
  (MemoryBufferPool. (->buffer-pool-child-allocator allocator metrics-registry) (TreeMap.)))

(defn ->arrow-footer-cache ^Cache [^long max-entries]
  (-> (Caffeine/newBuilder)
      (.maximumSize max-entries)
      (.build)))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn open-local-storage ^xtdb.IBufferPool [^BufferAllocator allocator, ^Storage$LocalStorageFactory factory, ^MeterRegistry metrics-registry]
  (let [max-cache-bytes (or (.getMaxCacheBytes factory) (quot (util/max-direct-memory) 2))
        memory-cache (MemoryCache. allocator max-cache-bytes)]
    (metrics/add-mem-cache-gauges metrics-registry "memorycache" (util/throttle #(.getStats memory-cache) 2000))
    (LocalBufferPool. (->buffer-pool-child-allocator allocator metrics-registry)
                      memory-cache
                      (doto (-> (.getPath factory) (.resolve storage-root)) util/mkdirs)
                      (->arrow-footer-cache 1024)
                      (metrics/add-counter metrics-registry "record-batch-requests")
                      (metrics/add-counter metrics-registry "memory-cache-misses"))))

(defn dir->buffer-pool
  "Creates a local storage buffer pool from the given directory."
  ^xtdb.IBufferPool [^BufferAllocator allocator, ^Path dir]
  (let [bp-path (util/tmp-dir "tmp-buffer-pool")
        storage-root (.resolve bp-path storage-root)]
    (util/copy-dir dir storage-root)
    (open-local-storage allocator (Storage/localStorage bp-path) (SimpleMeterRegistry.))))

(defmethod xtn/apply-config! ::local [^Xtdb$Config config _ {:keys [path max-cache-bytes max-cache-entries]}]
  (.storage config (cond-> (Storage/localStorage (util/->path path))
                     max-cache-bytes (.maxCacheBytes max-cache-bytes)
                     max-cache-entries (.maxCacheEntries max-cache-entries))))

(set! *unchecked-math* :warn-on-boxed)

(defn calculate-limit-from-percentage-of-disk [^Path local-disk-cache ^long percentage]
  ;; Creating the empty directory if it doesn't exist
  (when-not (util/path-exists local-disk-cache)
    (util/mkdirs local-disk-cache))

  (let [file-store (Files/getFileStore local-disk-cache)
        total-disk-space-bytes (.getTotalSpace file-store)
        disk-size-limit (long (* total-disk-space-bytes (/ percentage 100.0)))]
    (log/debugf "%s%% of total disk space on filestore %s is %s bytes" percentage (.name file-store) disk-size-limit)
    disk-size-limit))

(defn open-remote-storage ^xtdb.IBufferPool [^BufferAllocator allocator, ^Storage$RemoteStorageFactory factory, ^FileLog file-log, ^MeterRegistry metrics-registry]
  (util/with-close-on-catch [object-store (.openObjectStore (.getObjectStore factory))]
    (let [!os-files (ConcurrentSkipListMap.)
          !os-file-name-subscription (promise)
          ^Path disk-cache-root (.getLocalDiskCache factory)]

      (.subscribeFileNotifications file-log
                                   (reify FileLog$Subscriber
                                     (accept [_ {:keys [added]}]
                                       (.putAll !os-files (->> added
                                                               (into {} (comp (filter map?) (map (juxt :k :size)))))))

                                     (onSubscribe [_ subscription]
                                       (deliver !os-file-name-subscription subscription))))

      (doseq [{:keys [k size]} (.listAllObjects object-store)]
        (.put !os-files k size))

      (let [max-cache-bytes (or (.getMaxCacheBytes factory) (quot (util/max-direct-memory) 2))
            memory-cache (MemoryCache. allocator max-cache-bytes)
            disk-cache (DiskCache. disk-cache-root
                                   (or (.getMaxDiskCacheBytes factory)
                                       (calculate-limit-from-percentage-of-disk disk-cache-root (.getMaxDiskCachePercentage factory))))]
        (metrics/add-mem-cache-gauges metrics-registry "memorycache" (util/throttle #(.getStats memory-cache) 2000))
        (metrics/add-cache-gauges metrics-registry "diskcache" (util/throttle #(.getStats disk-cache) 2000))
        (RemoteBufferPool. (->buffer-pool-child-allocator allocator metrics-registry)
                           memory-cache
                           disk-cache
                           (->arrow-footer-cache 1024)
                           file-log
                           object-store
                           !os-files
                           @!os-file-name-subscription
                           (metrics/add-counter metrics-registry "record-batch-requests")
                           (metrics/add-counter metrics-registry "memory-cache-misses")
                           (metrics/add-counter metrics-registry "disk-cache-misses"))))))

(defmulti ->object-store-factory
  #_{:clj-kondo/ignore [:unused-binding]}
  (fn [tag opts]
    (when-let [ns (namespace tag)]
      (doseq [k [(symbol ns)
                 (symbol (str ns "." (name tag)))]]

        (try
          (require k)
          (catch Throwable _))))

    tag))

(defmethod ->object-store-factory :in-memory [_ opts] (->object-store-factory :xtdb.object-store-test/memory-object-store opts))
(defmethod ->object-store-factory :s3 [_ opts] (->object-store-factory :xtdb.aws.s3/object-store opts))
(defmethod ->object-store-factory :google-cloud [_ opts] (->object-store-factory :xtdb.google-cloud/object-store opts))
(defmethod ->object-store-factory :azure [_ opts] (->object-store-factory :xtdb.azure/object-store opts))

(defmethod xtn/apply-config! ::remote [^Xtdb$Config config _ {:keys [object-store local-disk-cache max-cache-bytes max-cache-entries max-disk-cache-bytes max-disk-cache-percentage]}]
  (.storage config (cond-> (Storage/remoteStorage (let [[tag opts] object-store]
                                                    (->object-store-factory tag opts))
                                                  (util/->path local-disk-cache))
                     max-cache-bytes (.maxCacheBytes max-cache-bytes)
                     max-cache-entries (.maxCacheEntries max-cache-entries)
                     max-disk-cache-bytes (.maxDiskCacheBytes max-disk-cache-bytes)
                     max-disk-cache-percentage (.maxDiskCachePercentage max-disk-cache-percentage))))

(defn open-vsr ^VectorSchemaRoot [^IBufferPool bp ^Path path allocator]
  (let [footer (.getFooter bp path)
        schema (.getSchema footer)]
    (VectorSchemaRoot/create schema allocator)))

(defmethod xtn/apply-config! ::storage [config _ [tag opts]]
  (xtn/apply-config! config
                     (case tag
                       :in-memory ::in-memory
                       :local ::local
                       :remote ::remote)
                     opts))

(defmethod ig/prep-key :xtdb/buffer-pool [_ factory]
  {:allocator (ig/ref :xtdb/allocator)
   :factory factory
   :file-log (ig/ref :xtdb/log)
   :metrics-registry (ig/ref :xtdb.metrics/registry)})

(defmethod ig/init-key :xtdb/buffer-pool [_ {:keys [allocator ^Storage$Factory factory, ^FileLog file-log metrics-registry]}]
  (.openStorage factory allocator file-log metrics-registry))

(defmethod ig/halt-key! :xtdb/buffer-pool [_ ^IBufferPool buffer-pool]
  (util/close buffer-pool))
