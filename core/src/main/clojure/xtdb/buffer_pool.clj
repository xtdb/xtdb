(ns xtdb.buffer-pool
  (:require [integrant.core :as ig]
            [xtdb.node :as xtn]
            [xtdb.util :as util])
  (:import [io.micrometer.core.instrument.simple SimpleMeterRegistry]
           [java.nio.file Path]
           [org.apache.arrow.memory BufferAllocator]
           (org.apache.arrow.vector VectorSchemaRoot)
           (xtdb IBufferPool)
           (xtdb.api.log FileLog)
           (xtdb.api.storage Storage Storage$Factory)
           xtdb.api.Xtdb$Config
           (xtdb.buffer_pool LocalBufferPool)))

(set! *unchecked-math* :warn-on-boxed)

;; only used from tests now
(defn dir->buffer-pool
  "Creates a local storage buffer pool from the given directory."
  ^xtdb.IBufferPool [^BufferAllocator allocator, ^Path dir]
  (let [bp-path (util/tmp-dir "tmp-buffer-pool")
        storage-root (.resolve bp-path Storage/storageRoot)]
    (util/copy-dir dir storage-root)
    (LocalBufferPool. allocator (Storage/localStorage bp-path) (SimpleMeterRegistry.))))

(defmethod xtn/apply-config! ::local [^Xtdb$Config config _ {:keys [path max-cache-bytes max-cache-entries]}]
  (.storage config (cond-> (Storage/localStorage (util/->path path))
                     max-cache-bytes (.maxCacheBytes max-cache-bytes)
                     max-cache-entries (.maxCacheEntries max-cache-entries))))

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
  (.open factory allocator file-log metrics-registry))

(defmethod ig/halt-key! :xtdb/buffer-pool [_ ^IBufferPool buffer-pool]
  (util/close buffer-pool))
