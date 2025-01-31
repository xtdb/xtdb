(ns xtdb.buffer-pool
  (:require [integrant.core :as ig]
            [xtdb.node :as xtn]
            [xtdb.util :as util])
  (:import [java.nio.file Path]
           (org.apache.arrow.vector VectorSchemaRoot)
           (xtdb BufferPool)
           (xtdb.api.storage Storage Storage$Factory)
           xtdb.api.Xtdb$Config))

(set! *unchecked-math* :warn-on-boxed)

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
(defmethod ->object-store-factory :s3 [_ opts] (->object-store-factory :xtdb.aws/s3 opts))
(defmethod ->object-store-factory :google-cloud [_ opts] (->object-store-factory :xtdb.gcp/object-store opts))
(defmethod ->object-store-factory :azure [_ opts] (->object-store-factory :xtdb.azure/object-store opts))

(defmethod xtn/apply-config! ::remote [^Xtdb$Config config _ {:keys [object-store local-disk-cache max-cache-bytes max-cache-entries max-disk-cache-bytes max-disk-cache-percentage]}]
  (.storage config (cond-> (Storage/remoteStorage (let [[tag opts] object-store]
                                                    (->object-store-factory tag opts))
                                                  (util/->path local-disk-cache))
                     max-cache-bytes (.maxCacheBytes max-cache-bytes)
                     max-cache-entries (.maxCacheEntries max-cache-entries)
                     max-disk-cache-bytes (.maxDiskCacheBytes max-disk-cache-bytes)
                     max-disk-cache-percentage (.maxDiskCachePercentage max-disk-cache-percentage))))

(defn open-vsr ^VectorSchemaRoot [^BufferPool bp ^Path path allocator]
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
   :metrics-registry (ig/ref :xtdb.metrics/registry)})

(defmethod ig/init-key :xtdb/buffer-pool [_ {:keys [allocator ^Storage$Factory factory, metrics-registry]}]
  (.open factory allocator metrics-registry))

(defmethod ig/halt-key! :xtdb/buffer-pool [_ ^BufferPool buffer-pool]
  (util/close buffer-pool))
