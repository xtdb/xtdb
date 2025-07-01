(ns xtdb.buffer-pool
  (:require [integrant.core :as ig]
            [xtdb.node :as xtn]
            [xtdb.util :as util])
  (:import (xtdb BufferPool)
           (xtdb.api.storage Storage Storage$Factory)
           xtdb.api.Xtdb$Config))

(set! *unchecked-math* :warn-on-boxed)

(defmethod xtn/apply-config! ::local [^Xtdb$Config config _ {:keys [path]}]
  (.storage config (Storage/localStorage (util/->path path))))

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

(defmethod xtn/apply-config! ::remote [^Xtdb$Config config _ {:keys [object-store]}]
  (.storage config (Storage/remoteStorage (let [[tag opts] object-store]
                                            (->object-store-factory tag opts)))))

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
   :metrics-registry (ig/ref :xtdb.metrics/registry)
   :memory-cache (ig/ref :xtdb.cache/memory)
   :disk-cache (ig/ref :xtdb.cache/disk)})

(defmethod ig/init-key :xtdb/buffer-pool [_ {:keys [allocator metrics-registry memory-cache disk-cache ^Storage$Factory factory]}]
  (.open factory allocator memory-cache disk-cache metrics-registry Storage/VERSION))

(defmethod ig/halt-key! :xtdb/buffer-pool [_ ^BufferPool buffer-pool]
  (util/close buffer-pool))

(defn <-node ^xtdb.BufferPool [node]
  (util/component node :xtdb/buffer-pool))
