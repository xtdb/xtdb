(ns xtdb.buffer-pool
  (:require [integrant.core :as ig]
            [xtdb.node :as xtn]
            [xtdb.util :as util])
  (:import (xtdb.api.storage Storage Storage$Factory)
           xtdb.api.Xtdb$Config
           (xtdb.database Database$Mode)
           (xtdb.storage BufferPool ReadOnlyBufferPool)))

(set! *unchecked-math* :warn-on-boxed)

(defmethod xtn/apply-config! ::in-memory [^Xtdb$Config config _ {:keys [epoch]}]
  (.storage config (cond-> (Storage/inMemory)
                     epoch (.epoch epoch))))

(defmethod xtn/apply-config! ::local [^Xtdb$Config config _ {:keys [path epoch]}]
  (.storage config (cond-> (Storage/local (util/->path path))
                     epoch (.epoch epoch))))

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

(defmethod xtn/apply-config! ::remote [^Xtdb$Config config _ {:keys [object-store epoch]}]
  (.storage config (cond-> (Storage/remote (let [[tag opts] object-store]
                                             (->object-store-factory tag opts)))
                     epoch (.epoch epoch))))

(defmethod xtn/apply-config! ::storage [config _ [tag opts]]
  (xtn/apply-config! config
                     (case tag
                       :in-memory ::in-memory
                       :local ::local
                       :remote ::remote)
                     opts))

(defmethod ig/expand-key :xtdb/buffer-pool [k {:keys [base db-name factory mode]}]
  {k {:base base, :factory factory, :db-name db-name, :mode mode
      :allocator (ig/ref :xtdb.db-catalog/allocator)}})

(defmethod ig/init-key :xtdb/buffer-pool [_ {{:keys [meter-registry mem-cache disk-cache]} :base
                                             :keys [allocator ^Storage$Factory factory, db-name ^Database$Mode mode]}]
  (cond-> (.open factory allocator mem-cache disk-cache db-name meter-registry Storage/VERSION)
    (= mode Database$Mode/READ_ONLY) (ReadOnlyBufferPool.)))

(defmethod ig/halt-key! :xtdb/buffer-pool [_ ^BufferPool buffer-pool]
  (util/close buffer-pool))

