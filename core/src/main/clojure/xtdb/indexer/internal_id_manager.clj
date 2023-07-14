(ns xtdb.indexer.internal-id-manager
  (:require [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.buffer-pool]
            [xtdb.metadata :as meta]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr])
  (:import (java.io Closeable)
           java.nio.ByteBuffer
           (java.util Map)
           (java.util.concurrent ConcurrentHashMap)
           (java.util.function Consumer Function)
           xtdb.buffer_pool.IBufferPool
           xtdb.metadata.IMetadataManager))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface IInternalIdManager
  (^long getOrCreateInternalId [^String table, ^Object id, ^long row-id])
  (^boolean isKnownId [^String table, ^Object id]))

(defn- normalize-id [id]
  (cond-> id
    (bytes? id) (ByteBuffer/wrap)))

(defmethod ig/prep-key :xtdb.indexer/internal-id-manager [_ opts]
  (merge {:metadata-mgr (ig/ref ::meta/metadata-manager)
          :buffer-pool (ig/ref :xtdb.buffer-pool/buffer-pool)
          :allocator (ig/ref :xtdb/allocator)}
         opts))

(deftype InternalIdManager [^Map id->internal-id]
  IInternalIdManager
  (getOrCreateInternalId [_ table id row-id]
    (.computeIfAbsent id->internal-id
                      [table (normalize-id id)]
                      (reify Function
                        (apply [_ _]
                          ;; big endian for index distribution
                          (Long/reverseBytes row-id)))))

  (isKnownId [_ table id]
    (.containsKey id->internal-id [table (normalize-id id)]))

  Closeable
  (close [_]
    (.clear id->internal-id)))

(defmethod ig/init-key :xtdb.indexer/internal-id-manager [_ {:keys [^IBufferPool buffer-pool, ^IMetadataManager metadata-mgr]}]
  (let [iid-mgr (InternalIdManager. (ConcurrentHashMap.))]
    (doseq [[chunk-idx chunk-metadata] (.chunksMetadata metadata-mgr)
            table (keys (:tables chunk-metadata))]
      (with-open [id-chunks (-> @(.getBuffer buffer-pool (meta/->chunk-obj-key chunk-idx table "xt$id"))
                                (util/rethrowing-cause)
                                (util/->chunks {:close-buffer? true}))
                  row-id-chunks (-> @(.getBuffer buffer-pool (meta/->chunk-obj-key chunk-idx table "_row_id"))
                                    (util/->chunks {:close-buffer? true}))]
        (-> (util/combine-col-cursors {"_row_id" row-id-chunks, "xt$id" id-chunks})
            (.forEachRemaining
             (reify Consumer
               (accept [_ root]
                 (let [rel (vr/<-root root)
                       id-rdr (.readerForName rel "xt$id")
                       row-id-rdr (.readerForName rel "_row_id")]
                   (dotimes [idx (.rowCount rel)]
                     (.getOrCreateInternalId iid-mgr table
                                             (.getObject id-rdr idx)
                                             (.getLong row-id-rdr idx))))))))))
    iid-mgr))
