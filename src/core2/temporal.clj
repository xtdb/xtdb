(ns core2.temporal
  (:require [core2.metadata :as meta]
            [core2.types :as t]
            [core2.util :as util])
  (:import core2.buffer_pool.BufferPool
           core2.metadata.IMetadataManager
           org.apache.arrow.memory.util.ArrowBufPointer
           [org.apache.arrow.memory ArrowBuf BufferAllocator]
           [org.apache.arrow.vector.types.pojo Field Schema]
           [org.apache.arrow.vector BigIntVector TimeStampMilliVector VectorSchemaRoot]
           org.apache.arrow.vector.complex.DenseUnionVector
           org.roaringbitmap.longlong.Roaring64Bitmap
           java.nio.ByteBuffer
           [java.util Map HashMap SortedSet]
           [java.util.function Consumer LongConsumer]
           java.util.concurrent.ConcurrentHashMap
           java.io.Closeable))

(set! *unchecked-math* :warn-on-boxed)

(definterface ITemporalManager
  (^void updateTxEndTime [^Object id ^long row-id ^long tx-time])
  (^org.apache.arrow.vector.VectorSchemaRoot createTxEndTimeRoot [^org.roaringbitmap.longlong.Roaring64Bitmap row-id-bitmap]))

(def ^:private ^Field tx-end-time-field
  (t/->primitive-dense-union-field "_tx-end-time" #{:timestampmilli}))

(def ^:private timestampmilli-type-id
  (-> (t/primitive-type->arrow-type :timestampmilli)
      (t/arrow-type->type-id)))

(def ^:private tx-end-time-schema (Schema. [t/row-id-field tx-end-time-field]))

(deftype TemporalManager [^BufferAllocator allocator
                          ^BufferPool buffer-pool
                          ^Map row-id->tx-end-time
                          ^Map id->row-id]
  ITemporalManager
  (updateTxEndTime [_ id row-id tx-time]
    (let [id (if (bytes? id)
               (ByteBuffer/wrap id)
               id)]
      (when-let [prev-row-id (.get id->row-id id)]
        (.put row-id->tx-end-time prev-row-id tx-time))
      (.put id->row-id id row-id)))

  (createTxEndTimeRoot [_ row-id-bitmap]
    (let [out-root (VectorSchemaRoot/create allocator tx-end-time-schema)
          ^BigIntVector row-id-vec (.getVector out-root 0)
          ^DenseUnionVector tx-end-time-duv-vec (.getVector out-root 1)
          ^TimeStampMilliVector tx-end-time-vec (.getVectorByType tx-end-time-duv-vec timestampmilli-type-id)]
      (util/set-value-count row-id-vec (.getLongCardinality row-id-bitmap))
      (util/set-value-count tx-end-time-duv-vec (.getLongCardinality row-id-bitmap))
      (util/set-value-count tx-end-time-vec (.getLongCardinality row-id-bitmap))
      (-> (.stream row-id-bitmap)
          (.forEach (reify LongConsumer
                      (accept [_ row-id]
                        (let [row-count (.getRowCount out-root)
                              offset (util/write-type-id tx-end-time-duv-vec row-count timestampmilli-type-id)
                              ^long tx-end-time (.getOrDefault row-id->tx-end-time row-id Long/MAX_VALUE)]
                          (.set row-id-vec row-count row-id)
                          (.set tx-end-time-vec row-count tx-end-time)
                          (util/set-vector-schema-root-row-count out-root row-count))))))
      out-root))

  Closeable
  (close [_]
    (.clear row-id->tx-end-time)
    (.clear id->row-id)))

(defn- populate-known-chunks [^TemporalManager temporal-manager ^SortedSet known-chunks]
  (->> (for [chunk-idx known-chunks
             fut [(-> (.getBuffer ^BufferPool (.buffer-pool temporal-manager) (meta/->chunk-obj-key chunk-idx "_id"))
                      (util/then-apply util/try-close))
                  (-> (.getBuffer ^BufferPool (.buffer-pool temporal-manager) (meta/->chunk-obj-key chunk-idx "_tx-time"))
                      (util/then-apply util/try-close))]]
         fut)
       (vec)
       (mapv deref))
  (doseq [chunk-idx known-chunks]
    (with-open [^ArrowBuf id-buffer @(.getBuffer ^BufferPool (.buffer-pool temporal-manager) (meta/->chunk-obj-key chunk-idx "_id"))
                ^ArrowBuf tx-time-buffer @(.getBuffer ^BufferPool (.buffer-pool temporal-manager) (meta/->chunk-obj-key chunk-idx "_tx-time"))
                id-chunks (util/->chunks id-buffer (.allocator temporal-manager))
                tx-time-chunks (util/->chunks tx-time-buffer (.allocator temporal-manager))]
      (.forEachRemaining id-chunks
                         (reify Consumer
                           (accept [_ id-root]
                             (let [^VectorSchemaRoot id-root id-root
                                   ^BigIntVector row-id-vec (.getVector id-root 0)
                                   id-vec (.getVector id-root 1)]
                               (assert (.tryAdvance tx-time-chunks
                                                    (reify Consumer
                                                      (accept [_ tx-time-root]
                                                        (let [^VectorSchemaRoot tx-time-root tx-time-root
                                                              ^DenseUnionVector tx-time-duv-vec (.getVector tx-time-root 1)
                                                              ^TimeStampMilliVector tx-time-vec (.getVectorByType tx-time-duv-vec timestampmilli-type-id)]
                                                          (assert (= (.getRowCount id-root)
                                                                     (.getRowCount tx-time-root)))
                                                          (dotimes [n (.getRowCount tx-time-root)]
                                                            (.updateTxEndTime temporal-manager
                                                                              (.getObject id-vec n)
                                                                              (.get row-id-vec n)
                                                                              (.get tx-time-vec n)))))))))))))))

(defn ->temporal-manager ^core2.temporal.ITemporalManager [^BufferAllocator allocator
                                                           ^BufferPool buffer-pool
                                                           ^IMetadataManager metadata-manager]
  (doto (TemporalManager. allocator buffer-pool (ConcurrentHashMap.) (HashMap.))
    (populate-known-chunks (.knownChunks metadata-manager))))
