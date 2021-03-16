(ns core2.temporal
  (:require [core2.metadata :as meta]
            [core2.types :as t]
            core2.tx
            [core2.util :as util])
  (:import core2.buffer_pool.BufferPool
           core2.metadata.IMetadataManager
           core2.temporal.TemporalCoordinates
           [core2.tx TransactionInstant Watermark]
           org.apache.arrow.memory.util.ArrowBufPointer
           [org.apache.arrow.memory ArrowBuf BufferAllocator]
           [org.apache.arrow.vector.types.pojo Field Schema]
           [org.apache.arrow.vector BigIntVector TimeStampMilliVector VectorSchemaRoot]
           org.apache.arrow.vector.complex.DenseUnionVector
           org.roaringbitmap.longlong.Roaring64Bitmap
           java.nio.ByteBuffer
           [java.util Date List Map HashMap SortedMap SortedSet TreeMap]
           [java.util.function Consumer Function LongConsumer]
           [java.util.concurrent CompletableFuture ConcurrentHashMap]
           java.util.concurrent.locks.StampedLock
           java.io.Closeable))

;; Temporal proof-of-concept plan:

;; From a BCDM point of view, core2 (and Crux) are similar to Jensen's
;; event log approach, that is, we know tx-time, and we know the vt
;; range, but not the actual real state as expressed in the Snodgrass'
;; timestamped tuple approach, which is the relation we want scan to
;; produce. Theoretically, one can map between these via the BCDM, as
;; described in the paper for snapshot equivalent representations, and
;; that serves as a good reference, but not practical.

;; The only update that needs to happen to the append only data is
;; setting tx-time-end to the current tx-time when closing
;; rows. Working around this is what the current uni-temporal tx-time
;; support does. This fact will help later when and if we decide to
;; store the temporal index per chunk in Arrow and merge between them.

;; Further, I think we can decide that a put or delete always know its
;; full vt range, that is, if vt-time isn't known it's set to tx-time,
;; and if vt-time-end isn't know, it's set to end-of-time (at least
;; for the proof-of-concept).

;; In the temporal index structure, this means that when you do a put
;; (delete) you find any current rows (tx-time-end == UC) for the id
;; that overlaps the vt range, and mark those rows with the
;; tx-time-end to current tx-time (the part that cannot be done append
;; only). You then insert the new row entry (for put) normally. If the
;; put (delete) didn't fully overlap you copy the start (and/or) end
;; partial row entries forward, referring to the original row-id,
;; updating their vt-time-end (for start) and vt-time (for end) to
;; match the slice, you also set tx-time to that of the current tx,
;; and tx-time-end to UC.

;; We assume that the column store has a 1-to-1 mapping between
;; operations and row-ids, but the temporal index can refer to them
;; more than once in the case of splits. These could also be stored in
;; the column store if we later decide to break the 1-to-1 mapping.

;; For simplicitly, let's assume that this structure is an in-memory
;; kd-tree for now with 6 dimensions: id, row-id, vt-time,
;; vt-time-end, tx-time, tx-time-end. When updating tx-time-end, one
;; has a few options, either one deletes the node and reinserts it, or
;; one can have an extra value (not part of the actual index),
;; tx-time-delete, which if it exists, supersedes tx-time-end when
;; doing the element-level comparision. That would imply that these
;; nodes would needlessly be found by the kd-tree navigation itself,
;; so moving them might be better. But a reason to try to avoid moving
;; nodes is that later this tree can be an implicit kd-tree stored as
;; Arrow, one per chunk, and the query would need to merge them. How
;; to solve this problem well can be saved for later.

;; Once this structure exists, it could also potentially be used to
;; replace the tombstone check (to see if a row is a deletion) I added
;; as those rows won't sit in the tree. But again, we can postpone
;; that, as this might be superseded by a per-row _op struct.

(set! *unchecked-math* :warn-on-boxed)

(defrecord TemporalRoots [^Roaring64Bitmap row-id-bitmap ^Map roots])

(definterface ITemporalManager
  (^Object getTemporalWatermark [])
  (^void registerNewChunk [^long chunk-idx])
  (^void updateTemporalCoordinates [^java.util.SortedMap row-id->temporal-coordinates])
  (^org.roaringbitmap.longlong.Roaring64Bitmap removeTombstonesFrom [^org.roaringbitmap.longlong.Roaring64Bitmap row-id-bitmap])
  (^core2.temporal.TemporalRoots createTemporalRoots [^core2.tx.Watermark watermark
                                                      ^java.util.List columns
                                                      ^longs temporal-min-range
                                                      ^longs temporal-max-range
                                                      ^org.roaringbitmap.longlong.Roaring64Bitmap row-id-bitmap]))

(def ^:private temporal-columns
  (->> (for [col-name ["_tx-time" "_tx-time-end" "_valid-time" "_valie-time-end"]]
         [col-name (t/->primitive-dense-union-field col-name #{:timestampmilli})])
       (into {})))

(defn temporal-column? [col-name]
  (contains? temporal-columns col-name))

(def ^:private timestampmilli-type-id
  (-> (t/primitive-type->arrow-type :timestampmilli)
      (t/arrow-type->type-id)))

(defn ->temporal-root-schema ^org.apache.arrow.vector.types.pojo.Schema [col-name]
  (Schema. [t/row-id-field (get temporal-columns col-name)]))

(deftype TemporalManager [^BufferAllocator allocator
                          ^BufferPool buffer-pool
                          ^IMetadataManager metadata-manager
                          ^Map row-id->tx-time-end
                          ^Map id->row-id
                          ^Roaring64Bitmap tombstone-row-ids
                          ^StampedLock tombstone-row-ids-lock]
  ITemporalManager
  (getTemporalWatermark [_]
    nil)

  (registerNewChunk [_ chunk-idx])

  (updateTemporalCoordinates [_ row-id->temporal-coordinates]
    (doseq [^TemporalCoordinates coordinates (vals row-id->temporal-coordinates)
            :let [row-id (.rowId coordinates)
                  id (.id coordinates)
                  id (if (bytes? id)
                       (ByteBuffer/wrap id)
                       id)]]
      (when-let [prev-row-id (.get id->row-id id)]
        (.put row-id->tx-time-end prev-row-id (.txTime coordinates)))
      (.put id->row-id id row-id))

    ;; TODO: how to avoid this lock without copying?
    (let [stamp (.writeLock tombstone-row-ids-lock)]
      (try
        (doseq [^TemporalCoordinates coordinates (vals row-id->temporal-coordinates)
                :when (.tombstone coordinates)]
          (.addLong tombstone-row-ids (.rowId coordinates)))
        (finally
          (.unlock tombstone-row-ids-lock stamp)))))

  (removeTombstonesFrom [_ row-id-bitmap]
    (let [stamp (.readLock tombstone-row-ids-lock)]
      (try
        (doto row-id-bitmap
          (.andNot tombstone-row-ids))
        (finally
          (.unlock tombstone-row-ids-lock stamp)))))

  (createTemporalRoots [_ watermark columns temporal-min-range temporal-max-range row-id-bitmap]
    (when (not-empty columns)
      (assert (= ["_tx-time-end"] columns))
      (let [out-root (VectorSchemaRoot/create allocator (->temporal-root-schema "_tx-time-end"))
            ^BigIntVector row-id-vec (.getVector out-root 0)
            ^DenseUnionVector tx-time-end-duv-vec (.getVector out-root 1)
            ^TimeStampMilliVector tx-time-end-vec (.getVectorByType tx-time-end-duv-vec timestampmilli-type-id)
            tx-time-ms (.getTime ^Date (.tx-time ^TransactionInstant (.tx-instant watermark)))
            value-count (.getLongCardinality row-id-bitmap)]
        (util/set-value-count row-id-vec value-count)
        (util/set-value-count tx-time-end-duv-vec value-count)
        (util/set-value-count tx-time-end-vec value-count)
        (.forEach (.stream row-id-bitmap)
                  (reify LongConsumer
                    (accept [_ row-id]
                      (let [row-count (.getRowCount out-root)
                            offset (util/write-type-id tx-time-end-duv-vec row-count timestampmilli-type-id)
                            tx-time-end (.get row-id->tx-time-end row-id)
                            tx-time-end (if (or (nil? tx-time-end)
                                                (> ^long tx-time-end tx-time-ms))
                                          Long/MAX_VALUE
                                          ^long tx-time-end)]
                        (.set row-id-vec row-count row-id)
                        (.set tx-time-end-vec row-count tx-time-end)
                        (util/set-vector-schema-root-row-count out-root row-count)))))
        (->TemporalRoots row-id-bitmap {"_tx-time-end" out-root}))))

  Closeable
  (close [_]
    (.clear row-id->tx-time-end)
    (.clear id->row-id)
    (.clear tombstone-row-ids)))

(defn- populate-known-chunks ^core2.temporal.ITemporalManager [^TemporalManager temporal-manager]
  (let [^BufferPool buffer-pool (.buffer-pool temporal-manager)
        ^IMetadataManager metadata-manager (.metadata-manager temporal-manager)
        known-chunks (.knownChunks metadata-manager)
        futs (reduce (fn [acc chunk-idx]
                       (into acc (for [col-name ["_id" "_tx-time" "_valid-time" "_valid-time-end" "_tombstone"]]
                                   (-> (.getBuffer buffer-pool (meta/->chunk-obj-key chunk-idx col-name))
                                       (util/then-apply util/try-close)))))
                     []
                     known-chunks)]
    @(CompletableFuture/allOf (into-array CompletableFuture futs))
    (doseq [chunk-idx known-chunks
            :let [row-id->temporal-coordinates (TreeMap.)]]
      (with-open [^ArrowBuf id-buffer @(.getBuffer buffer-pool (meta/->chunk-obj-key chunk-idx "_id"))
                  id-chunks (util/->chunks id-buffer (.allocator temporal-manager))]
        (.forEachRemaining id-chunks
                           (reify Consumer
                             (accept [_ id-root]
                               (let [^VectorSchemaRoot id-root id-root
                                     ^BigIntVector row-id-vec (.getVector id-root 0)
                                     id-vec (.getVector id-root 1)]
                                 (dotimes [n (.getRowCount id-root)]
                                   (let [row-id (.get row-id-vec n)
                                         coordinates (TemporalCoordinates. row-id)]
                                     (set! (.id coordinates) (.getObject id-vec n))
                                     (.put row-id->temporal-coordinates row-id coordinates))))))))


      (doseq [col-name ["_tx-time" "_valid-time" "_valid-time-end"]]
        (when-let [temporal-buffer @(.getBuffer buffer-pool (meta/->chunk-obj-key chunk-idx col-name))]
          (with-open [^ArrowBuf temporal-buffer temporal-buffer
                      temporal-chunks (util/->chunks temporal-buffer (.allocator temporal-manager))]
            (.forEachRemaining temporal-chunks
                               (reify Consumer
                                 (accept [_ temporal-root]
                                   (let [^VectorSchemaRoot temporal-root temporal-root
                                         ^BigIntVector row-id-vec (.getVector temporal-root 0)
                                         ^DenseUnionVector temporal-duv-vec (.getVector temporal-root 1)
                                         ^TimeStampMilliVector temporal-vec (.getVectorByType temporal-duv-vec timestampmilli-type-id)]
                                     (dotimes [n (.getRowCount temporal-root)]
                                       (let [^TemporalCoordinates coordinates (.get row-id->temporal-coordinates (.get row-id-vec n))
                                             time-ms (.get temporal-vec n)]
                                         (case col-name
                                           "_tx-time" (set! (.txTime coordinates) time-ms)
                                           "_valid-time" (set! (.validTime coordinates) time-ms)
                                           "_valid-time-end" (set! (.validTimeEnd coordinates) time-ms)))))))))))

      (when-let [tombstone-buffer @(.getBuffer buffer-pool (meta/->chunk-obj-key chunk-idx "_tombstone"))]
        (with-open [^ArrowBuf tombstone-buffer tombstone-buffer
                    tombstone-chunks (util/->chunks tombstone-buffer (.allocator temporal-manager))]
          (.forEachRemaining tombstone-chunks
                             (reify Consumer
                               (accept [_ tombstone-root]
                                 (let [^VectorSchemaRoot tombstone-root tombstone-root
                                       ^BigIntVector row-id-vec (.getVector tombstone-root 0)
                                       ^DenseUnionVector tombstone-duv-vec (.getVector tombstone-root 1)]
                                   (dotimes [n (.getRowCount tombstone-root)]
                                     (when (.getObject tombstone-duv-vec n)
                                       (let [coordinates ^TemporalCoordinates (.get row-id->temporal-coordinates (.get row-id-vec n))]
                                         (set! (.tombstone coordinates) true))))))))))

      (.updateTemporalCoordinates temporal-manager row-id->temporal-coordinates))

    temporal-manager))

(defn ->temporal-manager ^core2.temporal.ITemporalManager [^BufferAllocator allocator
                                                           ^BufferPool buffer-pool
                                                           ^IMetadataManager metadata-manager]
  (-> (TemporalManager. allocator buffer-pool metadata-manager (ConcurrentHashMap.) (HashMap.) (Roaring64Bitmap.) (StampedLock.))
      (populate-known-chunks)))
