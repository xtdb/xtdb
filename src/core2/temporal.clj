(ns core2.temporal
  (:require [core2.metadata :as meta]
            [core2.types :as t]
            [core2.temporal.kd-tree :as kd]
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
           [java.util Arrays Comparator Date List Map HashMap SortedMap SortedSet Spliterator Spliterators TreeMap]
           [java.util.function Consumer Function LongConsumer Predicate ToLongFunction]
           [java.util.concurrent CompletableFuture ConcurrentHashMap]
           java.util.concurrent.locks.StampedLock
           java.util.concurrent.atomic.AtomicLong
           java.util.stream.StreamSupport
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

(def ^java.util.Date end-of-time #inst "9999-12-31T23:59:59.999Z")

(defn row-id->coordinates ^core2.temporal.TemporalCoordinates  [^long row-id]
  (let [coords (TemporalCoordinates. row-id)]
    (set! (.validTimeEnd coords) (.getTime end-of-time))
    (set! (.txTimeEnd coords) (.getTime end-of-time))
    coords))

(defn ->coordinates ^core2.temporal.TemporalCoordinates [{:keys [id ^long row-id ^Date tt-start ^Date vt-start ^Date vt-end tombstone?]}]
  (let [coords (TemporalCoordinates. row-id)]
    (set! (.id coords) id)
    (set! (.validTime coords) (.getTime (or vt-start tt-start)))
    (set! (.validTimeEnd coords) (.getTime (or vt-end end-of-time)))
    (set! (.txTime coords) (.getTime tt-start))
    (set! (.txTimeEnd coords) (.getTime end-of-time))
    (set! (.tombstone coords) (boolean tombstone?))
    coords))

(defrecord TemporalRoots [^Roaring64Bitmap row-id-bitmap ^Map roots]
  Closeable
  (close [_]
    (doseq [root (vals roots)]
      (util/try-close root))))

(definterface ITemporalManager
  (^Object getTemporalWatermark [])
  (^long getInternalId [^Object id])
  (^void registerNewChunk [^long chunk-idx])
  (^void updateTemporalCoordinates [^java.util.SortedMap row-id->temporal-coordinates])
  (^core2.temporal.TemporalRoots createTemporalRoots [^core2.tx.Watermark watermark
                                                      ^java.util.List columns
                                                      ^longs temporal-min-range
                                                      ^longs temporal-max-range
                                                      ^org.roaringbitmap.longlong.Roaring64Bitmap row-id-bitmap]))

(def ^:private temporal-columns
  (->> (for [col-name ["_tx-time" "_tx-time-end" "_valid-time" "_valid-time-end"]]
         [col-name (t/->primitive-dense-union-field col-name #{:timestampmilli})])
       (into {})))

(defn temporal-column? [col-name]
  (contains? temporal-columns col-name))

(def ^:private timestampmilli-type-id
  (-> (t/primitive-type->arrow-type :timestampmilli)
      (t/arrow-type->type-id)))

(defn ->temporal-root-schema ^org.apache.arrow.vector.types.pojo.Schema [col-name]
  (Schema. [t/row-id-field (get temporal-columns col-name)]))

(def ^:const ^int k 6)

(def ^:const ^int id-idx 0)
(def ^:const ^int row-id-idx 1)
(def ^:const ^int valid-time-idx 2)
(def ^:const ^int valid-time-end-idx 3)
(def ^:const ^int tx-time-idx 4)
(def ^:const ^int tx-time-end-idx 5)

(def ^:private column->idx {"_valid-time" valid-time-idx
                            "_valid-time-end" valid-time-end-idx
                            "_tx-time" tx-time-idx
                            "_tx-time-end" tx-time-end-idx})

(declare insert-coordinates)

(deftype TemporalManager [^BufferAllocator allocator
                          ^BufferPool buffer-pool
                          ^IMetadataManager metadata-manager
                          ^AtomicLong id-counter
                          ^Map id->internal-id
                          ^:volatile-mutable kd-tree]
  ITemporalManager
  (getTemporalWatermark [_]
    kd-tree)

  (getInternalId [_ id]
    (.computeIfAbsent id->internal-id
                      (if (bytes? id)
                        (ByteBuffer/wrap id)
                        id)
                      (reify Function
                        (apply [_ x]
                          (.incrementAndGet id-counter)))))

  (registerNewChunk [this chunk-idx]
    ;; TODO: currently too slow to rebuild the tree like this during
    ;; core2.ts-devices-small-test
    #_(set! (.kd-tree this) (kd/rebuild-node-kd-tree kd-tree)))

  (updateTemporalCoordinates [this row-id->temporal-coordinates]
    (let [id->long-id-fn (reify ToLongFunction
                           (applyAsLong [_ id]
                             (.getInternalId this id)))]
      (set! (.kd-tree this)
            (reduce
             (fn [kd-tree coordinates]
               (insert-coordinates kd-tree id->long-id-fn coordinates))
             kd-tree
             (vals row-id->temporal-coordinates)))))

  (createTemporalRoots [_ watermark columns temporal-min-range temporal-max-range row-id-bitmap]
    (let [kd-tree (.temporal-watermark watermark)
          row-id-bitmap-out (Roaring64Bitmap.)
          roots (HashMap.)
          coordinates (StreamSupport/stream (kd/kd-tree-range-search kd-tree temporal-min-range temporal-max-range) false)]
      (if (empty? columns)
        (.forEach coordinates
                  (reify Consumer
                    (accept [_ x]
                      (.addLong row-id-bitmap-out (aget ^longs x row-id-idx)))))
        (let [coordinates (-> coordinates
                              (.filter (reify Predicate
                                         (test [_ x]
                                           (.contains row-id-bitmap (aget ^longs x row-id-idx)))))
                              (.sorted (Comparator/comparingLong (reify ToLongFunction
                                                                   (applyAsLong [_ x]
                                                                     (aget ^longs x row-id-idx)))))
                              (.toArray))
              value-count (count coordinates)]
          (doseq [col-name columns]
            (let [col-idx (get column->idx col-name)
                  out-root (VectorSchemaRoot/create allocator (->temporal-root-schema col-name))
                  ^BigIntVector row-id-vec (.getVector out-root 0)
                  ^DenseUnionVector temporal-duv-vec (.getVector out-root 1)
                  ^TimeStampMilliVector temporal-vec (.getVectorByType temporal-duv-vec timestampmilli-type-id)]
              (util/set-value-count row-id-vec value-count)
              (util/set-value-count temporal-duv-vec value-count)
              (util/set-value-count temporal-vec value-count)
              (dotimes [n value-count]
                (let [offset (util/write-type-id temporal-duv-vec n timestampmilli-type-id)
                      ^longs coordinate (aget coordinates n)
                      row-id (aget coordinate row-id-idx)]
                  (.addLong row-id-bitmap-out row-id)
                  (.set row-id-vec n row-id)
                  (.set temporal-vec n (aget coordinate col-idx))))
              (util/set-vector-schema-root-row-count out-root value-count)
              (.put roots col-name out-root)))))
      (->TemporalRoots (doto row-id-bitmap-out
                         (.and row-id-bitmap))
                       roots)))

  Closeable
  (close [this]
    (set! (.kd-tree this) nil)
    (.clear id->internal-id)))

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
                                         ^TemporalCoordinates coordinates (row-id->coordinates row-id)]
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

      (.updateTemporalCoordinates temporal-manager row-id->temporal-coordinates)
      (.registerNewChunk temporal-manager chunk-idx))

    temporal-manager))

(defn ->temporal-manager ^core2.temporal.ITemporalManager [^BufferAllocator allocator
                                                           ^BufferPool buffer-pool
                                                           ^IMetadataManager metadata-manager]
  (-> (TemporalManager. allocator buffer-pool metadata-manager (AtomicLong.) (ConcurrentHashMap.) nil)
      (populate-known-chunks)))

;; Bitemporal Spike, this will turn into the temporal manager.

(defn ->min-range ^longs []
  (long-array k Long/MIN_VALUE))

(defn ->max-range ^longs []
  (long-array k Long/MAX_VALUE))

(defn ->copy-range ^longs [^longs range]
  (some-> range (Arrays/copyOf (alength range))))

(defn insert-coordinates [kd-tree ^ToLongFunction id->internal-id ^TemporalCoordinates coordinates]
  (let [id (.applyAsLong id->internal-id (.id coordinates))
        row-id (.rowId coordinates)
        min-range (doto (->min-range)
                    (aset id-idx id)
                    (aset valid-time-end-idx (.validTime coordinates))
                    (aset tx-time-end-idx (.txTimeEnd coordinates)))
        max-range (doto (->max-range)
                    (aset id-idx id)
                    (aset valid-time-idx (dec (.validTimeEnd coordinates)))
                    (aset tx-time-end-idx (.txTimeEnd coordinates)))
        overlap (-> ^Spliterator (kd/kd-tree-range-search
                                  kd-tree
                                  min-range
                                  max-range)
                    (StreamSupport/stream false)
                    (.toArray))
        tt-start-ms (.txTime coordinates)
        vt-start-ms (.validTime coordinates)
        vt-end-ms (.validTimeEnd coordinates)
        end-of-time-ms (.getTime end-of-time)
        kd-tree (reduce kd/kd-tree-delete kd-tree overlap)
        kd-tree (cond-> kd-tree
                  (not (.tombstone coordinates))
                  (kd/kd-tree-insert (doto (long-array k)
                                       (aset id-idx id)
                                       (aset row-id-idx row-id)
                                       (aset valid-time-idx vt-start-ms)
                                       (aset valid-time-end-idx vt-end-ms)
                                       (aset tx-time-idx tt-start-ms)
                                       (aset tx-time-end-idx end-of-time-ms))))]
    (reduce
     (fn [kd-tree ^longs coord]
       (cond-> (kd/kd-tree-insert kd-tree (doto (->copy-range coord)
                                            (aset tx-time-end-idx tt-start-ms)))
         (< (aget coord valid-time-idx) vt-start-ms)
         (kd/kd-tree-insert (doto (->copy-range coord)
                              (aset tx-time-idx tt-start-ms)
                              (aset valid-time-end-idx vt-start-ms)))

         (> (aget coord valid-time-end-idx) vt-end-ms)
         (kd/kd-tree-insert (doto (->copy-range coord)
                              (aset tx-time-idx tt-start-ms)
                              (aset valid-time-idx vt-end-ms)))))
     kd-tree
     overlap)))
