(ns core2.temporal
  (:require core2.buffer-pool
            [core2.metadata :as meta]
            [core2.temporal.grid :as grid]
            [core2.temporal.kd-tree :as kd]
            [core2.types :as t]
            [core2.util :as util]
            [juxt.clojars-mirrors.integrant.core :as ig])
  (:import core2.buffer_pool.IBufferPool
           core2.DenseUnionUtil
           core2.metadata.IMetadataManager
           core2.object_store.ObjectStore
           [core2.temporal.kd_tree IKdTreePointAccess MergedKdTree]
           java.io.Closeable
           java.nio.ByteBuffer
           [java.util Arrays Collections Comparator Date HashMap Map TreeMap]
           [java.util.concurrent CompletableFuture ConcurrentHashMap Executors ExecutorService]
           java.util.concurrent.atomic.AtomicLong
           [java.util.function Consumer Function LongConsumer LongFunction LongPredicate Predicate ToLongFunction]
           java.util.stream.LongStream
           [org.apache.arrow.memory ArrowBuf BufferAllocator]
           [org.apache.arrow.vector BigIntVector TimeStampMilliVector TimeStampVector VectorSchemaRoot]
           org.apache.arrow.vector.complex.DenseUnionVector
           [org.apache.arrow.vector.types.pojo ArrowType$Union Schema]
           org.apache.arrow.vector.types.UnionMode
           org.roaringbitmap.longlong.Roaring64Bitmap))

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

(def ^:const ^int k 6)

(defn ->min-range ^longs []
  (long-array k Long/MIN_VALUE))

(defn ->max-range ^longs []
  (long-array k Long/MAX_VALUE))

(defn ->copy-range ^longs [^longs range]
  (some-> range (Arrays/copyOf (alength range))))

(defrecord TemporalRoots [^Roaring64Bitmap row-id-bitmap ^Map roots]
  Closeable
  (close [_]
    (doseq [root (vals roots)]
      (util/try-close root))))

(definterface ITemporalTxIndexer
  (^void indexPut [^Object eid, ^long row-id
                   ^org.apache.arrow.vector.TimeStampVector vt-start-vec
                   ^org.apache.arrow.vector.TimeStampVector vt-end-vec
                   ^int idx])
  (^void indexDelete [^Object eid, ^long row-id
                      ^org.apache.arrow.vector.TimeStampVector vt-start-vec
                      ^org.apache.arrow.vector.TimeStampVector vt-end-vec
                      ^int idx])
  (^void indexEvict [^Object eid, ^long row-id])
  (^org.roaringbitmap.longlong.Roaring64Bitmap endTx []))

(definterface ITemporalManager
  (^Object getTemporalWatermark [])
  (^void registerNewChunk [^long chunk-idx])
  (^core2.temporal.ITemporalTxIndexer startTx [^core2.api.TransactionInstant tx-instant])
  (^core2.temporal.TemporalRoots createTemporalRoots [^core2.tx.Watermark watermark
                                                      ^java.util.List columns
                                                      ^longs temporal-min-range
                                                      ^longs temporal-max-range
                                                      ^org.roaringbitmap.longlong.Roaring64Bitmap row-id-bitmap]))

(definterface IInternalIdManager
  (^long getOrCreateInternalId [^Object id ^long row-id])
  (^boolean isKnownId [^Object id]))

(definterface TemporalManagerPrivate
  (^void populateKnownChunks [])
  (^Long latestTemporalSnapshotIndex [^int chunk-idx])
  (^void reloadTemporalIndex [^int chunk-idx ^Long snapshot-idx])
  (^void awaitSnapshotBuild [])
  (^void buildTemporalSnapshot [^int chunk-idx ^Long snapshot-idx])
  (^java.io.Closeable buildStaticTree [^Object base-kd-tree ^int chunk-idx ^Long snapshot-idx]))

(deftype TemporalCoordinates [^long rowId, ^Object id,
                              ^long txTimeStart, ^long txTimeEnd
                              ^long validTimeStart, ^long validTimeEnd
                              ^boolean tombstone])

(def ->temporal-field
  (->> (for [col-name ["_tx-time-start" "_tx-time-end" "_valid-time-start" "_valid-time-end"]]
         [col-name (t/->field col-name t/timestamp-milli-type false)])
       (into {})))

(defn temporal-column? [col-name]
  (contains? ->temporal-field (name col-name)))

(defn ->temporal-root-schema ^org.apache.arrow.vector.types.pojo.Schema [col-name]
  (Schema. [t/row-id-field (get ->temporal-field (name col-name))]))

(def ^:const ^int tx-time-end-idx 0)
(def ^:const ^int id-idx 1)
(def ^:const ^int tx-time-start-idx 2)
(def ^:const ^int row-id-idx 3)
(def ^:const ^int valid-time-start-idx 4)
(def ^:const ^int valid-time-end-idx 5)

(def ^:private column->idx {"_valid-time-start" valid-time-start-idx
                            "_valid-time-end" valid-time-end-idx
                            "_tx-time-start" tx-time-start-idx
                            "_tx-time-end" tx-time-end-idx})

(defn ->temporal-column-idx ^long [col-name]
  (long (get column->idx (name col-name))))

(defn evict-id [kd-tree, ^BufferAllocator allocator, ^long internal-id, ^Roaring64Bitmap evicted-row-ids]
  (let [min-range (doto (->min-range)
                    (aset id-idx internal-id))

        max-range (doto (->max-range)
                    (aset id-idx internal-id))

        ^IKdTreePointAccess point-access (kd/kd-tree-point-access kd-tree)

        overlap (-> ^LongStream (kd/kd-tree-range-search
                                 kd-tree
                                 min-range
                                 max-range)
                    (.mapToObj (reify LongFunction
                                 (apply [_ x]
                                   (.getArrayPoint point-access x))))
                    (.toArray))]

    (reduce (fn [kd-tree ^longs point]
              (.addLong evicted-row-ids (aget point row-id-idx))
              (kd/kd-tree-delete kd-tree allocator (->copy-range point)))
            kd-tree
            overlap)))

(defn insert-coordinates [kd-tree ^BufferAllocator allocator ^IInternalIdManager id-manager ^TemporalCoordinates coordinates]
  (let [new-id? (not (.isKnownId id-manager (.id coordinates)))
        row-id (.rowId coordinates)
        id (.getOrCreateInternalId id-manager (.id coordinates) row-id)
        tx-time-start-ms (.txTimeStart coordinates)
        tx-time-end-ms (.txTimeEnd coordinates)
        valid-time-start-ms (.validTimeStart coordinates)
        valid-time-end-ms (.validTimeEnd coordinates)

        end-of-time-ms (.getTime end-of-time)

        min-range (doto (->min-range)
                    (aset id-idx id)
                    (aset valid-time-end-idx (inc valid-time-start-ms))
                    (aset tx-time-end-idx tx-time-start-ms))

        max-range (doto (->max-range)
                    (aset id-idx id)
                    (aset valid-time-start-idx (dec valid-time-end-ms))
                    (aset tx-time-end-idx tx-time-end-ms))

        ^IKdTreePointAccess point-access (kd/kd-tree-point-access kd-tree)

        overlap (when-not new-id?
                  (-> ^LongStream (kd/kd-tree-range-search
                                   kd-tree
                                   min-range
                                   max-range)
                      (.mapToObj (reify LongFunction
                                   (apply [_ x]
                                     (.getArrayPoint point-access x))))
                      (.toArray)))
        kd-tree (reduce
                 (fn [kd-tree ^longs point]
                   (kd/kd-tree-delete kd-tree allocator (->copy-range point)))
                 kd-tree
                 overlap)
        kd-tree (cond-> kd-tree
                  (not (.tombstone coordinates))
                  (kd/kd-tree-insert allocator
                                     (doto (long-array k)
                                       (aset id-idx id)
                                       (aset row-id-idx row-id)
                                       (aset valid-time-start-idx valid-time-start-ms)
                                       (aset valid-time-end-idx valid-time-end-ms)
                                       (aset tx-time-start-idx tx-time-start-ms)
                                       (aset tx-time-end-idx end-of-time-ms))))]
    (reduce
     (fn [kd-tree ^longs coord]
       (cond-> (kd/kd-tree-insert kd-tree allocator (doto (->copy-range coord)
                                                      (aset tx-time-end-idx tx-time-start-ms)))
         (< (aget coord valid-time-start-idx) valid-time-start-ms)
         (kd/kd-tree-insert allocator (doto (->copy-range coord)
                                        (aset tx-time-start-idx tx-time-start-ms)
                                        (aset valid-time-end-idx valid-time-start-ms)))

         (> (aget coord valid-time-end-idx) valid-time-end-ms)
         (kd/kd-tree-insert allocator (doto (->copy-range coord)
                                        (aset tx-time-start-idx tx-time-start-ms)
                                        (aset valid-time-start-idx valid-time-end-ms)))))
     kd-tree
     overlap)))

(defn- ->temporal-obj-key [chunk-idx]
  (format "temporal-%016x.arrow" chunk-idx))

(defn- ->temporal-snapshot-obj-key [chunk-idx]
  (format "temporal-snapshot-%016x.arrow" chunk-idx))

(defn- temporal-snapshot-obj-key->chunk-idx ^long [obj-key]
  (Long/parseLong (second (re-find #"temporal-snapshot-(\p{XDigit}{16})\.arrow" obj-key)) 16))

(defn- normalize-id [id]
  (if (bytes? id)
    (ByteBuffer/wrap id)
    id))

(defn- ->big-endian-internal-id ^long [^long row-id]
  (Long/reverseBytes row-id))

(defn- wrap-with-current-entity-cache [kd-tree ^Map current-entities-cache]
  (reify
    kd/KdTree
    (kd-tree-insert [_ allocator point]
      (throw (UnsupportedOperationException.)))
    (kd-tree-delete [_ allocator point]
      (throw (UnsupportedOperationException.)))
    (kd-tree-range-search [_ min-range max-range]
      (let [min-range (kd/->longs min-range)
            max-range (kd/->longs max-range)
            end-of-time-ms (.getTime end-of-time)]
        (if (and (= (aget min-range tx-time-end-idx)
                    (aget max-range tx-time-end-idx)
                    end-of-time-ms)
                 (= (aget min-range id-idx)
                    (aget max-range id-idx)))
          (let [id (aget min-range id-idx)
                new-min-range (doto (->min-range)
                                (aset id-idx id)
                                (aset tx-time-end-idx end-of-time-ms))
                new-max-range (doto (->max-range)
                                (aset id-idx id)
                                (aset tx-time-end-idx end-of-time-ms))
                ^IKdTreePointAccess access (kd/kd-tree-point-access kd-tree)
                axis-mask (-> (kd/range-bitmask min-range max-range)
                              (bit-and-not (bit-or (bit-shift-left 1 id-idx)
                                                   (bit-shift-left 1 tx-time-end-idx))))]
            (-> (LongStream/of ^longs (.computeIfAbsent current-entities-cache
                                                        id
                                                        (reify Function
                                                          (apply [_ _]
                                                            (.toArray ^LongStream (kd/kd-tree-range-search kd-tree new-min-range new-max-range))))))
                (.filter (reify LongPredicate
                           (test [_ x]
                             (.isInRange access x min-range max-range axis-mask))))))
          (kd/kd-tree-range-search kd-tree min-range max-range))))
    (kd-tree-points [_ deletes?]
      (kd/kd-tree-points kd-tree deletes?))
    (kd-tree-height [_]
      (kd/kd-tree-height kd-tree))
    (kd-tree-retain [_ allocator]
      (wrap-with-current-entity-cache (kd/kd-tree-retain kd-tree allocator) current-entities-cache))
    (kd-tree-point-access [_]
      (kd/kd-tree-point-access kd-tree))
    (kd-tree-size [_]
      (kd/kd-tree-size kd-tree))
    (kd-tree-value-count [_]
      (kd/kd-tree-value-count kd-tree))
    (kd-tree-dimensions [_]
      (kd/kd-tree-dimensions kd-tree))

    Closeable
    (close [_]
      (util/try-close kd-tree))))

(deftype TemporalManager [^BufferAllocator allocator
                          ^ObjectStore object-store
                          ^IBufferPool buffer-pool
                          ^IMetadataManager metadata-manager
                          ^AtomicLong id-counter
                          ^Map id->internal-id
                          ^ExecutorService snapshot-pool
                          ^:unsynchronized-mutable snapshot-future
                          ^:unsynchronized-mutable kd-tree-snapshot-idx
                          ^:volatile-mutable kd-tree
                          ^boolean async-snapshot?]
  TemporalManagerPrivate
  (latestTemporalSnapshotIndex [this chunk-idx]
    (->> (.listObjects object-store "temporal-snapshot-")
         (map temporal-snapshot-obj-key->chunk-idx)
         (filter #(<= ^long % chunk-idx))
         (last)))

  (buildStaticTree [this base-kd-tree chunk-idx snapshot-idx]
    (let [kd-tree (atom base-kd-tree)]
      (try
        (let [snapshot-idx (long (or snapshot-idx -1))
              new-chunk-idxs (for [^long idx (distinct (concat (.knownChunks metadata-manager) [chunk-idx]))
                                   :when (> idx snapshot-idx)
                                   :while (<= idx chunk-idx)]
                               idx)
              futs (for [chunk-idx new-chunk-idxs]
                     (-> (.getBuffer buffer-pool (->temporal-obj-key chunk-idx))
                         (util/then-apply util/try-close)))]
          @(CompletableFuture/allOf (into-array CompletableFuture futs))
          (doseq [chunk-idx new-chunk-idxs
                  :let [obj-key (->temporal-obj-key chunk-idx)
                        chunk-kd-tree (grid/->arrow-buf-grid  @(.getBuffer buffer-pool obj-key))]]
            (swap! kd-tree #(if %
                              (kd/->merged-kd-tree % chunk-kd-tree)
                              chunk-kd-tree)))
          @kd-tree)
        (catch Exception e
          (util/try-close @kd-tree)
          (throw e)))))

  (reloadTemporalIndex [this chunk-idx snapshot-idx]
    (if snapshot-idx
      (let [^ArrowBuf temporal-buffer @(.getBuffer buffer-pool (->temporal-snapshot-obj-key snapshot-idx))]
        (set! (.kd-tree this) (kd/->merged-kd-tree
                               (.buildStaticTree this
                                                 (grid/->arrow-buf-grid temporal-buffer)
                                                 chunk-idx
                                                 snapshot-idx)
                               nil))
        (when (and kd-tree-snapshot-idx (not= kd-tree-snapshot-idx snapshot-idx))
          (.evictBuffer buffer-pool (->temporal-snapshot-obj-key kd-tree-snapshot-idx)))
        (set! (.kd-tree-snapshot-idx this) snapshot-idx))
      (set! (.kd-tree this) (some-> (.buildStaticTree this nil chunk-idx snapshot-idx)
                                    (kd/->merged-kd-tree nil)))))

  (populateKnownChunks [this]
    (let [known-chunks (.knownChunks metadata-manager)
          futs (for [chunk-idx known-chunks]
                 (-> (.getBuffer buffer-pool (meta/->chunk-obj-key chunk-idx "_id"))
                     (util/then-apply util/try-close)))]
      @(CompletableFuture/allOf (into-array CompletableFuture futs))
      (doseq [chunk-idx known-chunks]
        (with-open [^ArrowBuf id-buffer @(.getBuffer buffer-pool (meta/->chunk-obj-key chunk-idx "_id"))
                    id-chunks (util/->chunks id-buffer)]
          (.forEachRemaining id-chunks
                             (reify Consumer
                               (accept [_ id-root]
                                 (let [^VectorSchemaRoot id-root id-root
                                       ^BigIntVector row-id-vec (.getVector id-root 0)
                                       id-vec (.getVector id-root 1)]
                                   (dotimes [n (.getRowCount id-root)]
                                     (.getOrCreateInternalId this (t/get-object id-vec n) (.get row-id-vec n)))))))))
      (when-let [temporal-chunk-idx (last known-chunks)]
        (.reloadTemporalIndex this temporal-chunk-idx (.latestTemporalSnapshotIndex this temporal-chunk-idx)))))

  (awaitSnapshotBuild [_]
    (some-> snapshot-future (deref)))

  (buildTemporalSnapshot [this chunk-idx snapshot-idx]
    (let [new-snapshot-obj-key (->temporal-snapshot-obj-key chunk-idx)
          path (util/->temp-file new-snapshot-obj-key "")]
      (try
        (if snapshot-idx
          (let [^ArrowBuf temporal-buffer @(.getBuffer buffer-pool (->temporal-snapshot-obj-key snapshot-idx))]
            (with-open [kd-tree (.buildStaticTree this
                                                  (grid/->arrow-buf-grid temporal-buffer)
                                                  chunk-idx
                                                  snapshot-idx)]
              (let [temporal-buf (-> (grid/->disk-grid allocator path kd-tree {:k k})
                                     (util/->mmap-path))]
                @(.putObject object-store new-snapshot-obj-key temporal-buf))))
          (when-let [kd-tree (.buildStaticTree this nil chunk-idx snapshot-idx)]
            (with-open [^Closeable kd-tree kd-tree]
              (let [temporal-buf (-> (grid/->disk-grid allocator path kd-tree {:k k})
                                     (util/->mmap-path))]
                @(.putObject object-store new-snapshot-obj-key temporal-buf)))))
        (finally
          (util/delete-file path)))))

  IInternalIdManager
  (getOrCreateInternalId [_ id row-id]
    (.computeIfAbsent id->internal-id
                      (normalize-id id)
                      (reify Function
                        (apply [_ _]
                          (->big-endian-internal-id row-id)))))

  (isKnownId [_ id]
    (.containsKey id->internal-id (normalize-id id)))

  ITemporalManager
  (getTemporalWatermark [_]
    (some-> kd-tree (kd/kd-tree-retain allocator)))

  (registerNewChunk [this chunk-idx]
    (when kd-tree
      (let [new-temporal-obj-key (->temporal-obj-key chunk-idx)
            path (util/->temp-file new-temporal-obj-key "")]
        (try
          (let [temporal-buf (-> (grid/->disk-grid allocator
                                                   path
                                                   (if (instance? MergedKdTree kd-tree)
                                                     (.getDynamicKdTree ^MergedKdTree kd-tree)
                                                     kd-tree)
                                                   {:k k
                                                    :cell-size 256
                                                    :deletes? true})
                                 (util/->mmap-path))]
            @(.putObject object-store new-temporal-obj-key temporal-buf))
          (finally
            (util/delete-file path)))))
    (.awaitSnapshotBuild this)
    (when kd-tree
      (with-open [^Closeable old-kd-tree kd-tree]
        (let [snapshot-idx (.latestTemporalSnapshotIndex this chunk-idx)
              fut (.submit snapshot-pool ^Runnable #(.buildTemporalSnapshot this chunk-idx snapshot-idx))]
          (set! (.snapshot-future this) fut)
          (when-not async-snapshot?
            @fut)
          (.reloadTemporalIndex this chunk-idx snapshot-idx)))))

  (startTx [this tx-instant]
    (let [tx-time-ms (.getTime ^Date (.tx-time tx-instant))
          end-of-time-ms (.getTime end-of-time)
          row-id->operations (TreeMap.)
          evicted-row-ids (Roaring64Bitmap.)]
      (letfn [(->temporal-coordinates [row-id eid
                                       ^TimeStampVector vt-start-vec
                                       ^TimeStampVector vt-end-vec
                                       idx tombstone?]
                (TemporalCoordinates. row-id eid
                                      tx-time-ms
                                      end-of-time-ms
                                      (if-not (.isNull vt-start-vec idx)
                                        (.get vt-start-vec idx)
                                        tx-time-ms)
                                      (if-not (.isNull vt-end-vec idx)
                                        (.get vt-end-vec idx)
                                        end-of-time-ms)
                                      tombstone?))]
        (reify ITemporalTxIndexer
          (indexPut [_ eid row-id vt-start-vec vt-end-vec idx]
            (.put row-id->operations row-id
                  (fn [kd-tree]
                    (insert-coordinates kd-tree allocator this
                                        (->temporal-coordinates row-id eid vt-start-vec vt-end-vec idx false)))))

          (indexDelete [_ eid row-id vt-start-vec vt-end-vec idx]
            (.put row-id->operations row-id
                  (fn [kd-tree]
                    (insert-coordinates kd-tree allocator this
                                        (->temporal-coordinates row-id eid vt-start-vec vt-end-vec idx true)))))

          (indexEvict [_ eid row-id]
            (.put row-id->operations row-id
                  (fn [kd-tree]
                    (evict-id kd-tree allocator
                              (.getOrCreateInternalId this eid row-id)
                              evicted-row-ids))))

          (endTx [_]
            (set! (.kd-tree this)
                  (reduce (fn [kd-tree op]
                            (op kd-tree))
                          (.kd-tree this)
                          (.values row-id->operations)))
            evicted-row-ids)))))

  (createTemporalRoots [_ watermark columns temporal-min-range temporal-max-range row-id-bitmap]
    (let [kd-tree (.temporal-watermark watermark)
          row-id-bitmap-out (Roaring64Bitmap.)
          ^IKdTreePointAccess point-access (kd/kd-tree-point-access kd-tree)
          ^LongStream  kd-tree-idxs (if (.isEmpty row-id-bitmap)
                                      (LongStream/empty)
                                      (kd/kd-tree-range-search kd-tree temporal-min-range temporal-max-range))]
      (if (empty? columns)
        (do (.forEach kd-tree-idxs
                      (reify LongConsumer
                        (accept [_ x]
                          (.addLong row-id-bitmap-out (.getCoordinate point-access x row-id-idx)))))
            (->TemporalRoots (doto row-id-bitmap-out
                               (.and row-id-bitmap))
                             (Collections/emptyMap)))
        (let [roots (HashMap.)
              coordinates (-> kd-tree-idxs
                              (.mapToObj (reify LongFunction
                                           (apply [_ x]
                                             (.getArrayPoint point-access x))))
                              (.filter (reify Predicate
                                         (test [_ x]
                                           (.contains row-id-bitmap (aget ^longs x row-id-idx)))))
                              (.sorted (Comparator/comparingLong (reify ToLongFunction
                                                                   (applyAsLong [_ x]
                                                                     (aget ^longs x row-id-idx)))))
                              (.toArray))
              value-count (alength coordinates)]
          (doseq [col-name columns]
            (let [col-idx (->temporal-column-idx col-name)
                  out-root (VectorSchemaRoot/create (->temporal-root-schema col-name) allocator)
                  ^BigIntVector row-id-vec (.getVector out-root 0)
                  ^TimeStampMilliVector temporal-vec (.getVector out-root 1)]
              (util/set-vector-schema-root-row-count out-root value-count)
              (dotimes [n value-count]
                (let [^longs coordinate (aget coordinates n)
                      row-id (aget coordinate row-id-idx)]
                  (.addLong row-id-bitmap-out row-id)
                  (.set row-id-vec n row-id)
                  (.set temporal-vec n (aget coordinate col-idx))))
              (.put roots col-name out-root)))
          (->TemporalRoots (doto row-id-bitmap-out
                             (.and row-id-bitmap))
                           roots)))))

  Closeable
  (close [this]
    (util/shutdown-pool snapshot-pool)
    (set! (.snapshot-future this) nil)
    (util/try-close kd-tree)
    (set! (.kd-tree this) nil)
    (.clear id->internal-id)))

(defmethod ig/prep-key ::temporal-manager [_ opts]
  (merge {:allocator (ig/ref :core2/allocator)
          :object-store (ig/ref :core2/object-store)
          :buffer-pool (ig/ref :core2.buffer-pool/buffer-pool)
          :metadata-manager (ig/ref :core2.metadata/metadata-manager)
          :async-snapshot? true}
         opts))

(defmethod ig/init-key ::temporal-manager
  [_ {:keys [^BufferAllocator allocator
             ^ObjectStore object-store
             ^IBufferPool buffer-pool
             ^IMetadataManager metadata-manager
             async-snapshot?]}]

  (let [pool (Executors/newSingleThreadExecutor (util/->prefix-thread-factory "temporal-snapshot-"))]
    (doto (TemporalManager. allocator object-store buffer-pool metadata-manager
                            (AtomicLong.) (ConcurrentHashMap.)
                            pool nil nil nil async-snapshot?)
      (.populateKnownChunks))))

(defmethod ig/halt-key! ::temporal-manager [_ ^TemporalManager mgr]
  (.close mgr))
