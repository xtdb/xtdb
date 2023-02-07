(ns core2.temporal
  (:require core2.buffer-pool
            [core2.metadata :as meta]
            [core2.temporal.grid :as grid]
            [core2.temporal.kd-tree :as kd]
            [core2.util :as util]
            [core2.vector :as vec]
            [core2.vector.indirect :as iv]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [core2.types :as types])
  (:import core2.buffer_pool.IBufferPool
           core2.metadata.IMetadataManager
           core2.object_store.ObjectStore
           [core2.temporal.kd_tree IKdTreePointAccess MergedKdTree]
           java.io.Closeable
           [java.util ArrayList Arrays Comparator]
           [java.util.concurrent CompletableFuture ExecutorService Executors]
           [java.util.function LongFunction Predicate ToLongFunction]
           java.util.stream.LongStream
           [org.apache.arrow.memory ArrowBuf BufferAllocator]
           org.apache.arrow.vector.BaseFixedWidthVector
           org.roaringbitmap.longlong.Roaring64Bitmap))

;; Temporal proof-of-concept plan:

;; From a BCDM point of view, core2 (and XTDB) are similar to Jensen's
;; event log approach, that is, we know sys-time, and we know the app-time
;; range, but not the actual real state as expressed in the Snodgrass'
;; timestamped tuple approach, which is the relation we want scan to
;; produce. Theoretically, one can map between these via the BCDM, as
;; described in the paper for snapshot equivalent representations, and
;; that serves as a good reference, but not practical.

;; The only update that needs to happen to the append only data is
;; setting sys-time-end to the current sys-time when closing
;; rows. Working around this is what the current uni-temporal sys-time
;; support does. This fact will help later when and if we decide to
;; store the temporal index per chunk in Arrow and merge between them.

;; Further, I think we can decide that a put or delete always know its
;; full app-time range, that is, if app-time isn't known it's set to sys-time,
;; and if app-time-end isn't know, it's set to end-of-time (at least
;; for the proof-of-concept).

;; In the temporal index structure, this means that when you do a put
;; (delete) you find any current rows (sys-time-end == UC) for the id
;; that overlaps the app-time range, and mark those rows with the
;; sys-time-end to current sys-time (the part that cannot be done append
;; only). You then insert the new row entry (for put) normally. If the
;; put (delete) didn't fully overlap you copy the start (and/or) end
;; partial row entries forward, referring to the original row-id,
;; updating their app-time-end (for start) and app-time (for end) to
;; match the slice, you also set sys-time to that of the current tx,
;; and sys-time-end to UC.

;; We assume that the column store has a 1-to-1 mapping between
;; operations and row-ids, but the temporal index can refer to them
;; more than once in the case of splits. These could also be stored in
;; the column store if we later decide to break the 1-to-1 mapping.

;; For simplicitly, let's assume that this structure is an in-memory
;; kd-tree for now with 6 dimensions: id, row-id, app-time,
;; app-time-end, sys-time, sys-time-end. When updating sys-time-end, one
;; has a few options, either one deletes the node and reinserts it, or
;; one can have an extra value (not part of the actual index),
;; sys-time-delete, which if it exists, supersedes sys-time-end when
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

(def ^:const ^int k 6)

(defn ->min-range ^longs []
  (long-array k Long/MIN_VALUE))

(defn ->max-range ^longs []
  (long-array k Long/MAX_VALUE))

(defn ->copy-range ^longs [^longs range]
  (some-> range (Arrays/copyOf (alength range))))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface ITemporalRelationSource
  (^core2.vector.IIndirectRelation createTemporalRelation [^org.apache.arrow.memory.BufferAllocator allocator
                                                           ^java.util.List columns
                                                           ^longs temporalMinRange
                                                           ^longs temporalMaxRange
                                                           ^org.roaringbitmap.longlong.Roaring64Bitmap rowIdBitmap]))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface ITemporalTxIndexer
  (^void indexPut [^long iid, ^long rowId, ^long startValidTime, ^long endValidTime, ^boolean newEntity])
  (^void indexDelete [^long iid, ^long rowId, ^long startValidTime, ^long endValidTime, ^boolean newEntity])
  (^void indexEvict [^long iid])
  (^org.roaringbitmap.longlong.Roaring64Bitmap commit [])
  (^void abort []))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface ITemporalManager
  (^core2.temporal.ITemporalRelationSource getTemporalWatermark [])
  (^core2.vector.IIndirectRelation createTemporalRelation [^org.apache.arrow.memory.BufferAllocator allocator
                                                           ^java.util.List columns
                                                           ^longs temporalMinRange
                                                           ^longs temporalMaxRange
                                                           ^org.roaringbitmap.longlong.Roaring64Bitmap rowIdBitmap])
  (^void registerNewChunk [^long chunkIdx])
  (^core2.temporal.ITemporalTxIndexer startTx [^core2.api.TransactionInstant txKey]))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface TemporalManagerPrivate
  (^void populateKnownChunks [])
  (^Long latestTemporalSnapshotIndex [^int chunk-idx])
  (^void reloadTemporalIndex [^int chunk-idx ^Long snapshot-idx])
  (^void awaitSnapshotBuild [])
  (^void buildTemporalSnapshot [^int chunk-idx ^Long snapshot-idx])
  (^java.io.Closeable buildStaticTree [^Object base-kd-tree ^int chunk-idx ^Long snapshot-idx]))

(deftype TemporalCoordinates [^long rowId, ^long iid,
                              ^long sysTimeStart, ^long sysTimeEnd
                              ^long appTimeStart, ^long appTimeEnd
                              ^boolean newEntity, ^boolean tombstone])

(def temporal-col-type [:timestamp-tz :micro "UTC"])

(def temporal-col-types
  {"_iid" :i64, "_row-id" :i64
   "system_time_start" temporal-col-type, "system_time_end" temporal-col-type
   "application_time_start" temporal-col-type, "application_time_end" temporal-col-type})

(defn temporal-column? [col-name]
  (contains? temporal-col-types (name col-name)))

(def ^:const ^int sys-time-end-idx 0)
(def ^:const ^int id-idx 1)
(def ^:const ^int sys-time-start-idx 2)
(def ^:const ^int row-id-idx 3)
(def ^:const ^int app-time-start-idx 4)
(def ^:const ^int app-time-end-idx 5)

(def ^:private column->idx {"_iid" id-idx
                            "_row-id" row-id-idx
                            "application_time_start" app-time-start-idx
                            "application_time_end" app-time-end-idx
                            "system_time_start" sys-time-start-idx
                            "system_time_end" sys-time-end-idx})

(defn ->temporal-column-idx ^long [col-name]
  (long (get column->idx (name col-name))))

(defn evict-id [kd-tree, ^BufferAllocator allocator, ^long iid, ^Roaring64Bitmap evicted-row-ids]
  (let [min-range (doto (->min-range)
                    (aset id-idx iid))

        max-range (doto (->max-range)
                    (aset id-idx iid))

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

(defn insert-coordinates [kd-tree, ^BufferAllocator allocator, ^TemporalCoordinates coordinates]
  (let [new-entity? (.newEntity coordinates)
        row-id (.rowId coordinates)
        iid (.iid coordinates)
        sys-time-start-μs (.sysTimeStart coordinates)
        sys-time-end-μs (.sysTimeEnd coordinates)
        app-time-start-μs (.appTimeStart coordinates)
        app-time-end-μs (.appTimeEnd coordinates)

        min-range (doto (->min-range)
                    (aset id-idx iid)
                    (aset app-time-end-idx (inc app-time-start-μs))
                    (aset sys-time-end-idx sys-time-start-μs))

        max-range (doto (->max-range)
                    (aset id-idx iid)
                    (aset app-time-start-idx (dec app-time-end-μs))
                    (aset sys-time-end-idx sys-time-end-μs))

        ^IKdTreePointAccess point-access (kd/kd-tree-point-access kd-tree)

        overlap (when-not new-entity?
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
                                       (aset id-idx iid)
                                       (aset row-id-idx row-id)
                                       (aset app-time-start-idx app-time-start-μs)
                                       (aset app-time-end-idx app-time-end-μs)
                                       (aset sys-time-start-idx sys-time-start-μs)
                                       (aset sys-time-end-idx util/end-of-time-μs))))]
    (reduce
     (fn [kd-tree ^longs coord]
       (cond-> (kd/kd-tree-insert kd-tree allocator (doto (->copy-range coord)
                                                      (aset sys-time-end-idx sys-time-start-μs)))
         (< (aget coord app-time-start-idx) app-time-start-μs)
         (kd/kd-tree-insert allocator (doto (->copy-range coord)
                                        (aset sys-time-start-idx sys-time-start-μs)
                                        (aset app-time-end-idx app-time-start-μs)))

         (> (aget coord app-time-end-idx) app-time-end-μs)
         (kd/kd-tree-insert allocator (doto (->copy-range coord)
                                        (aset sys-time-start-idx sys-time-start-μs)
                                        (aset app-time-start-idx app-time-end-μs)))))
     kd-tree
     overlap)))

(defn- ->temporal-obj-key [chunk-idx]
  (format "chunk-%s/temporal.arrow" (util/->lex-hex-string chunk-idx)))

(defn- ->temporal-snapshot-obj-key [chunk-idx]
  (format "temporal-snapshots/%s.arrow" (util/->lex-hex-string chunk-idx)))

(defn- temporal-snapshot-obj-key->chunk-idx ^long [obj-key]
  (util/<-lex-hex-string (second (re-find #"temporal-snapshots/(\p{XDigit}+)\.arrow" obj-key))))

(defn- ->temporal-rel ^core2.vector.IIndirectRelation [^BufferAllocator allocator, kd-tree columns temporal-min-range temporal-max-range ^Roaring64Bitmap row-id-bitmap]
  (let [^IKdTreePointAccess point-access (kd/kd-tree-point-access kd-tree)
        ^LongStream kd-tree-idxs (if (.isEmpty row-id-bitmap)
                                   (LongStream/empty)
                                   (kd/kd-tree-range-search kd-tree temporal-min-range temporal-max-range))
        coordinates (-> kd-tree-idxs
                        (.mapToObj (reify LongFunction
                                     (apply [_ x]
                                       (.getArrayPoint point-access x))))
                        (.filter (reify Predicate
                                   (test [_ x]
                                     (.contains row-id-bitmap (aget ^longs x row-id-idx)))))

                        ;; HACK we seem to be creating zero-length app-time ranges, I don't know why, #403
                        ;; we filter them out here, but likely best that we don't create them in the first place
                        (.filter (reify Predicate
                                   (test [_ x]
                                     (not= (aget ^longs x app-time-start-idx)
                                           (aget ^longs x app-time-end-idx)))))

                        (.sorted (Comparator/comparingLong (reify ToLongFunction
                                                             (applyAsLong [_ x]
                                                               (aget ^longs x row-id-idx)))))
                        (.toArray))
        value-count (alength coordinates)

        cols (ArrayList. (count columns))]
    (try
      (doseq [col-name columns]
        (let [col-idx (->temporal-column-idx col-name)
              ^BaseFixedWidthVector temporal-vec (-> (types/col-type->field col-name (get temporal-col-types col-name))
                                                     (.createVector allocator))
              temporal-vec-wtr (vec/->mono-writer temporal-vec)]
          (.allocateNew temporal-vec value-count)
          (dotimes [n value-count]
            (let [^longs coordinate (aget coordinates n)]
              (.writeLong temporal-vec-wtr (aget coordinate col-idx))))
          (.setValueCount temporal-vec value-count)
          (.add cols (iv/->direct-vec temporal-vec))))

      (iv/->indirect-rel cols value-count)

      (catch Throwable e
        (run! util/try-close cols)
        (throw e)))))

(deftype TemporalManager [^BufferAllocator allocator
                          ^ObjectStore object-store
                          ^IBufferPool buffer-pool
                          ^IMetadataManager metadata-manager
                          ^ExecutorService snapshot-pool
                          ^:unsynchronized-mutable snapshot-future
                          ^:unsynchronized-mutable kd-tree-snapshot-idx
                          ^:volatile-mutable kd-tree
                          ^boolean async-snapshot?]
  TemporalManagerPrivate
  (latestTemporalSnapshotIndex [_ chunk-idx]
    (->> (.listObjects object-store "temporal-snapshots/")
         (map temporal-snapshot-obj-key->chunk-idx)
         (filter #(<= ^long % chunk-idx))
         (last)))

  (buildStaticTree [_ base-kd-tree chunk-idx snapshot-idx]
    (let [kd-tree (atom base-kd-tree)]
      (try
        (let [snapshot-idx (long (or snapshot-idx -1))
              new-chunk-idxs (for [^long idx (distinct (concat (keys (.chunksMetadata metadata-manager)) [chunk-idx]))
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
    (when-let [temporal-chunk-idx (last (keys (.chunksMetadata metadata-manager)))]
      (.reloadTemporalIndex this temporal-chunk-idx (.latestTemporalSnapshotIndex this temporal-chunk-idx))))

  (awaitSnapshotBuild [_]
    (some-> snapshot-future (deref)))

  (buildTemporalSnapshot [this chunk-idx snapshot-idx]
    (let [new-snapshot-obj-key (->temporal-snapshot-obj-key chunk-idx)
          path (util/->temp-file "temporal-snapshot" "")]
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

  ITemporalManager
  (getTemporalWatermark [_]
    (let [kd-tree (some-> kd-tree (kd/kd-tree-retain allocator))]
      (reify
        ITemporalRelationSource
        (createTemporalRelation [_ allocator columns temporal-min-range temporal-max-range row-id-bitmap]
          (->temporal-rel allocator kd-tree columns temporal-min-range temporal-max-range row-id-bitmap))

        Closeable
        (close [_]
          (util/try-close kd-tree)))))

  (registerNewChunk [this chunk-idx]
    (when kd-tree
      (let [new-temporal-obj-key (->temporal-obj-key chunk-idx)
            path (util/->temp-file "temporal-idx" "")]
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
      (with-open [^Closeable _old-kd-tree kd-tree]
        (let [snapshot-idx (.latestTemporalSnapshotIndex this chunk-idx)
              fut (.submit snapshot-pool ^Runnable #(.buildTemporalSnapshot this chunk-idx snapshot-idx))]
          (set! (.snapshot-future this) fut)
          (when-not async-snapshot?
            @fut)
          (.reloadTemporalIndex this chunk-idx snapshot-idx)))))

  (startTx [this-tm tx-key]
    (let [sys-time-μs (util/instant->micros (.sys-time tx-key))
          evicted-row-ids (Roaring64Bitmap.)
          !kd-tree (volatile! kd-tree)]
      (reify
        ITemporalTxIndexer
        (indexPut [_ iid row-id start-app-time end-app-time new-entity?]
          (vswap! !kd-tree
                  insert-coordinates allocator (TemporalCoordinates. row-id iid
                                                                     sys-time-μs util/end-of-time-μs
                                                                     start-app-time end-app-time
                                                                     new-entity? false)))

        (indexDelete [_ iid row-id start-app-time end-app-time new-entity?]
          (vswap! !kd-tree
                  insert-coordinates allocator (TemporalCoordinates. row-id iid
                                                                     sys-time-μs util/end-of-time-μs
                                                                     start-app-time end-app-time
                                                                     new-entity? true)))

        (indexEvict [_ iid]
          (vswap! !kd-tree evict-id allocator iid evicted-row-ids))

        (commit [_]
          (set! (.kd-tree this-tm) @!kd-tree)
          evicted-row-ids)

        (abort [_])

        ITemporalRelationSource
        (createTemporalRelation [_ allocator columns temporal-min-range temporal-max-range row-id-bitmap]
          (->temporal-rel allocator @!kd-tree columns temporal-min-range temporal-max-range row-id-bitmap)))))

  ITemporalRelationSource
  (createTemporalRelation [_ allocator columns temporal-min-range temporal-max-range row-id-bitmap]
    (->temporal-rel allocator kd-tree columns temporal-min-range temporal-max-range row-id-bitmap))

  Closeable
  (close [this]
    (util/shutdown-pool snapshot-pool)
    (set! (.snapshot-future this) nil)
    (util/try-close kd-tree)
    (set! (.kd-tree this) nil)))

(defmethod ig/prep-key ::temporal-manager [_ opts]
  (merge {:allocator (ig/ref :core2/allocator)
          :object-store (ig/ref :core2/object-store)
          :buffer-pool (ig/ref :core2.buffer-pool/buffer-pool)
          :metadata-mgr (ig/ref :core2.metadata/metadata-manager)
          :async-snapshot? true}
         opts))

(defmethod ig/init-key ::temporal-manager
  [_ {:keys [^BufferAllocator allocator
             ^ObjectStore object-store
             ^IBufferPool buffer-pool
             ^IMetadataManager metadata-mgr
             async-snapshot?]}]

  (let [pool (Executors/newSingleThreadExecutor (util/->prefix-thread-factory "temporal-snapshot-"))]
    (doto (TemporalManager. allocator object-store buffer-pool metadata-mgr
                            pool nil nil nil async-snapshot?)
      (.populateKnownChunks))))

(defmethod ig/halt-key! ::temporal-manager [_ ^TemporalManager mgr]
  (.close mgr))
