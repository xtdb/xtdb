(ns core2.indexer
  (:require [clojure.tools.logging :as log]
            [core2.api :as c2]
            [core2.await :as await]
            [core2.blocks :as blocks]
            [core2.bloom :as bloom]
            [core2.buffer-pool :as bp]
            [core2.metadata :as meta]
            [core2.relation :as rel]
            [core2.temporal :as temporal]
            [core2.tx :as tx]
            [core2.types :as t]
            [core2.util :as util]
            [juxt.clojars-mirrors.integrant.core :as ig])
  (:import clojure.lang.MapEntry
           [core2 DenseUnionUtil ICursor]
           core2.api.TransactionInstant
           core2.buffer_pool.BufferPool
           core2.metadata.IMetadataManager
           core2.object_store.ObjectStore
           core2.temporal.ITemporalManager
           core2.tx.Watermark
           java.io.Closeable
           java.lang.AutoCloseable
           [java.util Collections Date Map Map$Entry Set TreeMap]
           [java.util.concurrent CompletableFuture ConcurrentHashMap ConcurrentSkipListMap PriorityBlockingQueue]
           java.util.concurrent.atomic.AtomicInteger
           java.util.concurrent.locks.StampedLock
           [java.util.function Consumer Function]
           java.util.stream.IntStream
           [org.apache.arrow.memory ArrowBuf BufferAllocator]
           [org.apache.arrow.vector BigIntVector TimeStampMilliVector TimeStampVector ValueVector VectorLoader VectorSchemaRoot VectorUnloader]
           [org.apache.arrow.vector.complex DenseUnionVector ListVector StructVector]
           org.apache.arrow.vector.ipc.ArrowStreamReader
           [org.apache.arrow.vector.types.pojo ArrowType$Union Field Schema]
           org.apache.arrow.vector.types.UnionMode))

(set! *unchecked-math* :warn-on-boxed)

(definterface IChunkManager
  (^org.apache.arrow.vector.VectorSchemaRoot getLiveRoot [^String fieldName])
  (^core2.tx.Watermark getWatermark []))

(definterface TransactionIndexer
  (^core2.api.TransactionInstant indexTx [^core2.api.TransactionInstant tx ^java.nio.ByteBuffer txOps])
  (^core2.api.TransactionInstant latestCompletedTx [])
  (^java.util.concurrent.CompletableFuture #_<TransactionInstant> awaitTxAsync [^core2.api.TransactionInstant tx]))

(definterface IndexerPrivate
  (^int indexTx [^core2.api.TransactionInstant tx-instant, ^java.nio.ByteBuffer tx-ops, ^long nextRowId])
  (^java.nio.ByteBuffer writeColumn [^org.apache.arrow.vector.VectorSchemaRoot live-root])
  (^void closeCols [])
  (^void finishChunk []))

(defn- ->live-root [field-name allocator]
  (VectorSchemaRoot/create (Schema. [t/row-id-field (t/->field field-name t/dense-union-type false)]) allocator))

(defn ->live-slices [^Watermark watermark, col-names]
  (into {}
        (keep (fn [col-name]
                (when-let [root (-> (.column->root watermark)
                                    (get col-name))]
                  (let [row-counts (blocks/row-id-aligned-blocks root
                                                                 (.chunk-idx watermark)
                                                                 (.max-rows-per-block watermark))]
                    (MapEntry/create col-name (blocks/->slices root row-counts))))))
        col-names))

(defn- ->empty-watermark ^core2.tx.Watermark [^long chunk-idx ^TransactionInstant tx-instant temporal-watermark ^long max-rows-per-block]
  (tx/->Watermark chunk-idx 0 (Collections/emptySortedMap) tx-instant temporal-watermark (AtomicInteger. 1) max-rows-per-block (ConcurrentHashMap.)))

(defn- snapshot-roots [^Map live-roots]
  (Collections/unmodifiableSortedMap
   (reduce
    (fn [^Map acc ^Map$Entry kv]
      (let [k (.getKey kv)
            v (util/slice-root ^VectorSchemaRoot (.getValue kv))]
        (doto acc
          (.put k v))))
    (TreeMap.)
    live-roots)))

(defn- remove-closed-watermarks [^Set open-watermarks]
  (let [i (.iterator open-watermarks)]
    (while (.hasNext i)
      (let [^Watermark open-watermark (.next i)]
        (when (empty? (.thread->count open-watermark))
          (.remove i))))))

(def ^:private log-schema
  (Schema. [(t/->field "_tx-id" t/bigint-type false)
            (t/->field "_tx-time" t/timestamp-milli-type false)
            (t/->field "ops" t/list-type true
                       (t/->field "ops" t/struct-type false
                                  (t/->field "_row-id" t/bigint-type false)
                                  (t/->field "op" (ArrowType$Union. UnionMode/Dense (int-array [0 1 2])) false
                                             (t/->field "put" t/struct-type false
                                                        (t/->field "_valid-time-start" t/timestamp-milli-type true)
                                                        (t/->field "_valid-time-end" t/timestamp-milli-type true))
                                             (t/->field "delete" t/struct-type false
                                                        (t/->field "_id" t/dense-union-type false)
                                                        (t/->field "_valid-time-start" t/timestamp-milli-type true)
                                                        (t/->field "_valid-time-end" t/timestamp-milli-type true))
                                             (t/->field "evict" t/struct-type false))))]))

(definterface ILogOpIndexer
  (^void logPut [^long rowId, ^long txOpIdx])
  (^void logDelete [^long rowId, ^long txOpIdx])
  (^void logEvict [^long rowId, ^long txOpIdx])
  (^void endTx []))

(definterface ILogIndexer
  (^core2.indexer.ILogOpIndexer startTx [^core2.api.TransactionInstant txInstant,
                                         ^org.apache.arrow.vector.VectorSchemaRoot txRoot])
  (^java.nio.ByteBuffer writeLog [])
  (^void clear [])
  (^void close []))

(defn- ->log-indexer [^BufferAllocator allocator, ^long max-rows-per-block]
  (let [log-root (VectorSchemaRoot/create log-schema allocator)
        ^BigIntVector tx-id-vec (.getVector log-root "_tx-id")
        ^TimeStampMilliVector tx-time-vec (.getVector log-root "_tx-time")

        ^ListVector ops-vec (.getVector log-root "ops")
        ^StructVector ops-data-vec (.getDataVector ops-vec)
        ^BigIntVector row-id-vec (.getChild ops-data-vec "_row-id")
        ^DenseUnionVector op-vec (.getChild ops-data-vec "op")

        ^StructVector put-vec (.getStruct op-vec 0)
        ^TimeStampMilliVector put-vt-start-vec (.getChild put-vec "_valid-time-start")
        ^TimeStampMilliVector put-vt-end-vec (.getChild put-vec "_valid-time-end")

        ^StructVector delete-vec (.getStruct op-vec 1)
        delete-id-vec (.getChild delete-vec "_id")
        delete-id-writer (rel/vec->writer delete-id-vec)
        ^TimeStampMilliVector delete-vt-start-vec (.getChild delete-vec "_valid-time-start")
        ^TimeStampMilliVector delete-vt-end-vec (.getChild delete-vec "_valid-time-end")

        ^StructVector evict-vec (.getStruct op-vec 2)]

    (reify ILogIndexer
      (startTx [_ tx-instant tx-root]
        (let [tx-idx (.getRowCount log-root)]
          (.setSafe tx-id-vec tx-idx (.tx-id tx-instant))
          (.setSafe tx-time-vec tx-idx (.getTime ^Date (.tx-time tx-instant)))
          (.setRowCount log-root (inc tx-idx))

          (let [start-op-idx (.startNewValue ops-vec tx-idx)

                ^DenseUnionVector tx-ops-vec (.getVector tx-root "tx-ops")

                tx-put-vec (.getStruct tx-ops-vec 0)
                tx-put-vt-start-vec (.getChild tx-put-vec "_valid-time-start")
                tx-put-vt-end-vec (.getChild tx-put-vec "_valid-time-end")

                tx-delete-vec (.getStruct tx-ops-vec 1)
                tx-delete-id-vec (.getChild tx-delete-vec "_id")
                tx-delete-vt-start-vec (.getChild tx-delete-vec "_valid-time-start")
                tx-delete-vt-end-vec (.getChild tx-delete-vec "_valid-time-end")
                delete-id-row-appender (.rowAppender delete-id-writer (rel/vec->reader tx-delete-id-vec))]

            (letfn [(log-op [^long row-id]
                      (let [op-idx (.getValueCount ops-data-vec)]
                        (util/set-value-count ops-data-vec (inc op-idx))
                        (.setIndexDefined ops-data-vec op-idx)
                        (.setSafe row-id-vec op-idx row-id)
                        op-idx))]

              (reify ILogOpIndexer
                (logPut [_ row-id tx-op-idx]
                  (let [op-idx (log-op row-id)
                        dest-offset (DenseUnionUtil/writeTypeId op-vec op-idx 0)
                        src-offset (.getOffset tx-ops-vec tx-op-idx)]
                    (.setIndexDefined put-vec dest-offset)
                    (.copyFromSafe put-vt-start-vec src-offset dest-offset tx-put-vt-start-vec)
                    (.copyFromSafe put-vt-end-vec src-offset dest-offset tx-put-vt-end-vec)))

                (logDelete [_ row-id tx-op-idx]
                  (let [op-idx (log-op row-id)
                        dest-offset (DenseUnionUtil/writeTypeId op-vec op-idx 1)
                        src-offset (.getOffset tx-ops-vec tx-op-idx)]
                    (.setIndexDefined delete-vec dest-offset)
                    (.copyFromSafe delete-vt-start-vec src-offset dest-offset tx-delete-vt-start-vec)
                    (.copyFromSafe delete-vt-end-vec src-offset dest-offset tx-delete-vt-end-vec)

                    (.appendRow delete-id-row-appender src-offset dest-offset)))

                (logEvict [_ row-id tx-op-idx]
                  (let [op-idx (log-op row-id)
                        dest-offset (DenseUnionUtil/writeTypeId op-vec op-idx 2)]
                    (.setIndexDefined evict-vec dest-offset)))

                (endTx [_]
                  (.endValue ops-vec tx-idx (- (.getValueCount ops-data-vec) start-op-idx))))))))

      (writeLog [_]
        (.syncSchema log-root)
        (with-open [write-root (VectorSchemaRoot/create (.getSchema log-root) allocator)]
          (let [loader (VectorLoader. write-root)
                row-counts (blocks/list-count-blocks ops-vec max-rows-per-block)]
            (with-open [^ICursor slices (blocks/->slices log-root row-counts)]
              (util/build-arrow-ipc-byte-buffer write-root :file
                (fn [write-batch!]
                  (.forEachRemaining slices
                                     (reify Consumer
                                       (accept [_ sliced-root]
                                         (with-open [arb (.getRecordBatch (VectorUnloader. sliced-root))]
                                           (.load loader arb)
                                           (write-batch!)))))))))))

      (clear [_]
        (.clear log-root))

      Closeable
      (close [_]
        (.close log-root)))))

(defn- with-latest-log-chunk [{:keys [^ObjectStore object-store ^BufferPool buffer-pool]} f]
  (when-let [latest-log-k (last (.listObjects object-store "log-"))]
    @(-> (.getBuffer buffer-pool latest-log-k)
         (util/then-apply
           (fn [^ArrowBuf log-buffer]
             (assert log-buffer)

             (when log-buffer
               (f log-buffer)))))))

(defn latest-tx [deps]
  (with-latest-log-chunk deps
    (fn [log-buf]
      (util/with-last-block log-buf
        (fn [^VectorSchemaRoot log-root]
          (let [tx-count (.getRowCount log-root)
                ^BigIntVector tx-id-vec (.getVector log-root "_tx-id")
                ^TimeStampMilliVector tx-time-vec (.getVector log-root "_tx-time")
                ^BigIntVector row-id-vec (-> ^ListVector (.getVector log-root "ops")
                                             ^StructVector (.getDataVector)
                                             (.getChild "_row-id"))]
            {:latest-tx (c2/->TransactionInstant (.get tx-id-vec (dec tx-count))
                                                 (Date. (.get tx-time-vec (dec tx-count))))
             :latest-row-id (.get row-id-vec (dec (.getValueCount row-id-vec)))}))))))

(defn- copy-docs [^IChunkManager chunk-manager, ^DenseUnionVector tx-ops-vec, ^long base-row-id]
  (let [row-ids (let [row-ids (IntStream/builder)]
                  (dotimes [op-idx (.getValueCount tx-ops-vec)]
                    (when (zero? (.getTypeId tx-ops-vec op-idx))
                      (.add row-ids (+ base-row-id op-idx))))
                  (.toArray (.build row-ids)))
        doc-vec (-> (.getStruct tx-ops-vec 0)
                    (.getChild "document" StructVector))]
    (doseq [^DenseUnionVector child-vec doc-vec]
      (let [col-name (.getName child-vec)
            live-root (.getLiveRoot chunk-manager col-name)
            ^BigIntVector row-id-vec (.getVector live-root "_row-id")
            vec-writer (rel/vec->writer (.getVector live-root col-name))
            row-appender (.rowAppender vec-writer (rel/vec->reader child-vec))]
        (dotimes [src-idx (.getValueCount doc-vec)]
          (when-not (neg? (.getTypeId child-vec src-idx))
            (let [dest-idx (.getValueCount row-id-vec)]
              (.setValueCount row-id-vec (inc dest-idx))
              (.set row-id-vec dest-idx (aget row-ids src-idx)))
            (.appendRow row-appender src-idx)
            (util/set-vector-schema-root-row-count live-root (inc (.getRowCount live-root)))))
        (.syncSchema live-root)))))

(deftype Indexer [^BufferAllocator allocator
                  ^ObjectStore object-store
                  ^IMetadataManager metadata-mgr
                  ^ITemporalManager temporal-mgr
                  ^long max-rows-per-chunk
                  ^long max-rows-per-block
                  ^Map live-roots
                  ^ILogIndexer log-indexer
                  ^Set open-watermarks
                  ^StampedLock open-watermarks-lock
                  ^:volatile-mutable ^Watermark watermark
                  ^PriorityBlockingQueue awaiters
                  ^:volatile-mutable ^Throwable ingester-error]

  IChunkManager
  (getLiveRoot [_ field-name]
    (.computeIfAbsent live-roots field-name
                      (util/->jfn
                        (fn [field-name]
                          (->live-root field-name allocator)))))

  (getWatermark [_]
    (let [stamp (.writeLock open-watermarks-lock)]
      (try
        (remove-closed-watermarks open-watermarks)
        (finally
          (.unlock open-watermarks-lock stamp)))
      (loop []
        (when-let [current-watermark watermark]
          (if (pos? (util/inc-ref-count (.ref-count current-watermark)))
            (let [stamp (.writeLock open-watermarks-lock)]
              (try
                (let [^Map thread->count (.thread->count current-watermark)
                      ^AtomicInteger thread-ref-count (.computeIfAbsent thread->count
                                                                        (Thread/currentThread)
                                                                        (reify Function
                                                                          (apply [_ k]
                                                                            (AtomicInteger. 0))))]
                  (.incrementAndGet thread-ref-count)
                  (.add open-watermarks current-watermark)
                  current-watermark)
                (finally
                  (.unlock open-watermarks-lock stamp))))
            (recur))))))

  TransactionIndexer
  (indexTx [this tx-instant tx-ops]
    (try
      (let [chunk-idx (.chunk-idx watermark)
            row-count (.row-count watermark)
            next-row-id (+ chunk-idx row-count)
            number-of-new-rows (.indexTx this tx-instant tx-ops next-row-id)
            new-chunk-row-count (+ row-count number-of-new-rows)]
        (with-open [_old-watermark watermark]
          (set! (.watermark this)
                (tx/->Watermark chunk-idx
                                new-chunk-row-count
                                (snapshot-roots live-roots)
                                tx-instant
                                (.getTemporalWatermark temporal-mgr)
                                (AtomicInteger. 1)
                                max-rows-per-block
                                (ConcurrentHashMap.))))
        (when (>= new-chunk-row-count max-rows-per-chunk)
          (.finishChunk this)))

      (await/notify-tx tx-instant awaiters)

      tx-instant
      (catch Throwable e
        (set! (.ingester-error this) e)
        (await/notify-ex e awaiters)
        (throw e))))

  (latestCompletedTx [_]
    (some-> watermark .tx-instant))

  (awaitTxAsync [this tx]
    (await/await-tx-async tx
                          #(or (some-> ingester-error throw)
                               (.latestCompletedTx this))
                          awaiters))

  IndexerPrivate
  (indexTx [this tx-instant tx-ops next-row-id]
    (with-open [tx-ops-ch (util/->seekable-byte-channel tx-ops)
                sr (ArrowStreamReader. tx-ops-ch allocator)
                tx-root (.getVectorSchemaRoot sr)]

      (.loadNextBatch sr)

      (let [^DenseUnionVector tx-ops-vec (.getVector tx-root "tx-ops")
            op-count (.getValueCount tx-ops-vec)
            op-type-ids (object-array (mapv (fn [^Field field]
                                              (keyword (.getName field)))
                                            (.getChildren (.getField tx-ops-vec))))
            log-op-idxer (.startTx log-indexer tx-instant tx-root)
            temporal-idxer (.startTx temporal-mgr tx-instant)]

        (dotimes [tx-op-idx op-count]
          (let [op-type-id (.getTypeId tx-ops-vec tx-op-idx)
                per-op-offset (.getOffset tx-ops-vec tx-op-idx)
                op-vec (.getStruct tx-ops-vec op-type-id)

                ^TimeStampVector valid-time-start-vec (.getChild op-vec "_valid-time-start")
                ^TimeStampVector valid-time-end-vec (.getChild op-vec "_valid-time-end")
                row-id (+ next-row-id tx-op-idx)
                op (aget op-type-ids op-type-id)]
            (case op
              :put (let [id-vec (-> ^StructVector (.getChild op-vec "document" StructVector)
                                    (.getChild "_id"))]
                     (.logPut log-op-idxer row-id tx-op-idx)
                     (.indexPut temporal-idxer (t/get-object id-vec per-op-offset) row-id
                                valid-time-start-vec valid-time-end-vec per-op-offset))

              :delete (let [^DenseUnionVector id-vec (.getChild op-vec "_id" DenseUnionVector)]
                        (.logDelete log-op-idxer row-id tx-op-idx)
                        (.indexDelete temporal-idxer (t/get-object id-vec per-op-offset) row-id
                                      valid-time-start-vec valid-time-end-vec per-op-offset))

              :evict (let [^DenseUnionVector id-vec (.getChild op-vec "_id" DenseUnionVector)]
                       (.logEvict log-op-idxer row-id tx-op-idx)
                       (.indexEvict temporal-idxer (t/get-object id-vec per-op-offset) row-id)))))

        (copy-docs this tx-ops-vec next-row-id)

        (.endTx log-op-idxer)
        (let [evicted-row-ids (.endTx temporal-idxer)]
          (when-not (.isEmpty evicted-row-ids)
            ;; TODO create work item
            ))

        (.getValueCount tx-ops-vec))))

  (writeColumn [_this live-root]
    (with-open [write-root (VectorSchemaRoot/create (.getSchema live-root) allocator)]
      (let [loader (VectorLoader. write-root)
            row-counts (blocks/row-id-aligned-blocks live-root (.chunk-idx watermark) max-rows-per-block)]
        (with-open [^ICursor slices (blocks/->slices live-root row-counts)]
          (util/build-arrow-ipc-byte-buffer write-root :file
            (fn [write-batch!]
              (.forEachRemaining slices
                                 (reify Consumer
                                   (accept [_ sliced-root]
                                     (with-open [arb (.getRecordBatch (VectorUnloader. sliced-root))]
                                       (.load loader arb)
                                       (write-batch!)))))))))))

  (closeCols [_this]
    (doseq [^VectorSchemaRoot live-root (vals live-roots)]
      (util/try-close live-root))

    (.clear live-roots)
    (.clear log-indexer))

  (finishChunk [this]
    (when-not (.isEmpty live-roots)
      (log/debugf "finishing chunk '%x', tx '%s'" (.chunk-idx watermark) (pr-str (.latestCompletedTx this)))

      (try
        (let [chunk-idx (.chunk-idx watermark)]
          @(CompletableFuture/allOf (->> (cons
                                          (.putObject object-store (format "log-%016x.arrow" chunk-idx) (.writeLog log-indexer))
                                          (for [[^String col-name, ^VectorSchemaRoot live-root] live-roots]
                                            (.putObject object-store (meta/->chunk-obj-key chunk-idx col-name) (.writeColumn this live-root))))
                                         (into-array CompletableFuture)))
          (.registerNewChunk temporal-mgr chunk-idx)
          (.registerNewChunk metadata-mgr live-roots chunk-idx max-rows-per-block)

          (with-open [old-watermark watermark]
            (set! (.watermark this) (->empty-watermark (+ chunk-idx (.row-count old-watermark)) (.tx-instant old-watermark)
                                                       (.getTemporalWatermark temporal-mgr) max-rows-per-block)))
          (let [stamp (.writeLock open-watermarks-lock)]
            (try
              (remove-closed-watermarks open-watermarks)
              (finally
                (.unlock open-watermarks-lock stamp)))))
        (log/debug "finished chunk.")
        (finally
          (.closeCols this)))))

  Closeable
  (close [this]
    (.closeCols this)
    (.close watermark)
    (.close log-indexer)
    (let [stamp (.writeLock open-watermarks-lock)]
      (try
        (let [i (.iterator open-watermarks)]
          (while (.hasNext i)
            (let [^Watermark open-watermark (.next i)
                  ^AtomicInteger watermark-ref-cnt (.ref-count open-watermark)]
              (doseq [[^Thread thread ^AtomicInteger thread-ref-count] (.thread->count open-watermark)
                      :let [rc (.get thread-ref-count)]
                      :when (pos? rc)]
                (log/warn "interrupting:" thread "on close, has outstanding watermarks:" rc)
                (.interrupt thread))
              (loop [rc (.get watermark-ref-cnt)]
                (when (pos? rc)
                  (util/try-close open-watermark)
                  (recur (.get watermark-ref-cnt)))))))
        (finally
          (.unlock open-watermarks-lock stamp))))
    (.clear open-watermarks)
    (set! (.watermark this) nil)))

(defmethod ig/prep-key ::indexer [_ opts]
  (merge {:max-rows-per-block 1000
          :max-rows-per-chunk 100000
          :allocator (ig/ref :core2/allocator)
          :object-store (ig/ref :core2/object-store)
          :metadata-mgr (ig/ref ::meta/metadata-manager)
          :temporal-mgr (ig/ref ::temporal/temporal-manager)
          :buffer-pool (ig/ref ::bp/buffer-pool)}
         opts))

(defmethod ig/init-key ::indexer
  [_ {:keys [allocator object-store metadata-mgr ^ITemporalManager temporal-mgr
             max-rows-per-chunk max-rows-per-block]
      :as deps}]

  (let [{:keys [latest-row-id latest-tx]} (latest-tx deps)
        chunk-idx (if latest-row-id
                    (inc (long latest-row-id))
                    0)
        bloom-false-positive-probability (bloom/bloom-false-positive-probability? max-rows-per-chunk)]
    (when (> bloom-false-positive-probability 0.05)
      (log/warn "Bloom should be sized for large chunks:" max-rows-per-chunk
                "false positive probability:" bloom-false-positive-probability
                "bits:" bloom/bloom-bits
                "can be set via system property core2.bloom.bits"))
    (Indexer. allocator
              object-store
              metadata-mgr
              temporal-mgr
              max-rows-per-chunk
              max-rows-per-block
              (ConcurrentSkipListMap.)
              (->log-indexer allocator max-rows-per-block)
              (util/->identity-set)
              (StampedLock.)
              (->empty-watermark chunk-idx latest-tx (.getTemporalWatermark temporal-mgr) max-rows-per-block)
              (PriorityBlockingQueue.)
              nil)))

(defmethod ig/halt-key! ::indexer [_ ^AutoCloseable indexer]
  (.close indexer))
