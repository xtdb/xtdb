(ns core2.indexer
  (:require [clojure.tools.logging :as log]
            [core2.await :as await]
            [core2.blocks :as blocks]
            [core2.bloom :as bloom]
            [core2.metadata :as meta]
            [core2.temporal :as temporal]
            [core2.tx :as tx]
            [core2.types :as t]
            [core2.util :as util]
            [juxt.clojars-mirrors.integrant.core :as ig])
  (:import clojure.lang.MapEntry
           [core2 DenseUnionUtil ICursor]
           core2.metadata.IMetadataManager
           core2.object_store.ObjectStore
           [core2.temporal ITemporalManager TemporalCoordinates]
           [core2.tx TransactionInstant Watermark]
           java.io.Closeable
           [java.util Collections Date Map Map$Entry Set TreeMap]
           [java.util.concurrent CompletableFuture ConcurrentHashMap ConcurrentSkipListMap PriorityBlockingQueue]
           java.util.concurrent.atomic.AtomicInteger
           java.util.concurrent.locks.StampedLock
           [java.util.function Consumer Function]
           org.apache.arrow.memory.BufferAllocator
           [org.apache.arrow.vector BigIntVector TimeStampMilliVector TimeStampVector ValueVector VectorLoader VectorSchemaRoot VectorUnloader]
           [org.apache.arrow.vector.complex DenseUnionVector ListVector StructVector]
           org.apache.arrow.vector.ipc.ArrowStreamReader
           org.apache.arrow.vector.types.UnionMode
           [org.apache.arrow.vector.types.pojo ArrowType$Union Field Schema]))

(set! *unchecked-math* :warn-on-boxed)

(definterface IChunkManager
  (^org.apache.arrow.vector.VectorSchemaRoot getLiveRoot [^String fieldName])
  (^core2.tx.Watermark getWatermark []))

(definterface TransactionIndexer
  (^core2.tx.TransactionInstant indexTx [^core2.tx.TransactionInstant tx ^java.nio.ByteBuffer txOps])
  (^core2.tx.TransactionInstant latestCompletedTx [])
  (^java.util.concurrent.CompletableFuture #_<TransactionInstant> awaitTxAsync [^core2.tx.TransactionInstant tx]))

(definterface IndexerPrivate
  (^int indexTx [^core2.tx.TransactionInstant tx-instant, ^java.nio.ByteBuffer tx-ops, ^long nextRowId])
  (^java.nio.ByteBuffer writeColumn [^org.apache.arrow.vector.VectorSchemaRoot live-root])
  (^void closeCols [])
  (^void finishChunk []))

(defn- copy-duv-safe! [^DenseUnionVector src-vec, src-idx,
                       ^DenseUnionVector dest-vec, dest-idx]
  (let [type-id (.getTypeId src-vec src-idx)
        offset (DenseUnionUtil/writeTypeId dest-vec dest-idx type-id)]
    (.copyFromSafe (.getVectorByType dest-vec type-id)
                   (.getOffset src-vec src-idx)
                   offset
                   (.getVectorByType src-vec type-id))))

(defn- copy-safe! [^VectorSchemaRoot content-root ^ValueVector src-vec src-idx row-id]
  (let [^BigIntVector row-id-vec (.getVector content-root 0)
        ^DenseUnionVector field-vec (.getVector content-root 1)
        value-count (.getRowCount content-root)]

    (.setSafe row-id-vec value-count ^int row-id)

    (if (instance? DenseUnionVector src-vec)
      (copy-duv-safe! src-vec src-idx field-vec value-count)

      (let [type-id (t/arrow-type->type-id (.getType (.getField src-vec)))
            offset (DenseUnionUtil/writeTypeId field-vec (.getValueCount field-vec) type-id)]
        (.copyFromSafe (.getVectorByType field-vec type-id)
                       src-idx
                       offset
                       src-vec)))

    (util/set-vector-schema-root-row-count content-root (inc value-count))))

(def ^:private ^Field tx-time-field
  (t/->primitive-dense-union-field "_tx-time" #{:timestamp-milli}))

(def ^:private timestampmilli-type-id
  (-> (t/->arrow-type :timestamp-milli) (t/arrow-type->type-id)))

(defn ->tx-time-vec ^org.apache.arrow.vector.complex.DenseUnionVector [^BufferAllocator allocator, ^Date tx-time]
  (doto ^DenseUnionVector (.createVector tx-time-field allocator)
    (util/set-value-count 1)
    (DenseUnionUtil/writeTypeId 0 timestampmilli-type-id)
    (-> (.getTimeStampMilliVector timestampmilli-type-id)
        (.setSafe 0 (.getTime tx-time)))))

(def ^:private ^Field tx-id-field
  (t/->primitive-dense-union-field "_tx-id" #{:bigint}))

(def ^:private bigint-type-id
  (-> (t/->arrow-type :bigint)
      (t/arrow-type->type-id)))

(defn ->tx-id-vec ^org.apache.arrow.vector.complex.DenseUnionVector [^BufferAllocator allocator, ^long tx-id]
  (doto ^DenseUnionVector (.createVector tx-id-field allocator)
    (util/set-value-count 1)
    (DenseUnionUtil/writeTypeId 0 bigint-type-id)
    (-> (.getBigIntVector bigint-type-id)
        (.setSafe 0 tx-id))))

(def ^:private ^Field tombstone-field
  (t/->primitive-dense-union-field "_tombstone" #{:bit}))

(def ^:private bit-type-id
  (-> (t/arrow-type->type-id (t/->arrow-type :bit))))

(defn ->tombstone-vec ^org.apache.arrow.vector.complex.DenseUnionVector [^BufferAllocator allocator, ^Boolean tombstone?]
  (doto ^DenseUnionVector (.createVector tombstone-field allocator)
    (util/set-value-count 1)
    (DenseUnionUtil/writeTypeId 0 bit-type-id)
    (-> (.getBitVector bit-type-id)
        (.setSafe 0 (if tombstone? 1 0)))))

(defn- ->live-root [field-name allocator]
  (VectorSchemaRoot/create (Schema. [t/row-id-field (t/->primitive-dense-union-field field-name)]) allocator))

(defn ->live-slices [^Watermark watermark, col-names]
  (into {}
        (keep (fn [col-name]
                (when-let [root (-> (.column->root watermark)
                                    (get col-name))]
                  (MapEntry/create col-name
                                   (blocks/->slices root
                                                    (.chunk-idx watermark)
                                                    (.max-rows-per-block watermark))))))
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
  (Schema. [(t/->field "_tx-id" (t/->arrow-type :bigint) false)
            (t/->field "_tx-time" (t/->arrow-type :timestamp-milli) false)
            (t/->field "ops" t/list-type true
                       (t/->field "ops" t/struct-type false
                                  (t/->field "_row-id" (t/->arrow-type :bigint) false)
                                  (t/->field "op" (ArrowType$Union. UnionMode/Dense (int-array [0 1])) false
                                             (t/->field "put" t/struct-type false
                                                        (t/->field "_valid-time-start" (t/->arrow-type :timestamp-milli) true)
                                                        (t/->field "_valid-time-end" (t/->arrow-type :timestamp-milli) true))
                                             (t/->field "delete" t/struct-type false
                                                        (t/->primitive-dense-union-field "_id")
                                                        (t/->field "_valid-time-start" (t/->arrow-type :timestamp-milli) true)
                                                        (t/->field "_valid-time-end" (t/->arrow-type :timestamp-milli) true)))))]))

(definterface ILogOpIndexer
  (^void logPut [^long rowId, ^long txOpIdx])
  (^void logDelete [^long rowId, ^long txOpIdx])
  (^void endTx []))

(definterface ILogIndexer
  (^core2.indexer.ILogOpIndexer startTx [^core2.tx.TransactionInstant txInstant,
                                         ^org.apache.arrow.vector.VectorSchemaRoot txRoot])
  (^java.nio.ByteBuffer writeLog [])
  (^void clear [])
  (^void close []))

(defn- ->log-indexer [^BufferAllocator allocator]
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
        ^TimeStampMilliVector delete-vt-start-vec (.getChild delete-vec "_valid-time-start")
        ^TimeStampMilliVector delete-vt-end-vec (.getChild delete-vec "_valid-time-end")]

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
                tx-delete-vt-end-vec (.getChild tx-delete-vec "_valid-time-end")]

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

                    (copy-duv-safe! tx-delete-id-vec src-offset delete-id-vec dest-offset)))

                (endTx [_]
                  (.endValue ops-vec tx-idx (- (.getValueCount ops-data-vec) start-op-idx))))))))

      (writeLog [_]
        ;; TODO chunkify
        (util/build-arrow-ipc-byte-buffer log-root :file
          (fn [write-batch!]
            (write-batch!))))

      (clear [_]
        (.clear log-root))

      Closeable
      (close [_]
        (.close log-root)))))

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
                tx-root (.getVectorSchemaRoot sr)
                ^DenseUnionVector tx-id-vec (->tx-id-vec allocator (.tx-id tx-instant))
                ^DenseUnionVector tx-time-vec (->tx-time-vec allocator (.tx-time tx-instant))
                ^DenseUnionVector tombstone-vec (->tombstone-vec allocator true)]

      (.loadNextBatch sr)

      (let [^DenseUnionVector tx-ops-vec (.getVector tx-root "tx-ops")
            op-count (.getValueCount tx-ops-vec)
            op-type-ids (object-array (mapv (fn [^Field field]
                                              (keyword (.getName field)))
                                            (.getChildren (.getField tx-ops-vec))))
            tx-time-ms (.getTime ^Date (.tx-time tx-instant))
            row-id->temporal-coordinates (TreeMap.)
            log-op-idxer (.startTx log-indexer tx-instant tx-root)]

        (dotimes [tx-op-idx op-count]
          (let [op-type-id (.getTypeId tx-ops-vec tx-op-idx)
                per-op-offset (.getOffset tx-ops-vec tx-op-idx)
                op-vec (.getStruct tx-ops-vec op-type-id)

                ^TimeStampVector valid-time-start-vec (.getChild op-vec "_valid-time-start")
                ^TimeStampVector valid-time-end-vec (.getChild op-vec "_valid-time-end")
                row-id (+ next-row-id per-op-offset)
                op (aget op-type-ids op-type-id)
                ^TemporalCoordinates temporal-coordinates (temporal/row-id->coordinates row-id)]
            (set! (.txTimeStart temporal-coordinates) tx-time-ms)
            (set! (.validTimeStart temporal-coordinates) tx-time-ms)
            (.put row-id->temporal-coordinates row-id temporal-coordinates)
            (case op
              :put (let [^StructVector document-vec (.getChild op-vec "document" StructVector)]
                     (.logPut log-op-idxer row-id tx-op-idx)
                     (doseq [^DenseUnionVector value-vec (.getChildrenFromFields document-vec)
                             :when (not (neg? (.getTypeId value-vec per-op-offset)))]
                       (when (= "_id" (.getName value-vec))
                         (set! (.id temporal-coordinates) (t/get-object value-vec per-op-offset)))

                       (copy-safe! (.getLiveRoot this (.getName value-vec))
                                   value-vec per-op-offset row-id)))

              :delete (let [^DenseUnionVector id-vec (.getChild op-vec "_id" DenseUnionVector)]
                        (.logDelete log-op-idxer row-id tx-op-idx)
                        (set! (.id temporal-coordinates) (t/get-object id-vec per-op-offset))
                        (set! (.tombstone temporal-coordinates) true)

                        (copy-safe! (.getLiveRoot this (.getName id-vec))
                                    id-vec per-op-offset row-id)

                        (copy-safe! (.getLiveRoot this (.getName tombstone-vec))
                                    tombstone-vec 0 row-id)))

            (copy-safe! (.getLiveRoot this (.getName tx-time-vec))
                        tx-time-vec 0 row-id)

            (copy-safe! (.getLiveRoot this (.getName tx-id-vec))
                        tx-id-vec 0 row-id)

            (set! (.validTimeStart temporal-coordinates)
                  (if-not (.isNull valid-time-start-vec per-op-offset)
                    (.get valid-time-start-vec per-op-offset)
                    tx-time-ms))

            (set! (.validTimeEnd temporal-coordinates)
                  (if-not (.isNull valid-time-end-vec per-op-offset)
                    (.get valid-time-end-vec per-op-offset)
                    (.getTime temporal/end-of-time)))))

        (.endTx log-op-idxer)
        (.updateTemporalCoordinates temporal-mgr row-id->temporal-coordinates)

        (.getValueCount tx-ops-vec))))


  (writeColumn [_this live-root]
    (with-open [write-root (VectorSchemaRoot/create (.getSchema live-root) allocator)]
      (let [loader (VectorLoader. write-root)
            chunk-idx (.chunk-idx watermark)]
        (util/build-arrow-ipc-byte-buffer write-root :file
          (fn [write-batch!]
            (with-open [^ICursor slices (blocks/->slices live-root chunk-idx max-rows-per-block)]
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
          :temporal-mgr (ig/ref ::temporal/temporal-manager)}
         opts))

(defmethod ig/init-key ::indexer
  [_ {:keys [allocator object-store metadata-mgr ^ITemporalManager temporal-mgr
             max-rows-per-chunk max-rows-per-block]}]

  (let [[latest-row-id latest-tx] @(meta/with-latest-metadata metadata-mgr
                                     (juxt meta/latest-row-id meta/latest-tx))
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
              (->log-indexer allocator)
              (util/->identity-set)
              (StampedLock.)
              (->empty-watermark chunk-idx latest-tx (.getTemporalWatermark temporal-mgr) max-rows-per-block)
              (PriorityBlockingQueue.)
              nil)))

(defmethod ig/halt-key! ::indexer [_ ^Indexer indexer]
  (.close indexer))
