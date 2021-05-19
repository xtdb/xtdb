(ns core2.indexer
  (:require [clojure.tools.logging :as log]
            [core2.blocks :as blocks]
            [core2.bloom :as bloom]
            [core2.metadata :as meta]
            [core2.system :as sys]
            [core2.temporal :as temporal]
            [core2.tx :as tx]
            [core2.types :as t]
            [core2.util :as util])
  (:import clojure.lang.MapEntry
           core2.DenseUnionUtil
           core2.metadata.IMetadataManager
           core2.object_store.ObjectStore
           [core2.temporal ITemporalManager TemporalCoordinates]
           [core2.tx TransactionInstant Watermark]
           core2.IChunkCursor
           java.io.Closeable
           [java.util Collections Date Map Map$Entry TreeMap]
           [java.util.concurrent CompletableFuture ConcurrentSkipListMap]
           java.util.concurrent.atomic.AtomicInteger
           java.util.function.Consumer
           org.apache.arrow.memory.BufferAllocator
           [org.apache.arrow.vector BigIntVector TimeStampVector VectorLoader VectorSchemaRoot VectorUnloader]
           [org.apache.arrow.vector.complex DenseUnionVector StructVector]
           org.apache.arrow.vector.ipc.ArrowStreamReader
           [org.apache.arrow.vector.types.pojo Field Schema]))

(set! *unchecked-math* :warn-on-boxed)

(definterface IChunkManager
  (^org.apache.arrow.vector.VectorSchemaRoot getLiveRoot [^String fieldName])
  (^core2.tx.Watermark getWatermark []))

(definterface TransactionIndexer
  (^core2.tx.TransactionInstant indexTx [^core2.tx.TransactionInstant tx ^java.nio.ByteBuffer txOps])
  (^core2.tx.TransactionInstant latestCompletedTx []))

(definterface IndexerPrivate
  (^int indexTx [^core2.tx.TransactionInstant tx-instant, ^java.nio.ByteBuffer tx-ops, ^long nextRowId])
  (^java.nio.ByteBuffer writeColumn [^org.apache.arrow.vector.VectorSchemaRoot live-root])
  (^void closeCols [])
  (^void finishChunk []))

(defn- copy-safe! [^VectorSchemaRoot content-root ^DenseUnionVector src-vec src-idx row-id]
  (let [^BigIntVector row-id-vec (.getVector content-root 0)
        ^DenseUnionVector field-vec (.getVector content-root 1)
        value-count (.getRowCount content-root)
        type-id (.getTypeId src-vec src-idx)
        offset (DenseUnionUtil/writeTypeId field-vec (.getValueCount field-vec) type-id)]

    (.setSafe row-id-vec value-count ^int row-id)

    (.copyFromSafe (.getVectorByType field-vec type-id)
                   (.getOffset src-vec src-idx)
                   offset
                   (.getVectorByType src-vec type-id))

    (util/set-vector-schema-root-row-count content-root (inc value-count))))

(def ^:private ^Field tx-time-field
  (t/->primitive-dense-union-field "_tx-time" #{:timestampmilli}))

(def ^:private timestampmilli-type-id
  (-> (t/primitive-type->arrow-type :timestampmilli)
      (t/arrow-type->type-id)))

(defn ->tx-time-vec ^org.apache.arrow.vector.complex.DenseUnionVector [^BufferAllocator allocator, ^Date tx-time]
  (doto ^DenseUnionVector (.createVector tx-time-field allocator)
    (util/set-value-count 1)
    (DenseUnionUtil/writeTypeId 0 timestampmilli-type-id)
    (-> (.getTimeStampMilliVector timestampmilli-type-id)
        (.setSafe 0 (.getTime tx-time)))))

(def ^:private ^Field tx-id-field
  (t/->primitive-dense-union-field "_tx-id" #{:bigint}))

(def ^:private bigint-type-id
  (-> (t/primitive-type->arrow-type :bigint)
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
  (-> (t/primitive-type->arrow-type :bit)
      (t/arrow-type->type-id)))

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
  (tx/->Watermark chunk-idx 0 (Collections/emptySortedMap) tx-instant temporal-watermark (AtomicInteger. 1) max-rows-per-block))

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

(deftype Indexer [^BufferAllocator allocator
                  ^ObjectStore object-store
                  ^IMetadataManager metadata-mgr
                  ^ITemporalManager temporal-mgr
                  ^long max-rows-per-chunk
                  ^long max-rows-per-block
                  ^Map live-roots
                  ^:volatile-mutable ^Watermark watermark]

  IChunkManager
  (getLiveRoot [_ field-name]
    (.computeIfAbsent live-roots field-name
                      (util/->jfn
                        (fn [field-name]
                          (->live-root field-name allocator)))))

  (getWatermark [_]
    (loop []
      (when-let [current-watermark watermark]
        (if (pos? (util/inc-ref-count (.ref-count current-watermark)))
          current-watermark
          (recur)))))

  TransactionIndexer
  (indexTx [this tx-instant tx-ops]
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
                              max-rows-per-block)))
      (when (>= new-chunk-row-count max-rows-per-chunk)
        (.finishChunk this)))

    tx-instant)

  (latestCompletedTx [_]
    (some-> watermark .tx-instant))

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
            op-type-ids (object-array (mapv (fn [^Field field]
                                              (keyword (.getName field)))
                                            (.getChildren (.getField tx-ops-vec))))
            tx-time-ms (.getTime ^Date (.tx-time tx-instant))
            row-id->temporal-coordinates (TreeMap.)]
        (dotimes [tx-op-idx (.getValueCount tx-ops-vec)]
          (let [op-type-id (.getTypeId tx-ops-vec tx-op-idx)
                per-op-offset (.getOffset tx-ops-vec tx-op-idx)
                op-vec (.getStruct tx-ops-vec op-type-id)

                ^TimeStampVector valid-time-start-vec (.getChild op-vec "_valid-time-start")
                ^TimeStampVector valid-time-end-vec (.getChild op-vec "_valid-time-end")
                row-id (+ next-row-id per-op-offset)
                op (aget op-type-ids op-type-id)
                ^TemporalCoordinates temporal-coordinates (temporal/row-id->coordinates row-id)]
            (set! (.txTimeStart temporal-coordinates) tx-time-ms)
            (.put row-id->temporal-coordinates row-id temporal-coordinates)
            (case op
              :put (let [^StructVector document-vec (.getChild op-vec "document" StructVector)]
                     (doseq [^DenseUnionVector value-vec (.getChildrenFromFields document-vec)
                             :when (not (neg? (.getTypeId value-vec per-op-offset)))]

                       (when (= "_id" (.getName value-vec))
                         (set! (.id temporal-coordinates) (t/get-object value-vec per-op-offset)))

                       (copy-safe! (.getLiveRoot this (.getName value-vec))
                                   value-vec per-op-offset row-id)))

              :delete (let [^DenseUnionVector id-vec (.getChild op-vec "_id" DenseUnionVector)]
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

            (when (not (.isNull valid-time-start-vec per-op-offset))
              (set! (.validTimeStart temporal-coordinates) (.get valid-time-start-vec per-op-offset))
              (copy-safe! (.getLiveRoot this (.getName valid-time-start-vec))
                          valid-time-start-vec per-op-offset row-id))

            (when (not (.isNull valid-time-end-vec per-op-offset))
              (set! (.validTimeEnd temporal-coordinates) (.get valid-time-end-vec per-op-offset))
              (copy-safe! (.getLiveRoot this (.getName valid-time-end-vec))
                          valid-time-end-vec per-op-offset row-id))))

        (.updateTemporalCoordinates temporal-mgr row-id->temporal-coordinates)

        (.getValueCount tx-ops-vec))))


  (writeColumn [_this live-root]
    (with-open [write-root (VectorSchemaRoot/create (.getSchema live-root) allocator)]
      (let [loader (VectorLoader. write-root)
            chunk-idx (.chunk-idx watermark)]
        (util/build-arrow-ipc-byte-buffer write-root :file
          (fn [write-batch!]
            (with-open [^IChunkCursor slices (blocks/->slices live-root chunk-idx max-rows-per-block)]
              (.forEachRemaining slices
                                 (reify Consumer
                                   (accept [_ sliced-root]
                                     (with-open [arb (.getRecordBatch (VectorUnloader. sliced-root))]
                                       (.load loader arb)
                                       (write-batch!)))))))))))

  (closeCols [_this]
    (doseq [^VectorSchemaRoot live-root (vals live-roots)]
      (util/try-close live-root))

    (.clear live-roots))

  (finishChunk [this]
    (when-not (.isEmpty live-roots)
      (try
        (let [chunk-idx (.chunk-idx watermark)]
          @(CompletableFuture/allOf (->> (for [[^String col-name, ^VectorSchemaRoot live-root] live-roots]
                                           (.putObject object-store (meta/->chunk-obj-key chunk-idx col-name) (.writeColumn this live-root)))
                                         (into-array CompletableFuture)))
          (.registerNewChunk temporal-mgr chunk-idx)
          (.registerNewChunk metadata-mgr live-roots chunk-idx max-rows-per-block)

          (with-open [old-watermark watermark]
            (set! (.watermark this) (->empty-watermark (+ chunk-idx (.row-count old-watermark)) (.tx-instant old-watermark)
                                                       (.getTemporalWatermark temporal-mgr) max-rows-per-block))))
        (finally
          (.closeCols this)))))

  Closeable
  (close [this]
    (.closeCols this)
    (.close watermark)
    (set! (.watermark this) nil)))

(defn ->indexer {::sys/deps {:allocator :core2/allocator
                             :object-store :core2/object-store
                             :metadata-mgr :core2/metadata-manager
                             :temporal-mgr :core2/temporal-manager}
                 ::sys/args {:max-rows-per-block {:spec ::sys/pos-int, :default 1000}
                             :max-rows-per-chunk {:spec ::sys/pos-int, :default 100000}}}
  [{:keys [allocator object-store metadata-mgr ^ITemporalManager temporal-mgr
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
              (->empty-watermark chunk-idx latest-tx (.getTemporalWatermark temporal-mgr) max-rows-per-block))))
