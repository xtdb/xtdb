(ns core2.indexer
  (:require core2.metadata
            core2.object-store
            [core2.tx :as tx]
            [core2.types :as t]
            [core2.util :as util]
            [core2.metadata :as meta])
  (:import core2.metadata.IMetadataManager
           core2.object_store.ObjectStore
           [core2.tx TransactionInstant Watermark]
           java.io.Closeable
           [java.util Collections Date Map Map$Entry TreeMap]
           [java.util.concurrent CompletableFuture ConcurrentSkipListMap]
           java.util.concurrent.atomic.AtomicInteger
           org.apache.arrow.memory.BufferAllocator
           [org.apache.arrow.vector BigIntVector VectorLoader VectorSchemaRoot VectorUnloader]
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
        offset (util/write-type-id field-vec (.getValueCount field-vec) type-id)]

    (.setSafe row-id-vec value-count ^int row-id)

    (.copyFromSafe (.getVectorByType field-vec type-id)
                   (.getOffset src-vec src-idx)
                   offset
                   (.getVectorByType src-vec type-id))

    (util/set-vector-schema-root-row-count content-root (inc value-count))))

(def ^:private ^org.apache.arrow.vector.types.pojo.Field tx-time-field
  (t/->primitive-dense-union-field "_tx-time" #{:timestampmilli}))

(def ^:private timestampmilli-type-id
  (-> (t/primitive-type->arrow-type :timestampmilli)
      (t/arrow-type->type-id)))

(defn ->tx-time-vec ^org.apache.arrow.vector.complex.DenseUnionVector [^BufferAllocator allocator, ^Date tx-time]
  (doto ^DenseUnionVector (.createVector tx-time-field allocator)
    (util/set-value-count 1)
    (util/write-type-id 0 timestampmilli-type-id)
    (-> (.getTimeStampMilliVector timestampmilli-type-id)
        (.setSafe 0 (.getTime tx-time)))))

(def ^:private ^org.apache.arrow.vector.types.pojo.Field tx-id-field
  (t/->primitive-dense-union-field "_tx-id" #{:bigint}))

(def ^:private bigint-type-id
  (-> (t/primitive-type->arrow-type :bigint)
      (t/arrow-type->type-id)))

(defn ->tx-id-vec ^org.apache.arrow.vector.complex.DenseUnionVector [^BufferAllocator allocator, ^long tx-id]
  (doto ^DenseUnionVector (.createVector tx-id-field allocator)
    (util/set-value-count 1)
    (util/write-type-id 0 bigint-type-id)
    (-> (.getBigIntVector bigint-type-id)
        (.setSafe 0 tx-id))))

(defn- ->live-root [field-name allocator]
  (VectorSchemaRoot/create (Schema. [t/row-id-field (t/->primitive-dense-union-field field-name)]) allocator))

(defn- with-slices [^VectorSchemaRoot root, ^long start-row-id, ^long max-rows-per-block, f]
  (let [^BigIntVector row-id-vec (.getVector root (.getName t/row-id-field))
        row-count (.getRowCount root)]

    (loop [start-idx 0
           target-row-id (+ start-row-id max-rows-per-block)]
      (let [len (loop [len 0]
                  (let [idx (+ start-idx len)]
                    (if (or (>= idx row-count)
                            (>= (.get row-id-vec idx) target-row-id))
                      len
                      (recur (inc len)))))
            end-idx (+ start-idx ^long len)]

        (with-open [^VectorSchemaRoot sliced-root (util/slice-root root start-idx len)]
          (f sliced-root))

        (when (< end-idx row-count)
          (recur end-idx (+ target-row-id max-rows-per-block)))))))

(defn- chunk-object-key [^long chunk-idx col-name]
  (format "chunk-%08x-%s.arrow" chunk-idx col-name))

(defn- ->watermark ^core2.tx.Watermark [^long chunk-idx ^long row-count ^Map column->root ^TransactionInstant tx-instant]
  (tx/->Watermark chunk-idx
                  row-count
                  (Collections/unmodifiableSortedMap
                   (reduce
                    (fn [^Map acc ^Map$Entry kv]
                      (let [k (.getKey kv)
                            v (util/slice-root ^VectorSchemaRoot (.getValue kv) 0)]
                        (doto acc
                          (.put k v))))
                    (TreeMap.)
                    column->root))
                  tx-instant
                  (AtomicInteger. 1)))

(defn- ->empty-watermark ^core2.tx.Watermark [^long chunk-idx ^TransactionInstant tx-instant]
  (tx/->Watermark chunk-idx 0 (Collections/emptySortedMap) tx-instant (AtomicInteger. 1)))

(deftype Indexer [^BufferAllocator allocator
                  ^ObjectStore object-store
                  ^IMetadataManager metadata-mgr
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
      (with-open [old-watermark watermark]
        (set! (.watermark this) (->watermark chunk-idx new-chunk-row-count live-roots tx-instant)))
      (when (>= new-chunk-row-count max-rows-per-chunk)
        (.finishChunk this)))

    tx-instant)

  (latestCompletedTx [_]
    (.tx-instant watermark))

  IndexerPrivate
  (indexTx [this tx-instant tx-ops next-row-id]
    (with-open [tx-ops-ch (util/->seekable-byte-channel tx-ops)
                sr (ArrowStreamReader. tx-ops-ch allocator)
                tx-root (.getVectorSchemaRoot sr)
                ^DenseUnionVector tx-id-vec (->tx-id-vec allocator (.tx-id tx-instant))
                ^DenseUnionVector tx-time-vec (->tx-time-vec allocator (.tx-time tx-instant))]

      (.loadNextBatch sr)

      (let [^DenseUnionVector tx-ops-vec (.getVector tx-root "tx-ops")
            op-type-ids (object-array (mapv (fn [^Field field]
                                              (keyword (.getName field)))
                                            (.getChildren (.getField tx-ops-vec))))]
        (dotimes [tx-op-idx (.getValueCount tx-ops-vec)]
          (let [op-type-id (.getTypeId tx-ops-vec tx-op-idx)
                op-vec (.getStruct tx-ops-vec op-type-id)
                per-op-offset (.getOffset tx-ops-vec tx-op-idx)]
            (case (aget op-type-ids op-type-id)
              :put (let [^StructVector document-vec (.getChild op-vec "document" StructVector)
                         row-id (+ next-row-id per-op-offset)]
                     (doseq [^DenseUnionVector value-vec (.getChildrenFromFields document-vec)
                             :when (not (neg? (.getTypeId value-vec per-op-offset)))]

                       (copy-safe! (.getLiveRoot this (.getName value-vec))
                                   value-vec per-op-offset row-id))

                     (copy-safe! (.getLiveRoot this (.getName tx-time-vec))
                                 tx-time-vec 0 row-id)

                     (copy-safe! (.getLiveRoot this (.getName tx-id-vec))
                                 tx-id-vec 0 row-id)))))

        (.getValueCount tx-ops-vec))))


  (writeColumn [_this live-root]
    (with-open [write-root (VectorSchemaRoot/create (.getSchema live-root) allocator)]
      (let [loader (VectorLoader. write-root)
            chunk-idx (.chunk-idx watermark)]
        (util/build-arrow-ipc-byte-buffer write-root :file
          (fn [write-batch!]
            (with-slices live-root chunk-idx max-rows-per-block
              (fn [sliced-root]
                (with-open [arb (.getRecordBatch (VectorUnloader. sliced-root))]
                  (.load loader arb)
                  (write-batch!)))))))))

  (closeCols [_this]
    (doseq [^VectorSchemaRoot live-root (vals live-roots)]
      (util/try-close live-root))

    (.clear live-roots))

  (finishChunk [this]
    (when-not (.isEmpty live-roots)
      (try
        (let [chunk-idx (.chunk-idx watermark)
              futs (reduce
                    (fn [acc [^String col-name, ^VectorSchemaRoot live-root]]
                      (conj acc (.putObject object-store (chunk-object-key chunk-idx col-name) (.writeColumn this live-root))))
                    []
                    live-roots)]

          @(-> (CompletableFuture/allOf (into-array CompletableFuture futs))
               (util/then-apply
                 (fn [_]
                   (.registerNewChunk metadata-mgr live-roots chunk-idx))))

          (with-open [old-watermark watermark]
            (set! (.watermark this) (->empty-watermark (+ chunk-idx (.row-count old-watermark)) (.tx-instant old-watermark)))))
        (finally
          (.closeCols this)))))

  Closeable
  (close [this]
    (.closeCols this)
    (.close watermark)
    (set! (.watermark this) nil)))

(defn ->indexer
  (^core2.indexer.Indexer [^BufferAllocator allocator
                           ^ObjectStore object-store
                           ^IMetadataManager metadata-mgr]
   (->indexer allocator object-store metadata-mgr {}))

  (^core2.indexer.Indexer [^BufferAllocator allocator
                           ^ObjectStore object-store
                           ^IMetadataManager metadata-mgr
                           {:keys [max-rows-per-chunk max-rows-per-block]
                            :or {max-rows-per-chunk 10000
                                 max-rows-per-block 1000}}]
   (let [[latest-row-id latest-tx] @(meta/with-latest-metadata metadata-mgr
                                      (juxt meta/latest-row-id meta/latest-tx))
         chunk-idx (if latest-row-id
                       (inc (long latest-row-id))
                       0)]
     (Indexer. allocator
               object-store
               metadata-mgr
               max-rows-per-chunk
               max-rows-per-block
               (ConcurrentSkipListMap.)
               (->empty-watermark chunk-idx latest-tx)))))
