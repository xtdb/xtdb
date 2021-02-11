(ns core2.indexer
  (:require core2.buffer-pool
            [core2.metadata :as meta]
            [core2.types :as t]
            [core2.util :as util])
  (:import core2.buffer_pool.BufferPool
           core2.object_store.ObjectStore
           java.io.Closeable
           [java.nio.file Files Path StandardOpenOption]
           java.nio.file.attribute.FileAttribute
           [java.util Date List Map SortedSet]
           [java.util.concurrent CompletableFuture ConcurrentSkipListMap ConcurrentSkipListSet]
           [org.apache.arrow.memory ArrowBuf BufferAllocator]
           [org.apache.arrow.vector BigIntVector VectorSchemaRoot VectorLoader VectorUnloader]
           [org.apache.arrow.vector.complex DenseUnionVector StructVector]
           [org.apache.arrow.vector.ipc ArrowFileWriter ArrowStreamReader]
           [org.apache.arrow.vector.types.pojo Field Schema]))

(set! *unchecked-math* :warn-on-boxed)

(defrecord TransactionInstant [^long tx-id, ^Date tx-time])

(definterface IChunkManager
  (^org.apache.arrow.vector.VectorSchemaRoot getLiveRoot [^String fieldName])
  (^void registerNewMetadata [^String metadata])
  (^core2.indexer.TransactionInstant latestStoredTx [])
  (^Long latestStoredRowId []))

(definterface TransactionIndexer
  (^core2.indexer.TransactionInstant indexTx [^core2.indexer.TransactionInstant tx ^java.nio.ByteBuffer txOps]))

(definterface IndexerPrivate
  (^java.nio.ByteBuffer writeColumn [^org.apache.arrow.vector.VectorSchemaRoot live-root])
  (^void closeCols [])
  (^void finishChunk []))

(defn- copy-safe! [^VectorSchemaRoot content-root ^DenseUnionVector src-vec src-idx row-id]
  (let [[^BigIntVector row-id-vec, ^DenseUnionVector field-vec] (.getFieldVectors content-root)
        value-count (.getRowCount content-root)
        type-id (.getTypeId src-vec src-idx)
        offset (util/write-type-id field-vec (.getValueCount field-vec) type-id)]

    (.setSafe row-id-vec value-count ^int row-id)

    (.copyFromSafe (.getVectorByType field-vec type-id)
                   (.getOffset src-vec src-idx)
                   offset
                   (.getVectorByType src-vec type-id))

    (.setRowCount content-root (inc value-count))))

(def ^:private ^org.apache.arrow.vector.types.pojo.Field tx-time-field
  (t/->primitive-dense-union-field "_tx-time" #{:timestampmilli}))

(def ^:private timestampmilli-type-id
  (-> (t/primitive-type->arrow-type :timestampmilli)
      (t/arrow-type->type-id)))

(defn ->tx-time-vec ^org.apache.arrow.vector.complex.DenseUnionVector [^BufferAllocator allocator, ^Date tx-time]
  (doto ^DenseUnionVector (.createVector tx-time-field allocator)
    (.setValueCount 1)
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
    (.setValueCount 1)
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
            end-idx (+ start-idx ^long len)

            sliced-root (.slice root start-idx len)]

        (try
          (f sliced-root)

          (finally
            ;; slice returns the same VSR object if it's asked to slice the whole thing
            ;; we don't want to close it in that case, because that will mutate the live-root.
            (when-not (identical? sliced-root root)
              (.close sliced-root))))

        (when (< end-idx row-count)
          (recur end-idx (+ target-row-id max-rows-per-block)))))))

(definterface IJustIndex
  (^int indexTx [^core2.indexer.IChunkManager chunkMgr, ^org.apache.arrow.memory.BufferAllocator allocator,
                 ^core2.indexer.TransactionInstant tx-instant, ^java.nio.ByteBuffer tx-ops,
                 ^long nextRowId]))

(deftype JustIndexer []
  IJustIndex
  (indexTx [_ chunk-mgr allocator tx-instant tx-ops next-row-id]
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

                       (copy-safe! (.getLiveRoot chunk-mgr (.getName (.getField value-vec)))
                                   value-vec per-op-offset row-id))

                     (copy-safe! (.getLiveRoot chunk-mgr (.getName (.getField tx-time-vec)))
                                 tx-time-vec 0 row-id)

                     (copy-safe! (.getLiveRoot chunk-mgr (.getName (.getField tx-id-vec)))
                                 tx-id-vec 0 row-id)))))

        (.getValueCount tx-ops-vec)))))

(defn- latest-stored-row-id [^BufferAllocator allocator ^BufferPool buffer-pool ^SortedSet known-metadata]
  (when-let [^ArrowBuf latest-metadata-buffer @(.getBuffer buffer-pool (last known-metadata))]
    (with-open [latest-metadata-buffer latest-metadata-buffer]
      (reduce (completing
               (fn [_ metadata-root]
                 (reduced (meta/max-value metadata-root "_tx-id" "_row-id"))))
              nil
              (util/block-stream latest-metadata-buffer allocator)))))

(deftype Indexer [^BufferAllocator allocator
                  ^ObjectStore object-store
                  ^long max-rows-per-chunk
                  ^long max-rows-per-block
                  ^:volatile-mutable ^long chunk-idx
                  ^:volatile-mutable ^long row-count
                  ^Map live-roots
                  ^BufferPool buffer-pool
                  ^SortedSet known-metadata]

  IChunkManager
  (getLiveRoot [_ field-name]
    (.computeIfAbsent live-roots field-name
                      (util/->jfn
                        (fn [field-name]
                          (->live-root field-name allocator)))))

  (registerNewMetadata [_ metadata]
    (.add known-metadata metadata))

  (latestStoredTx [_]
    (when-let [^ArrowBuf latest-metadata-buffer @(.getBuffer buffer-pool (last known-metadata))]
      (with-open [latest-metadata-buffer latest-metadata-buffer]
        (reduce (completing
                 (fn [_ metadata-root]
                   (when-let [max-tx-id (meta/max-value metadata-root "_tx-id")]
                     (->> (util/local-date-time->date (meta/max-value metadata-root "_tx-time"))
                          (->TransactionInstant max-tx-id)
                          (reduced)))))
                nil
                (util/block-stream latest-metadata-buffer allocator)))))

  (latestStoredRowId [_]
    (latest-stored-row-id allocator buffer-pool known-metadata))

  TransactionIndexer
  (indexTx [this tx-instant tx-ops]
    (let [new-row-count (.indexTx (JustIndexer.) this allocator tx-instant tx-ops (+ chunk-idx row-count))]
      (set! (.row-count this) (+ row-count new-row-count)))

    ;; TODO better metric here?
    ;; row-id? bytes? tx-id?
    (when (>= row-count max-rows-per-chunk)
      (.finishChunk this))

    tx-instant)

  IndexerPrivate
  (writeColumn [_this live-root]
    (with-open [write-root (VectorSchemaRoot/create (.getSchema live-root) allocator)]
      (let [loader (VectorLoader. write-root)]
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
      (with-open [metadata-root (VectorSchemaRoot/create meta/metadata-schema allocator)]
        (let [futs (vec
                    (for [[^String col-name, ^VectorSchemaRoot live-root] live-roots]
                      (let [obj-key (format "chunk-%08x-%s.arrow" chunk-idx col-name)
                            fut (.putObject object-store obj-key (.writeColumn this live-root))]
                        (meta/write-col-meta metadata-root live-root col-name obj-key)
                        fut)))

              metadata-obj-key (format "metadata-%08x.arrow" chunk-idx)]

          (let [metadata-buf (util/root->arrow-ipc-byte-buffer metadata-root :file)]

            @(-> (CompletableFuture/allOf (into-array CompletableFuture futs))
                 (util/then-compose
                   (fn [_]
                     (.putObject object-store metadata-obj-key metadata-buf)))
                 (util/then-apply
                   (fn [_]
                     (.registerNewMetadata this metadata-obj-key)))))

          (.closeCols this)
          (set! (.chunk-idx this) (+ chunk-idx row-count))
          (set! (.row-count this) 0)))))

  Closeable
  (close [this]
    (.closeCols this)
    (.clear known-metadata)))

(defn ->indexer
  (^core2.indexer.Indexer [^BufferAllocator allocator
                           ^ObjectStore object-store
                           ^BufferPool buffer-pool]
   (->indexer allocator object-store buffer-pool {}))

  (^core2.indexer.Indexer [^BufferAllocator allocator
                           ^ObjectStore object-store
                           ^BufferPool buffer-pool
                           {:keys [max-rows-per-chunk max-rows-per-block]
                            :or {max-rows-per-chunk 10000
                                 max-rows-per-block 1000}}]
   (let [known-metadata (ConcurrentSkipListSet. ^List @(.listObjects object-store "metadata-*"))
         latest-row-id (latest-stored-row-id allocator buffer-pool known-metadata)
         next-row-id (if latest-row-id
                       (inc (long latest-row-id))
                       0)]
     (Indexer. allocator
               object-store
               max-rows-per-chunk
               max-rows-per-block
               next-row-id
               0
               (ConcurrentSkipListMap.)
               buffer-pool
               known-metadata))))
