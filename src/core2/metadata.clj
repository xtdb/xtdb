(ns core2.metadata
  (:require [core2.blocks :as blocks]
            [core2.bloom :as bloom]
            core2.buffer-pool
            [core2.expression.comparator :as expr.comp]
            core2.object-store
            [core2.system :as sys]
            [core2.tx :as tx]
            [core2.types :as t]
            [core2.util :as util])
  (:import core2.buffer_pool.IBufferPool
           core2.object_store.ObjectStore
           core2.tx.Watermark
           core2.IChunkCursor
           java.io.Closeable
           [java.util Date HashMap List Map SortedSet]
           [java.util.concurrent CompletableFuture ConcurrentSkipListSet]
           [java.util.function Consumer Function]
           [org.apache.arrow.memory ArrowBuf BufferAllocator]
           [org.apache.arrow.vector BigIntVector FieldVector TimeStampMilliVector VarBinaryVector VarCharVector VectorSchemaRoot]
           [org.apache.arrow.vector.complex DenseUnionVector ListVector StructVector]
           [org.apache.arrow.vector.types Types Types$MinorType]
           [org.apache.arrow.vector.types.pojo ArrowType ArrowType$List Schema]
           org.apache.arrow.vector.util.Text
           org.roaringbitmap.RoaringBitmap))

(set! *unchecked-math* :warn-on-boxed)

(definterface IMetadataManager
  (^void registerNewChunk [^java.util.Map roots, ^long chunk-idx, ^long max-rows-per-block])
  (^java.util.SortedSet knownChunks [])
  (^java.util.concurrent.CompletableFuture withMetadata [^long chunkIdx, ^java.util.function.BiFunction f]))

(definterface IMetadataIndices
  (^Long columnIndex [^String columnName])
  (^Long blockIndex [^String column-name, ^long blockIdx])
  (^long blockCount []))

(defrecord ChunkMatch [^long chunk-idx, ^RoaringBitmap block-idxs])

(defn type->field-name [^ArrowType arrow-type]
  (-> (Types/getMinorTypeForArrowType arrow-type)
      (.toString)
      (.toLowerCase)))

(def ^:private type-meta-fields
  (for [arrow-type (sort-by t/arrow-type->type-id (keys t/arrow-type->vector-type))]
    (t/->field (type->field-name arrow-type) arrow-type true)))

(def ^org.apache.arrow.vector.types.pojo.Schema metadata-schema
  (Schema. [(t/->field "column" (.getType Types$MinorType/VARCHAR) false)

            (t/->field "count" (.getType Types$MinorType/BIGINT) false)
            (t/->field "min-row-id" (.getType Types$MinorType/BIGINT) false)
            (t/->field "max-row-id" (.getType Types$MinorType/BIGINT) false)

            (apply t/->field "min"
                   (.getType Types$MinorType/STRUCT) false
                   type-meta-fields)
            (apply t/->field "max"
                   (.getType Types$MinorType/STRUCT) false
                   type-meta-fields)
            (t/->field "bloom" (.getType Types$MinorType/VARBINARY) false)

            (t/->field "blocks" (ArrowType$List.) false
                       (t/->field "block-meta" (.getType Types$MinorType/STRUCT) true
                                  (apply t/->field "min"
                                         (.getType Types$MinorType/STRUCT) false
                                         type-meta-fields)
                                  (apply t/->field "max"
                                         (.getType Types$MinorType/STRUCT) false
                                         type-meta-fields)
                                  (t/->field "bloom" (.getType Types$MinorType/VARBINARY) false)))]))

(defn- ->metadata-obj-key [chunk-idx]
  (format "metadata-%08x.arrow" chunk-idx))

(defn ->chunk-obj-key [chunk-idx column-name]
  (format "chunk-%08x-%s.arrow" chunk-idx column-name))

(defn- obj-key->chunk-idx [obj-key]
  (some-> (second (re-matches #"metadata-(\p{XDigit}{8}).arrow" obj-key))
          (Long/parseLong 16)))

(defn- write-min-max [^FieldVector field-vec,
                      ^StructVector min-meta-vec, ^StructVector max-meta-vec,
                      ^long meta-idx]
  (when (pos? (.getValueCount field-vec))
    (let [arrow-type (.getType (.getField field-vec))
          type-name (type->field-name arrow-type)
          vec-comparator (expr.comp/->comparator arrow-type)

          min-vec (.getChild min-meta-vec type-name)
          max-vec (.getChild max-meta-vec type-name)]

      (.setIndexDefined min-meta-vec meta-idx)
      (.setIndexDefined max-meta-vec meta-idx)

      (dotimes [field-vec-idx (.getValueCount field-vec)]
        (when (or (.isNull min-vec meta-idx)
                  (and (not (.isNull field-vec field-vec-idx))
                       (neg? (.compareIdx vec-comparator field-vec field-vec-idx min-vec meta-idx))))
          (.copyFromSafe min-vec field-vec-idx meta-idx field-vec))

        (when (or (.isNull max-vec meta-idx)
                  (and (not (.isNull field-vec field-vec-idx))
                       (pos? (.compareIdx vec-comparator field-vec field-vec-idx max-vec meta-idx))))
          (.copyFromSafe max-vec field-vec-idx meta-idx field-vec))))))

(defn write-meta [^VectorSchemaRoot metadata-root, live-roots, ^long chunk-idx, ^long max-rows-per-block]
  (let [col-count (count live-roots)

        ^VarCharVector column-name-vec (.getVector metadata-root "column")
        ^BigIntVector count-vec (.getVector metadata-root "count")
        ^BigIntVector min-row-id-vec (.getVector metadata-root "min-row-id")
        ^BigIntVector max-row-id-vec (.getVector metadata-root "max-row-id")

        ^StructVector min-vec (.getVector metadata-root "min")
        ^StructVector max-vec (.getVector metadata-root "max")
        ^VarBinaryVector bloom-vec (.getVector metadata-root "bloom")

        ^ListVector blocks-vec (.getVector metadata-root "blocks")
        ^StructVector blocks-data-vec (.getDataVector blocks-vec)
        ^StructVector blocks-min-vec (.getChild blocks-data-vec "min")
        ^StructVector blocks-max-vec (.getChild blocks-data-vec "max")
        ^VarBinaryVector blocks-bloom-vec (.getChild blocks-data-vec "bloom")]

    (.setRowCount metadata-root col-count)

    (dorun
     (->> (sort-by key live-roots)
          (map-indexed
           (fn [^long column-idx [^String col-name, ^VectorSchemaRoot live-root]]
             (let [row-count (.getRowCount live-root)
                   ^DenseUnionVector column-vec (.getVector live-root col-name)
                   ^BigIntVector row-id-vec (.getVector live-root "_row-id")]
               (.setSafe column-name-vec column-idx (Text. col-name))
               (.setSafe count-vec column-idx row-count)
               (.setSafe min-row-id-vec column-idx (.get row-id-vec 0))
               (.setSafe max-row-id-vec column-idx (.get row-id-vec (dec row-count)))

               (.setIndexDefined min-vec column-idx)
               (.setIndexDefined max-vec column-idx)

               (with-open [^IChunkCursor slices (blocks/->slices live-root chunk-idx max-rows-per-block)]
                 (let [start-block-idx (.startNewValue blocks-vec column-idx)

                       ^long end-block-idx
                       (loop [block-idx start-block-idx]
                         (if (.tryAdvance slices
                                          (reify Consumer
                                            (accept [_ sliced-root]
                                              (let [^VectorSchemaRoot sliced-root sliced-root
                                                    ^DenseUnionVector column-vec (.getVector sliced-root col-name)]
                                                (.setValueCount blocks-data-vec (inc block-idx))

                                                (.setValueCount column-vec (.getRowCount sliced-root))

                                                (when (pos? (.getRowCount sliced-root))
                                                  (.setIndexDefined blocks-data-vec block-idx)

                                                  (doseq [child-vec (.getChildrenFromFields column-vec)]
                                                    (write-min-max child-vec blocks-min-vec blocks-max-vec block-idx)

                                                    (write-min-max child-vec min-vec max-vec column-idx))

                                                  (bloom/write-bloom blocks-bloom-vec block-idx column-vec))))))
                           (recur (inc block-idx))
                           block-idx))]

                   (.endValue blocks-vec column-idx (- end-block-idx start-block-idx))))

               (bloom/write-bloom bloom-vec column-idx column-vec))))))

    ;; we set row-count twice in this function
    ;; - at the beginning to allocate memory
    ;; - at the end to finalise the offset vectors of the variable width vectors
    ;; there may be a more idiomatic way to achieve this
    (.setRowCount metadata-root col-count)))

(deftype MetadataIndices [^Map col-idx-cache
                          ^long col-count
                          ^VarCharVector column-name-vec
                          ^ListVector blocks-vec]
  IMetadataIndices
  (columnIndex [_ col-name]
    (.computeIfAbsent col-idx-cache col-name
                      (reify Function
                        (apply [_ field-name]
                          (loop [idx 0]
                            (when (< idx col-count)
                              (if (= col-name (t/get-object column-name-vec idx))
                                idx
                                (recur (inc idx)))))))))

  (blockIndex [this col-name block-idx]
    (when-let [col-idx (.columnIndex this col-name)]
      (let [block-idx (+ (.getElementStartIndex blocks-vec col-idx) block-idx)]
        (when (< block-idx (.getElementEndIndex blocks-vec col-idx))
          (when-not (.isNull (.getDataVector blocks-vec) block-idx)
            block-idx)))))

  (blockCount [this]
    (let [tx-id-idx (.columnIndex this "_tx-id")]
      (- (.getElementEndIndex blocks-vec tx-id-idx)
         (.getElementStartIndex blocks-vec tx-id-idx)))))

(defn ->metadata-idxs ^core2.metadata.IMetadataIndices [^VectorSchemaRoot metadata-root]
  (MetadataIndices. (HashMap.)
                    (.getRowCount metadata-root)
                    (.getVector metadata-root "column")
                    (.getVector metadata-root "blocks")))

(defn with-metadata [^IMetadataManager metadata-mgr, ^long chunk-idx, f]
  (.withMetadata metadata-mgr chunk-idx (util/->jbifn f)))

(defn with-latest-metadata [^IMetadataManager metadata-mgr, f]
  (if-let [chunk-idx (last (.knownChunks metadata-mgr))]
    (with-metadata metadata-mgr chunk-idx f)
    (CompletableFuture/completedFuture nil)))

(defn latest-tx [_chunk-idx ^VectorSchemaRoot metadata-root]
  (let [metadata-idxs (->metadata-idxs metadata-root)
        ^StructVector max-vec (.getVector metadata-root "max")]
    (tx/->TransactionInstant (-> max-vec
                                 ^BigIntVector
                                 (.getChild (type->field-name (.getType Types$MinorType/BIGINT)))
                                 (.get (.columnIndex metadata-idxs "_tx-id")))
                             (Date. (-> max-vec
                                        ^TimeStampMilliVector
                                        (.getChild (type->field-name (.getType Types$MinorType/TIMESTAMPMILLI)))
                                        (.get (.columnIndex metadata-idxs "_tx-time")))))))

(defn latest-row-id [_chunk-idx ^VectorSchemaRoot metadata-root]
  (let [metadata-idxs (->metadata-idxs metadata-root)]
    (.get ^BigIntVector (.getVector metadata-root "max-row-id")
          (.columnIndex metadata-idxs "_tx-id"))))

(defn matching-chunks [^IMetadataManager metadata-mgr, ^Watermark watermark, metadata-pred]
  (->> (for [^long chunk-idx (.knownChunks metadata-mgr)
             :while (or (nil? watermark) (< chunk-idx (.chunk-idx watermark)))]
         (with-metadata metadata-mgr chunk-idx metadata-pred))
       vec
       (into [] (keep deref))))

(deftype MetadataManager [^BufferAllocator allocator
                          ^ObjectStore object-store
                          ^IBufferPool buffer-pool
                          ^SortedSet known-chunks]
  IMetadataManager
  (registerNewChunk [_ live-roots chunk-idx max-rows-per-block]
    (let [metadata-buf (with-open [metadata-root (VectorSchemaRoot/create metadata-schema allocator)]
                         (write-meta metadata-root live-roots chunk-idx max-rows-per-block)

                         (util/root->arrow-ipc-byte-buffer metadata-root :file))]

      @(.putObject object-store (->metadata-obj-key chunk-idx) metadata-buf)

      (.add known-chunks chunk-idx)))

  (withMetadata [_ chunk-idx f]
    (-> (.getBuffer buffer-pool (->metadata-obj-key chunk-idx))
        (util/then-apply
          (fn [^ArrowBuf metadata-buffer]
            (assert metadata-buffer)

            (when metadata-buffer
              (let [res (promise)]
                (try
                  (with-open [chunk (util/->chunks metadata-buffer)]
                    (.tryAdvance chunk
                                 (reify Consumer
                                   (accept [_ metadata-root]
                                     (deliver res (.apply f chunk-idx metadata-root))))))

                  (assert (realized? res))
                  @res

                  (finally
                    (.close metadata-buffer)))))))))

  (knownChunks [_] known-chunks)

  Closeable
  (close [_]
    (.clear known-chunks)))

(defn ->metadata-manager {::sys/deps {:allocator :core2/allocator
                                      :object-store :core2/object-store
                                      :buffer-pool :core2/buffer-pool}}
  [{:keys [allocator ^ObjectStore object-store buffer-pool]}]
  (MetadataManager. allocator object-store buffer-pool
                    (ConcurrentSkipListSet. ^List (keep obj-key->chunk-idx (.listObjects object-store "metadata-")))))
