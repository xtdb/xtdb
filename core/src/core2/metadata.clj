(ns core2.metadata
  (:require [core2.api :as c2]
            [core2.blocks :as blocks]
            [core2.bloom :as bloom]
            core2.buffer-pool
            [core2.expression.comparator :as expr.comp]
            core2.tx
            [core2.types :as t]
            [core2.util :as util]
            [juxt.clojars-mirrors.integrant.core :as ig])
  (:import core2.buffer_pool.IBufferPool
           core2.ICursor
           core2.object_store.ObjectStore
           core2.tx.Watermark
           java.io.Closeable
           [java.util Date HashMap List Map SortedSet]
           [java.util.concurrent CompletableFuture ConcurrentSkipListSet]
           [java.util.function Consumer Function]
           [org.apache.arrow.memory ArrowBuf BufferAllocator]
           [org.apache.arrow.vector BigIntVector TimeStampMilliVector ValueVector VarBinaryVector VarCharVector VectorSchemaRoot]
           [org.apache.arrow.vector.complex DenseUnionVector ListVector StructVector]
           [org.apache.arrow.vector.types.pojo ArrowType ArrowType$List FieldType Schema]
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
  (name (t/<-arrow-type arrow-type)))

(def ^org.apache.arrow.vector.types.pojo.Schema metadata-schema
  (Schema. [(t/->field "column" (t/->arrow-type :varchar) false)

            (t/->field "count" (t/->arrow-type :bigint) false)
            (t/->field "min-row-id" (t/->arrow-type :bigint) false)
            (t/->field "max-row-id" (t/->arrow-type :bigint) false)

            (t/->field "min" t/struct-type false)
            (t/->field "max" t/struct-type false)
            (t/->field "bloom" (t/->arrow-type :varbinary) false)

            (t/->field "blocks" (ArrowType$List.) false
                       (t/->field "block-meta" t/struct-type true
                                  (t/->field "min" t/struct-type false)
                                  (t/->field "max" t/struct-type false)
                                  (t/->field "bloom" (t/->arrow-type :varbinary) false)))]))

(defn- ->metadata-obj-key [chunk-idx]
  (format "metadata-%016x.arrow" chunk-idx))

(defn ->chunk-obj-key [chunk-idx column-name]
  (format "chunk-%016x-%s.arrow" chunk-idx column-name))

(defn- obj-key->chunk-idx [obj-key]
  (some-> (second (re-matches #"metadata-(\p{XDigit}{16}).arrow" obj-key))
          (Long/parseLong 16)))

(defn- get-or-add-child ^org.apache.arrow.vector.ValueVector [^StructVector parent, ^ArrowType arrow-type]
  (let [field-name (type->field-name arrow-type)]
    (or (.getChild parent field-name)
        (doto (.addOrGet parent field-name (FieldType/nullable arrow-type) ValueVector)
          (.setValueCount (.getValueCount parent))))))

(defn- write-min-max [^ValueVector field-vec,
                      ^StructVector min-meta-vec, ^StructVector max-meta-vec,
                      ^long meta-idx]
  (when (pos? (.getValueCount field-vec))
    (let [arrow-type (.getType (.getField field-vec))

          min-vec (get-or-add-child min-meta-vec arrow-type)
          max-vec (get-or-add-child max-meta-vec arrow-type)

          col-comparator (expr.comp/->comparator arrow-type)]

      (.setIndexDefined min-meta-vec meta-idx)
      (.setIndexDefined max-meta-vec meta-idx)

      (dotimes [field-idx (.getValueCount field-vec)]
        (when (or (.isNull min-vec meta-idx)
                  (and (not (.isNull field-vec field-idx))
                       (neg? (.compareIdx col-comparator field-vec field-idx min-vec meta-idx))))
          (.copyFromSafe min-vec field-idx meta-idx field-vec))

        (when (or (.isNull max-vec meta-idx)
                  (and (not (.isNull field-vec field-idx))
                       (pos? (.compareIdx col-comparator field-vec field-idx max-vec meta-idx))))
          (.copyFromSafe max-vec field-idx meta-idx field-vec))))))

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

               (let [row-counts (blocks/row-id-aligned-blocks live-root chunk-idx max-rows-per-block)]
                 (with-open [^ICursor slices (blocks/->slices live-root row-counts)]
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

                     (.endValue blocks-vec column-idx (- end-block-idx start-block-idx)))))

               (bloom/write-bloom bloom-vec column-idx column-vec))))))

    ;; we set row-count twice in this function
    ;; - at the beginning to allocate memory
    ;; - at the end to finalise the offset vectors of the variable width vectors
    ;; there may be a more idiomatic way to achieve this
    (.setRowCount metadata-root col-count)
    (.syncSchema metadata-root)))

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
    (let [id-idx (.columnIndex this "_id")]
      (- (.getElementEndIndex blocks-vec id-idx)
         (.getElementStartIndex blocks-vec id-idx)))))

(defn ->metadata-idxs ^core2.metadata.IMetadataIndices [^VectorSchemaRoot metadata-root]
  (MetadataIndices. (HashMap.)
                    (.getRowCount metadata-root)
                    (.getVector metadata-root "column")
                    (.getVector metadata-root "blocks")))

(defn with-metadata [^IMetadataManager metadata-mgr, ^long chunk-idx, f]
  (.withMetadata metadata-mgr chunk-idx (util/->jbifn f)))

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

(defmethod ig/prep-key ::metadata-manager [_ opts]
  (merge {:allocator (ig/ref :core2/allocator)
          :object-store (ig/ref :core2/object-store)
          :buffer-pool (ig/ref :core2.buffer-pool/buffer-pool)}
         opts))

(defmethod ig/init-key ::metadata-manager [_ {:keys [allocator ^ObjectStore object-store buffer-pool]}]
  (MetadataManager. allocator object-store buffer-pool
                    (ConcurrentSkipListSet. ^List (keep obj-key->chunk-idx (.listObjects object-store "metadata-")))))

(defmethod ig/halt-key! ::metadata-manager [_ ^MetadataManager mgr]
  (.close mgr))
