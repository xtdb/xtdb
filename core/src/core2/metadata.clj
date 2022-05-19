(ns core2.metadata
  (:require [core2.blocks :as blocks]
            [core2.bloom :as bloom]
            core2.buffer-pool
            [core2.expression.comparator :as expr.comp]
            core2.object-store
            core2.tx
            [core2.types :as t]
            [core2.util :as util]
            [core2.vector.indirect :as iv]
            [juxt.clojars-mirrors.integrant.core :as ig])
  (:import core2.buffer_pool.IBufferPool
           core2.object_store.ObjectStore
           core2.tx.Watermark
           core2.ICursor
           java.io.Closeable
           (java.util HashMap List SortedSet)
           java.util.concurrent.ConcurrentSkipListSet
           (java.util.function Consumer Function)
           (org.apache.arrow.memory ArrowBuf BufferAllocator)
           (org.apache.arrow.vector BigIntVector BitVector IntVector ValueVector VarBinaryVector VarCharVector VectorSchemaRoot)
           (org.apache.arrow.vector.complex DenseUnionVector StructVector)
           (org.apache.arrow.vector.types.pojo ArrowType ArrowType$Binary ArrowType$Bool ArrowType$Date ArrowType$ExtensionType ArrowType$FloatingPoint ArrowType$Int ArrowType$List
                                               ArrowType$Null ArrowType$Struct ArrowType$Timestamp ArrowType$Utf8 Field FieldType Schema)
           org.apache.arrow.vector.types.Types
           org.apache.arrow.vector.util.Text
           org.roaringbitmap.RoaringBitmap))

(set! *unchecked-math* :warn-on-boxed)

#_{:clj-kondo/ignore [:unused-binding]}
(definterface IMetadataManager
  (^void registerNewChunk [^java.util.Map roots, ^long chunk-idx, ^long max-rows-per-block])
  (^java.util.SortedSet knownChunks [])
  (^java.util.concurrent.CompletableFuture withMetadata [^long chunkIdx, ^java.util.function.BiFunction f]))

#_{:clj-kondo/ignore [:unused-binding]}
(definterface IMetadataIndices
  (^Long columnIndex [^String columnName])
  (^Long blockIndex [^String column-name, ^int blockIdx])
  (^long blockCount []))

(defrecord ChunkMatch [^long chunk-idx, ^RoaringBitmap block-idxs])

(defn- ->metadata-obj-key [chunk-idx]
  (format "metadata-%016x.arrow" chunk-idx))

(defn ->chunk-obj-key [chunk-idx column-name]
  (format "chunk-%016x-%s.arrow" chunk-idx column-name))

(defn- obj-key->chunk-idx [obj-key]
  (some-> (second (re-matches #"metadata-(\p{XDigit}{16}).arrow" obj-key))
          (Long/parseLong 16)))

(def ^org.apache.arrow.vector.types.pojo.Schema metadata-schema
  (Schema. [(t/->field "column" t/varchar-type false)
            (t/->field "block-idx" t/int-type true)

            (t/->field "root-column" t/struct-type true
                       ;; here because they're only easily accessible for non-nested columns.
                       ;; and we happen to need a marker for root columns anyway.
                       (t/->field "min-row-id" t/bigint-type true)
                       (t/->field "max-row-id" t/bigint-type true))

            (t/->field "count" t/bigint-type false)

            (t/->field "types" t/struct-type true)

            (t/->field "bloom" t/varbinary-type true)]))

(definterface ContentMetadataWriter
  (^void writeContentMetadata [^int typesVecIdx]))

(definterface NestedMetadataWriter
  (appendNestedMetadata ^core2.metadata.ContentMetadataWriter [^org.apache.arrow.vector.ValueVector contentVector]))

#_{:clj-kondo/ignore [:unused-binding]}
(defmulti type->metadata-writer (fn [types-vec arrow-type] (class arrow-type)))
(defmulti type->type-metadata class)

(defmethod type->type-metadata :default [^ArrowType arrow-type]
  {"minor-type" (str (Types/getMinorTypeForArrowType arrow-type))})

(defn- add-struct-child ^org.apache.arrow.vector.ValueVector [^StructVector parent, ^Field field]
  (doto (.addOrGet parent (.getName field) (.getFieldType field) ValueVector)
    (.initializeChildrenFromFields (.getChildren field))
    (.setValueCount (.getValueCount parent))))

(defn- ->bool-type-handler [^VectorSchemaRoot metadata-root, ^ArrowType arrow-type]
  (let [^StructVector types-vec (.getVector metadata-root "types")
        ^BitVector bit-vec (add-struct-child types-vec
                                             (Field. (t/type->field-name arrow-type)
                                                     (FieldType. true t/bool-type nil (type->type-metadata arrow-type))
                                                     []))]
    (reify NestedMetadataWriter
      (appendNestedMetadata [_ _content-vec]
        (reify ContentMetadataWriter
          (writeContentMetadata [_ types-vec-idx]
            (.setSafeToOne bit-vec types-vec-idx)))))))

(defmethod type->metadata-writer ArrowType$Null [metadata-root arrow-type] (->bool-type-handler metadata-root arrow-type))
(defmethod type->metadata-writer ArrowType$Bool [metadata-root arrow-type] (->bool-type-handler metadata-root arrow-type))

;; TODO these need to be more than bool flags when we properly support nested types
(defmethod type->metadata-writer ArrowType$List [metadata-root arrow-type] (->bool-type-handler metadata-root arrow-type))
(defmethod type->metadata-writer ArrowType$Struct [metadata-root arrow-type] (->bool-type-handler metadata-root arrow-type))
(defmethod type->metadata-writer ArrowType$ExtensionType [metadata-root arrow-type] (->bool-type-handler metadata-root arrow-type))

(defn- ->min-max-type-handler [^VectorSchemaRoot metadata-root, ^ArrowType arrow-type]
  (let [^StructVector types-vec (.getVector metadata-root "types")
        ^StructVector struct-vec (add-struct-child types-vec
                                                   (Field. (t/type->field-name arrow-type)
                                                           (FieldType. true t/struct-type nil (type->type-metadata arrow-type))
                                                           [(t/->field "min" arrow-type true)
                                                            (t/->field "max" arrow-type true)]))
        min-vec (.getChild struct-vec "min")
        max-vec (.getChild struct-vec "max")]
    (reify NestedMetadataWriter
      (appendNestedMetadata [_ content-vec]
        (reify ContentMetadataWriter
          (writeContentMetadata [_ types-vec-idx]
            (.setIndexDefined struct-vec types-vec-idx)

            (let [min-comparator (expr.comp/->comparator (iv/->direct-vec content-vec) (iv/->direct-vec min-vec) :nulls-last)
                  max-comparator (expr.comp/->comparator (iv/->direct-vec content-vec) (iv/->direct-vec max-vec) :nulls-last)]

              (dotimes [values-idx (.getValueCount content-vec)]
                (when (or (.isNull min-vec types-vec-idx)
                          (and (not (.isNull content-vec values-idx))
                               (neg? (.applyAsInt min-comparator values-idx types-vec-idx))))
                  (.copyFromSafe min-vec values-idx types-vec-idx content-vec))

                (when (or (.isNull max-vec types-vec-idx)
                          (and (not (.isNull content-vec values-idx))
                               (pos? (.applyAsInt max-comparator values-idx types-vec-idx))))
                  (.copyFromSafe max-vec values-idx types-vec-idx content-vec))))))))))

(defmethod type->metadata-writer ArrowType$Int [metadata-root arrow-type] (->min-max-type-handler metadata-root arrow-type))
(defmethod type->metadata-writer ArrowType$FloatingPoint [metadata-root arrow-type] (->min-max-type-handler metadata-root arrow-type))
(defmethod type->metadata-writer ArrowType$Utf8 [metadata-root arrow-type] (->min-max-type-handler metadata-root arrow-type))
(defmethod type->metadata-writer ArrowType$Binary [metadata-root arrow-type] (->min-max-type-handler metadata-root arrow-type))
(defmethod type->metadata-writer ArrowType$Timestamp [metadata-root arrow-type] (->min-max-type-handler metadata-root arrow-type))
(defmethod type->metadata-writer ArrowType$Date [metadata-root arrow-type] (->min-max-type-handler metadata-root arrow-type))

(defn write-meta [^VectorSchemaRoot metadata-root, live-roots, ^long chunk-idx, ^long max-rows-per-block]
  (let [^VarCharVector column-name-vec (.getVector metadata-root "column")

        ^IntVector block-idx-vec (.getVector metadata-root "block-idx")

        ^StructVector root-col-vec (.getVector metadata-root "root-column")
        ^BigIntVector min-row-id-vec (.getChild root-col-vec "min-row-id")
        ^BigIntVector max-row-id-vec (.getChild root-col-vec "max-row-id")

        ^BigIntVector count-vec (.getVector metadata-root "count")

        ^StructVector types-vec (.getVector metadata-root "types")

        ^VarBinaryVector bloom-vec (.getVector metadata-root "bloom")

        type-metadata-writers (HashMap.)]

    (letfn [(write-root-col-row-ids! [^BigIntVector row-id-vec]
              (let [value-count (.getValueCount row-id-vec)]
                (when-not (zero? value-count)
                  (let [meta-idx (dec (.getRowCount metadata-root))]
                    (.setIndexDefined root-col-vec meta-idx)
                    (.setSafe min-row-id-vec meta-idx (.get row-id-vec 0))
                    (.setSafe max-row-id-vec meta-idx (.get row-id-vec (dec value-count)))))))

            (write-col-meta! [^DenseUnionVector content-vec]
              (let [content-writers (->> (seq content-vec)
                                         (into [] (keep (fn [^ValueVector values-vec]
                                                          (when-not (zero? (.getValueCount values-vec))
                                                            (let [^NestedMetadataWriter nested-meta-writer
                                                                  (.computeIfAbsent type-metadata-writers (.getType (.getField values-vec))
                                                                                    (reify Function
                                                                                      (apply [_ arrow-type]
                                                                                        (type->metadata-writer metadata-root arrow-type))))]

                                                              (.appendNestedMetadata nested-meta-writer values-vec)))))))

                    meta-idx (.getRowCount metadata-root)]
                (.setRowCount metadata-root (inc meta-idx))
                (.setSafe column-name-vec meta-idx (Text. (.getName content-vec)))
                (.setSafe count-vec meta-idx (.getValueCount content-vec))
                (bloom/write-bloom bloom-vec meta-idx content-vec)

                (.setIndexDefined types-vec meta-idx)

                (doseq [^ContentMetadataWriter content-writer content-writers]
                  (.writeContentMetadata content-writer meta-idx))))]

      (doseq [[^String col-name, ^VectorSchemaRoot live-root] live-roots]
        (write-col-meta! (.getVector live-root col-name))
        (write-root-col-row-ids! (.getVector live-root "_row-id"))

        (let [block-row-counts (blocks/row-id-aligned-blocks live-root chunk-idx max-rows-per-block)]
          (with-open [^ICursor slices (blocks/->slices live-root block-row-counts)]
            (loop [block-idx 0]
              (when (.tryAdvance slices
                                 (reify Consumer
                                   (accept [_ live-slice]
                                     (let [^VectorSchemaRoot live-slice live-slice]
                                       (when-not (zero? (.getRowCount live-slice))
                                         (write-col-meta! (.getVector live-slice col-name))
                                         (write-root-col-row-ids! (.getVector live-slice "_row-id"))
                                         (.setSafe block-idx-vec (dec (.getRowCount metadata-root)) block-idx))))))
                (recur (inc block-idx))))))))

    (.syncSchema metadata-root)))

(defn ->metadata-idxs ^core2.metadata.IMetadataIndices [^VectorSchemaRoot metadata-root]
  (let [col-idx-cache (HashMap.)
        block-idx-cache (HashMap.)
        ^VarCharVector column-name-vec (.getVector metadata-root "column")
        ^IntVector block-idx-vec (.getVector metadata-root "block-idx")]
    (dotimes [meta-idx (.getRowCount metadata-root)]
      (let [col-name (str (.getObject column-name-vec meta-idx))]
        (if (.isNull block-idx-vec meta-idx)
          (.put col-idx-cache col-name meta-idx)
          (.put block-idx-cache [col-name (.get block-idx-vec meta-idx)] meta-idx))))

    (let [block-count (->> (keys block-idx-cache)
                           (map second)
                           ^long (apply max)
                           inc)]
      (reify IMetadataIndices
        IMetadataIndices
        (columnIndex [_ col-name] (get col-idx-cache col-name))
        (blockIndex [_ col-name block-idx] (get block-idx-cache [col-name block-idx]))
        (blockCount [_] block-count)))))

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
