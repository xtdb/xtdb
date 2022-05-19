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

            (t/->field "count" t/bigint-type false)
            (t/->field "min-row-id" t/bigint-type true)
            (t/->field "max-row-id" t/bigint-type true)

            (t/->field "types" t/struct-type true)

            (t/->field "bloom" t/varbinary-type true)]))

(definterface MetadataTypeHandler
  (^void writeMetadataForType [^int typesVecIdx, ^org.apache.arrow.vector.ValueVector valueVector]))

#_{:clj-kondo/ignore [:unused-binding]}
(defmulti ->metadata-type-handler (fn [types-vec arrow-type] (class arrow-type)))
(defmulti type->type-metadata class)

(defmethod type->type-metadata :default [^ArrowType arrow-type]
  {"minor-type" (str (Types/getMinorTypeForArrowType arrow-type))})

(defn- add-struct-child ^org.apache.arrow.vector.ValueVector [^StructVector parent, ^Field field]
  (doto (.addOrGet parent (.getName field) (.getFieldType field) ValueVector)
    (.initializeChildrenFromFields (.getChildren field))
    (.setValueCount (.getValueCount parent))))

(defn- ->bool-type-handler [^StructVector types-vec, ^ArrowType arrow-type]
  (let [^BitVector bit-vec (add-struct-child types-vec
                                             (Field. (t/type->field-name arrow-type)
                                                     (FieldType. true t/bool-type nil (type->type-metadata arrow-type))
                                                     []))]
    (reify MetadataTypeHandler
      (writeMetadataForType [_ types-vec-idx _values-vec]
        (.setSafeToOne bit-vec types-vec-idx)))))

(defmethod ->metadata-type-handler ArrowType$Null [types-vec arrow-type] (->bool-type-handler types-vec arrow-type))
(defmethod ->metadata-type-handler ArrowType$Bool [types-vec arrow-type] (->bool-type-handler types-vec arrow-type))

;; TODO these need to be more than bool flags when we properly support nested types
(defmethod ->metadata-type-handler ArrowType$List [types-vec arrow-type] (->bool-type-handler types-vec arrow-type))
(defmethod ->metadata-type-handler ArrowType$Struct [types-vec arrow-type] (->bool-type-handler types-vec arrow-type))
(defmethod ->metadata-type-handler ArrowType$ExtensionType [types-vec arrow-type] (->bool-type-handler types-vec arrow-type))

(defn- ->min-max-type-handler [^StructVector types-vec, ^ArrowType arrow-type]
  (let [^StructVector struct-vec (add-struct-child types-vec
                                                   (Field. (t/type->field-name arrow-type)
                                                           (FieldType. true t/struct-type nil (type->type-metadata arrow-type))
                                                           [(t/->field "min" arrow-type true)
                                                            (t/->field "max" arrow-type true)]))
        min-vec (.getChild struct-vec "min")
        max-vec (.getChild struct-vec "max")]
    (reify MetadataTypeHandler
      (writeMetadataForType [_ types-vec-idx values-vec]
        (.setIndexDefined struct-vec types-vec-idx)

        (let [min-comparator (expr.comp/->comparator (iv/->direct-vec values-vec) (iv/->direct-vec min-vec) :nulls-last)
              max-comparator (expr.comp/->comparator (iv/->direct-vec values-vec) (iv/->direct-vec max-vec) :nulls-last)]

          (dotimes [values-idx (.getValueCount values-vec)]
            (when (or (.isNull min-vec types-vec-idx)
                      (and (not (.isNull values-vec values-idx))
                           (neg? (.applyAsInt min-comparator values-idx types-vec-idx))))
              (.copyFromSafe min-vec values-idx types-vec-idx values-vec))

            (when (or (.isNull max-vec types-vec-idx)
                      (and (not (.isNull values-vec values-idx))
                           (pos? (.applyAsInt max-comparator values-idx types-vec-idx))))
              (.copyFromSafe max-vec values-idx types-vec-idx values-vec))))))))

(defmethod ->metadata-type-handler ArrowType$Int [types-vec arrow-type] (->min-max-type-handler types-vec arrow-type))
(defmethod ->metadata-type-handler ArrowType$FloatingPoint [types-vec arrow-type] (->min-max-type-handler types-vec arrow-type))
(defmethod ->metadata-type-handler ArrowType$Utf8 [types-vec arrow-type] (->min-max-type-handler types-vec arrow-type))
(defmethod ->metadata-type-handler ArrowType$Binary [types-vec arrow-type] (->min-max-type-handler types-vec arrow-type))
(defmethod ->metadata-type-handler ArrowType$Timestamp [types-vec arrow-type] (->min-max-type-handler types-vec arrow-type))
(defmethod ->metadata-type-handler ArrowType$Date [types-vec arrow-type] (->min-max-type-handler types-vec arrow-type))

(defn write-meta [^VectorSchemaRoot metadata-root, live-roots, ^long chunk-idx, ^long max-rows-per-block]
  (let [col-count (count live-roots)

        ^VarCharVector column-name-vec (.getVector metadata-root "column")

        ^IntVector block-idx-vec (.getVector metadata-root "block-idx")

        ^BigIntVector count-vec (.getVector metadata-root "count")
        ^BigIntVector min-row-id-vec (.getVector metadata-root "min-row-id")
        ^BigIntVector max-row-id-vec (.getVector metadata-root "max-row-id")

        ^StructVector types-vec (.getVector metadata-root "types")

        ^VarBinaryVector bloom-vec (.getVector metadata-root "bloom")

        metadata-type-handlers (HashMap.)]

    (.setRowCount metadata-root col-count)

    (letfn [(write-block-meta! [^String col-name ^VectorSchemaRoot live-slice ^long meta-idx]
              (let [row-count (.getRowCount live-slice)
                    ^DenseUnionVector column-vec (.getVector live-slice col-name)
                    ^BigIntVector row-id-vec (.getVector live-slice "_row-id")]

                (.setSafe column-name-vec meta-idx (Text. col-name))
                (.setSafe count-vec meta-idx (.getRowCount live-slice))

                (when (pos? (.getRowCount live-slice))
                  (.setSafe min-row-id-vec meta-idx (.get row-id-vec 0))
                  (.setSafe max-row-id-vec meta-idx (.get row-id-vec (dec row-count)))

                  (.setIndexDefined types-vec meta-idx)

                  (doseq [^ValueVector values-vec (seq column-vec)]
                    (let [value-count (.getValueCount values-vec)]
                      (when-not (zero? value-count)
                        (let [^MetadataTypeHandler type-handler
                              (.computeIfAbsent metadata-type-handlers (.getType (.getField values-vec))
                                                (reify Function
                                                  (apply [_ arrow-type]
                                                    (->metadata-type-handler types-vec arrow-type))))]

                          (.setIndexDefined types-vec meta-idx)
                          (.writeMetadataForType type-handler meta-idx values-vec)

                          (bloom/write-bloom bloom-vec meta-idx column-vec))))))))]
      (->> live-roots
           (reduce
            (fn [^long meta-idx [^String col-name, ^VectorSchemaRoot live-root]]
              (let [block-row-counts (blocks/row-id-aligned-blocks live-root chunk-idx max-rows-per-block)
                    block-count (count block-row-counts)]

                (.setRowCount metadata-root (+ meta-idx (inc block-count)))

                (write-block-meta! col-name live-root meta-idx)

                (let [meta-idx (inc meta-idx)]
                  (with-open [^ICursor slices (blocks/->slices live-root block-row-counts)]
                    (loop [block-idx 0]
                      (let [meta-idx (+ meta-idx block-idx)]
                        (if (.tryAdvance slices
                                           (reify Consumer
                                             (accept [_ live-slice]
                                               (write-block-meta! col-name live-slice meta-idx))))
                          (do
                            (.setSafe block-idx-vec meta-idx block-idx)
                            (recur (inc block-idx)))
                          meta-idx)))))))
            0)))

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
