(ns core2.metadata
  (:require [core2.blocks :as blocks]
            [core2.bloom :as bloom]
            core2.buffer-pool
            [core2.expression.comparator :as expr.comp]
            core2.object-store
            core2.watermark
            [core2.types :as t]
            [core2.util :as util]
            [core2.vector.indirect :as iv]
            [juxt.clojars-mirrors.integrant.core :as ig])
  (:import core2.buffer_pool.IBufferPool
           core2.object_store.ObjectStore
           core2.watermark.Watermark
           core2.ICursor
           java.io.Closeable
           (java.util HashMap List SortedSet)
           (java.util.concurrent ConcurrentSkipListSet)
           (java.util.function BiFunction Consumer Function)
           (org.apache.arrow.memory ArrowBuf BufferAllocator)
           (org.apache.arrow.vector BigIntVector BitVector IntVector ValueVector VarBinaryVector VarCharVector VectorSchemaRoot)
           (org.apache.arrow.vector.complex DenseUnionVector ListVector StructVector)
           (org.apache.arrow.vector.types Types$MinorType)
           (org.apache.arrow.vector.types.pojo ArrowType$Bool Field FieldType Schema)
           org.apache.arrow.vector.util.Text
           org.roaringbitmap.RoaringBitmap))

(set! *unchecked-math* :warn-on-boxed)

#_{:clj-kondo/ignore [:unused-binding]}
(definterface IMetadataManager
  (^void registerNewChunk [^java.util.Map roots, ^long chunk-idx])
  (^java.util.SortedSet knownChunks [])
  (^java.util.concurrent.CompletableFuture withMetadata [^long chunkIdx, ^java.util.function.BiFunction f])
  (columnType [^String colName]))

#_{:clj-kondo/ignore [:unused-binding]}
(definterface IMetadataIndices
  (^java.util.Set columnNames [])
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
  (Schema. [(t/col-type->field "column" :utf8)
            (t/col-type->field "block-idx" [:union #{:i32 :null}])

            (t/->field "root-column" t/struct-type true
                       ;; here because they're only easily accessible for non-nested columns.
                       ;; and we happen to need a marker for root columns anyway.
                       (t/col-type->field "min-row-id" [:union #{:null :i64}])
                       (t/col-type->field "max-row-id" [:union #{:null :i64}]))

            (t/col-type->field "count" :i64)

            (t/->field "types" t/struct-type true)

            (t/col-type->field "bloom" [:union #{:null :varbinary}])]))

(definterface ContentMetadataWriter
  (^void writeContentMetadata [^int typesVecIdx]))

(definterface NestedMetadataWriter
  (appendNestedMetadata ^core2.metadata.ContentMetadataWriter [^org.apache.arrow.vector.ValueVector contentVector]))

#_{:clj-kondo/ignore [:unused-binding]}
(defmulti type->metadata-writer
  (fn [write-col-meta! types-vec col-type] (t/col-type-head col-type))
  :hierarchy #'t/col-type-hierarchy)

(defmulti col-type->type-metadata t/col-type-head, :hierarchy #'t/col-type-hierarchy)

(defmethod col-type->type-metadata :default [col-type]
  {"type-head" (name (t/col-type-head col-type))})

#_{:clj-kondo/ignore [:unused-binding]}
(defmulti type-metadata->col-type
  (fn [type-metadata]
    (get type-metadata "type-head"))
  :hierarchy #'t/col-type-hierarchy)

(defmethod type-metadata->col-type :default [type-metadata] (get type-metadata "type-head"))

(defn- add-struct-child ^org.apache.arrow.vector.ValueVector [^StructVector parent, ^Field field]
  (doto (.addOrGet parent (.getName field) (.getFieldType field) ValueVector)
    (.initializeChildrenFromFields (.getChildren field))
    (.setValueCount (.getValueCount parent))))

(defn- ->bool-type-handler [^VectorSchemaRoot metadata-root, col-type]
  (let [^StructVector types-vec (.getVector metadata-root "types")
        ^BitVector bit-vec (add-struct-child types-vec
                                             (Field. (t/col-type->field-name col-type)
                                                     (FieldType. true ArrowType$Bool/INSTANCE nil (col-type->type-metadata col-type))
                                                     []))]
    (reify NestedMetadataWriter
      (appendNestedMetadata [_ _content-vec]
        (reify ContentMetadataWriter
          (writeContentMetadata [_ types-vec-idx]
            (.setSafeToOne bit-vec types-vec-idx)))))))

(defmethod type->metadata-writer :null [_write-col-meta! metadata-root col-type] (->bool-type-handler metadata-root col-type))
(defmethod type->metadata-writer :bool [_write-col-meta! metadata-root col-type] (->bool-type-handler metadata-root col-type))

;; TODO anything we can do for extension types? I suppose we should probably special case our own...
;; they'll get included in the bloom filter, which is sufficient for UUIDs/keywords/etc.
(defmethod type->metadata-writer :extension-type [_write-col-meta! metadata-root col-type] (->bool-type-handler metadata-root col-type))

(defmethod col-type->type-metadata :extension-type [[type-head xname storage-type xdata]]
  {"type-head" (name type-head)
   "xname" (name xname)
   "xstorage-type" (pr-str (col-type->type-metadata storage-type))
   "xdata" xdata})

(defmethod type-metadata->col-type :extension-type [type-metadata]
  [:extension-type
   (keyword (get type-metadata "xname"))
   (type-metadata->col-type (-> (read-string (get type-metadata "xstorage-type"))
                                (update "type-head" keyword)))
   (get type-metadata "xdata")])

(defn- ->min-max-type-handler [^VectorSchemaRoot metadata-root, col-type]
  (let [^StructVector types-vec (.getVector metadata-root "types")
        ^StructVector struct-vec (add-struct-child types-vec
                                                   (Field. (t/col-type->field-name col-type)
                                                           (FieldType. true t/struct-type nil (col-type->type-metadata col-type))
                                                           [(t/col-type->field "min" [:union #{:null col-type}])
                                                            (t/col-type->field "max" [:union #{:null col-type}])]))
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

(doseq [type-head #{:int :float :utf8 :varbinary :timestamp-tz :timestamp-local :date :interval :time}]
  (defmethod type->metadata-writer type-head [_write-col-meta! metadata-root col-type] (->min-max-type-handler metadata-root col-type)))

(defmethod col-type->type-metadata :fixed-size-binary [[type-head byte-width]]
  {"type-head" (name type-head), "byte-width" (str byte-width)})

(defmethod type-metadata->col-type :fixed-size-binary [type-metadata]
  [(get type-metadata "type-head"), (Long/parseLong (get type-metadata "byte-width"))])

(defmethod col-type->type-metadata :timestamp-tz [[type-head time-unit tz]]
  {"type-head" (name type-head), "time-unit" (name time-unit), "tz" tz})

(defmethod type-metadata->col-type :timestamp-tz [type-metadata]
  [(get type-metadata "type-head"), (keyword (get type-metadata "time-unit")), (get type-metadata "tz")])

(defmethod col-type->type-metadata :timestamp-local [[type-head time-unit]]
  {"type-head" (name type-head), "time-unit" (name time-unit)})

(defmethod type-metadata->col-type :timestamp-local [type-metadata]
  [(get type-metadata "type-head"), (keyword (get type-metadata "time-unit"))])

(defmethod col-type->type-metadata :date [[type-head date-unit]]
  {"type-head" (name type-head), "date-unit" (name date-unit)})

(defmethod type-metadata->col-type :date [type-metadata]
  [(get type-metadata "type-head"), (keyword (get type-metadata "date-unit"))])

(defmethod col-type->type-metadata :time [[type-head time-unit]]
  {"type-head" (name type-head), "time-unit" (name time-unit)})

(defmethod type-metadata->col-type :time [type-metadata]
  [(get type-metadata "type-head"), (keyword (get type-metadata "time-unit"))])

(defmethod col-type->type-metadata :interval [[type-head interval-unit]]
  {"type-head" (name type-head), "interval-unit" (name interval-unit)})

(defmethod type-metadata->col-type :interval [type-metadata]
  [(get type-metadata "type-head"), (keyword (get type-metadata "interval-unit"))])

(defmethod type->metadata-writer :list [write-col-meta! ^VectorSchemaRoot metadata-root col-type]
  (let [^StructVector types-vec (.getVector metadata-root "types")
        ^IntVector list-meta-vec (add-struct-child types-vec
                                                   (Field. (t/col-type->field-name col-type)
                                                           (FieldType. true (.getType Types$MinorType/INT) nil (col-type->type-metadata col-type))
                                                           []))]
    (reify NestedMetadataWriter
      (appendNestedMetadata [_ content-vec]
        (let [^ListVector content-vec content-vec]
          (write-col-meta! (.getDataVector content-vec))
          (let [data-meta-idx (dec (.getRowCount metadata-root))]
            (reify ContentMetadataWriter
              (writeContentMetadata [_ types-vec-idx]
                (.setSafe list-meta-vec types-vec-idx data-meta-idx)))))))))

(defmethod col-type->type-metadata :struct [[type-head children]]
  {"type-head" (name type-head)
   "key-set" (pr-str (into (sorted-set) (keys children)))})

(defmethod type->metadata-writer :struct [write-col-meta! ^VectorSchemaRoot metadata-root, col-type]
  (let [^StructVector types-vec (.getVector metadata-root "types")
        ^ListVector struct-meta-vec (add-struct-child types-vec
                                                      (Field. (str (t/col-type->field-name col-type) "-" (count (seq types-vec)))
                                                              (FieldType. true t/list-type nil (col-type->type-metadata col-type))
                                                              [(t/col-type->field "$data" [:union #{:null :i32}])]))
        ^IntVector nested-col-idxs-vec (.getDataVector struct-meta-vec)]
    (reify NestedMetadataWriter
      (appendNestedMetadata [_ content-vec]
        (let [^StructVector content-vec content-vec
              sub-cols (vec content-vec)
              sub-col-count (count sub-cols)
              sub-col-idxs (int-array sub-col-count)]

          (dotimes [n sub-col-count]
            (write-col-meta! (nth sub-cols n))
            (aset sub-col-idxs n (dec (.getRowCount metadata-root))))

          (reify ContentMetadataWriter
            (writeContentMetadata [_ types-vec-idx]
              (let [start-idx (.startNewValue struct-meta-vec types-vec-idx)]
                (dotimes [n sub-col-count]
                  (.setSafe nested-col-idxs-vec (+ start-idx n) (aget sub-col-idxs n)))
                (.endValue struct-meta-vec types-vec-idx sub-col-count)))))))))

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
                                                                  (.computeIfAbsent type-metadata-writers (t/field->col-type (.getField values-vec))
                                                                                    (reify Function
                                                                                      (apply [_ col-type]
                                                                                        (type->metadata-writer write-col-meta! metadata-root col-type))))]

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
        ^IntVector block-idx-vec (.getVector metadata-root "block-idx")
        root-col-vec (.getVector metadata-root "root-column")]
    (dotimes [meta-idx (.getRowCount metadata-root)]
      (let [col-name (str (.getObject column-name-vec meta-idx))]
        (when-not (.isNull root-col-vec meta-idx)
          (if (.isNull block-idx-vec meta-idx)
            (.put col-idx-cache col-name meta-idx)
            (.put block-idx-cache [col-name (.get block-idx-vec meta-idx)] meta-idx)))))

    (let [block-count (->> (keys block-idx-cache)
                           (map second)
                           ^long (apply max)
                           inc)]
      (reify IMetadataIndices
        IMetadataIndices
        (columnNames [_] (set (keys col-idx-cache)))
        (columnIndex [_ col-name] (get col-idx-cache col-name))
        (blockIndex [_ col-name block-idx] (get block-idx-cache [col-name block-idx]))
        (blockCount [_] block-count)))))

(defn- with-metadata* [^IBufferPool buffer-pool, ^long chunk-idx, ^BiFunction f]
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

(defn- ->column-types [^VectorSchemaRoot metadata-root]
  (let [^VarCharVector col-name-vec (.getVector metadata-root "column")
        ^StructVector types-vec (.getVector metadata-root "types")
        meta-idxs (->metadata-idxs metadata-root)]
    (letfn [(->col-type [^long col-idx]
              (let [type-vecs (->> (seq types-vec)
                                   (remove (fn [^ValueVector type-vec]
                                             (.isNull type-vec col-idx))))]
                (letfn [(->col-type* [^ValueVector type-vec]
                          (let [type-metadata (-> (into {} (.getMetadata (.getField type-vec)))
                                                  (update "type-head" keyword))
                                type-head (get type-metadata "type-head")]

                            (case type-head
                              :list
                              [:list (->col-type (.get ^IntVector type-vec col-idx))]

                              :struct
                              [:struct (let [^ListVector type-vec type-vec
                                             ^IntVector type-vec-data (.getDataVector type-vec)]
                                         (->> (for [type-vec-data-idx (range (.getElementStartIndex type-vec col-idx)
                                                                             (.getElementEndIndex type-vec col-idx))
                                                    :let [col-idx (.get type-vec-data type-vec-data-idx)
                                                          col-name (symbol (str (.getObject col-name-vec col-idx)))]]
                                                [col-name (->col-type col-idx)])
                                              (into {})))]

                              (type-metadata->col-type type-metadata))))]

                  (if (= 1 (count type-vecs))
                    (->col-type* (first type-vecs))

                    [:union (->> type-vecs (into #{} (map ->col-type*)))]))))]

      (->> (for [col-name (.columnNames meta-idxs)]
             [col-name (->col-type (.columnIndex meta-idxs col-name))])
           (into {})))))

(defn- merge-column-types [column-types new-column-types]
  (merge-with t/merge-col-types column-types new-column-types))

(defn- load-column-types [^IBufferPool buffer-pool, known-chunks]
  (let [cf-futs (doall (for [chunk-idx known-chunks]
                         (with-metadata* buffer-pool chunk-idx
                           (reify BiFunction
                             (apply [_ _chunk-idx metadata-root]
                               (->column-types metadata-root))))))]
    (->> cf-futs
         (transduce (map deref)
                    (completing
                     (fn
                       ([] {})
                       ([cfs new-cfs] (merge-column-types cfs new-cfs))))))))

(deftype MetadataManager [^BufferAllocator allocator
                          ^ObjectStore object-store
                          ^IBufferPool buffer-pool
                          ^long max-rows-per-block
                          ^SortedSet known-chunks
                          ^:unsynchronized-mutable column-types]
  IMetadataManager
  (registerNewChunk [this live-roots chunk-idx]
    (let [metadata-buf (with-open [metadata-root (VectorSchemaRoot/create metadata-schema allocator)]
                         (write-meta metadata-root live-roots chunk-idx max-rows-per-block)
                         (set! (.column-types this) (merge-column-types column-types (->column-types metadata-root)))
                         (util/root->arrow-ipc-byte-buffer metadata-root :file))]

      @(.putObject object-store (->metadata-obj-key chunk-idx) metadata-buf)

      (.add known-chunks chunk-idx)))

  (withMetadata [_ chunk-idx f] (with-metadata* buffer-pool chunk-idx f))

  (knownChunks [_] known-chunks)

  (columnType [_ col-name] (get column-types col-name))

  Closeable
  (close [_]
    (.clear known-chunks)))

(defn with-metadata [^IMetadataManager metadata-mgr, ^long chunk-idx, f]
  (.withMetadata metadata-mgr chunk-idx (util/->jbifn f)))

(defn matching-chunks [^IMetadataManager metadata-mgr, ^Watermark watermark, metadata-pred]
  (->> (for [^long chunk-idx (.knownChunks metadata-mgr)
             :while (or (nil? watermark) (< chunk-idx (.chunk-idx watermark)))]
         (with-metadata metadata-mgr chunk-idx metadata-pred))
       vec
       (into [] (keep deref))))

(defmethod ig/prep-key ::metadata-manager [_ opts]
  (merge {:allocator (ig/ref :core2/allocator)
          :object-store (ig/ref :core2/object-store)
          :buffer-pool (ig/ref :core2.buffer-pool/buffer-pool)
          :row-counts (ig/ref :core2/row-counts)}
         opts))

(defmethod ig/init-key ::metadata-manager [_ {:keys [allocator ^ObjectStore object-store buffer-pool]
                                              {:keys [max-rows-per-block]} :row-counts}]
  (let [known-chunks (ConcurrentSkipListSet. ^List (keep obj-key->chunk-idx (.listObjects object-store "metadata-")))
        column-types (load-column-types buffer-pool known-chunks)]
    (MetadataManager. allocator object-store buffer-pool max-rows-per-block known-chunks column-types)))

(defmethod ig/halt-key! ::metadata-manager [_ ^MetadataManager mgr]
  (.close mgr))
