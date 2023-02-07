(ns core2.metadata
  (:require [core2.api :as c2]
            [core2.blocks :as blocks]
            [core2.bloom :as bloom]
            core2.buffer-pool
            [core2.expression.comparator :as expr.comp]
            core2.object-store
            [core2.types :as t]
            [core2.util :as util]
            [core2.vector.indirect :as iv]
            [core2.vector.writer :as vw]
            [juxt.clojars-mirrors.integrant.core :as ig])
  (:import [clojure.lang MapEntry]
           core2.buffer_pool.IBufferPool
           core2.ICursor
           core2.object_store.ObjectStore
           java.io.Closeable
           (java.util ArrayList HashMap Map NavigableMap TreeMap)
           (java.util.concurrent CompletableFuture)
           (java.util.function Consumer Function)
           (org.apache.arrow.memory ArrowBuf BufferAllocator)
           (org.apache.arrow.vector BigIntVector BitVector IntVector ValueVector VarBinaryVector VarCharVector VectorSchemaRoot)
           (org.apache.arrow.vector.complex DenseUnionVector ListVector StructVector)
           (org.apache.arrow.vector.types Types$MinorType)
           (org.apache.arrow.vector.types.pojo ArrowType$Bool Field FieldType Schema)
           org.apache.arrow.vector.util.Text
           org.roaringbitmap.RoaringBitmap))

(set! *unchecked-math* :warn-on-boxed)

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface IMetadataManager
  (^void registerNewChunk [^java.util.Map roots, ^long chunk-idx])
  (^java.util.NavigableMap chunksMetadata [])
  (^java.util.concurrent.CompletableFuture withMetadata [^long chunkIdx, ^String tableName, ^java.util.function.BiFunction f])
  (columnType [^String tableName, ^String colName]))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface IMetadataIndices
  (^java.util.Set columnNames [])
  (^Long columnIndex [^String columnName])
  (^Long blockIndex [^String column-name, ^int blockIdx])
  (^long blockCount []))

(defrecord ChunkMatch [^long chunk-idx, ^RoaringBitmap block-idxs])

(defn- with-single-root [^IBufferPool buffer-pool, obj-key, f]
  (-> (.getBuffer buffer-pool obj-key)
      (util/then-apply
        (fn [^ArrowBuf buffer]
          (assert buffer)

          (when buffer
            (let [res (promise)]
              (try
                (with-open [chunk (util/->chunks buffer)]
                  (.tryAdvance chunk
                               (reify Consumer
                                 (accept [_ vsr]
                                   (deliver res (f vsr))))))

                (assert (realized? res))
                @res

                (finally
                  (.close buffer)))))))))

(defn- ->table-metadata-obj-key [chunk-idx table-name]
  (format "chunk-%s/%s/metadata.arrow" (util/->lex-hex-string chunk-idx) table-name))

(defn ->chunk-obj-key [chunk-idx table-name column-name]
  (format "chunk-%s/%s/content-%s.arrow" (util/->lex-hex-string chunk-idx) table-name column-name))

(defn- obj-key->chunk-idx [obj-key]
  (some-> (second (re-matches #"chunk-metadata/(\p{XDigit}+).arrow" obj-key))
          (util/<-lex-hex-string)))

(defn- ->chunk-metadata-obj-key [chunk-idx]
  (format "chunk-metadata/%s.arrow" (util/->lex-hex-string chunk-idx)))

(def ^org.apache.arrow.vector.types.pojo.Schema chunk-metadata-schema
  (Schema. [(t/col-type->field 'table :utf8)
            (t/col-type->field 'min-row-id :i64)
            (t/col-type->field 'max-row-id :i64)
            (t/col-type->field 'col-types '[:list [:struct {col-name :utf8, col-type [:extension-type :c2/clj-form :utf8 ""]}]])]))

(defn- live-roots->chunk-metadata [^Map live-roots]
  (vec (for [[table-name ^Map live-roots] live-roots
             :let [^VectorSchemaRoot id-root (.get live-roots "id")
                   ^BigIntVector row-id-vec (.getVector id-root "_row-id")
                   row-count (.getRowCount id-root)]
             :when (pos? row-count)]
         {:table table-name
          :min-row-id (.get row-id-vec 0)
          :max-row-id (.get row-id-vec (dec row-count))
          :col-types (->> (for [[^String col-name, ^VectorSchemaRoot col-root] live-roots]
                            (MapEntry/create col-name
                                             (t/field->col-type (.getField (.getVector col-root col-name)))))
                          (into {}))})))

(defn- write-chunk-metadata ^java.nio.ByteBuffer [^BufferAllocator allocator, chunk-meta]
  (let [table-count (count chunk-meta)]
    (with-open [vsr (VectorSchemaRoot/create chunk-metadata-schema allocator)]
      (let [table-writer (vw/vec->writer (.getVector vsr "table"))
            min-row-id-writer (vw/vec->writer (.getVector vsr "min-row-id"))
            max-row-id-writer (vw/vec->writer (.getVector vsr "max-row-id"))

            col-types-writer (.asList (vw/vec->writer (.getVector vsr "col-types")))
            col-types-data-writer (.asStruct (.getDataWriter col-types-writer))
            col-name-writer (.writerForName col-types-data-writer "col-name")
            col-type-writer (.writerForName col-types-data-writer "col-type")]

        (doseq [{:keys [table min-row-id max-row-id col-types]} (->> chunk-meta (sort-by :table))]
          (doto table-writer (.startValue) (->> (t/write-value! (name table))) (.endValue))
          (doto min-row-id-writer (.startValue) (->> (t/write-value! min-row-id)) (.endValue))
          (doto max-row-id-writer (.startValue) (->> (t/write-value! max-row-id)) (.endValue))

          (.startValue col-types-writer)
          (doseq [[col-name col-type] (->> col-types (sort-by key))]
            (.startValue col-types-data-writer)
            (doto col-name-writer (->> (t/write-value! (name col-name))))
            (doto col-type-writer (->> (t/write-value! (c2/->ClojureForm col-type))))
            (.endValue col-types-data-writer))
          (.endValue col-types-writer)))

      (.setRowCount vsr table-count)

      (util/root->arrow-ipc-byte-buffer vsr :file))))

(defn- merge-col-types [col-types new-chunk-metadata]
  (reduce (fn [col-types {:keys [table], new-col-types :col-types}]
            (update col-types table
                    (fn [col-types new-col-types]
                      (merge-with t/merge-col-types col-types new-col-types))
                    new-col-types))
          col-types
          new-chunk-metadata))

(defn- read-chunk-metadata [^VectorSchemaRoot cm-root]
  (let [table-vec (.getVector cm-root "table")
        ^ListVector col-types-vec (.getVector cm-root "col-types")
        ^StructVector col-types-data-vec (.getDataVector col-types-vec)
        col-name-vec (.getChild col-types-data-vec "col-name")
        col-type-vec (.getChild col-types-data-vec "col-type")]
    (vec
     (for [idx (range (.getRowCount cm-root))]
       {:table (t/get-object table-vec idx)
        :col-types (let [start-idx (.getElementStartIndex col-types-vec idx)
                         end-idx (.getElementEndIndex col-types-vec idx)]
                     (->> (for [col-type-idx (range start-idx (- end-idx start-idx))]
                            (MapEntry/create (t/get-object col-name-vec col-type-idx)
                                             (:form (t/get-object col-type-vec col-type-idx))))
                          (into {})))}))))

(defn- load-chunks-metadata ^java.util.Map [{:keys [buffer-pool]} cm-obj-keys]
  (->> cm-obj-keys
       (mapv (fn [cm-obj-key]
               (doto (with-single-root buffer-pool cm-obj-key
                       (fn [cm-root]
                         (MapEntry/create (obj-key->chunk-idx cm-obj-key)
                                          (read-chunk-metadata cm-root))))
                 deref)))
       (mapv deref)
       (into {})))

(def ^org.apache.arrow.vector.types.pojo.Schema table-metadata-schema
  (Schema. [(t/col-type->field 'column :utf8)
            (t/col-type->field 'block-idx [:union #{:i32 :null}])

            (t/->field "root-column" t/struct-type true
                       ;; here because they're only easily accessible for non-nested columns.
                       ;; and we happen to need a marker for root columns anyway.
                       (t/col-type->field "min-row-id" [:union #{:null :i64}])
                       (t/col-type->field "max-row-id" [:union #{:null :i64}])
                       (t/col-type->field "row-id-bloom" [:union #{:null :varbinary}]))

            (t/col-type->field 'count :i64)

            (t/->field "types" t/struct-type true)

            (t/col-type->field 'bloom [:union #{:null :varbinary}])]))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface ContentMetadataWriter
  (^void writeContentMetadata [^int typesVecIdx]))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
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

(defmethod col-type->type-metadata :extension-type [[type-head xname storage-type xdata]]
  {"type-head" (name type-head)
   "xname" (str (symbol xname))
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

(doseq [type-head #{:int :float :utf8 :varbinary :timestamp-tz :timestamp-local :date :interval :time-local}]
  (defmethod type->metadata-writer type-head [_write-col-meta! metadata-root col-type] (->min-max-type-handler metadata-root col-type)))

(defmulti extension-type->metadata-writer
  (fn [_write-col-meta! _metadata-root col-type] (second col-type))
  :default ::default)

(defmethod extension-type->metadata-writer ::default [_write-col-meta! metadata-root col-type]
  (->bool-type-handler metadata-root col-type))

(defmethod extension-type->metadata-writer :c2/clj-keyword [_write-col-meta! metadata-root col-type]
  (->min-max-type-handler metadata-root col-type))

;; TODO anything we can do for extension types? I suppose we should probably special case our own...
;; they'll get included in the bloom filter, which is sufficient for UUIDs/keywords/etc.
(defmethod type->metadata-writer :extension-type [write-col-meta! metadata-root col-type]
  ;; (clojure.pprint/pprint col-type)
  (extension-type->metadata-writer write-col-meta! metadata-root col-type))

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

(defmethod col-type->type-metadata :time-local [[type-head time-unit]]
  {"type-head" (name type-head), "time-unit" (name time-unit)})

(defmethod type-metadata->col-type :time-local [type-metadata]
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
        ^VarBinaryVector row-id-bloom-vec (.getChild root-col-vec "row-id-bloom")

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
                    (.setSafe max-row-id-vec meta-idx (.get row-id-vec (dec value-count)))
                    (bloom/write-bloom row-id-bloom-vec meta-idx row-id-vec)))))

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

      (doseq [[^String col-name, ^VectorSchemaRoot live-root] (sort-by key live-roots)]
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

(deftype MetadataManager [^BufferAllocator allocator
                          ^ObjectStore object-store
                          ^IBufferPool buffer-pool
                          ^long max-rows-per-block
                          ^NavigableMap chunks-metadata
                          ^:unsynchronised-mutable ^Map col-types]
  IMetadataManager
  (registerNewChunk [this live-roots chunk-idx]
    @(->> (for [[table-name live-roots] live-roots]
            (let [metadata-buf (with-open [metadata-root (VectorSchemaRoot/create table-metadata-schema allocator)]
                                 (write-meta metadata-root live-roots chunk-idx max-rows-per-block)
                                 (util/root->arrow-ipc-byte-buffer metadata-root :file))]

              (.putObject object-store (->table-metadata-obj-key chunk-idx table-name) metadata-buf)))
          (into-array CompletableFuture)
          (CompletableFuture/allOf))

    (let [new-chunk-metadata (live-roots->chunk-metadata live-roots)]
      @(.putObject object-store (->chunk-metadata-obj-key chunk-idx) (write-chunk-metadata allocator new-chunk-metadata))
      (set! (.col-types this) (merge-col-types col-types new-chunk-metadata))
      (.put chunks-metadata chunk-idx new-chunk-metadata)))

  (withMetadata [_ chunk-idx table-name f]
    (with-single-root buffer-pool (->table-metadata-obj-key chunk-idx table-name)
      (fn [metadata-root]
        (.apply f chunk-idx metadata-root))))

  (chunksMetadata [_] chunks-metadata)

  (columnType [_ table-name col-name] (get-in col-types [table-name col-name]))

  Closeable
  (close [_]
    (.clear chunks-metadata)))

(defn with-metadata [^IMetadataManager metadata-mgr, ^long chunk-idx, ^String table-name, f]
  (.withMetadata metadata-mgr chunk-idx table-name (util/->jbifn f)))

(defn matching-chunks [^IMetadataManager metadata-mgr, table-name, metadata-pred]
  (->> (for [[^long chunk-idx, chunk-metadata] (.chunksMetadata metadata-mgr)
             :when (some #(= table-name %) (map :table chunk-metadata))]
         (with-metadata metadata-mgr chunk-idx table-name metadata-pred))
       vec
       (into [] (keep deref))))

(defn row-id->cols [^IMetadataManager metadata-mgr, ^String table-name, ^long row-id]
  ;; TODO cache which chunk each row-id is in.
  (let [bloom-hash (bloom/literal-hashes (.allocator ^MetadataManager metadata-mgr) row-id)]
    (->> (matching-chunks metadata-mgr table-name
                          (fn [^long chunk-idx ^VectorSchemaRoot metadata-root]
                            (let [cols (ArrayList.)
                                  ^VarCharVector col-name-vec (.getVector metadata-root "column")
                                  ^IntVector block-idx-vec (.getVector metadata-root "block-idx")
                                  ^StructVector root-col-vec (.getVector metadata-root "root-column")
                                  ^BigIntVector min-row-id-vec (.getChild root-col-vec "min-row-id" BigIntVector)
                                  ^BigIntVector max-row-id-vec (.getChild root-col-vec "max-row-id" BigIntVector)
                                  ^VarBinaryVector row-id-bloom-vec (.getChild root-col-vec "row-id-bloom" VarBinaryVector)]
                              (dotimes [idx (.getRowCount metadata-root)]
                                (when (and (not (.isNull block-idx-vec idx))
                                           (not (.isNull root-col-vec idx))
                                           (>= row-id (.get min-row-id-vec idx))
                                           (<= row-id (.get max-row-id-vec idx))
                                           (bloom/bloom-contains? row-id-bloom-vec idx bloom-hash))
                                  (.add cols {:col-name (str (.getObject col-name-vec idx))
                                              :block-idx (.get block-idx-vec idx)})))
                              (when-not (.isEmpty cols)
                                {:chunk-idx chunk-idx, :cols cols}))))
         (remove nil?)
         first)))

(defmethod ig/prep-key ::metadata-manager [_ opts]
  (merge {:allocator (ig/ref :core2/allocator)
          :object-store (ig/ref :core2/object-store)
          :buffer-pool (ig/ref :core2.buffer-pool/buffer-pool)
          :row-counts (ig/ref :core2/row-counts)}
         opts))

(defmethod ig/init-key ::metadata-manager [_ {:keys [allocator ^ObjectStore object-store buffer-pool]
                                              {:keys [max-rows-per-block]} :row-counts
                                              :as deps}]
  (let [cm-obj-keys (.listObjects object-store "chunk-metadata/")
        chunks-metadata (load-chunks-metadata deps cm-obj-keys)]
    (MetadataManager. allocator object-store buffer-pool max-rows-per-block
                      (TreeMap. chunks-metadata)
                      (->> (vals chunks-metadata) (reduce merge-col-types {})))))

(defmethod ig/halt-key! ::metadata-manager [_ mgr]
  (util/try-close mgr))
