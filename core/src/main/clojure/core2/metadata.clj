(ns core2.metadata
  (:require [cognitect.transit :as transit]
            [core2.bloom :as bloom]
            core2.buffer-pool
            [core2.expression.comparator :as expr.comp]
            core2.object-store
            [core2.transit :as c2.transit]
            [core2.types :as types]
            [core2.util :as util]
            [core2.vector.indirect :as iv]
            [juxt.clojars-mirrors.integrant.core :as ig])
  (:import [clojure.lang MapEntry]
           core2.buffer_pool.IBufferPool
           core2.object_store.ObjectStore
           (core2.vector IIndirectVector)
           (java.io ByteArrayInputStream ByteArrayOutputStream)
           java.lang.AutoCloseable
           java.nio.ByteBuffer
           (java.util ArrayList HashMap HashSet Map NavigableMap TreeMap)
           (java.util.concurrent ConcurrentHashMap)
           java.util.concurrent.atomic.AtomicInteger
           (java.util.function BiFunction Consumer Function)
           java.util.stream.IntStream
           (org.apache.arrow.memory ArrowBuf BufferAllocator)
           (org.apache.arrow.vector BigIntVector BitVector IntVector ValueVector VarBinaryVector VarCharVector VectorSchemaRoot)
           (org.apache.arrow.vector.complex DenseUnionVector ListVector StructVector)
           (org.apache.arrow.vector.types Types$MinorType)
           (org.apache.arrow.vector.types.pojo ArrowType$Bool ArrowType$Union Field FieldType Schema)
           org.apache.arrow.vector.util.Text
           (org.roaringbitmap RoaringBitmap)))

(set! *unchecked-math* :warn-on-boxed)

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface ITableMetadata
  (^org.apache.arrow.vector.VectorSchemaRoot metadataRoot [])
  (^java.util.Set columnNames [])
  (^Long rowIndex [^String column-name, ^int blockIdx]
   "pass blockIdx = -1 for metadata about the whole chunk")
  (^long blockCount []))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface IColumnMetadataWriter
  (^void writeMetadata [^core2.vector.IIndirectRelation liveRel, ^int blockIdx]
   "blockIdx = -1 for metadata for the whole column"))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface ITableMetadataWriter
  (^core2.metadata.IColumnMetadataWriter columnMetadataWriter [^String colName])
  (^core2.metadata.ITableMetadata tableMetadata [])
  (^java.util.concurrent.CompletableFuture finishChunk [])
  (^void close []))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface IMetadataManager
  (^core2.metadata.ITableMetadataWriter openTableMetadataWriter [^String table-name, ^long chunk-idx])
  (^void finishChunk [^long chunkIdx, newChunkMetadata])
  (^java.util.NavigableMap chunksMetadata [])
  (^java.util.concurrent.CompletableFuture withMetadata [^long chunkIdx, ^String tableName, ^java.util.function.Function #_<ITableMetadata> f])
  (columnType [^String tableName, ^String colName]))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface IMetadataPredicate
  (^java.util.function.IntPredicate build [^core2.metadata.ITableMetadata tableMetadata]))

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

(defn- get-bytes ^java.util.concurrent.CompletableFuture #_<bytes> [^IBufferPool buffer-pool, obj-key]
  (-> (.getBuffer buffer-pool obj-key)
      (util/then-apply
        (fn [^ArrowBuf buffer]
          (assert buffer)

          (when buffer
            (try
              (let [bb (.nioBuffer buffer 0 (.capacity buffer))
                    ba (byte-array (.remaining bb))]
                (.get bb ba)
                ba)
              (finally
                (.close buffer))))))))

(defn- ->table-metadata-obj-key [chunk-idx table-name]
  (format "chunk-%s/%s/metadata.arrow" (util/->lex-hex-string chunk-idx) table-name))

(defn ->chunk-obj-key [chunk-idx table-name column-name]
  (format "chunk-%s/%s/content-%s.arrow" (util/->lex-hex-string chunk-idx) table-name column-name))

(defn- obj-key->chunk-idx [obj-key]
  (some-> (second (re-matches #"chunk-metadata/(\p{XDigit}+).transit.json" obj-key))
          (util/<-lex-hex-string)))

(defn- ->chunk-metadata-obj-key [chunk-idx]
  (format "chunk-metadata/%s.transit.json" (util/->lex-hex-string chunk-idx)))

(defn live-roots->chunk-metadata [^String table-name, ^Map live-roots]
  (when-let [^VectorSchemaRoot id-root (.get live-roots "id")]
    (when (pos? (.getRowCount id-root))
      (MapEntry/create table-name
                       {:col-types (->> (for [[^String col-name, ^VectorSchemaRoot col-root] live-roots]
                                          (MapEntry/create col-name
                                                           (types/field->col-type (.getField (.getVector col-root col-name)))))
                                        (into {}))}))))

(defn- write-chunk-metadata ^java.nio.ByteBuffer [chunk-meta]
  (with-open [os (ByteArrayOutputStream.)]
    (let [w (transit/writer os :json {:handlers c2.transit/tj-write-handlers})]
      (transit/write w chunk-meta))
    (ByteBuffer/wrap (.toByteArray os))))

(defn- merge-col-types [col-types {:keys [tables]}]
  (reduce (fn [col-types [table {new-col-types :col-types}]]
            (update col-types table
                    (fn [col-types new-col-types]
                      (merge-with types/merge-col-types col-types new-col-types))
                    new-col-types))
          col-types
          tables))

(defn- load-chunks-metadata ^java.util.NavigableMap [{:keys [buffer-pool ^ObjectStore object-store]}]
  (let [cm (TreeMap.)]
    (doseq [cm-obj-key (.listObjects object-store "chunk-metadata/")]
      (with-open [is (ByteArrayInputStream. @(get-bytes buffer-pool cm-obj-key))]
        (let [rdr (transit/reader is :json {:handlers c2.transit/tj-read-handlers})]
          (.put cm (obj-key->chunk-idx cm-obj-key) (transit/read rdr)))))
    cm))

(def ^org.apache.arrow.vector.types.pojo.Schema table-metadata-schema
  (Schema. [(types/col-type->field 'column :utf8)
            (types/col-type->field 'block-idx :i32) ; -1 for whole chunk

            (types/->field "root-column" types/struct-type true
                           ;; here because they're only easily accessible for non-nested columns.
                           ;; and we happen to need a marker for root columns anyway.
                           (types/col-type->field "min-row-id" [:union #{:null :i64}])
                           (types/col-type->field "max-row-id" [:union #{:null :i64}])
                           (types/col-type->field "row-id-bloom" [:union #{:null :varbinary}]))

            (types/col-type->field 'count :i64)

            (types/->field "types" types/struct-type true)

            (types/col-type->field 'bloom [:union #{:null :varbinary}])]))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface ContentMetadataWriter
  (^void writeContentMetadata [^int typesVecIdx]))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface NestedMetadataWriter
  (appendNestedMetadata ^core2.metadata.ContentMetadataWriter [^core2.vector.IIndirectVector contentCol]))

#_{:clj-kondo/ignore [:unused-binding]}
(defmulti type->metadata-writer
  (fn [write-col-meta! types-vec col-type] (types/col-type-head col-type))
  :hierarchy #'types/col-type-hierarchy)

(defmulti col-type->type-metadata types/col-type-head, :hierarchy #'types/col-type-hierarchy)

(defmethod col-type->type-metadata :default [col-type]
  {"type-head" (name (types/col-type-head col-type))})

#_{:clj-kondo/ignore [:unused-binding]}
(defmulti type-metadata->col-type
  (fn [type-metadata]
    (get type-metadata "type-head"))
  :hierarchy #'types/col-type-hierarchy)

(defmethod type-metadata->col-type :default [type-metadata] (get type-metadata "type-head"))

(defn- add-struct-child ^org.apache.arrow.vector.ValueVector [^StructVector parent, ^Field field]
  (doto (.addOrGet parent (.getName field) (.getFieldType field) ValueVector)
    (.initializeChildrenFromFields (.getChildren field))
    (.setValueCount (.getValueCount parent))))

(defn- ->bool-type-handler [^VectorSchemaRoot metadata-root, col-type]
  (let [^StructVector types-vec (.getVector metadata-root "types")
        ^BitVector bit-vec (add-struct-child types-vec
                                             (Field. (types/col-type->field-name col-type)
                                                     (FieldType. true ArrowType$Bool/INSTANCE nil (col-type->type-metadata col-type))
                                                     []))]
    (reify NestedMetadataWriter
      (appendNestedMetadata [_ _content-col]
        (reify ContentMetadataWriter
          (writeContentMetadata [_ types-vec-idx]
            (.setSafeToOne bit-vec types-vec-idx)))))))

(defmethod type->metadata-writer :null [_write-col-meta! metadata-root col-type] (->bool-type-handler metadata-root col-type))
(defmethod type->metadata-writer :bool [_write-col-meta! metadata-root col-type] (->bool-type-handler metadata-root col-type))

(defn- ->min-max-type-handler [^VectorSchemaRoot metadata-root, col-type]
  (let [^StructVector types-vec (.getVector metadata-root "types")
        ^StructVector struct-vec (add-struct-child types-vec
                                                   (Field. (types/col-type->field-name col-type)
                                                           (FieldType. true types/struct-type nil (col-type->type-metadata col-type))
                                                           [(types/col-type->field "min" [:union #{:null col-type}])
                                                            (types/col-type->field "max" [:union #{:null col-type}])]))
        min-vec (.getChild struct-vec "min")
        max-vec (.getChild struct-vec "max")]
    (reify NestedMetadataWriter
      (appendNestedMetadata [_ content-col]
        (let [content-vec (.getVector content-col)]
          (reify ContentMetadataWriter
            (writeContentMetadata [_ types-vec-idx]
              (.setIndexDefined struct-vec types-vec-idx)

              (let [min-comparator (expr.comp/->comparator content-col (iv/->direct-vec min-vec) :nulls-last)
                    max-comparator (expr.comp/->comparator content-col (iv/->direct-vec max-vec) :nulls-first)]

                (.setNull min-vec types-vec-idx)
                (.setNull max-vec types-vec-idx)
                (dotimes [values-idx (.getValueCount content-col)]
                  (let [values-vec-idx (.getIndex content-col values-idx)]
                    (when (or (.isNull min-vec types-vec-idx)
                              (and (not (.isNull content-vec values-vec-idx))
                                   (neg? (.applyAsInt min-comparator values-idx types-vec-idx))))
                      (.copyFromSafe min-vec values-vec-idx types-vec-idx content-vec))

                    (when (or (.isNull max-vec types-vec-idx)
                              (and (not (.isNull content-vec values-vec-idx))
                                   (pos? (.applyAsInt max-comparator values-idx types-vec-idx))))
                      (.copyFromSafe max-vec values-vec-idx types-vec-idx content-vec))))))))))))

(doseq [type-head #{:int :float :utf8 :varbinary :timestamp-tz :timestamp-local :date :interval :time-local}]
  (defmethod type->metadata-writer type-head [_write-col-meta! metadata-root col-type] (->min-max-type-handler metadata-root col-type)))

(defmethod type->metadata-writer :keyword [_write-col-meta! metadata-root col-type] (->min-max-type-handler metadata-root col-type))
(defmethod type->metadata-writer :uri [_write-col-meta! metadata-root col-type] (->min-max-type-handler metadata-root col-type))
(defmethod type->metadata-writer :uuid [_write-col-meta! metadata-root col-type] (->min-max-type-handler metadata-root col-type))
(defmethod type->metadata-writer :clj-form [_write-col-meta! metadata-root col-type] (->bool-type-handler metadata-root col-type))

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
                                                   (Field. (types/col-type->field-name col-type)
                                                           (FieldType. true (.getType Types$MinorType/INT) nil (col-type->type-metadata col-type))
                                                           []))]
    (reify NestedMetadataWriter
      (appendNestedMetadata [_ content-col]
        (let [list-rdr (.listReader content-col)
              ^ListVector content-vec (.getVector content-col)
              data-vec (.getDataVector content-vec)
              idxs (RoaringBitmap.)]
          (dotimes [idx (.getValueCount content-col)]
            (.add idxs
                  (.getElementStartIndex list-rdr idx)
                  (.getElementEndIndex list-rdr idx)))
          ;; HACK needs to be selected
          (write-col-meta! (iv/->indirect-vec data-vec (.toArray idxs)))
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
                                                      (Field. (str (types/col-type->field-name col-type) "-" (count (seq types-vec)))
                                                              (FieldType. true types/list-type nil (col-type->type-metadata col-type))
                                                              [(types/col-type->field "$data" [:union #{:null :i32}])]))
        ^IntVector nested-col-idxs-vec (.getDataVector struct-meta-vec)]
    (reify NestedMetadataWriter
      (appendNestedMetadata [_ content-col]
        (let [struct-rdr (.structReader content-col)
              struct-ks (vec (.structKeys struct-rdr))
              sub-col-count (count struct-ks)
              sub-col-idxs (int-array sub-col-count)]

          (dotimes [n sub-col-count]
            (write-col-meta! (.readerForKey struct-rdr (nth struct-ks n)))
            (aset sub-col-idxs n (dec (.getRowCount metadata-root))))

          (reify ContentMetadataWriter
            (writeContentMetadata [_ types-vec-idx]
              (let [start-idx (.startNewValue struct-meta-vec types-vec-idx)]
                (dotimes [n sub-col-count]
                  (.setSafe nested-col-idxs-vec (+ start-idx n) (aget sub-col-idxs n)))
                (.endValue struct-meta-vec types-vec-idx sub-col-count)))))))))

(defn ->table-metadata-idxs [^VectorSchemaRoot metadata-root]
  (let [block-idx-cache (HashMap.)
        row-count (.getRowCount metadata-root)
        ^VarCharVector column-name-vec (.getVector metadata-root "column")
        ^IntVector block-idx-vec (.getVector metadata-root "block-idx")
        root-col-vec (.getVector metadata-root "root-column")
        col-names (HashSet.)]
    (dotimes [meta-idx (.getRowCount metadata-root)]
      (let [col-name (str (.getObject column-name-vec meta-idx))]
        (.add col-names col-name)
        (when-not (.isNull root-col-vec meta-idx)
          (.put block-idx-cache [col-name (.get block-idx-vec meta-idx)] meta-idx))))

    {:col-names (into #{} col-names)
     :block-idx-cache block-idx-cache
     :block-count (loop [block-count 0, idx 0]
                    (cond
                      (>= idx row-count) (inc block-count)
                      :else (recur (max (.get block-idx-vec idx) block-count)
                                   (inc idx))))}))

(defn ->table-metadata ^core2.metadata.ITableMetadata [^VectorSchemaRoot vsr, {:keys [col-names block-idx-cache, block-count]}]
  (reify ITableMetadata
    (metadataRoot [_] vsr)
    (columnNames [_] col-names)
    (rowIndex [_ col-name block-idx] (get block-idx-cache [col-name block-idx]))
    (blockCount [_] block-count)))

(defn open-table-meta-writer [^BufferAllocator allocator, ^ObjectStore object-store, ^String table-name, ^long chunk-idx]
  (let [metadata-root (VectorSchemaRoot/create table-metadata-schema allocator)
        ^VarCharVector column-name-vec (.getVector metadata-root "column")

        ^IntVector block-idx-vec (.getVector metadata-root "block-idx")

        ^StructVector root-col-vec (.getVector metadata-root "root-column")
        ^BigIntVector min-row-id-vec (.getChild root-col-vec "min-row-id")
        ^BigIntVector max-row-id-vec (.getChild root-col-vec "max-row-id")
        ^VarBinaryVector row-id-bloom-vec (.getChild root-col-vec "row-id-bloom")

        ^BigIntVector count-vec (.getVector metadata-root "count")

        ^StructVector types-vec (.getVector metadata-root "types")

        ^VarBinaryVector bloom-vec (.getVector metadata-root "bloom")

        type-metadata-writers (HashMap.)

        col-names (HashSet.)
        block-idx-cache (HashMap.)
        !block-count (AtomicInteger. 0)]

    (letfn [(write-root-col-row-ids! [^IIndirectVector row-id-col]
              (let [value-count (.getValueCount row-id-col)
                    row-id-rdr (.monoReader row-id-col :i64)]
                (when-not (zero? value-count)
                  (let [meta-idx (dec (.getRowCount metadata-root))]
                    (.setIndexDefined root-col-vec meta-idx)
                    (.setSafe min-row-id-vec meta-idx (.readLong row-id-rdr 0))
                    (.setSafe max-row-id-vec meta-idx (.readLong row-id-rdr (dec value-count)))
                    (bloom/write-bloom row-id-bloom-vec meta-idx row-id-col)))))

            (write-col-meta! [^IIndirectVector content-col]
              (let [^DenseUnionVector content-vec (.getVector content-col)
                    ^ArrowType$Union duv-type (.getType (.getField content-vec))
                    content-writers (->> (.getTypeIds duv-type)
                                         (into [] (keep (fn [^long type-id]
                                                          (let [^ValueVector values-vec (.getVectorByType content-vec type-id)
                                                                sel (IntStream/builder)]

                                                            (dotimes [idx (.getValueCount content-col)]
                                                              (let [idx (.getIndex content-col idx)]
                                                                (when (= type-id (.getTypeId content-vec idx))
                                                                  (.add sel (.getOffset content-vec idx)))))

                                                            (let [values-col (iv/->indirect-vec values-vec (.toArray (.build sel)))]
                                                              (when-not (zero? (.getValueCount values-col))
                                                                (let [^NestedMetadataWriter nested-meta-writer
                                                                      (.computeIfAbsent type-metadata-writers (types/field->col-type (.getField values-vec))
                                                                                        (reify Function
                                                                                          (apply [_ col-type]
                                                                                            (type->metadata-writer write-col-meta! metadata-root col-type))))]

                                                                  (.appendNestedMetadata nested-meta-writer values-col)))))))))

                    meta-idx (.getRowCount metadata-root)]
                (.setSafe column-name-vec meta-idx (Text. (.getName content-col)))
                (.setSafe count-vec meta-idx (.getValueCount content-col))
                (bloom/write-bloom bloom-vec meta-idx content-col)

                (.setIndexDefined types-vec meta-idx)

                (doseq [^ContentMetadataWriter content-writer content-writers]
                  (.writeContentMetadata content-writer meta-idx))

                (.setRowCount metadata-root (inc meta-idx))))]

      (reify ITableMetadataWriter
        (columnMetadataWriter [_ col-name]
          (.add col-names col-name)

          (reify IColumnMetadataWriter
            (writeMetadata [_ live-rel block-idx]
              (when (not (zero? (.rowCount live-rel)))
                (write-col-meta! (.vectorForName live-rel col-name))
                (write-root-col-row-ids! (.vectorForName live-rel "_row-id"))
                (.setSafe block-idx-vec (dec (.getRowCount metadata-root)) block-idx)
                (when-not (neg? block-idx)
                  (.set !block-count block-idx))

                (.put block-idx-cache [col-name block-idx] (dec (.getRowCount metadata-root)))))))

        (tableMetadata [_]
          (->table-metadata metadata-root
                            {:col-names (into #{} col-names)
                             :block-idx-cache (into {} block-idx-cache)
                             :block-count (.get !block-count)}))

        (finishChunk [_]
          (.syncSchema metadata-root)

          (let [metadata-buf (util/root->arrow-ipc-byte-buffer metadata-root :file)]
            (.putObject object-store (->table-metadata-obj-key chunk-idx table-name) metadata-buf)))

        AutoCloseable
        (close [_]
          (.close metadata-root))))))

(deftype MetadataManager [^BufferAllocator allocator
                          ^ObjectStore object-store
                          ^IBufferPool buffer-pool
                          ^NavigableMap chunks-metadata
                          ^Map table-metadata-idxs
                          ^:volatile-mutable ^Map col-types]
  IMetadataManager
  (openTableMetadataWriter [_ table-name chunk-idx]
    (open-table-meta-writer allocator object-store table-name chunk-idx))

  (finishChunk [this chunk-idx new-chunk-metadata]
    @(.putObject object-store (->chunk-metadata-obj-key chunk-idx) (write-chunk-metadata new-chunk-metadata))
    (set! (.col-types this) (merge-col-types col-types new-chunk-metadata))
    (.put chunks-metadata chunk-idx new-chunk-metadata))

  (withMetadata [_ chunk-idx table-name f]
    (with-single-root buffer-pool (->table-metadata-obj-key chunk-idx table-name)
      (fn [metadata-root]
        (.apply f (->table-metadata metadata-root
                                    (.computeIfAbsent table-metadata-idxs
                                                      [chunk-idx table-name]
                                                      (reify Function
                                                        (apply [_ _]
                                                          (->table-metadata-idxs metadata-root)))))))))

  (chunksMetadata [_] chunks-metadata)

  (columnType [_ table-name col-name] (get-in col-types [table-name col-name]))

  AutoCloseable
  (close [_]
    (.clear chunks-metadata)))

(defmethod ig/prep-key ::metadata-manager [_ opts]
  (merge {:allocator (ig/ref :core2/allocator)
          :object-store (ig/ref :core2/object-store)
          :buffer-pool (ig/ref :core2.buffer-pool/buffer-pool)}
         opts))

(defmethod ig/init-key ::metadata-manager [_ {:keys [allocator ^ObjectStore object-store buffer-pool], :as deps}]
  (let [chunks-metadata (load-chunks-metadata deps)]
    (MetadataManager. allocator object-store buffer-pool
                      chunks-metadata
                      (ConcurrentHashMap.)
                      (->> (vals chunks-metadata) (reduce merge-col-types {})))))

(defmethod ig/halt-key! ::metadata-manager [_ mgr]
  (util/try-close mgr))

(defn with-metadata [^IMetadataManager metadata-mgr, ^long chunk-idx, ^String table-name, ^Function f]
  (.withMetadata metadata-mgr chunk-idx table-name f))

(defn with-all-metadata [^IMetadataManager metadata-mgr, table-name, ^BiFunction f]
  (->> (for [[^long chunk-idx, chunk-metadata] (.chunksMetadata metadata-mgr)
             :let [table (get-in chunk-metadata [:tables table-name])]
             :when table]
         (with-metadata metadata-mgr chunk-idx table-name
           (util/->jfn
             (fn [table-meta]
               (.apply f chunk-idx table-meta)))))
       vec
       (into [] (keep deref))))

(defn matching-chunks [^IMetadataManager metadata-mgr, table-name, ^IMetadataPredicate metadata-pred]
  (with-all-metadata metadata-mgr table-name
    (util/->jbifn
      (fn [^long chunk-idx, ^ITableMetadata table-metadata]
        (let [pred (.build metadata-pred table-metadata)]
          (when (.test pred -1)
            (let [block-idxs (RoaringBitmap.)]
              (dotimes [block-idx (.blockCount table-metadata)]
                (when (.test pred block-idx)
                  (.add block-idxs block-idx)))

              (when-not (.isEmpty block-idxs)
                (->ChunkMatch chunk-idx block-idxs)))))))))

(defn row-id->cols [^IMetadataManager metadata-mgr, ^String table-name, ^long row-id]
  ;; TODO cache which chunk each row-id is in.
  (let [bloom-hash (bloom/literal-hashes (.allocator ^MetadataManager metadata-mgr) row-id)]
    (->> (with-all-metadata metadata-mgr table-name
           (util/->jbifn
             (fn [^long chunk-idx ^ITableMetadata table-metadata]
               (let [cols (ArrayList.)
                     metadata-root (.metadataRoot table-metadata)
                     ^VarCharVector col-name-vec (.getVector metadata-root "column")
                     ^IntVector block-idx-vec (.getVector metadata-root "block-idx")
                     ^StructVector root-col-vec (.getVector metadata-root "root-column")
                     ^BigIntVector min-row-id-vec (.getChild root-col-vec "min-row-id" BigIntVector)
                     ^BigIntVector max-row-id-vec (.getChild root-col-vec "max-row-id" BigIntVector)
                     ^VarBinaryVector row-id-bloom-vec (.getChild root-col-vec "row-id-bloom" VarBinaryVector)]
                 (dotimes [idx (.getRowCount metadata-root)]
                   (let [block-idx (.get block-idx-vec idx)]
                     (when (and (not (neg? block-idx))
                                (not (.isNull root-col-vec idx))
                                (>= row-id (.get min-row-id-vec idx))
                                (<= row-id (.get max-row-id-vec idx))
                                (bloom/bloom-contains? row-id-bloom-vec idx bloom-hash))
                       (.add cols {:col-name (str (.getObject col-name-vec idx))
                                   :block-idx block-idx}))))
                 (when-not (.isEmpty cols)
                   {:chunk-idx chunk-idx, :cols cols})))))
         (remove nil?)
         first)))

(defn latest-chunk-metadata [^IMetadataManager metadata-mgr]
  (some-> (.lastEntry (.chunksMetadata metadata-mgr))
          (.getValue)))
