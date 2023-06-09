(ns xtdb.metadata
  (:require [cognitect.transit :as transit]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.bloom :as bloom]
            xtdb.buffer-pool
            [xtdb.expression.comparator :as expr.comp]
            xtdb.object-store
            [xtdb.transit :as xt.transit]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector :as vec]
            [xtdb.vector.indirect :as iv]
            [xtdb.vector.writer :as vw])
  (:import [clojure.lang MapEntry]
           (java.io ByteArrayInputStream ByteArrayOutputStream)
           java.lang.AutoCloseable
           java.nio.ByteBuffer
           (java.util HashMap HashSet Map NavigableMap Set TreeMap)
           (java.util.concurrent ConcurrentHashMap)
           java.util.concurrent.atomic.AtomicInteger
           (java.util.function BiFunction Consumer Function IntFunction IntPredicate)
           (java.util.stream Collectors IntStream)
           (org.apache.arrow.memory ArrowBuf BufferAllocator)
           (org.apache.arrow.vector FieldVector ValueVector VarCharVector VectorSchemaRoot)
           (org.apache.arrow.vector.complex DenseUnionVector ListVector StructVector)
           (org.apache.arrow.vector.types.pojo Field Schema)
           (org.roaringbitmap RoaringBitmap)
           xtdb.buffer_pool.IBufferPool
           xtdb.object_store.ObjectStore
           (xtdb.vector IIndirectRelation IIndirectVector IVectorWriter IRelationWriter)))

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
  (^long writeMetadata [^xtdb.vector.IIndirectVector liveCol, ^int blockIdx]
   "blockIdx = -1 for metadata for the whole column"))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface ITableMetadataWriter
  (^xtdb.metadata.IColumnMetadataWriter columnMetadataWriter [^String colName])
  (^xtdb.metadata.ITableMetadata tableMetadata [])
  (^java.util.concurrent.CompletableFuture finishChunk [])
  (^void close []))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface IMetadataManager
  (^xtdb.metadata.ITableMetadataWriter openTableMetadataWriter [^String table-name, ^long chunk-idx])
  (^void finishChunk [^long chunkIdx, newChunkMetadata])
  (^java.util.NavigableMap chunksMetadata [])
  (^java.util.concurrent.CompletableFuture withMetadata [^long chunkIdx, ^String tableName, ^java.util.function.Function #_<ITableMetadata> f])
  (columnTypes [^String tableName])
  (columnType [^String tableName, ^String colName]))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface IMetadataPredicate
  (^java.util.function.IntPredicate build [^xtdb.metadata.ITableMetadata tableMetadata]))

(defrecord ChunkMatch [^long chunk-idx, ^RoaringBitmap block-idxs, ^Set col-names])

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

(defn live-rel->chunk-metadata [^String table-name, ^IIndirectRelation live-rel]
  (when (pos? (.rowCount live-rel))
    (MapEntry/create table-name
                     {:col-types (->> (for [^IIndirectVector live-col live-rel]
                                        (MapEntry/create (.getName live-col)
                                                         (types/field->col-type (.getField (.getVector live-col)))))
                                      (into {}))})))

(defn- write-chunk-metadata ^java.nio.ByteBuffer [chunk-meta]
  (with-open [os (ByteArrayOutputStream.)]
    (let [w (transit/writer os :json {:handlers xt.transit/tj-write-handlers})]
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
        (let [rdr (transit/reader is :json {:handlers xt.transit/tj-read-handlers})]
          (.put cm (obj-key->chunk-idx cm-obj-key) (transit/read rdr)))))
    cm))

(def ^org.apache.arrow.vector.types.pojo.Schema table-metadata-schema
  (Schema. [(types/col-type->field 'column :utf8)
            (types/col-type->field 'block-idx :i32) ; -1 for whole chunk

            (types/col-type->field 'root-column :bool)

            (types/col-type->field "count" :i64)

            (types/->field "types" types/struct-type true)

            (types/col-type->field "bloom" [:union #{:null :varbinary}])]))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface ContentMetadataWriter
  (^void writeContentMetadata []))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface NestedMetadataWriter
  (appendNestedMetadata ^xtdb.metadata.ContentMetadataWriter [^xtdb.vector.IIndirectVector contentCol, ^int blockIdx]))

#_{:clj-kondo/ignore [:unused-binding]}
(defmulti type->metadata-writer
  (fn [write-col-meta! types-vec col-type] (types/col-type-head col-type))
  :hierarchy #'types/col-type-hierarchy)

(defn- ->bool-type-handler [^IVectorWriter types-wtr, col-type]
  (let [bit-wtr (.structKeyWriter types-wtr (types/col-type->field-name col-type) [:union #{:null :bool}])]
    (reify NestedMetadataWriter
      (appendNestedMetadata [_ _content-col block-idx]
        (reify ContentMetadataWriter
          (writeContentMetadata [_]
            (.writeBoolean bit-wtr true)))))))

(defmethod type->metadata-writer :null [_write-col-meta! metadata-root col-type] (->bool-type-handler metadata-root col-type))
(defmethod type->metadata-writer :bool [_write-col-meta! metadata-root col-type] (->bool-type-handler metadata-root col-type))

(defn- ->min-max-type-handler [^IVectorWriter types-wtr, col-type]
  ;; we get vectors out here because this code was largely written pre writers.
  (let [types-wp (.writerPosition types-wtr)

        struct-wtr (.structKeyWriter types-wtr (types/col-type->field-name col-type)
                                     [:union #{:null
                                               [:struct {'min [:union #{:null col-type}]
                                                         'max [:union #{:null col-type}]}]}])



        min-wtr (.structKeyWriter struct-wtr "min")
        ^FieldVector min-vec (.getVector min-wtr)

        max-wtr (.structKeyWriter struct-wtr "max")
        ^FieldVector max-vec (.getVector max-wtr)]

    (reify NestedMetadataWriter
      (appendNestedMetadata [_ content-col block-idx]
        (let [content-vec (.getVector content-col)]
          (reify ContentMetadataWriter
            (writeContentMetadata [_]
              (.startStruct struct-wtr)

              (let [pos (.getPosition types-wp)
                    min-comparator (expr.comp/->comparator content-col (vw/vec-wtr->rdr min-wtr) :nulls-last)
                    max-comparator (expr.comp/->comparator content-col (vw/vec-wtr->rdr max-wtr) :nulls-first)]

                (.writeNull min-wtr nil)
                (.writeNull max-wtr nil)

                (dotimes [values-idx (.getValueCount content-col)]
                  (let [values-vec-idx (.getIndex content-col values-idx)]
                    (when (or (.isNull min-vec pos)
                              (and (not (.isNull content-vec values-vec-idx))
                                   (neg? (.applyAsInt min-comparator values-idx pos))))
                      (.copyFromSafe min-vec values-vec-idx pos content-vec))

                    (when (or (doto (.isNull max-vec pos))
                              (and (not (.isNull content-vec values-vec-idx))
                                   (pos? (.applyAsInt max-comparator values-idx pos))))
                      (.copyFromSafe max-vec values-vec-idx pos content-vec))))

                (.endStruct struct-wtr)))))))))

(doseq [type-head #{:int :float :utf8 :varbinary :timestamp-tz :timestamp-local :date :interval :time-local}]
  (defmethod type->metadata-writer type-head [_write-col-meta! metadata-root col-type] (->min-max-type-handler metadata-root col-type)))

(defmethod type->metadata-writer :keyword [_write-col-meta! metadata-root col-type] (->min-max-type-handler metadata-root col-type))
(defmethod type->metadata-writer :uri [_write-col-meta! metadata-root col-type] (->min-max-type-handler metadata-root col-type))
(defmethod type->metadata-writer :uuid [_write-col-meta! metadata-root col-type] (->min-max-type-handler metadata-root col-type))
(defmethod type->metadata-writer :clj-form [_write-col-meta! metadata-root col-type] (->bool-type-handler metadata-root col-type))

(defmethod type->metadata-writer :list [write-col-meta! ^IVectorWriter types-wtr col-type]
  (let [types-wp (.writerPosition types-wtr)
        list-type-wtr (.structKeyWriter types-wtr (types/col-type->field-name col-type) [:union #{:null :i32}])]
    (reify NestedMetadataWriter
      (appendNestedMetadata [_ content-col block-idx]
        (let [list-rdr (.listReader content-col)
              ^ListVector content-vec (.getVector content-col)
              data-vec (.getDataVector content-vec)
              idxs (RoaringBitmap.)]
          (dotimes [idx (.getValueCount content-col)]
            (.add idxs
                  (.getElementStartIndex list-rdr idx)
                  (.getElementEndIndex list-rdr idx)))

          ;; HACK needs to be selected - content-col is technically indirect,
          ;; this assumes there's no actual indirection in practice
          (write-col-meta! (iv/->indirect-vec data-vec (.toArray idxs)) block-idx)

          (let [data-meta-idx (dec (.getPosition types-wp))]
            (reify ContentMetadataWriter
              (writeContentMetadata [_]
                (.writeInt list-type-wtr data-meta-idx)))))))))

(defmethod type->metadata-writer :struct [write-col-meta! ^IVectorWriter types-wtr, col-type]
  (let [types-wp (.writerPosition types-wtr)
        struct-type-wtr (.structKeyWriter types-wtr
                                          (str (types/col-type->field-name col-type) "-" (count (seq types-wtr)))
                                          [:union #{:null [:list [:union #{:null :i32}]]}])
        struct-type-el-wtr (.listElementWriter struct-type-wtr)]
    (reify NestedMetadataWriter
      (appendNestedMetadata [_ content-col block-idx]
        (let [struct-rdr (.structReader content-col)
              struct-ks (vec (.structKeys struct-rdr))
              sub-col-count (count struct-ks)
              sub-col-idxs (int-array sub-col-count)]

          (dotimes [n sub-col-count]
            (write-col-meta! (.readerForKey struct-rdr (nth struct-ks n)) block-idx)
            (aset sub-col-idxs n (dec (.getPosition types-wp))))

          (reify ContentMetadataWriter
            (writeContentMetadata [_]
              (.startList struct-type-wtr)
              (dotimes [n sub-col-count]
                (.writeInt struct-type-el-wtr (aget sub-col-idxs n)))
              (.endList struct-type-wtr))))))))

(defn ->col-meta-wtr ^xtdb.metadata.IColumnMetadataWriter [^IRelationWriter metadata-wtr]
  (let [metadata-wp (.writerPosition metadata-wtr)
        column-name-wtr (.writerForName metadata-wtr "column")
        block-idx-wtr (.writerForName metadata-wtr "block-idx")
        root-col-wtr (.writerForName metadata-wtr "root-column")
        count-wtr (.writerForName metadata-wtr "count")
        types-wtr (.writerForName metadata-wtr "types")
        bloom-wtr (.writerForName metadata-wtr "bloom")

        type-metadata-writers (HashMap.)]

    (letfn [(->nested-meta-writer [^IIndirectVector values-col, ^long block-idx]
              (let [^NestedMetadataWriter nested-meta-writer
                    (.computeIfAbsent type-metadata-writers (types/field->col-type (.getField (.getVector values-col)))
                                      (reify Function
                                        (apply [_ col-type]
                                          (type->metadata-writer (partial write-col-meta! false) types-wtr col-type))))]

                (.appendNestedMetadata nested-meta-writer values-col block-idx)))

            (write-col-meta! [root-col?, ^IIndirectVector content-col, ^long block-idx]
              (let [content-writers (if (= "_row_id" (.getName content-col))
                                      [(->nested-meta-writer content-col block-idx)]

                                      (let [^DenseUnionVector content-vec (.getVector content-col)]
                                        (->> (range (count (seq content-vec)))
                                             (into [] (keep (fn [^long type-id]
                                                              (let [^ValueVector values-vec (.getVectorByType content-vec type-id)
                                                                    sel (IntStream/builder)]

                                                                (dotimes [idx (.getValueCount content-col)]
                                                                  (let [idx (.getIndex content-col idx)]
                                                                    (when (= type-id (.getTypeId content-vec idx))
                                                                      (.add sel (.getOffset content-vec idx)))))

                                                                (let [values-col (iv/->indirect-vec values-vec (.toArray (.build sel)))]
                                                                  (when-not (zero? (.getValueCount values-col))
                                                                    (->nested-meta-writer values-col block-idx))))))))))]
                (.writeBoolean root-col-wtr root-col?)
                (.writeInt block-idx-wtr block-idx)
                (.writeObject column-name-wtr (.getName content-col))
                (.writeLong count-wtr (.getValueCount content-col))
                (bloom/write-bloom bloom-wtr content-col)

                (.startStruct types-wtr)
                (doseq [^ContentMetadataWriter content-writer content-writers]
                  (.writeContentMetadata content-writer))
                (.endStruct types-wtr)
                (.endRow metadata-wtr)))]

      (reify IColumnMetadataWriter
        (writeMetadata [_ live-col block-idx]
          (if-not (zero? (.getValueCount live-col))
            (do
              (write-col-meta! true live-col block-idx)
              (dec (.getPosition metadata-wp)))

            -1))))))

(defn ->table-metadata-idxs [^VectorSchemaRoot metadata-root]
  (let [block-idx-cache (HashMap.)
        row-count (.getRowCount metadata-root)
        ^VarCharVector column-name-vec (.getVector metadata-root "column")
        block-idx-rdr (-> (.getVector metadata-root "block-idx")
                          (vec/->mono-reader :i32))
        root-col-rdr (-> (.getVector metadata-root "root-column")
                         (vec/->mono-reader :bool))
        col-names (HashSet.)]
    (dotimes [meta-idx (.getRowCount metadata-root)]
      (let [col-name (str (.getObject column-name-vec meta-idx))]
        (.add col-names col-name)
        (when (.readBoolean root-col-rdr meta-idx)
          (.put block-idx-cache [col-name (.readInt block-idx-rdr meta-idx)] meta-idx))))

    {:col-names (into #{} col-names)
     :block-idx-cache block-idx-cache
     :block-count (loop [block-count 0, idx 0]
                    (cond
                      (>= idx row-count) (inc block-count)
                      :else (recur (max (.readInt block-idx-rdr idx) block-count)
                                   (inc idx))))}))

(defn ->table-metadata ^xtdb.metadata.ITableMetadata [^VectorSchemaRoot vsr, {:keys [col-names block-idx-cache, block-count]}]
  (reify ITableMetadata
    (metadataRoot [_] vsr)
    (columnNames [_] col-names)
    (rowIndex [_ col-name block-idx] (get block-idx-cache [col-name block-idx]))
    (blockCount [_] block-count)))

(defn open-table-meta-writer [^VectorSchemaRoot metadata-root, ^ObjectStore object-store, ^String table-name, ^long chunk-idx]
  (let [col-names (HashSet.)
        block-idx-cache (HashMap.)
        !block-count (AtomicInteger. 0)

        metadata-wtr (vw/root->writer metadata-root)
        col-meta-wtr (->col-meta-wtr metadata-wtr)]

    (reify ITableMetadataWriter
      (columnMetadataWriter [_ col-name]
        (.add col-names col-name)

        (reify IColumnMetadataWriter
          (writeMetadata [_ live-col block-idx]
            (let [meta-idx (.writeMetadata col-meta-wtr live-col block-idx)]
              (when-not (neg? meta-idx)
                (when-not (neg? block-idx)
                  (.set !block-count block-idx))

                (.put block-idx-cache [col-name block-idx] meta-idx))

              meta-idx))))

      (tableMetadata [_]
        (->table-metadata metadata-root
                          {:col-names (into #{} col-names)
                           :block-idx-cache (into {} block-idx-cache)
                           :block-count (.get !block-count)}))

      (finishChunk [_]
        (.syncSchema metadata-root)
        (.syncRowCount metadata-wtr)

        (let [metadata-buf (util/root->arrow-ipc-byte-buffer metadata-root :file)]
          (.putObject object-store (->table-metadata-obj-key chunk-idx table-name) metadata-buf)))

      AutoCloseable
      (close [_]
        (.close metadata-root)))))

(deftype MetadataManager [^BufferAllocator allocator
                          ^ObjectStore object-store
                          ^IBufferPool buffer-pool
                          ^NavigableMap chunks-metadata
                          ^Map table-metadata-idxs
                          ^:volatile-mutable ^Map col-types]
  IMetadataManager
  (openTableMetadataWriter [_ table-name chunk-idx]
    (open-table-meta-writer (VectorSchemaRoot/create table-metadata-schema allocator) object-store table-name chunk-idx))

  (finishChunk [this chunk-idx new-chunk-metadata]
    (-> @(.putObject object-store (->chunk-metadata-obj-key chunk-idx) (write-chunk-metadata new-chunk-metadata))
        (util/rethrowing-cause))
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
  (columnTypes [_ table-name] (get col-types table-name))

  AutoCloseable
  (close [_]
    (.clear chunks-metadata)))

(defmethod ig/prep-key ::metadata-manager [_ opts]
  (merge {:allocator (ig/ref :xtdb/allocator)
          :object-store (ig/ref :xtdb/object-store)
          :buffer-pool (ig/ref :xtdb.buffer-pool/buffer-pool)}
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
       (into [] (keep deref))
       (util/rethrowing-cause)))

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
                (->ChunkMatch chunk-idx block-idxs (.columnNames table-metadata))))))))))

(defn row-id->chunk [^IMetadataManager metadata-mgr, ^String table-name, ^long row-id]
  ;; TODO cache which chunk each row-id is in.
  (->> (with-all-metadata metadata-mgr table-name
         (util/->jbifn
           (fn [^long chunk-idx ^ITableMetadata table-metadata]
             (let [metadata-root (.metadataRoot table-metadata)
                   ^StructVector types-vec (.getVector metadata-root "types")
                   ^StructVector i64-vec (.getChild types-vec "i64")
                   min-i64-rdr (-> (.getChild i64-vec "min") (vec/->mono-reader :i64))
                   max-i64-rdr (-> (.getChild i64-vec "max") (vec/->mono-reader :i64))

                   block-matches? (reify IntPredicate
                                    (test [_ block-idx]
                                      (boolean
                                       (when-let [meta-idx (.rowIndex table-metadata "_row_id" block-idx)]
                                         (<= (.readLong min-i64-rdr meta-idx)
                                             row-id
                                             (.readLong max-i64-rdr meta-idx))))))]
               (when (.test block-matches? -1)
                 {:chunk-idx chunk-idx
                  :block-idx (-> (IntStream/range 0 (.blockCount table-metadata))
                                 (.filter block-matches?)
                                 (.findFirst)
                                 (.getAsInt))
                  :col-names (let [^VarCharVector col-name-vec (.getVector metadata-root "column")]
                               (-> (IntStream/range 0 (.getRowCount metadata-root))
                                   (.mapToObj (reify IntFunction
                                                (apply [_ idx]
                                                  (types/get-object col-name-vec idx))))
                                   (.collect (Collectors/toSet))
                                   (set)
                                   (disj "_row_id")))})))))
       (remove nil?)
       first))

(defn latest-chunk-metadata [^IMetadataManager metadata-mgr]
  (some-> (.lastEntry (.chunksMetadata metadata-mgr))
          (.getValue)))
