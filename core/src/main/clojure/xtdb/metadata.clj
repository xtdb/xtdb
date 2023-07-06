(ns xtdb.metadata
  (:require [cognitect.transit :as transit]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.bloom :as bloom]
            xtdb.buffer-pool
            [xtdb.expression.comparator :as expr.comp]
            xtdb.expression.temporal
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
           (java.util.function BiFunction Consumer Function)
           (java.util.stream IntStream)
           (org.apache.arrow.memory ArrowBuf BufferAllocator)
           (org.apache.arrow.vector FieldVector IntVector ValueVector VectorSchemaRoot)
           (org.apache.arrow.vector.complex DenseUnionVector ListVector StructVector)
           (org.apache.arrow.vector.types.pojo Schema)
           (org.roaringbitmap RoaringBitmap)
           xtdb.buffer_pool.IBufferPool
           xtdb.object_store.ObjectStore
           (xtdb.vector IIndirectRelation IIndirectVector IVectorWriter)))

(set! *unchecked-math* :warn-on-boxed)

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface ITableMetadata
  (^org.apache.arrow.vector.VectorSchemaRoot metadataRoot [])
  (^java.util.Set columnNames [])
  (^Long rowIndex [^String column-name, ^int blockIdx]
   "pass blockIdx = -1 for metadata about the whole chunk")
  (^long blockCount []))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface IBlockMetadataWriter
  (^long writeMetadata [^xtdb.vector.IIndirectVector liveCol])
  (^void endBlock []))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface ITableMetadataWriter
  (^xtdb.metadata.IBlockMetadataWriter writeBlockMetadata [^int blockIdx]
   "blockIdx = -1 for metadata for the whole column")

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
  (columnType [^String tableName, ^String colName])
  (allColumnTypes []))

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
  (Schema. [(types/col-type->field 'block-idx [:union #{:null :i32}]) ; null for whole chunk
            (types/col-type->field 'columns
                                   [:list
                                    [:struct
                                     '{col-name :utf8
                                       root-col? :bool
                                       count :i64
                                       types [:struct {}]
                                       bloom [:union #{:null :varbinary}]}]])]))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface ContentMetadataWriter
  (^void writeContentMetadata []))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface NestedMetadataWriter
  (appendNestedMetadata ^xtdb.metadata.ContentMetadataWriter [^xtdb.vector.IIndirectVector contentCol]))

#_{:clj-kondo/ignore [:unused-binding]}
(defmulti type->metadata-writer
  (fn [write-col-meta! types-vec col-type] (types/col-type-head col-type))
  :hierarchy #'types/col-type-hierarchy)

(defn- ->bool-type-handler [^IVectorWriter types-wtr, col-type]
  (let [bit-wtr (.structKeyWriter types-wtr (types/col-type->field-name col-type) [:union #{:null :bool}])]
    (reify NestedMetadataWriter
      (appendNestedMetadata [_ _content-col]
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
      (appendNestedMetadata [_ content-col]
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

(doseq [type-head #{:int :float :utf8 :varbinary :fixed-size-binary :timestamp-tz :timestamp-local :date :interval :time-local}]
  (defmethod type->metadata-writer type-head [_write-col-meta! metadata-root col-type] (->min-max-type-handler metadata-root col-type)))

(defmethod type->metadata-writer :keyword [_write-col-meta! metadata-root col-type] (->min-max-type-handler metadata-root col-type))
(defmethod type->metadata-writer :uri [_write-col-meta! metadata-root col-type] (->min-max-type-handler metadata-root col-type))
(defmethod type->metadata-writer :uuid [_write-col-meta! metadata-root col-type] (->min-max-type-handler metadata-root col-type))
(defmethod type->metadata-writer :clj-form [_write-col-meta! metadata-root col-type] (->bool-type-handler metadata-root col-type))

(defmethod type->metadata-writer :list [write-col-meta! ^IVectorWriter types-wtr col-type]
  (let [types-wp (.writerPosition types-wtr)
        list-type-wtr (.structKeyWriter types-wtr (types/col-type->field-name col-type) [:union #{:null :i32}])]
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

          ;; HACK needs to be selected - content-col is technically indirect,
          ;; I think this assumes there's no actual indirection in practice
          (write-col-meta! (iv/->indirect-vec data-vec (.toArray idxs)))

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
      (appendNestedMetadata [_ content-col]
        (let [struct-rdr (.structReader content-col)
              struct-ks (vec (.structKeys struct-rdr))
              sub-col-count (count struct-ks)
              sub-col-idxs (int-array sub-col-count)]

          (dotimes [n sub-col-count]
            (write-col-meta! (.readerForKey struct-rdr (nth struct-ks n)))
            (aset sub-col-idxs n (dec (.getPosition types-wp))))

          (reify ContentMetadataWriter
            (writeContentMetadata [_]
              (.startList struct-type-wtr)
              (dotimes [n sub-col-count]
                (.writeInt struct-type-el-wtr (aget sub-col-idxs n)))
              (.endList struct-type-wtr))))))))

(defn ->cols-meta-wtr ^xtdb.metadata.IBlockMetadataWriter [^IVectorWriter cols-wtr]
  (let [cols-wp (.writerPosition cols-wtr)
        col-name-wtr (.structKeyWriter cols-wtr "col-name")
        root-col-wtr (.structKeyWriter cols-wtr "root-col?")
        count-wtr (.structKeyWriter cols-wtr "count")
        types-wtr (.structKeyWriter cols-wtr "types")
        bloom-wtr (.structKeyWriter cols-wtr "bloom")

        type-metadata-writers (HashMap.)]

    (letfn [(->nested-meta-writer [^IIndirectVector values-col]
              (let [^NestedMetadataWriter nested-meta-writer
                    (.computeIfAbsent type-metadata-writers (types/field->col-type (.getField (.getVector values-col)))
                                      (reify Function
                                        (apply [_ col-type]
                                          (type->metadata-writer (partial write-col-meta! false) types-wtr col-type))))]

                (.appendNestedMetadata nested-meta-writer values-col)))

            (write-col-meta! [root-col?, ^IIndirectVector content-col]
              (let [content-writers (if-not (instance? DenseUnionVector (.getVector content-col))
                                      [(->nested-meta-writer content-col)]

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
                                                                    (->nested-meta-writer values-col))))))))))]

                (.startStruct cols-wtr)
                (.writeBoolean root-col-wtr root-col?)
                (.writeObject col-name-wtr (.getName content-col))
                (.writeLong count-wtr (.getValueCount content-col))
                (bloom/write-bloom bloom-wtr content-col)

                (.startStruct types-wtr)
                (doseq [^ContentMetadataWriter content-writer content-writers]
                  (.writeContentMetadata content-writer))
                (.endStruct types-wtr)

                (.endStruct cols-wtr)))]

      (reify IBlockMetadataWriter
        (writeMetadata [_ live-col]
          (if-not (zero? (.getValueCount live-col))
            (do
              (write-col-meta! true live-col)
              (dec (.getPosition cols-wp)))

            -1))))))

(defn ->table-metadata-idxs [^VectorSchemaRoot metadata-root]
  (let [block-idx-cache (HashMap.)
        meta-row-count (.getRowCount metadata-root)
        ^IntVector block-idx-vec (.getVector metadata-root "block-idx")
        ^ListVector cols-vec (.getVector metadata-root "columns")
        ^StructVector cols-data-vec (.getDataVector cols-vec)
        column-name-vec (.getChild cols-data-vec "col-name")
        root-col-rdr (-> (.getChild cols-data-vec "root-col?")
                         (vec/->mono-reader :bool))
        col-names (HashSet.)]

    (dotimes [meta-idx meta-row-count]
      (let [cols-start-idx (.getElementStartIndex cols-vec meta-idx)
            block-idx (if (.isNull block-idx-vec meta-idx)
                        -1
                        (.get block-idx-vec meta-idx))]
        (dotimes [cols-data-idx (- (.getElementEndIndex cols-vec meta-idx) cols-start-idx)]
          (let [cols-data-idx (+ cols-data-idx cols-start-idx)
                col-name (str (.getObject column-name-vec cols-data-idx))]
            (.add col-names col-name)
            (when (.readBoolean root-col-rdr cols-data-idx)
              (.put block-idx-cache [col-name block-idx] cols-data-idx))))))

    {:col-names (into #{} col-names)
     :block-idx-cache (into {} block-idx-cache)
     :block-count (loop [block-count 0, idx 0]
                    (cond
                      (>= idx meta-row-count) (inc block-count)
                      :else (recur (if (.isNull block-idx-vec idx)
                                     block-count
                                     (max (.get block-idx-vec idx) block-count))
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
        block-idx-wtr (.writerForName metadata-wtr "block-idx")
        cols-wtr (.writerForName metadata-wtr "columns")
        cols-meta-wtr (->cols-meta-wtr (.listElementWriter cols-wtr))]

    (reify ITableMetadataWriter
      (writeBlockMetadata [_ block-idx]
        (if (neg? block-idx)
          (.writeNull block-idx-wtr nil)
          (do
            (.writeInt block-idx-wtr block-idx)
            (.set !block-count block-idx)))

        (.startList cols-wtr)

        (reify IBlockMetadataWriter
          (writeMetadata [_ live-col]
            (let [col-name (.getName live-col)]
              (.add col-names col-name)

              (let [meta-idx (.writeMetadata cols-meta-wtr live-col)]
                (when-not (neg? meta-idx)
                  (.put block-idx-cache [col-name block-idx] meta-idx))

                meta-idx)))

          (endBlock [_]
            (.endList cols-wtr)
            (.endRow metadata-wtr))))

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
  (allColumnTypes [_] col-types)

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

(defn latest-chunk-metadata [^IMetadataManager metadata-mgr]
  (some-> (.lastEntry (.chunksMetadata metadata-mgr))
          (.getValue)))
