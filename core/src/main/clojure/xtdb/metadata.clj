(ns xtdb.metadata
  (:require [cognitect.transit :as transit]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.bloom :as bloom]
            xtdb.buffer-pool
            [xtdb.expression.comparator :as expr.comp]
            xtdb.expression.temporal
            [xtdb.serde :as serde]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw])
  (:import (com.cognitect.transit TransitFactory)
           (com.github.benmanes.caffeine.cache Cache Caffeine RemovalListener)
           (java.io ByteArrayInputStream ByteArrayOutputStream)
           java.lang.AutoCloseable
           java.nio.ByteBuffer
           (java.nio.file Path)
           (java.util HashMap HashSet Map NavigableMap TreeMap)
           (java.util.concurrent.atomic AtomicInteger)
           (java.util.function Function IntPredicate)
           (java.util.stream IntStream)
           (org.apache.arrow.memory ArrowBuf)
           (org.apache.arrow.vector VectorLoader VectorSchemaRoot)
           (org.apache.arrow.vector FieldVector)
           (org.apache.arrow.vector.types.pojo ArrowType Field FieldType)
           xtdb.IBufferPool
           (xtdb.trie HashTrie ArrowHashTrie)
           (xtdb.vector IVectorReader IVectorWriter RelationReader)))

(def arrow-read-handlers
  {"xtdb/arrow-type" (transit/read-handler types/->arrow-type)
   "xtdb/field-type" (transit/read-handler (fn [[arrow-type nullable?]]
                                             (if nullable?
                                               (FieldType/nullable arrow-type)
                                               (FieldType/notNullable arrow-type))))
   "xtdb/field" (transit/read-handler (fn [[name field-type children]]
                                        (Field. name field-type children)))})

(def arrow-write-handlers
  {ArrowType (transit/write-handler "xtdb/arrow-type" #(types/<-arrow-type %))
   ;; beware that this currently ignores dictionary encoding and metadata of FieldType's
   FieldType (transit/write-handler "xtdb/field-type"
                                    (fn [^FieldType field-type]
                                      (TransitFactory/taggedValue "array" [(.getType field-type) (.isNullable field-type)])))
   Field (transit/write-handler "xtdb/field"
                                (fn [^Field field]
                                  (TransitFactory/taggedValue "array" [(.getName field) (.getFieldType field) (.getChildren field)])))})
(set! *unchecked-math* :warn-on-boxed)

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface ITableMetadata
  (^xtdb.vector.IVectorReader metadataReader [])
  (^java.util.Set columnNames [])
  (^Long rowIndex [^String columnName, ^int pageIdx])
  (^org.roaringbitmap.buffer.ImmutableRoaringBitmap iidBloomBitmap [^int pageIdx]))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface IPageMetadataWriter
  (^void writeMetadata [^Iterable cols]))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface IMetadataManager
  (^void finishChunk [^long chunkIdx, newChunkMetadata])
  (^java.util.NavigableMap chunksMetadata [])
  (^xtdb.metadata.ITableMetadata openTableMetadata [^java.nio.file.Path metaFilePath])
  (columnFields [^String tableName])
  (columnField [^String tableName, ^String colName])
  (allColumnFields []))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface IMetadataPredicate
  (^java.util.function.IntPredicate build [^xtdb.metadata.ITableMetadata tableMetadata]))

(defn- obj-key->chunk-idx [^Path obj-key]
  (some->> (.getFileName obj-key)
           (str)
           (re-matches #"(\p{XDigit}+).transit.json")
           (second)
           (util/<-lex-hex-string)))

(def ^Path chunk-metadata-path (util/->path "chunk-metadata"))

(defn- ->chunk-metadata-obj-key [chunk-idx]
  (.resolve chunk-metadata-path (format "%s.transit.json" (util/->lex-hex-string chunk-idx))))

(defn- write-chunk-metadata ^java.nio.ByteBuffer [chunk-meta]
  (with-open [os (ByteArrayOutputStream.)]
    (let [w (transit/writer os :json {:handlers (merge serde/transit-write-handlers
                                                       arrow-write-handlers)})]
      (transit/write w chunk-meta))
    (ByteBuffer/wrap (.toByteArray os))))

(defn- merge-fields [fields {:keys [tables]}]
  (reduce (fn [fields [table {new-fields :fields}]]
            (update fields table
                    (fn [fields new-fields]
                      (->>
                       (merge-with types/merge-fields fields new-fields)
                       (map (fn [[col-name field]] [col-name (types/field-with-name field col-name)]))
                       (into {})))
                    new-fields))
          fields
          tables))

(def metadata-col-type
  '[:list
    [:struct
     {col-name :utf8
      root-col? :bool
      count :i64
      types [:struct {}]
      bloom [:union #{:null :varbinary}]}]])

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface ContentMetadataWriter
  (^void writeContentMetadata []))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface NestedMetadataWriter
  (^xtdb.metadata.ContentMetadataWriter appendNestedMetadata [^xtdb.vector.IVectorReader contentCol]))

#_{:clj-kondo/ignore [:unused-binding]}
(defmulti type->metadata-writer
  (fn [write-col-meta! types-vec col-type] (types/col-type-head col-type))
  :hierarchy #'types/col-type-hierarchy)

(defn- ->bool-type-handler [^IVectorWriter types-wtr, col-type]
  (let [bit-wtr (.structKeyWriter types-wtr (types/col-type->field-name col-type) (FieldType/nullable #xt.arrow/type :bool))]
    (reify NestedMetadataWriter
      (appendNestedMetadata [_ _content-col]
        (reify ContentMetadataWriter
          (writeContentMetadata [_]
            (.writeBoolean bit-wtr true)))))))

(defn- ->min-max-type-handler [^IVectorWriter types-wtr, col-type]
  ;; we get vectors out here because this code was largely written pre writers.
  (let [types-wp (.writerPosition types-wtr)

        struct-wtr (.structKeyWriter types-wtr (types/col-type->field-name col-type) (FieldType/nullable #xt.arrow/type :struct))

        min-wtr (.structKeyWriter struct-wtr "min" (FieldType/nullable (types/->arrow-type col-type)))
        ^FieldVector min-vec (.getVector min-wtr)
        min-wp (.writerPosition min-wtr)

        max-wtr (.structKeyWriter struct-wtr "max" (FieldType/nullable (types/->arrow-type col-type)))
        ^FieldVector max-vec (.getVector max-wtr)
        max-wp (.writerPosition max-wtr)]

    (reify NestedMetadataWriter
      (appendNestedMetadata [_ content-col]
        (reify ContentMetadataWriter
          (writeContentMetadata [_]
            (.startStruct struct-wtr)

            (let [pos (.getPosition types-wp)
                  min-copier (.rowCopier content-col min-wtr)
                  max-copier (.rowCopier content-col max-wtr)

                  min-comparator (expr.comp/->comparator content-col (vw/vec-wtr->rdr min-wtr) :nulls-last)
                  max-comparator (expr.comp/->comparator content-col (vw/vec-wtr->rdr max-wtr) :nulls-first)]

              (.writeNull min-wtr)
              (.writeNull max-wtr)

              (dotimes [value-idx (.valueCount content-col)]
                (when (and (not (.isNull content-col value-idx))
                           (or (.isNull min-vec pos)
                               (neg? (.applyAsInt min-comparator value-idx pos))))
                  (.setPosition min-wp pos)
                  (.copyRow min-copier value-idx))

                (when (and (not (.isNull content-col value-idx))
                           (or (.isNull max-vec pos)
                               (pos? (.applyAsInt max-comparator value-idx pos))))
                  (.setPosition max-wp pos)
                  (.copyRow max-copier value-idx)))

              (.endStruct struct-wtr))))))))

(doseq [type-head #{:null :absent :bool :fixed-size-binary :transit}]
  (defmethod type->metadata-writer type-head [_write-col-meta! metadata-root col-type] (->bool-type-handler metadata-root col-type)))

(doseq [type-head #{:int :float :utf8 :varbinary :keyword :uri :uuid
                    :timestamp-tz :timestamp-local :date :interval :time-local}]
  (defmethod type->metadata-writer type-head [_write-col-meta! metadata-root col-type] (->min-max-type-handler metadata-root col-type)))

(defmethod type->metadata-writer :list [write-col-meta! ^IVectorWriter types-wtr col-type]
  (let [types-wp (.writerPosition types-wtr)
        list-type-wtr (.structKeyWriter types-wtr (types/col-type->field-name col-type)
                                        (FieldType/nullable #xt.arrow/type :i32)
                                        #_(types/col-type->field  [:union #{:null :i32}]))]
    (reify NestedMetadataWriter
      (appendNestedMetadata [_ content-col]
        (write-col-meta! (.listElementReader ^IVectorReader content-col))

        (let [data-meta-idx (dec (.getPosition types-wp))]
          (reify ContentMetadataWriter
            (writeContentMetadata [_]
              (.writeInt list-type-wtr data-meta-idx))))))))

(defmethod type->metadata-writer :set [write-col-meta! ^IVectorWriter types-wtr col-type]
  (let [types-wp (.writerPosition types-wtr)
        set-type-wtr (.structKeyWriter types-wtr (types/col-type->field-name col-type)
                                        (FieldType/nullable #xt.arrow/type :i32))]
    (reify NestedMetadataWriter
      (appendNestedMetadata [_ content-col]
        (write-col-meta! (.listElementReader ^IVectorReader content-col))

        (let [data-meta-idx (dec (.getPosition types-wp))]
          (reify ContentMetadataWriter
            (writeContentMetadata [_]
              (.writeInt set-type-wtr data-meta-idx))))))))

(defmethod type->metadata-writer :struct [write-col-meta! ^IVectorWriter types-wtr, col-type]
  (let [types-wp (.writerPosition types-wtr)
        struct-type-wtr (.structKeyWriter types-wtr
                                          (str (types/col-type->field-name col-type) "-" (count (seq types-wtr)))
                                          (FieldType/nullable #xt.arrow/type :list))
        struct-type-el-wtr (.listElementWriter struct-type-wtr (FieldType/nullable #xt.arrow/type :i32))]
    (reify NestedMetadataWriter
      (appendNestedMetadata [_ content-col]
        (let [struct-keys (.structKeys content-col)
              sub-col-idxs (IntStream/builder)]

          (doseq [^String struct-key struct-keys]
            (write-col-meta! (.structKeyReader content-col struct-key))
            (.add sub-col-idxs (dec (.getPosition types-wp))))

          (reify ContentMetadataWriter
            (writeContentMetadata [_]
              (.startList struct-type-wtr)
              (doseq [sub-col-idx (.toArray (.build sub-col-idxs))]
                (.writeInt struct-type-el-wtr sub-col-idx))
              (.endList struct-type-wtr))))))))

(defn ->page-meta-wtr ^xtdb.metadata.IPageMetadataWriter [^IVectorWriter cols-wtr]
  (let [col-wtr (.listElementWriter cols-wtr)
        col-name-wtr (.structKeyWriter col-wtr "col-name")
        root-col-wtr (.structKeyWriter col-wtr "root-col?")
        count-wtr (.structKeyWriter col-wtr "count")
        types-wtr (.structKeyWriter col-wtr "types")
        bloom-wtr (.structKeyWriter col-wtr "bloom")

        type-metadata-writers (HashMap.)]

    (letfn [(->nested-meta-writer [^IVectorReader content-col]
              (when-let [col-type (first (-> (types/field->col-type (.getField content-col))
                                             (types/flatten-union-types)
                                             (disj :null)
                                             (doto (-> count (<= 1) (assert (str (pr-str (.getField content-col)) "should just be nullable mono-vecs here"))))))]
                (-> ^NestedMetadataWriter
                 (.computeIfAbsent type-metadata-writers col-type
                                   (reify Function
                                     (apply [_ col-type]
                                       (type->metadata-writer (partial write-col-meta! false) types-wtr col-type))))
                    (.appendNestedMetadata content-col))))

            (write-col-meta! [root-col?, ^IVectorReader content-col]
              (let [content-writers (->> (if (= #xt.arrow/type :union (.getType (.getField content-col)))
                                           (->> (.legs content-col)
                                                (mapv (fn [leg]
                                                        (->nested-meta-writer (.legReader content-col leg)))))
                                           [(->nested-meta-writer content-col)])
                                         (remove nil?))]

                (.startStruct col-wtr)
                (.writeBoolean root-col-wtr root-col?)
                (.writeObject col-name-wtr (.getName content-col))
                (.writeLong count-wtr (-> (IntStream/range 0 (.valueCount content-col))
                                          (.filter (reify IntPredicate
                                                     (test [_ idx]
                                                       (not (.isNull content-col idx)))))
                                          (.count)))
                (bloom/write-bloom bloom-wtr content-col)

                (.startStruct types-wtr)
                (doseq [^ContentMetadataWriter content-writer content-writers]
                  (.writeContentMetadata content-writer))
                (.endStruct types-wtr)

                (.endStruct col-wtr)))]

      (reify IPageMetadataWriter
        (writeMetadata [_ cols]
          (.startList cols-wtr)
          (doseq [^IVectorReader col cols
                  :when (pos? (.valueCount col))]
            (write-col-meta! true col))
          (.endList cols-wtr))))))

(defrecord PageIndexKey [col-name page-idx])

(defn ->table-metadata-idxs [^IVectorReader metadata-rdr]
  (let [page-idx-cache (HashMap.)
        meta-row-count (.valueCount metadata-rdr)
        data-page-idx-rdr (.structKeyReader metadata-rdr "data-page-idx")
        cols-rdr (.structKeyReader metadata-rdr "columns")
        col-rdr (.listElementReader cols-rdr)
        column-name-rdr (.structKeyReader col-rdr "col-name")
        root-col-rdr (.structKeyReader col-rdr "root-col?")
        col-names (HashSet.)]

    (dotimes [meta-idx meta-row-count]
      (when-not (.isNull cols-rdr meta-idx)
        (let [cols-start-idx (.getListStartIndex cols-rdr meta-idx)
              data-page-idx (if-let [data-page-idx (.getObject data-page-idx-rdr meta-idx)]
                              data-page-idx
                              -1)]
          (dotimes [cols-data-idx (.getListCount cols-rdr meta-idx)]
            (let [cols-data-idx (+ cols-start-idx cols-data-idx)
                  col-name (str (.getObject column-name-rdr cols-data-idx))]
              (.add col-names col-name)
              (when (.getBoolean root-col-rdr cols-data-idx)
                (.put page-idx-cache (->PageIndexKey col-name data-page-idx) cols-data-idx)))))))

    {:col-names (into #{} col-names)
     :page-idx-cache page-idx-cache}))

(defrecord TableMetadata [^HashTrie trie
                          ^RelationReader meta-rel-reader
                          ^ArrowBuf buf
                          ^IVectorReader metadata-leaf-rdr
                          col-names
                          ^Map page-idx-cache
                          ^AtomicInteger ref-count]
  ITableMetadata
  (metadataReader [_] metadata-leaf-rdr)
  (columnNames [_] col-names)
  (rowIndex [_ col-name page-idx] (.get page-idx-cache (->PageIndexKey col-name page-idx)))
  (iidBloomBitmap [_ page-idx]
    (let [bloom-rdr (-> (.structKeyReader metadata-leaf-rdr "columns")
                        (.listElementReader)
                        (.structKeyReader "bloom"))]

      (when-let [bloom-vec-idx (.get page-idx-cache (->PageIndexKey "xt$iid" page-idx))]
        (when (.getObject bloom-rdr bloom-vec-idx)
          (bloom/bloom->bitmap bloom-rdr bloom-vec-idx)))))

  AutoCloseable
  (close [_]
    (when (zero? (.decrementAndGet ref-count))
      (util/close meta-rel-reader)
      (util/close buf))))

(defn ->table-metadata ^xtdb.metadata.ITableMetadata [^IBufferPool buffer-pool ^Path file-path]
  (util/with-close-on-catch [^ArrowBuf buf @(.getBuffer buffer-pool file-path)]
    (let [{:keys [^VectorLoader loader ^VectorSchemaRoot root arrow-blocks]} (util/read-arrow-buf buf)
          nodes-vec (.getVector root "nodes")]
      (with-open [record-batch (util/->arrow-record-batch-view (first arrow-blocks) buf)]
        (.load loader record-batch)
        (let [rdr (vr/<-root root)
              ^IVectorReader metadata-reader (-> (.readerForName rdr "nodes")
                                                 (.legReader :leaf))
              {:keys [col-names page-idx-cache]} (->table-metadata-idxs metadata-reader)]
          (->TableMetadata (ArrowHashTrie. nodes-vec) rdr buf metadata-reader col-names page-idx-cache (AtomicInteger. 1)))))))

(deftype MetadataManager [^IBufferPool buffer-pool
                          ^Cache table-metadata-cache
                          ^NavigableMap chunks-metadata
                          ^:volatile-mutable ^Map fields]
  IMetadataManager
  (finishChunk [this chunk-idx new-chunk-metadata]
    (-> @(.putObject buffer-pool (->chunk-metadata-obj-key chunk-idx) (write-chunk-metadata new-chunk-metadata))
        (util/rethrowing-cause))
    (set! (.fields this) (merge-fields fields new-chunk-metadata))
    (.put chunks-metadata chunk-idx new-chunk-metadata))

  (openTableMetadata [_ file-path]
    (let [{:keys [^AtomicInteger ref-count] :as table-metadata}
          (.get table-metadata-cache file-path (reify Function
                                                 (apply [_ _]
                                                   (->table-metadata buffer-pool file-path))))]
      (.incrementAndGet ref-count)
      table-metadata))

  (chunksMetadata [_] chunks-metadata)
  (columnField [_ table-name col-name] (get-in fields [table-name col-name]))
  (columnFields [_ table-name] (get fields table-name))
  (allColumnFields [_] fields)

  AutoCloseable
  (close [_]
    (.clear chunks-metadata)
    (util/close (.asMap table-metadata-cache))))

(defn latest-chunk-metadata [^IMetadataManager metadata-mgr]
  (some-> (.lastEntry (.chunksMetadata metadata-mgr))
          (.getValue)))

(defn- get-bytes ^java.util.concurrent.CompletableFuture #_<bytes> [^IBufferPool buffer-pool, ^Path obj-key]
  (-> (.getBuffer buffer-pool obj-key)
      (util/then-apply
       (fn [^ArrowBuf buffer]
         (assert buffer)

         (try
           (let [bb (.nioBuffer buffer 0 (.capacity buffer))
                 ba (byte-array (.remaining bb))]
             (.get bb ba)
             ba)
           (finally
             (.close buffer)))))))

(defn- load-chunks-metadata ^java.util.NavigableMap [{:keys [^IBufferPool buffer-pool]}]
  (let [cm (TreeMap.)]
    (doseq [cm-obj-key (.listObjects buffer-pool chunk-metadata-path)]
      (with-open [is (ByteArrayInputStream. @(get-bytes buffer-pool cm-obj-key))]
        (let [rdr (transit/reader is :json {:handlers (merge serde/transit-read-handlers
                                                             arrow-read-handlers)})]
          (.put cm (obj-key->chunk-idx cm-obj-key) (transit/read rdr)))))
    cm))

(defmethod ig/prep-key ::metadata-manager [_ opts]
  (merge {:buffer-pool (ig/ref :xtdb/buffer-pool)}
         opts))

(defmethod ig/init-key ::metadata-manager [_ {:keys [cache-size ^IBufferPool buffer-pool], :or {cache-size 128} :as deps}]
  (let [chunks-metadata (load-chunks-metadata deps)
        table-metadata-cache (-> (Caffeine/newBuilder)
                                 (.maximumSize cache-size)
                                 (.removalListener (reify RemovalListener
                                                     (onRemoval [_ _path table-metadata _reason]
                                                       (util/close table-metadata))))
                                 (.build))]
    (MetadataManager. buffer-pool
                      table-metadata-cache
                      chunks-metadata
                      (->> (vals chunks-metadata) (reduce merge-fields {})))))

(defmethod ig/halt-key! ::metadata-manager [_ mgr]
  (util/try-close mgr))
