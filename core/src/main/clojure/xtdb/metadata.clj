(ns xtdb.metadata
  (:require [integrant.core :as ig]
            [xtdb.bloom :as bloom]
            xtdb.buffer-pool
            [xtdb.expression.comparator :as expr.comp]
            xtdb.expression.temporal
            [xtdb.trie :as  trie]
            [xtdb.types :as types]
            [xtdb.util :as util])
  (:import (com.github.benmanes.caffeine.cache Cache Caffeine)
           java.lang.AutoCloseable
           (java.nio.file Path)
           (java.util HashMap HashSet Map)
           (java.util.function Function IntPredicate)
           (java.util.stream IntStream)
           (org.apache.arrow.memory BufferAllocator)
           (org.apache.arrow.vector.types.pojo ArrowType$Binary ArrowType$Bool ArrowType$Date ArrowType$FixedSizeBinary ArrowType$FloatingPoint ArrowType$Int ArrowType$Interval ArrowType$List ArrowType$Null ArrowType$Struct ArrowType$Time ArrowType$Time ArrowType$Timestamp ArrowType$Union ArrowType$Utf8 Field FieldType)
           (xtdb.arrow Relation Vector VectorReader VectorWriter)
           xtdb.BufferPool
           (xtdb.metadata ITableMetadata PageIndexKey PageMetadataWriter)
           (xtdb.trie ArrowHashTrie HashTrie)
           (xtdb.util TemporalBounds TemporalDimension)
           (xtdb.vector IVectorReader)
           (xtdb.vector.extensions KeywordType SetType TransitType TsTzRangeType UriType UuidType)))

(set! *unchecked-math* :warn-on-boxed)

(definterface IMetadataManager
  (^xtdb.metadata.ITableMetadata openTableMetadata [^java.nio.file.Path metaFilePath]))

(definterface IMetadataPredicate
  (^java.util.function.IntPredicate build [^xtdb.metadata.ITableMetadata tableMetadata]))

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
  (^xtdb.metadata.ContentMetadataWriter appendNestedMetadata [^xtdb.arrow.VectorReader contentCol]))

#_{:clj-kondo/ignore [:unused-binding]}
(defprotocol MetadataWriterFactory
  (type->metadata-writer [arrow-type write-col-meta! types-vec]))

(defn- ->bool-type-handler [^VectorWriter types-wtr, arrow-type]
  (let [bit-wtr (.keyWriter types-wtr (if (instance? ArrowType$FixedSizeBinary arrow-type)
                                        "fixed-size-binary"
                                        (name (types/arrow-type->leg arrow-type)))
                            (FieldType/nullable #xt.arrow/type :bool))]
    (reify NestedMetadataWriter
      (appendNestedMetadata [_ _content-col]
        (reify ContentMetadataWriter
          (writeContentMetadata [_]
            (.writeBoolean bit-wtr true)))))))

(defn- ->min-max-type-handler [^VectorWriter types-wtr, arrow-type]
  (let [struct-wtr (.keyWriter types-wtr (name (types/arrow-type->leg arrow-type)) (FieldType/nullable #xt.arrow/type :struct))

        min-wtr (.keyWriter struct-wtr "min" (FieldType/nullable arrow-type))
        max-wtr (.keyWriter struct-wtr "max" (FieldType/nullable arrow-type))]

    (reify NestedMetadataWriter
      (appendNestedMetadata [_ content-col]
        (reify ContentMetadataWriter
          (writeContentMetadata [_]

            (let [min-copier (.rowCopier content-col min-wtr)
                  max-copier (.rowCopier content-col max-wtr)

                  min-comparator (expr.comp/->comparator content-col content-col :nulls-last)
                  max-comparator (expr.comp/->comparator content-col content-col :nulls-first)]

              (loop [value-idx 0
                     min-idx -1
                     max-idx -1]
                (if (= value-idx (.getValueCount content-col))
                  (do
                    (if (neg? min-idx)
                      (.writeNull min-wtr)
                      (.copyRow min-copier min-idx))
                    (if (neg? max-idx)
                      (.writeNull max-wtr)
                      (.copyRow max-copier max-idx)))

                  (recur (inc value-idx)
                         (if (and (not (.isNull content-col value-idx))
                                  (or (neg? min-idx)
                                      (neg? (.applyAsInt min-comparator value-idx min-idx))))
                           value-idx
                           min-idx)
                         (if (and (not (.isNull content-col value-idx))
                                  (or (neg? max-idx)
                                      (pos? (.applyAsInt max-comparator value-idx max-idx))))
                           value-idx
                           max-idx))))

              (.endStruct struct-wtr))))))))

(extend-protocol MetadataWriterFactory
  ArrowType$Null (type->metadata-writer [arrow-type _write-col-meta! metadata-root] (->bool-type-handler metadata-root arrow-type))
  ArrowType$Bool (type->metadata-writer [arrow-type _write-col-meta! metadata-root] (->bool-type-handler metadata-root arrow-type))
  ArrowType$FixedSizeBinary (type->metadata-writer [arrow-type _write-col-meta! metadata-root] (->bool-type-handler metadata-root arrow-type))
  TransitType (type->metadata-writer [arrow-type _write-col-meta! metadata-root] (->bool-type-handler metadata-root arrow-type))
  TsTzRangeType (type->metadata-writer [arrow-type _write-col-meta! metadata-root] (->bool-type-handler metadata-root arrow-type)))

(extend-protocol MetadataWriterFactory
  ArrowType$Int (type->metadata-writer [arrow-type _write-col-meta! metadata-root] (->min-max-type-handler metadata-root arrow-type))
  ArrowType$FloatingPoint (type->metadata-writer [arrow-type _write-col-meta! metadata-root] (->min-max-type-handler metadata-root arrow-type))
  ArrowType$Utf8 (type->metadata-writer [arrow-type _write-col-meta! metadata-root] (->min-max-type-handler metadata-root arrow-type))
  ArrowType$Binary (type->metadata-writer [arrow-type _write-col-meta! metadata-root] (->min-max-type-handler metadata-root arrow-type))
  KeywordType (type->metadata-writer [arrow-type _write-col-meta! metadata-root] (->min-max-type-handler metadata-root arrow-type))
  UriType (type->metadata-writer [arrow-type _write-col-meta! metadata-root] (->min-max-type-handler metadata-root arrow-type))
  UuidType (type->metadata-writer [arrow-type _write-col-meta! metadata-root] (->min-max-type-handler metadata-root arrow-type))
  ArrowType$Timestamp (type->metadata-writer [arrow-type _write-col-meta! metadata-root] (->min-max-type-handler metadata-root arrow-type))
  ArrowType$Date (type->metadata-writer [arrow-type _write-col-meta! metadata-root] (->min-max-type-handler metadata-root arrow-type))
  ArrowType$Interval (type->metadata-writer [arrow-type _write-col-meta! metadata-root] (->min-max-type-handler metadata-root arrow-type))
  ArrowType$Time (type->metadata-writer [arrow-type _write-col-meta! metadata-root] (->min-max-type-handler metadata-root arrow-type)))

(extend-protocol MetadataWriterFactory
  ArrowType$List
  (type->metadata-writer [arrow-type write-col-meta! ^VectorWriter types-wtr]
    (let [list-type-wtr (.keyWriter types-wtr (name (types/arrow-type->leg arrow-type))
                                    (FieldType/nullable #xt.arrow/type :i32))]
      (reify NestedMetadataWriter
        (appendNestedMetadata [_ content-col]
          (write-col-meta! (.elementReader ^VectorReader content-col))

          (let [data-meta-idx (dec (.getValueCount types-wtr))]
            (reify ContentMetadataWriter
              (writeContentMetadata [_]
                (.writeInt list-type-wtr data-meta-idx))))))))

  SetType
  (type->metadata-writer [arrow-type write-col-meta! ^VectorWriter types-wtr]
    (let [set-type-wtr (.keyWriter types-wtr (name (types/arrow-type->leg arrow-type))
                                   (FieldType/nullable #xt.arrow/type :i32))]
      (reify NestedMetadataWriter
        (appendNestedMetadata [_ content-col]
          (write-col-meta! (.elementReader ^VectorReader content-col))

          (let [data-meta-idx (dec (.getValueCount types-wtr))]
            (reify ContentMetadataWriter
              (writeContentMetadata [_]
                (.writeInt set-type-wtr data-meta-idx))))))))

  ArrowType$Struct
  (type->metadata-writer [arrow-type write-col-meta! ^Vector types-wtr]
    (let [struct-type-wtr (.keyWriter types-wtr
                                      (str (name (types/arrow-type->leg arrow-type)) "-" (count (.getChildren types-wtr)))
                                      (FieldType/nullable #xt.arrow/type :list))
          struct-type-el-wtr (.elementWriter struct-type-wtr (FieldType/nullable #xt.arrow/type :i32))]
      (reify NestedMetadataWriter
        (appendNestedMetadata [_ content-col]
          (let [struct-keys (.getKeys content-col)
                sub-col-idxs (IntStream/builder)]

            (doseq [^String struct-key struct-keys]
              (write-col-meta! (.keyReader content-col struct-key))
              (.add sub-col-idxs (dec (.getValueCount types-wtr))))

            (reify ContentMetadataWriter
              (writeContentMetadata [_]
                (doseq [sub-col-idx (.toArray (.build sub-col-idxs))]
                  (.writeInt struct-type-el-wtr sub-col-idx))
                (.endList struct-type-wtr)))))))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn ->page-meta-wtr ^xtdb.metadata.PageMetadataWriter [^VectorWriter cols-wtr]
  (let [col-wtr (.elementWriter cols-wtr)
        col-name-wtr (.keyWriter col-wtr "col-name")
        root-col-wtr (.keyWriter col-wtr "root-col?")
        count-wtr (.keyWriter col-wtr "count")
        types-wtr (.keyWriter col-wtr "types")
        bloom-wtr (.keyWriter col-wtr "bloom")

        type-metadata-writers (HashMap.)]

    (letfn [(->nested-meta-writer [^VectorReader content-col]
              (when-let [^Field field (first (-> (.getField content-col)
                                                 (types/flatten-union-field)
                                                 (->> (remove #(= ArrowType$Null/INSTANCE (.getType ^Field %))))
                                                 (doto (-> count (<= 1) (assert (str (pr-str (.getField content-col)) "should just be nullable mono-vecs here"))))))]
                (-> ^NestedMetadataWriter
                    (.computeIfAbsent type-metadata-writers (.getType field)
                                      (reify Function
                                        (apply [_ arrow-type]
                                          (type->metadata-writer arrow-type (partial write-col-meta! false) types-wtr))))
                    (.appendNestedMetadata content-col))))

            (write-col-meta! [root-col?, ^VectorReader content-col]
              (let [content-writers (->> (if (instance? ArrowType$Union (.getType (.getField content-col)))
                                           (->> (.getLegs content-col)
                                                (mapv (fn [leg]
                                                        (->nested-meta-writer (.legReader content-col leg)))))
                                           [(->nested-meta-writer content-col)])
                                         (remove nil?))]
                (.writeBoolean root-col-wtr root-col?)
                (.writeObject col-name-wtr (.getName content-col))
                (.writeLong count-wtr (-> (IntStream/range 0 (.getValueCount content-col))
                                          (.filter (reify IntPredicate
                                                     (test [_ idx]
                                                       (not (.isNull content-col idx)))))
                                          (.count)))
                (bloom/write-bloom bloom-wtr content-col)

                (doseq [^ContentMetadataWriter content-writer content-writers]
                  (.writeContentMetadata content-writer))
                (.endStruct types-wtr)

                (.endStruct col-wtr)))]

      (reify PageMetadataWriter
        (writeMetadata [_ cols]
          (doseq [^VectorReader col cols
                  :when (pos? (.getValueCount col))]
            (write-col-meta! true col))
          (.endList cols-wtr))))))

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
      (when-not (or (.isNull metadata-rdr meta-idx)
                    (.isNull cols-rdr meta-idx))
        (let [cols-start-idx (.getListStartIndex cols-rdr meta-idx)
              data-page-idx (if-let [data-page-idx (.getObject data-page-idx-rdr meta-idx)]
                              data-page-idx
                              -1)]
          (dotimes [cols-data-idx (.getListCount cols-rdr meta-idx)]
            (let [cols-data-idx (+ cols-start-idx cols-data-idx)
                  col-name (str (.getObject column-name-rdr cols-data-idx))]
              (.add col-names col-name)
              (when (.getBoolean root-col-rdr cols-data-idx)
                (.put page-idx-cache (PageIndexKey. col-name data-page-idx) cols-data-idx)))))))

    {:col-names (into #{} col-names)
     :page-idx-cache page-idx-cache}))


(defrecord TableMetadata [^HashTrie trie
                          ^Relation meta-rel
                          ^IVectorReader metadata-leaf-rdr
                          col-names
                          ^Map page-idx-cache
                          ^IVectorReader min-rdr
                          ^IVectorReader max-rdr]
  ITableMetadata
  (metadataReader [_] metadata-leaf-rdr)
  (columnNames [_] col-names)
  (rowIndex [_ col-name page-idx] (.getOrDefault page-idx-cache (PageIndexKey. col-name page-idx) -1))

  (iidBloomBitmap [_ page-idx]
    (let [bloom-rdr (-> (.structKeyReader metadata-leaf-rdr "columns")
                        (.listElementReader)
                        (.structKeyReader "bloom"))]

      (when-let [bloom-vec-idx (.get page-idx-cache (PageIndexKey. "_iid" page-idx))]
        (when (.getObject bloom-rdr bloom-vec-idx)
          (bloom/bloom->bitmap bloom-rdr bloom-vec-idx)))))

  (temporalBounds[_ page-idx]
    (let [^long system-from-idx (.get page-idx-cache (PageIndexKey. "_system_from" page-idx))
          ^long valid-from-idx (.get page-idx-cache (PageIndexKey. "_valid_from" page-idx))
          ^long valid-to-idx (.get page-idx-cache (PageIndexKey. "_valid_to" page-idx))]
      (TemporalBounds. (TemporalDimension. (.getLong min-rdr valid-from-idx) (.getLong max-rdr valid-to-idx))
                       (TemporalDimension. (.getLong min-rdr system-from-idx) Long/MAX_VALUE)
                       (.getLong max-rdr system-from-idx))))

  AutoCloseable
  (close [_]
    (util/close meta-rel)))

(def ^:private temporal-col-type-leg-name (name (types/arrow-type->leg (types/->arrow-type [:timestamp-tz :micro "UTC"]))))

(defn ->table-metadata ^xtdb.metadata.ITableMetadata [^BufferAllocator allocator, ^BufferPool buffer-pool,
                                                      ^Path file-path, ^Cache table-metadata-idx-cache]
  (let [footer (.getFooter buffer-pool file-path)]
    (util/with-open [rb (.getRecordBatch buffer-pool file-path 0)]
      (util/with-close-on-catch [rel (Relation/fromRecordBatch allocator (.getSchema footer) rb)]
        (let [nodes-vec (.get rel "nodes")
              rdr (.getOldRelReader rel)
              ^IVectorReader metadata-reader (-> (.readerForName rdr "nodes")
                                                 (.legReader "leaf"))
              {:keys [col-names page-idx-cache]} (.get table-metadata-idx-cache file-path
                                                       (fn [_]
                                                         (->table-metadata-idxs metadata-reader)))


              temporal-col-types-rdr (some-> (.structKeyReader metadata-reader "columns")
                                             (.listElementReader)
                                             (.structKeyReader "types")
                                             (.structKeyReader temporal-col-type-leg-name))

              min-rdr (some-> temporal-col-types-rdr (.structKeyReader "min"))
              max-rdr (some-> temporal-col-types-rdr (.structKeyReader "max"))]
          (->TableMetadata (ArrowHashTrie. nodes-vec) rel metadata-reader col-names page-idx-cache min-rdr max-rdr))))))

(deftype MetadataManager [^BufferAllocator allocator
                          ^BufferPool buffer-pool
                          ^Cache table-metadata-idx-cache]
  IMetadataManager
  (openTableMetadata [_ file-path]
    (->table-metadata allocator buffer-pool file-path table-metadata-idx-cache))

  AutoCloseable
  (close [_]
    (util/close allocator)))

(defmethod ig/prep-key ::metadata-manager [_ opts]
  (merge {:allocator (ig/ref :xtdb/allocator)
          :buffer-pool (ig/ref :xtdb/buffer-pool)}
         opts))

(defmethod ig/init-key ::metadata-manager [_ {:keys [allocator, ^BufferPool buffer-pool, cache-size], :or {cache-size 128} :as deps}]
  (MetadataManager. (util/->child-allocator allocator "metadata-mgr") buffer-pool
                    (-> (Caffeine/newBuilder)
                        (.maximumSize cache-size)
                        (.build))))

(defmethod ig/halt-key! ::metadata-manager [_ mgr]
  (util/try-close mgr))
