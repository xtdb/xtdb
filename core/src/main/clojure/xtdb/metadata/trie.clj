(ns xtdb.metadata.trie
  (:require [xtdb.bloom :as bloom]
            xtdb.buffer-pool
            [xtdb.expression.comparator :as expr.comp]
            xtdb.expression.temporal
            [xtdb.types :as types])
  (:import (java.util HashMap)
           (java.util.function Function IntPredicate)
           (java.util.stream IntStream)
           (org.apache.arrow.vector.types.pojo ArrowType$Binary ArrowType$Bool ArrowType$Date ArrowType$FixedSizeBinary ArrowType$FloatingPoint ArrowType$Int ArrowType$Interval ArrowType$List ArrowType$Null ArrowType$Struct ArrowType$Time ArrowType$Time ArrowType$Timestamp ArrowType$Union ArrowType$Utf8 Field FieldType)
           (xtdb.arrow VectorReader VectorWriter)
           (xtdb.vector.extensions KeywordType SetType TransitType TsTzRangeType UriType UuidType)))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface IPageMetadataWriter
  (^void writeMetadata [^Iterable cols]))

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
  (^xtdb.metadata.trie.ContentMetadataWriter appendNestedMetadata [^xtdb.arrow.VectorReader contentCol]))

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
  (type->metadata-writer [arrow-type write-col-meta! ^VectorWriter types-wtr]
    (let [struct-type-wtr (.keyWriter types-wtr
                                      (str (name (types/arrow-type->leg arrow-type)) "-" (count (seq types-wtr)))
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

(defn ->page-meta-wtr ^xtdb.metadata.trie.IPageMetadataWriter [^VectorWriter cols-wtr]
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

      (reify IPageMetadataWriter
        (writeMetadata [_ cols]
          (doseq [^VectorReader col cols
                  :when (pos? (.getValueCount col))]
            (write-col-meta! true col))
          (.endList cols-wtr))))))
