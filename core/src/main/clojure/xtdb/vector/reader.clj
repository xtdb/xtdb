(ns xtdb.vector.reader
  (:require [clojure.set :as set]
            [xtdb.types :as types]
            [xtdb.util :as util])
  (:import clojure.lang.MapEntry
           (org.apache.arrow.memory BufferAllocator)
           (org.apache.arrow.vector BigIntVector BitVector DateDayVector DateMilliVector DecimalVector DurationVector FixedSizeBinaryVector Float4Vector Float8Vector IntVector IntervalDayVector IntervalMonthDayNanoVector IntervalYearVector NullVector SmallIntVector TimeMicroVector TimeMilliVector TimeNanoVector TimeSecVector TimeStampMicroTZVector TimeStampMicroVector TimeStampMilliTZVector TimeStampMilliVector TimeStampNanoTZVector TimeStampNanoVector TimeStampSecTZVector TimeStampSecVector TinyIntVector VarBinaryVector VarCharVector VectorSchemaRoot)
           (org.apache.arrow.vector.complex DenseUnionVector ListVector MapVector StructVector)
           xtdb.api.query.IKeyFn
           (xtdb.vector IVectorReader RelationReader ValueVectorReader)
           (xtdb.vector.extensions AbsentVector KeywordVector SetVector TransitVector UriVector UuidVector)))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defprotocol ReaderFactory
  (^xtdb.vector.IVectorReader vec->reader [arrow-vec]))

(extend-protocol ReaderFactory
  NullVector (vec->reader [arrow-vec] (ValueVectorReader/nullVector arrow-vec))
  AbsentVector (vec->reader [arrow-vec] (ValueVectorReader/absentVector arrow-vec))

  BitVector (vec->reader [arrow-vec] (ValueVectorReader/bitVector arrow-vec))
  TinyIntVector (vec->reader [arrow-vec] (ValueVectorReader/tinyIntVector arrow-vec))
  SmallIntVector (vec->reader [arrow-vec] (ValueVectorReader/smallIntVector arrow-vec))
  IntVector (vec->reader [arrow-vec] (ValueVectorReader/intVector arrow-vec))
  BigIntVector (vec->reader [arrow-vec] (ValueVectorReader/bigIntVector arrow-vec))
  Float4Vector (vec->reader [arrow-vec] (ValueVectorReader/float4Vector arrow-vec))
  Float8Vector (vec->reader [arrow-vec] (ValueVectorReader/float8Vector arrow-vec))

  DecimalVector (vec->reader [arrow-vec] (ValueVectorReader. arrow-vec))

  VarCharVector (vec->reader [arrow-vec] (ValueVectorReader/varCharVector arrow-vec))
  VarBinaryVector (vec->reader [arrow-vec] (ValueVectorReader/varBinaryVector arrow-vec))
  FixedSizeBinaryVector (vec->reader [arrow-vec] (ValueVectorReader/fixedSizeBinaryVector arrow-vec))

  DateDayVector (vec->reader [arrow-vec] (ValueVectorReader/dateDayVector arrow-vec))
  DateMilliVector (vec->reader [arrow-vec] (ValueVectorReader/dateMilliVector arrow-vec))

  TimeStampSecTZVector (vec->reader [arrow-vec] (ValueVectorReader/timestampSecTzVector arrow-vec))
  TimeStampMilliTZVector (vec->reader [arrow-vec] (ValueVectorReader/timestampMilliTzVector arrow-vec))
  TimeStampMicroTZVector (vec->reader [arrow-vec] (ValueVectorReader/timestampMicroTzVector arrow-vec))
  TimeStampNanoTZVector (vec->reader [arrow-vec] (ValueVectorReader/timestampNanoTzVector arrow-vec))

  TimeStampSecVector (vec->reader [arrow-vec] (ValueVectorReader/timestampVector arrow-vec))
  TimeStampMilliVector (vec->reader [arrow-vec] (ValueVectorReader/timestampVector arrow-vec))
  TimeStampMicroVector (vec->reader [arrow-vec] (ValueVectorReader/timestampVector arrow-vec))
  TimeStampNanoVector (vec->reader [arrow-vec] (ValueVectorReader/timestampVector arrow-vec))

  TimeSecVector (vec->reader [arrow-vec] (ValueVectorReader/timeSecVector arrow-vec))
  TimeMilliVector (vec->reader [arrow-vec] (ValueVectorReader/timeMilliVector arrow-vec))
  TimeMicroVector (vec->reader [arrow-vec] (ValueVectorReader/timeMicroVector arrow-vec))
  TimeNanoVector (vec->reader [arrow-vec] (ValueVectorReader/timeNanoVector arrow-vec))

  IntervalYearVector (vec->reader [arrow-vec] (ValueVectorReader/intervalYearVector arrow-vec))
  IntervalDayVector (vec->reader [arrow-vec] (ValueVectorReader/intervalDayVector arrow-vec))
  IntervalMonthDayNanoVector (vec->reader [arrow-vec] (ValueVectorReader/intervalMdnVector arrow-vec))

  DurationVector (vec->reader [arrow-vec] (ValueVectorReader/durationVector arrow-vec))

  StructVector (vec->reader [arrow-vec] (ValueVectorReader/structVector arrow-vec))
  ListVector (vec->reader [arrow-vec] (ValueVectorReader/listVector arrow-vec))
  MapVector (vec->reader [arrow-vec] (ValueVectorReader/mapVector arrow-vec))
  SetVector (vec->reader [arrow-vec] (ValueVectorReader/setVector arrow-vec))
  DenseUnionVector (vec->reader [arrow-vec] (ValueVectorReader/denseUnionVector arrow-vec))

  KeywordVector (vec->reader [arrow-vec] (ValueVectorReader/keywordVector arrow-vec))
  UuidVector (vec->reader [arrow-vec] (ValueVectorReader/uuidVector arrow-vec))
  UriVector (vec->reader [arrow-vec] (ValueVectorReader/uriVector arrow-vec))
  TransitVector (vec->reader [arrow-vec] (ValueVectorReader/transitVector arrow-vec)))

(defn rel-reader
  (^xtdb.vector.RelationReader [cols] (RelationReader/from cols))
  (^xtdb.vector.RelationReader [cols ^long row-count] (RelationReader/from cols row-count)))

(defn <-root ^xtdb.vector.RelationReader [^VectorSchemaRoot root]
  (rel-reader (map vec->reader (.getFieldVectors root))
              (.getRowCount root)))

(defn ->absent-col [col-name allocator row-count]
  (vec->reader (doto (-> (types/col-type->field col-name :absent)
                         (.createVector allocator))
                 (.setValueCount row-count))))

;; we don't allocate anything here, but we need it because BaseValueVector
;; (a distant supertype of AbsentVector) thinks it needs one.
(defn with-absent-cols ^xtdb.vector.RelationReader [^RelationReader rel, ^BufferAllocator allocator, col-names]
  (let [row-count (.rowCount rel)
        available-col-names (into #{} (map #(.getName ^IVectorReader %)) rel)]
    (rel-reader (concat rel
                        (->> (set/difference col-names available-col-names)
                             (map #(->absent-col % allocator row-count))))
                (.rowCount rel))))

(defn rel->rows
  (^java.util.List [^RelationReader rel] (rel->rows rel #xt/key-fn :kebab-case-keyword))
  (^java.util.List [^RelationReader rel ^IKeyFn key-fn]
   (let [col-ks (for [^IVectorReader col rel]
                  [col (.denormalize key-fn (.getName col))])]
     (mapv (fn [idx]
             (->> col-ks
                  (into {} (keep (fn [[^IVectorReader col k]]
                                   (when-not (.isAbsent col idx)
                                     (MapEntry/create k (.getObject col idx key-fn))))))))
           (range (.rowCount rel))))))
