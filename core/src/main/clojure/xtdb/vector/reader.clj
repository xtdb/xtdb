(ns xtdb.vector.reader
  (:require [clojure.set :as set]
            [xtdb.types :as types])
  (:import (org.apache.arrow.memory BufferAllocator)
           (org.apache.arrow.vector BigIntVector BitVector DateDayVector DateMilliVector DecimalVector DurationVector FixedSizeBinaryVector Float4Vector Float8Vector IntVector IntervalDayVector IntervalMonthDayNanoVector IntervalYearVector NullVector SmallIntVector TimeMicroVector TimeMilliVector TimeNanoVector TimeSecVector TimeStampMicroTZVector TimeStampMicroVector TimeStampMilliTZVector TimeStampMilliVector TimeStampNanoTZVector TimeStampNanoVector TimeStampSecTZVector TimeStampSecVector TinyIntVector ValueVector VarBinaryVector VarCharVector VectorSchemaRoot)
           (org.apache.arrow.vector.complex DenseUnionVector ListVector StructVector)
           (xtdb.vector IVectorReader RelationReader ValueVectorReader)
           (xtdb.vector.extensions AbsentVector ClojureFormVector KeywordVector SetVector UriVector UuidVector)))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defprotocol ReaderFactory
  (^xtdb.vector.IVectorReader vec->reader [arrow-vec]))

(defn- arrow-vec->leg [^ValueVector v]
  (types/col-type->leg (types/field->col-type (.getField v))))

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

  ;; TODO specialise VVR for these vecs
  TimeStampSecVector (vec->reader [arrow-vec] (ValueVectorReader. arrow-vec))
  TimeStampMilliVector (vec->reader [arrow-vec] (ValueVectorReader. arrow-vec))
  TimeStampMicroVector (vec->reader [arrow-vec] (ValueVectorReader. arrow-vec))
  TimeStampNanoVector (vec->reader [arrow-vec] (ValueVectorReader. arrow-vec))

  TimeSecVector (vec->reader [arrow-vec] (ValueVectorReader/timeSecVector arrow-vec))
  TimeMilliVector (vec->reader [arrow-vec] (ValueVectorReader/timeMilliVector arrow-vec))
  TimeMicroVector (vec->reader [arrow-vec] (ValueVectorReader/timeMicroVector arrow-vec))
  TimeNanoVector (vec->reader [arrow-vec] (ValueVectorReader/timeNanoVector arrow-vec))

  IntervalYearVector (vec->reader [arrow-vec] (ValueVectorReader/intervalYearVector arrow-vec))
  IntervalDayVector (vec->reader [arrow-vec] (ValueVectorReader/intervalDayVector arrow-vec))
  IntervalMonthDayNanoVector (vec->reader [arrow-vec] (ValueVectorReader/intervalMdnVector arrow-vec))

  DurationVector (vec->reader [arrow-vec] (ValueVectorReader. arrow-vec))

  StructVector (vec->reader [arrow-vec] (ValueVectorReader/structVector arrow-vec))
  ListVector (vec->reader [arrow-vec] (ValueVectorReader/listVector arrow-vec))
  SetVector (vec->reader [arrow-vec] (ValueVectorReader/setVector arrow-vec))
  DenseUnionVector (vec->reader [arrow-vec] (ValueVectorReader/denseUnionVector arrow-vec))

  KeywordVector (vec->reader [arrow-vec] (ValueVectorReader. arrow-vec))
  UuidVector (vec->reader [arrow-vec] (ValueVectorReader. arrow-vec))
  UriVector (vec->reader [arrow-vec] (ValueVectorReader. arrow-vec))
  ClojureFormVector (vec->reader [arrow-vec] (ValueVectorReader. arrow-vec)))

(defn rel-reader
  (^xtdb.vector.RelationReader [cols] (RelationReader/from cols))
  (^xtdb.vector.RelationReader [cols ^long row-count] (RelationReader/from cols row-count)))

(defn <-root ^xtdb.vector.RelationReader [^VectorSchemaRoot root]
  (rel-reader (map vec->reader (.getFieldVectors root))
              (.getRowCount root)))

;; we don't allocate anything here, but we need it because BaseValueVector
;; (a distant supertype of AbsentVector) thinks it needs one.
(defn with-absent-cols ^xtdb.vector.RelationReader [^RelationReader rel, ^BufferAllocator allocator, col-names]
  (let [row-count (.rowCount rel)
        available-col-names (into #{} (map #(.getName ^IVectorReader %)) rel)]
    (rel-reader (concat rel
                          (for [absent-col-name (set/difference col-names available-col-names)]
                            (vec->reader (doto (-> (types/col-type->field absent-col-name :absent)
                                                   (.createVector allocator))
                                           (.setValueCount row-count)))))
                  (.rowCount rel))))

(defn rel->rows ^java.lang.Iterable [^RelationReader rel]
  (let [ks (for [^IVectorReader col rel]
             (keyword (.getName col)))]
    (mapv (fn [idx]
            (->> (zipmap ks
                         (for [^IVectorReader col rel]
                           (.getObject col idx)))
                 (into {} (remove (comp #(= :xtdb/absent %) val)))))
          (range (.rowCount rel)))))

