(ns xtdb.vector.writer
  (:require [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr])
  (:import (clojure.lang Keyword)
           (java.math BigDecimal)
           java.net.URI
           (java.nio ByteBuffer)
           (java.time Duration Instant LocalDate LocalDateTime LocalTime OffsetDateTime ZonedDateTime)
           (java.util Date List Map Set UUID)
           (org.apache.arrow.memory BufferAllocator)
           (org.apache.arrow.vector ValueVector VectorSchemaRoot)
           (org.apache.arrow.vector.types.pojo Field FieldType)
           (xtdb.arrow Relation RelationReader RelationWriter)
           xtdb.error.Anomaly
           xtdb.time.Interval
           xtdb.Types
           (xtdb.types ClojureForm ZonedDateTimeRange)
           (xtdb.vector FieldVectorWriters RootWriter)))

(set! *unchecked-math* :warn-on-boxed)

(defn ->writer ^xtdb.vector.IVectorWriter [arrow-vec]
  (FieldVectorWriters/writerFor arrow-vec))

(defn value->arrow-type [v] (Types/valueToArrowType v))

(defprotocol ArrowWriteable
  (value->col-type [v]))

(extend-protocol ArrowWriteable
  nil
  (value->col-type [_] :null)

  Boolean
  (value->col-type [_] :bool)

  Byte
  (value->col-type [_] :i8)

  Short
  (value->col-type [_] :i16)

  Integer
  (value->col-type [_] :i32)

  Long
  (value->col-type [_] :i64)

  Float
  (value->col-type [_] :f32)

  Double
  (value->col-type [_] :f64)

  BigDecimal
  (value->col-type [dec]
    (let [precision (* 32 (long (Math/ceil (/ (long (.precision dec)) 32.0))))]
      [:decimal precision (.scale dec) (* precision 4)])))

(extend-protocol ArrowWriteable
  Date
  (value->col-type [_] [:timestamp-tz :micro "UTC"])

  Instant
  (value->col-type [_] [:timestamp-tz :micro "UTC"])

  ZonedDateTime
  (value->col-type [v] [:timestamp-tz :micro (.getId (.getZone v))])

  OffsetDateTime
  (value->col-type [v] [:timestamp-tz :micro (.getId (.getOffset v))])

  LocalDateTime
  (value->col-type [_] [:timestamp-local :micro])

  Duration
  (value->col-type [_] [:duration :micro])

  LocalDate
  (value->col-type [_] [:date :day])

  LocalTime
  (value->col-type [_] [:time-local :nano])

  Interval
  (value->col-type [v]
    (types/<-arrow-type (value->arrow-type v)))

  ZonedDateTimeRange
  (value->col-type [_] :tstz-range))

(extend-protocol ArrowWriteable
  (Class/forName "[B")
  (value->col-type [_] :varbinary)

  ByteBuffer
  (value->col-type [_] :varbinary)

  CharSequence
  (value->col-type [_] :utf8))

(extend-protocol ArrowWriteable
  List
  (value->col-type [v] [:list (apply types/merge-col-types (into #{} (map value->col-type) v))])

  Set
  (value->col-type [v] [:set (apply types/merge-col-types (into #{} (map value->col-type) v))])

  Map
  (value->col-type [v]
    (if (or (every? keyword? (keys v)) (every? string? (keys v)))
      [:struct (->> v
                    (into {} (map (juxt (comp util/symbol->normal-form-symbol symbol key)
                                        (comp value->col-type val)))))]

      #_[:map
         (apply types/merge-col-types (into #{} (map (comp value->col-type key)) v))
         (apply types/merge-col-types (into #{} (map (comp value->col-type val)) v))]
      (throw (UnsupportedOperationException. "Arrow Maps currently not supported")))))

(extend-protocol ArrowWriteable
  Keyword
  (value->col-type [_] :keyword)

  UUID
  (value->col-type [_] :uuid)

  URI
  (value->col-type [_] :uri)

  ClojureForm
  (value->col-type [_] :transit)

  Anomaly
  (value->col-type [_] :transit))

(defn write-vec! [^ValueVector v, vs]
  (.clear v)

  (let [writer (->writer v)]
    (doseq [v vs]
      (.writeObject writer v))

    (.syncValueCount writer)

    v))

(defn rows->fields [rows]
  (->> (for [col-name (into #{} (mapcat keys) rows)]
         [(symbol col-name) (-> rows
                                (->> (into #{} (map (fn [row]
                                                      (types/col-type->field (value->col-type (get row col-name))))))
                                     (apply types/merge-fields))
                                (types/field-with-name (str (symbol col-name))))])
       (into {})))

(defn ->vec-writer ^xtdb.vector.IVectorWriter [^BufferAllocator allocator, ^String col-name, ^FieldType field-type]
  (->writer (.createNewSingleVector field-type col-name allocator nil)))

(defn ->rel-writer ^xtdb.arrow.RelationWriter [^BufferAllocator allocator]
  (xtdb.vector.RelationWriter. allocator))

(defn root->writer ^xtdb.arrow.RelationWriter [^VectorSchemaRoot root]
  (RootWriter. root))

(defmulti open-vec (fn [_allocator col-name-or-field _vs]
                     (if (instance? Field col-name-or-field)
                       :field
                       :col-name)))

(alter-meta! #'open-vec assoc :tag ValueVector)

(defmethod open-vec :col-name [^BufferAllocator allocator, ^String col-name, vs]
  (util/with-close-on-catch [res (-> (FieldType/notNullable #xt.arrow/type :union)
                                     (.createNewSingleVector (str col-name) allocator nil))]
    (doto res (write-vec! vs))))

(defmethod open-vec :field [allocator ^Field field vs]
  (util/with-close-on-catch [res (.createVector field allocator)]
    (doto res (write-vec! vs))))

(defn open-args ^xtdb.arrow.RelationReader [allocator args]
  (let [args-map (->> args
                      (into {} (map-indexed (fn [idx v]
                                              (if (map-entry? v)
                                                {(symbol (str "?" (symbol (key v)))) (val v)}
                                                {(symbol (str "?_" idx)) v})))))]

    (Relation/openFromRows allocator [args-map])))

(def empty-args RelationReader/DUAL)

(defn open-rel ^xtdb.arrow.Relation [^BufferAllocator al]
  (Relation. al))

(defn vec-wtr->rdr ^xtdb.arrow.VectorReader [^xtdb.arrow.VectorWriter w]
  (.getAsReader w))

(defn rel-wtr->rdr ^xtdb.arrow.RelationReader [^xtdb.arrow.RelationWriter w]
  (.getAsReader w))

(defn append-rel [^RelationWriter dest-rel, ^RelationReader src-rel]
  (.append dest-rel src-rel))
