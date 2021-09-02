(ns core2.types.json
  (:require [clojure.data.json :as json]
            [clojure.spec.alpha :as s])
  (:import [java.nio.charset StandardCharsets]
           [java.nio ByteBuffer]
           [java.util Date List Map]
           [java.time Duration Instant LocalDate LocalTime]
           [java.time.temporal ChronoField]
           [org.apache.arrow.vector.types Types$MinorType]
           [org.apache.arrow.vector.complex.writer BaseWriter$ListWriter BaseWriter$ScalarWriter BaseWriter$StructWriter]
           [org.apache.arrow.vector.complex Positionable]
           [org.apache.arrow.vector.complex.impl UnionWriter]
           [org.apache.arrow.memory BufferAllocator]))

(defn- kw-name ^String [x]
  (if (keyword? x)
    (subs (str x) 1)
    (str x)))

(defn- advance-writer [^Positionable writer]
  (.setPosition writer (inc (.getPosition writer))))

(defmulti append-writer (fn [allocator writer parent-type k x]
                          [parent-type (class x)]))

(defmethod append-writer [nil nil] [_ ^BaseWriter$ScalarWriter writer _ _ x]
  (doto writer
    (.writeNull)
    (advance-writer)))

(defmethod append-writer [Types$MinorType/LIST nil] [_ ^BaseWriter$ListWriter writer _ _ x]
  (doto writer
    (-> (.bit) (.writeNull))))

(defmethod append-writer [Types$MinorType/STRUCT nil] [_ ^BaseWriter$StructWriter writer _ k x]
  (doto writer
    (-> (.bit k) (.writeNull))))

(defmethod append-writer [nil Boolean] [_ ^BaseWriter$ScalarWriter writer _ _ x]
  (doto writer
    (.writeBit (if x 1 0))
    (advance-writer)))

(defmethod append-writer [Types$MinorType/LIST Boolean] [_ ^BaseWriter$ListWriter writer _ _ x]
  (doto writer
    (-> (.bit) (.writeBit (if x 1 0)))))

(defmethod append-writer [Types$MinorType/STRUCT Boolean] [_ ^BaseWriter$StructWriter writer _ k x]
  (doto writer
    (-> (.bit k) (.writeBit (if x 1 0)))))

(defmethod append-writer [nil Byte] [_ ^BaseWriter$ScalarWriter writer _ _ x]
  (doto writer
    (.writeTinyInt x)
    (advance-writer)))

(defmethod append-writer [Types$MinorType/LIST Byte] [_ ^BaseWriter$ListWriter writer _ _ x]
  (doto writer
    (-> (.tinyInt) (.writeTinyInt x))))

(defmethod append-writer [Types$MinorType/STRUCT Byte] [_ ^BaseWriter$StructWriter writer _ k x]
  (doto writer
    (-> (.tinyInt k) (.writeTinyInt x))))

(defmethod append-writer [nil Short] [_ ^BaseWriter$ScalarWriter writer _ _ x]
  (doto writer
    (.writeSmallInt x)
    (advance-writer)))

(defmethod append-writer [Types$MinorType/LIST Short] [_ ^BaseWriter$ListWriter writer _ _ x]
  (doto writer
    (-> (.smallInt) (.writeSmallInt x))))

(defmethod append-writer [Types$MinorType/STRUCT Short] [_ ^BaseWriter$StructWriter writer _ k x]
  (doto writer
    (-> (.smallInt k) (.writeSmallInt x))))

(defmethod append-writer [nil Integer] [_ ^BaseWriter$ScalarWriter writer _ _ x]
  (doto writer
    (.writeInt x)
    (advance-writer)))

(defmethod append-writer [Types$MinorType/LIST Integer] [_ ^BaseWriter$ListWriter writer _ _ x]
  (doto writer
    (-> (.integer) (.writeInt x))))

(defmethod append-writer [Types$MinorType/STRUCT Integer] [_ ^BaseWriter$StructWriter writer _ k x]
  (doto writer
    (-> (.integer k) (.writeInt x))))

(defmethod append-writer [nil Long] [_ ^BaseWriter$ScalarWriter writer _ _ x]
  (doto writer
    (.writeBigInt x)
    (advance-writer)))

(defmethod append-writer [Types$MinorType/LIST Long] [_ ^BaseWriter$ListWriter writer _ _ x]
  (doto writer
    (-> (.bigInt) (.writeBigInt x))))

(defmethod append-writer [Types$MinorType/STRUCT Long] [_ ^BaseWriter$StructWriter writer _ k x]
  (doto writer
    (-> (.bigInt k) (.writeBigInt x))))

(defmethod append-writer [nil Float] [_ ^BaseWriter$ScalarWriter writer _ _ x]
  (doto writer
    (.writeFloat4 x)
    (advance-writer)))

(defmethod append-writer [Types$MinorType/LIST Float] [_ ^BaseWriter$ListWriter writer _ _ x]
  (doto writer
    (-> (.float4) (.writeFloat4 x))))

(defmethod append-writer [Types$MinorType/STRUCT Float] [_ ^BaseWriter$StructWriter writer _ k x]
  (doto writer
    (-> (.float8 k) (.writeFloat8 x))))

(defmethod append-writer [nil Double] [_ ^BaseWriter$ScalarWriter writer _ _ x]
  (doto writer
    (.writeFloat8 x)
    (advance-writer)))

(defmethod append-writer [Types$MinorType/LIST Double] [_ ^BaseWriter$ListWriter writer _ _ x]
  (doto writer
    (-> (.float8) (.writeFloat8 x))))

(defmethod append-writer [Types$MinorType/STRUCT Double] [_ ^BaseWriter$StructWriter writer _ k x]
  (doto writer
    (-> (.float8 k) (.writeFloat8 x))))

(defn- write-local-date [^BaseWriter$ScalarWriter writer ^LocalDate x]
  (.writeDateMilli writer (* (.getLong x ChronoField/EPOCH_DAY) 86400000)))

(defmethod append-writer [nil LocalDate] [_ ^BaseWriter$ScalarWriter writer _ _ x]
  (doto writer
    (write-local-date x)
    (advance-writer)))

(defmethod append-writer [Types$MinorType/LIST LocalDate] [_ ^BaseWriter$ListWriter writer _ _ x]
  (doto writer
    (-> (.dateMilli) (write-local-date x))))

(defmethod append-writer [Types$MinorType/STRUCT LocalDate] [_ ^BaseWriter$StructWriter writer _ k x]
  (doto writer
    (-> (.dateMilli k) (write-local-date x))))

(defn- write-local-time [^BaseWriter$ScalarWriter writer ^LocalTime x]
  (.writeTimeMilli writer (.getLong x ChronoField/MILLI_OF_DAY)))

(defmethod append-writer [nil LocalTime] [_ ^BaseWriter$ScalarWriter writer _ _ x]
  (doto writer
    (write-local-time x)
    (advance-writer)))

(defmethod append-writer [Types$MinorType/LIST LocalTime] [_ ^BaseWriter$ListWriter writer _ _ x]
  (doto writer
    (-> (.timeMilli) (write-local-time x))))

(defmethod append-writer [Types$MinorType/STRUCT LocalTime] [_ ^BaseWriter$StructWriter writer _ k x]
  (doto writer
    (-> (.timeMilli k) (write-local-time x))))

(defn- write-instant [^BaseWriter$ScalarWriter writer ^Instant x]
  (.writeTimeStampMilli writer (.toEpochMilli x)))

(defmethod append-writer [nil Date] [_ ^BaseWriter$ScalarWriter writer _ _ x]
  (doto writer
    (write-instant (.toInstant ^Date x))
    (advance-writer)))

(defmethod append-writer [Types$MinorType/LIST Date] [_ ^BaseWriter$ListWriter writer _ _ x]
  (doto writer
    (-> (.timeStampMilli) (write-instant (.toInstant ^Date x)))))

(defmethod append-writer [Types$MinorType/STRUCT Date] [_ ^BaseWriter$StructWriter writer _ k x]
  (doto writer
    (-> (.timeStampMilli k) (write-instant (.toInstant ^Date x)))))

(defmethod append-writer [nil Instant] [_ ^BaseWriter$ScalarWriter writer _ _ x]
  (doto writer
    (write-instant x)
    (advance-writer)))

(defmethod append-writer [Types$MinorType/LIST Instant] [_ ^BaseWriter$ListWriter writer _ _ x]
  (doto writer
    (-> (.timeStampMilli) (write-instant x))))

(defmethod append-writer [Types$MinorType/STRUCT Instant] [_ ^BaseWriter$StructWriter writer _ k x]
  (doto writer
    (-> (.timeStampMilli k) (write-instant x))))

(defn- write-duration [^BaseWriter$ScalarWriter writer ^Duration x]
  (let [ms (.toMillis x)]
    (.writeIntervalDay writer
                       (/ ms 86400000)
                       (rem ms 86400000))))

(defmethod append-writer [nil Duration] [_ ^BaseWriter$ScalarWriter writer _ _ x]
  (doto writer
    (write-duration x)
    (advance-writer)))

(defmethod append-writer [Types$MinorType/LIST Duration] [_ ^BaseWriter$ListWriter writer _ _ x]
  (doto writer
    (-> (.intervalDay) (write-duration x))))

(defmethod append-writer [Types$MinorType/STRUCT Duration] [_ ^BaseWriter$StructWriter writer _ k x]
  (let [ms (.toMillis ^Duration x)]
    (doto writer
      (-> (.intervalDay k) (write-duration x)))))

(defn- write-varchar [^BufferAllocator allocator ^BaseWriter$ScalarWriter writer ^CharSequence x]
  (let [bs (.getBytes (str x) StandardCharsets/UTF_8)
        len (alength bs)]
    (with-open [buf (.buffer allocator len)]
      (.setBytes buf 0 bs)
      (.writeVarChar writer 0 len buf))))

(defmethod append-writer [nil Character] [^BufferAllocator allocator ^BaseWriter$ScalarWriter writer _ _ x]
  (write-varchar allocator writer (str x))
  (doto writer
    (advance-writer)))

(defmethod append-writer [Types$MinorType/LIST Character] [^BufferAllocator allocator ^BaseWriter$ListWriter writer _ _ x]
  (write-varchar allocator (.varChar writer) (str x))
  writer)

(defmethod append-writer [Types$MinorType/STRUCT Character] [^BufferAllocator allocator ^BaseWriter$StructWriter writer _ k x]
  (write-varchar allocator (.varChar writer k) (str x))
  writer)

(defmethod append-writer [nil CharSequence] [^BufferAllocator allocator ^BaseWriter$ScalarWriter writer _ _ x]
  (write-varchar allocator writer x)
  (doto writer
    (advance-writer)))

(defmethod append-writer [Types$MinorType/LIST CharSequence] [^BufferAllocator allocator ^BaseWriter$ListWriter writer _ _ x]
  (write-varchar allocator (.varChar writer) x)
  writer)

(defmethod append-writer [Types$MinorType/STRUCT CharSequence] [^BufferAllocator allocator ^BaseWriter$StructWriter writer _ k x]
  (write-varchar allocator (.varChar writer k) x)
  writer)

(defn- write-varbinary [^BufferAllocator allocator ^BaseWriter$ScalarWriter writer ^ByteBuffer x]
  (let [len (.remaining x)]
    (with-open [buf (.buffer allocator len)]
      (.setBytes buf 0 (.duplicate x))
      (.writeVarBinary writer 0 len buf))))

(defmethod append-writer [nil (Class/forName "[B")] [^BufferAllocator allocator ^BaseWriter$ScalarWriter writer _ _ x]
  (write-varbinary allocator writer (ByteBuffer/wrap x))
  (doto writer
    (advance-writer)))

(defmethod append-writer [Types$MinorType/LIST (Class/forName "[B")] [^BufferAllocator allocator ^BaseWriter$ListWriter writer _ _ x]
  (write-varbinary allocator (.varBinary writer) (ByteBuffer/wrap x))
  writer)

(defmethod append-writer [Types$MinorType/STRUCT (Class/forName "[B")] [^BufferAllocator allocator ^BaseWriter$StructWriter writer _ k x]
  (write-varbinary allocator (.varBinary writer k) (ByteBuffer/wrap x))
  writer)

(defmethod append-writer [nil ByteBuffer] [^BufferAllocator allocator ^BaseWriter$ScalarWriter writer _ _ x]
  (write-varbinary allocator writer x)
  (doto writer
    (advance-writer)))

(defmethod append-writer [Types$MinorType/LIST ByteBuffer] [^BufferAllocator allocator ^BaseWriter$ListWriter writer _ _ x]
  (write-varbinary allocator (.varBinary writer) x)
  writer)

(defmethod append-writer [Types$MinorType/STRUCT ByteBuffer] [^BufferAllocator allocator ^BaseWriter$StructWriter writer _ k x]
  (write-varbinary allocator (.varBinary writer k) x)
  writer)

(defn- write-list [allocator ^BaseWriter$ListWriter list-writer x]
  (.startList list-writer)
  (doseq [v x]
    (append-writer allocator list-writer Types$MinorType/LIST nil v))
  (.endList list-writer))

(defmethod append-writer [nil List] [allocator ^UnionWriter writer _ _ x]
  (write-list allocator (.asList writer) x)
  (doto writer
    (advance-writer)))

(defmethod append-writer [Types$MinorType/LIST List] [allocator ^BaseWriter$ListWriter writer _ _ x]
  (write-list allocator (.list writer) x)
  writer)

(defmethod append-writer [Types$MinorType/STRUCT List] [allocator ^BaseWriter$StructWriter writer _ k x]
  (write-list allocator (.list writer k) x)
  writer)

(defn- write-struct [allocator ^BaseWriter$StructWriter struct-writer x]
  (.start struct-writer)
  (doseq [[k v] x]
    (append-writer allocator struct-writer Types$MinorType/STRUCT (kw-name k) v))
  (.end struct-writer))

(defmethod append-writer [nil Map] [allocator ^UnionWriter writer _ _ x]
  (write-struct allocator (.asStruct writer) x)
  (doto writer
    (advance-writer)))

(defmethod append-writer [Types$MinorType/LIST Map] [allocator ^BaseWriter$ListWriter writer _ _ x]
  (write-struct allocator (.struct writer) x)
  writer)

(defmethod append-writer [Types$MinorType/STRUCT Map] [allocator ^BaseWriter$StructWriter writer _ k x]
  (write-struct allocator (.struct writer (kw-name k)) x)
  writer)

;; The below is currently unused, assumes a more manual mapping to
;; Arrow than using UnionWriter directly as above.

(s/def :json/null nil?)
(s/def :json/boolean boolean?)
(s/def :json/int int?)
(s/def :json/float float?)
(s/def :json/string string?)

(s/def :json/array (s/coll-of :json/value :kind vector?))
(s/def :json/object (s/map-of keyword? :json/value))

(s/def :json/value (s/or :json/null :json/null
                         :json/boolean :json/boolean
                         :json/int :json/int
                         :json/float :json/float
                         :json/string :json/string
                         :json/array :json/array
                         :json/object :json/object))

(def ^:private json-hierarchy
  (-> (make-hierarchy)
      (derive :json/int :json/number)
      (derive :json/float :json/number)
      (derive :json/null :json/scalar)
      (derive :json/boolean :json/scalar)
      (derive :json/number :json/scalar)
      (derive :json/string :json/scalar)
      (derive :json/array :json/value)
      (derive :json/object :json/value)
      (derive :json/scalar :json/value)))

(defn type-kind [[tag x]]
  (cond
    (isa? json-hierarchy tag :json/scalar)
    {:kind :json/scalar}
    (= :json/array tag)
    {:kind :json/array}
    (= :json/object tag)
    {:kind :json/object
     :keys (set (keys x))}))

(def json->arrow {:json/null Types$MinorType/NULL
                  :json/boolean Types$MinorType/BIT
                  :json/int Types$MinorType/BIGINT
                  :json/float Types$MinorType/FLOAT8
                  :json/string Types$MinorType/VARCHAR
                  :json/array Types$MinorType/LIST
                  :json/object Types$MinorType/STRUCT})
