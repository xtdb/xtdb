(ns core2.types.nested
  (:require [clojure.data.json :as json]
            [clojure.walk :as w])
  (:import [java.nio.charset StandardCharsets]
           [java.nio ByteBuffer CharBuffer]
           [java.util Date List Map]
           [java.time Duration Instant LocalDate LocalTime]
           [java.time.temporal ChronoField]
           [org.apache.arrow.vector.types Types$MinorType]
           [org.apache.arrow.vector.types.pojo Field Schema]
           [org.apache.arrow.vector.complex.writer BaseWriter$ListWriter BaseWriter$ScalarWriter BaseWriter$StructWriter]
           [org.apache.arrow.vector.complex DenseUnionVector ListVector Positionable StructVector UnionVector]
           [org.apache.arrow.vector.complex.impl UnionWriter]
           [org.apache.arrow.vector ValueVector]
           [org.apache.arrow.vector.util VectorBatchAppender]
           [org.apache.arrow.memory BufferAllocator]
           [core2 DenseUnionUtil]))

;; Type mapping aims to stay close to the Arrow JDBC adapter:
;; https://github.com/apache/arrow/blob/master/java/adapter/jdbc/src/main/java/org/apache/arrow/adapter/jdbc/JdbcToArrow.java

;; For example, unlike Arrow Java itself, TimeMilliVector is mapped to
;; LocalTime and DateMilliVector to LocalDate like they are above via
;; the java.sql definitions.

;; Timezones aren't supported as UnionVector doesn't support it.
;; Decimals aren't (currently) supported as UnionVector requires a
;; single scale/precision.

;; Java maps are always assumed to have named keys and are mapped to
;; structs. Arrow maps with arbitrary key types isn't (currently)
;; supported.

(set! *unchecked-math* :warn-on-boxed)

(defprotocol ArrowAppendable
  (append-value [_ writer allocator])
  (append-list-value [_ writer allocator])
  (append-struct-value [_ writer allocator k]))

(defn- kw-name ^String [x]
  (if (keyword? x)
    (subs (str x) 1)
    (str x)))

(defn- advance-writer [^Positionable writer]
  (.setPosition writer (inc (.getPosition writer))))

(defn- write-local-date [^BaseWriter$ScalarWriter writer ^LocalDate x]
  (.writeDateMilli writer (* (.getLong x ChronoField/EPOCH_DAY) 86400000)))

(defn- write-local-time [^BaseWriter$ScalarWriter writer ^LocalTime x]
  (.writeTimeMicro writer (quot (.getLong x ChronoField/NANO_OF_DAY) 1000)))

(defn- write-instant [^BaseWriter$ScalarWriter writer ^Instant x]
  (.writeTimeStampMicro writer (+ (* (.getEpochSecond x) 1000000)
                                  (quot (.getNano x) 1000))))

(defn- write-duration [^BaseWriter$ScalarWriter writer ^Duration x]
  (.writeIntervalDay writer
                     (.toDays x)
                     (rem (.toMillis x) 86400000)))

(defn- write-varchar [^BufferAllocator allocator ^BaseWriter$ScalarWriter writer ^CharSequence x]
  (let [bb (.encode (.newEncoder StandardCharsets/UTF_8) (CharBuffer/wrap x))
        len (.remaining bb)]
    (with-open [buf (.buffer allocator len)]
      (.setBytes buf 0 bb)
      (.writeVarChar writer 0 len buf))))

(defn- write-varbinary [^BufferAllocator allocator ^BaseWriter$ScalarWriter writer ^ByteBuffer x]
  (let [len (.remaining x)]
    (with-open [buf (.buffer allocator len)]
      (.setBytes buf 0 (.duplicate x))
      (.writeVarBinary writer 0 len buf))))

(defn- write-list [allocator ^BaseWriter$ListWriter list-writer x]
  (.startList list-writer)
  (doseq [v x]
    (append-list-value v list-writer allocator))
  (.endList list-writer))

(defn- write-struct [allocator ^BaseWriter$StructWriter struct-writer x]
  (.start struct-writer)
  (doseq [[k v] x]
    (append-struct-value v struct-writer allocator (kw-name k)))
  (.end struct-writer))

(extend-protocol ArrowAppendable
  (class (byte-array 0))
  (append-value [x ^BaseWriter$ScalarWriter writer ^BufferAllocator allocator]
    (write-varbinary allocator writer (ByteBuffer/wrap x))
    (doto writer
      (advance-writer)))

  (append-list-value [x ^BaseWriter$ListWriter writer ^BufferAllocator allocator]
    (write-varbinary allocator (.varBinary writer) (ByteBuffer/wrap x))
    writer)

  (append-struct-value [x ^BaseWriter$StructWriter writer ^BufferAllocator allocator k]
    (write-varbinary allocator (.varBinary writer k) (ByteBuffer/wrap x))
    writer)

  nil
  (append-value [_ ^BaseWriter$ScalarWriter writer _]
    (doto writer
      (.writeNull)
      (advance-writer)))

  (append-list-value [_ ^BaseWriter$ListWriter writer _]
    (doto writer
      (-> (.bit) (.writeNull))))

  (append-struct-value [_ ^BaseWriter$StructWriter writer _ k]
    (doto writer
      (-> (.bit k) (.writeNull))))

  Boolean
  (append-value [x ^BaseWriter$ScalarWriter writer _]
    (doto writer
      (.writeBit (if x 1 0))
      (advance-writer)))

  (append-list-value [x ^BaseWriter$ListWriter writer _]
    (doto writer
      (-> (.bit) (.writeBit (if x 1 0)))))

  (append-struct-value [x ^BaseWriter$StructWriter writer _ k]
    (doto writer
      (-> (.bit k) (.writeBit (if x 1 0)))))

  Byte
  (append-value [x ^BaseWriter$ScalarWriter writer _]
    (doto writer
      (.writeTinyInt x)
      (advance-writer)))

  (append-list-value [x ^BaseWriter$ListWriter writer _]
    (doto writer
      (-> (.tinyInt) (.writeTinyInt x))))

  (append-struct-value [x ^BaseWriter$StructWriter writer _ k]
    (doto writer
      (-> (.tinyInt k) (.writeTinyInt x))))

  Short
  (append-value [x ^BaseWriter$ScalarWriter writer _]
    (doto writer
      (.writeSmallInt x)
      (advance-writer)))

  (append-list-value [x ^BaseWriter$ListWriter writer _]
    (doto writer
      (-> (.smallInt) (.writeSmallInt x))))

  (append-struct-value [x ^BaseWriter$StructWriter writer _ k]
    (doto writer
      (-> (.smallInt k) (.writeSmallInt x))))

  Integer
  (append-value [x ^BaseWriter$ScalarWriter writer _]
    (doto writer
      (.writeInt x)
      (advance-writer)))

  (append-list-value [x ^BaseWriter$ListWriter writer _]
    (doto writer
      (-> (.integer) (.writeInt x))))

  (append-struct-value [x ^BaseWriter$StructWriter writer _ k]
    (doto writer
      (-> (.integer k) (.writeInt x))))

  Long
  (append-value [x ^BaseWriter$ScalarWriter writer _]
    (doto writer
      (.writeBigInt x)
      (advance-writer)))

  (append-list-value [x ^BaseWriter$ListWriter writer _]
    (doto writer
      (-> (.bigInt) (.writeBigInt x))))

  (append-struct-value [x ^BaseWriter$StructWriter writer _ k]
    (doto writer
      (-> (.bigInt k) (.writeBigInt x))))

  Float
  (append-value [x ^BaseWriter$ScalarWriter writer _]
    (doto writer
      (.writeFloat4 x)
      (advance-writer)))

  (append-list-value [x ^BaseWriter$ListWriter writer _]
    (doto writer
      (-> (.float4) (.writeFloat4 x))))

  (append-struct-value [x ^BaseWriter$StructWriter writer _ k]
    (doto writer
      (-> (.float4 k) (.writeFloat4 x))))

  Double
  (append-value [x ^BaseWriter$ScalarWriter writer _]
    (doto writer
      (.writeFloat8 x)
      (advance-writer)))

  (append-list-value [x ^BaseWriter$ListWriter writer _]
    (doto writer
      (-> (.float8) (.writeFloat8 x))))

  (append-struct-value [x ^BaseWriter$StructWriter writer _ k]
    (doto writer
      (-> (.float8 k) (.writeFloat8 x))))

  LocalDate
  (append-value [x ^BaseWriter$ScalarWriter writer _]
    (doto writer
      (write-local-date x)
      (advance-writer)))

  (append-list-value [x ^BaseWriter$ListWriter writer _]
    (doto writer
      (-> (.dateMilli) (write-local-date x))))

  (append-struct-value [x ^BaseWriter$StructWriter writer _ k]
    (doto writer
      (-> (.dateMilli k) (write-local-date x))))

  LocalTime
  (append-value [x ^BaseWriter$ScalarWriter writer _]
    (doto writer
      (write-local-time x)
      (advance-writer)))

  (append-list-value [x ^BaseWriter$ListWriter writer _]
    (doto writer
      (-> (.timeMicro) (write-local-time x))))

  (append-struct-value [x ^BaseWriter$StructWriter writer _ k]
    (doto writer
      (-> (.timeMicro k) (write-local-time x))))

  Date
  (append-value [x ^BaseWriter$ScalarWriter writer _]
    (doto writer
      (write-instant (.toInstant x))
      (advance-writer)))

  (append-list-value [x ^BaseWriter$ListWriter writer _]
    (doto writer
      (-> (.timeStampMicro) (write-instant (.toInstant x)))))

  (append-struct-value [x ^BaseWriter$StructWriter writer _ k]
    (doto writer
      (-> (.timeStampMicro k) (write-instant (.toInstant x)))))

  Instant
  (append-value [x ^BaseWriter$ScalarWriter writer _]
    (doto writer
      (write-instant x)
      (advance-writer)))

  (append-list-value [x ^BaseWriter$ListWriter writer _]
    (doto writer
      (-> (.timeStampMicro) (write-instant x))))

  (append-struct-value [x ^BaseWriter$StructWriter writer _ k]
    (doto writer
      (-> (.timeStampMicro k) (write-instant x))))

  Duration
  (append-value [x ^BaseWriter$ScalarWriter writer _]
    (doto writer
      (write-duration x)
      (advance-writer)))

  (append-list-value [x ^BaseWriter$ListWriter writer _]
    (doto writer
      (-> (.intervalDay) (write-duration x))))

  (append-struct-value [x ^BaseWriter$StructWriter writer _ k]
    (doto writer
      (-> (.intervalDay k) (write-duration x))))

  Character
  (append-value [x ^BaseWriter$ScalarWriter writer ^BufferAllocator allocator]
    (write-varchar allocator writer (str x))
    (doto writer
      (advance-writer)))

  (append-list-value [x ^BaseWriter$ListWriter writer ^BufferAllocator allocator]
    (write-varchar allocator (.varChar writer) (str x))
    writer)

  (append-struct-value [x ^BaseWriter$StructWriter writer ^BufferAllocator allocator k]
    (write-varchar allocator (.varChar writer k) (str x))
    writer)

  CharSequence
  (append-value [x ^BaseWriter$ScalarWriter writer ^BufferAllocator allocator]
    (write-varchar allocator writer x)
    (doto writer
      (advance-writer)))

  (append-list-value [x ^BaseWriter$ListWriter writer ^BufferAllocator allocator]
    (write-varchar allocator (.varChar writer) x)
    writer)

  (append-struct-value [x ^BaseWriter$StructWriter writer ^BufferAllocator allocator k]
    (write-varchar allocator (.varChar writer k) x)
    writer)

  ByteBuffer
  (append-value [x ^BaseWriter$ScalarWriter writer ^BufferAllocator allocator]
    (write-varbinary allocator writer x)
    (doto writer
      (advance-writer)))

  (append-list-value [x ^BaseWriter$ListWriter writer ^BufferAllocator allocator]
    (write-varbinary allocator (.varBinary writer) x)
    writer)

  (append-struct-value [x ^BaseWriter$StructWriter writer ^BufferAllocator allocator k]
    (write-varbinary allocator (.varBinary writer k) x)
    writer)

  List
  (append-value [x ^UnionWriter writer ^BufferAllocator allocator]
    (write-list allocator (.asList writer) x)
    (doto writer
      (advance-writer)))

  (append-list-value [x ^BaseWriter$ListWriter writer ^BufferAllocator allocator]
    (write-list allocator (.list writer) x)
    writer)

  (append-struct-value [x ^BaseWriter$StructWriter writer ^BufferAllocator allocator k]
    (write-list allocator (.list writer k) x)
    writer)

  Map
  (append-value [x ^UnionWriter writer ^BufferAllocator allocator]
    (write-struct allocator (.asStruct writer) x)
    (doto writer
      (advance-writer)))

  (append-list-value [x ^BaseWriter$ListWriter writer ^BufferAllocator allocator]
    (write-struct allocator (.struct writer) x)
    writer)

  (append-struct-value [x ^BaseWriter$StructWriter writer ^BufferAllocator allocator k]
    (write-struct allocator (.struct writer k) x)
    writer))

(defn- sparse->dense-union-field ^org.apache.arrow.vector.types.pojo.Field [^Field field]
  (let [schema-json (.toJson (Schema. [field]))]
    (first (.getFields (Schema/fromJSON (json/json-str
                                         (w/postwalk
                                          (fn [x]
                                            (if (and (= "union" (get x "name"))
                                                     (= "Sparse" (get x "mode")))
                                              (assoc x "mode" "Dense")
                                              x))
                                          (json/read-str schema-json))))))))

(defn sparse->dense-union ^org.apache.arrow.vector.ValueVector [^ValueVector v ^BufferAllocator allocator]
  (with-open [duv (.createVector (sparse->dense-union-field (.getField v)) allocator)]
    (.setInitialCapacity duv (.getValueCount v))
    ((fn copy-range [^ValueVector in-vec ^ValueVector out-vec ^Long in-start ^Long out-start ^Long size]
       (let [^long in-start in-start
             ^long out-start out-start
             ^long size size]
         (cond
           (instance? StructVector in-vec)
           (let [^StructVector in-vec in-vec
                 ^StructVector out-vec out-vec]
             (dotimes [n size]
               (.setIndexDefined out-vec (+ out-start n))
               (doseq [k (.getChildFieldNames in-vec)
                       :let [child-in-vec (.getChild in-vec k)]
                       :when (not (.isNull child-in-vec (+ in-start n)))]
                 (copy-range child-in-vec
                             (.getChild out-vec k)
                             (+ in-start n)
                             (+ out-start n)
                             1))))

           (instance? ListVector in-vec)
           (let [^ListVector in-vec in-vec
                 ^ListVector out-vec out-vec]
             (dotimes [n size]
               (let [in-n (+ in-start n)
                     element-size (- (.getElementEndIndex in-vec in-n)
                                     (.getElementStartIndex in-vec in-n))
                     out-list-offset (.startNewValue out-vec (+ out-start n))]
                 (copy-range (.getDataVector in-vec) (.getDataVector out-vec)
                             (.getElementStartIndex in-vec in-n) out-list-offset element-size)
                 (.endValue out-vec (+ out-start n) element-size))))

           (instance? UnionVector in-vec)
           (let [^UnionVector in-vec in-vec
                 ^DenseUnionVector out-vec out-vec]
             (dotimes [n size]
               (let [in-n (+ in-start n)
                     type-id (.getTypeValue in-vec in-n)
                     inner-in-vec (.getVector in-vec  in-n)]
                 (if-let [inner-out-vec (or (.getVectorByType out-vec type-id)
                                            (when inner-in-vec
                                              (.addVector out-vec type-id (.createVector (.getField inner-in-vec) allocator))))]
                   (let [inner-out-n (DenseUnionUtil/writeTypeId out-vec (+ out-start n) type-id)]
                     (copy-range inner-in-vec inner-out-vec in-n inner-out-n 1))
                   (.setTypeId out-vec (+ out-start n) type-id)))))

           :else
           (dotimes [n size]
             (.copyFromSafe out-vec (+ in-start n) (+ out-start n) in-vec)))))
     v duv 0 0 (.getValueCount v))
    (.setValueCount duv (.getValueCount v))
    ;; Without this extra copy IPC doesn't work for some reason.
    (let [copy (.createVector (.getField duv) allocator)]
      (try
        (doto copy
          (VectorBatchAppender/batchAppend (into-array [duv])))
        (catch Throwable t
          (.close copy)
          (throw t))))))
