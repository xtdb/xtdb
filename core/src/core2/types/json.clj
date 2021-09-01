(ns core2.types.json
  (:require [clojure.data.json :as json]
            [clojure.spec.alpha :as s])
  (:import [java.nio.charset StandardCharsets]
           [org.apache.arrow.vector.types Types$MinorType]
           [org.apache.arrow.vector.complex.writer BaseWriter BaseWriter$ListWriter BaseWriter$ScalarWriter BaseWriter$StructWriter]
           [org.apache.arrow.vector.complex.impl UnionWriter]
           [org.apache.arrow.vector.complex UnionVector]
           [org.apache.arrow.memory BufferAllocator RootAllocator]))

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

(defn- kw-name ^String [x]
  (if (keyword? x)
    (subs (str x) 1)
    (str x)))

(defn- advance-writer [^BaseWriter writer]
  (.setPosition writer (inc (.getPosition writer))))

(defmulti append-writer (fn [allocator writer parent-tag k [tag x]]
                          [parent-tag tag]))

(defmethod append-writer [nil :json/null] [_ ^BaseWriter writer _ _ [tag x]]
  (doto writer
    (.writeNull)
    (advance-writer)))

(defmethod append-writer [:json/array :json/null] [_ ^BaseWriter$ListWriter writer _ _ [tag x]]
  (doto writer
    (-> (.writeNull))))

(defmethod append-writer [:json/object :json/null] [_ ^BaseWriter$StructWriter writer _ k [tag x]]
  (doto writer
    (-> (.bit (kw-name k)) (.writeNull))))

(defmethod append-writer [nil :json/boolean] [_ ^BaseWriter$ScalarWriter writer _ _ [tag x]]
  (doto writer
    (.writeBit (if x 1 0))
    (advance-writer)))

(defmethod append-writer [:json/array :json/boolean] [_ ^BaseWriter$ListWriter writer _ _ [tag x]]
  (doto writer
    (-> (.bit) (.writeBit (if x 1 0)))))

(defmethod append-writer [:json/object :json/boolean] [_ ^BaseWriter$StructWriter writer _ k [tag x]]
  (doto writer
    (-> (.bit (kw-name k)) (.writeBit (if x 1 0)))))

(defmethod append-writer [nil :json/int] [_ ^BaseWriter$ScalarWriter writer _ _ [tag x]]
  (doto writer
    (.writeBigInt x)
    (advance-writer)))

(defmethod append-writer [:json/array :json/int] [_ ^BaseWriter$ListWriter writer _ _ [tag x]]
  (doto writer
    (-> (.bigInt) (.writeBigInt x))))

(defmethod append-writer [:json/object :json/int] [_ ^BaseWriter$StructWriter writer _ k [tag x]]
  (doto writer
    (-> (.bigInt (kw-name k)) (.writeBigInt x))))

(defmethod append-writer [nil :json/float] [_ ^BaseWriter$ScalarWriter writer _ _ [tag x]]
  (doto writer
    (.writeFloat8 x)
    (advance-writer)))

(defmethod append-writer [:json/array :json/float] [_ ^BaseWriter$ListWriter writer _ _ [tag x]]
  (doto writer
    (-> (.float8) (.writeFloat8 x))))

(defmethod append-writer [:json/object :json/float] [_ ^BaseWriter$StructWriter writer _ k [tag x]]
  (doto writer
    (-> (.float8 (kw-name k)) (.writeFloat8 x))))

(defn- write-varchar [^BufferAllocator allocator ^BaseWriter$ScalarWriter writer ^String x]
  (let [bs (.getBytes x StandardCharsets/UTF_8)
        len (alength bs)]
    (with-open [buf (.buffer allocator len)]
      (.setBytes buf 0 bs)
      (.writeVarChar writer 0 len buf))))

(defmethod append-writer [nil :json/string] [^BufferAllocator allocator ^BaseWriter$ScalarWriter writer _ _ [tag x]]
  (write-varchar allocator writer x)
  (doto writer
    (advance-writer)))

(defmethod append-writer [:json/array :json/string] [^BufferAllocator allocator ^BaseWriter$ListWriter writer _ _ [tag x]]
  (write-varchar allocator (.varChar writer) x)
  writer)

(defmethod append-writer [:json/object :json/string] [^BufferAllocator allocator ^BaseWriter$StructWriter writer _ k [tag x]]
  (write-varchar allocator (.varChar writer (kw-name k)) x)
  writer)

(defmethod append-writer [nil :json/array] [allocator ^UnionWriter writer _ _ [tag x]]
  (let [list-writer (.asList writer)]
    (.startList writer)
    (doseq [v x]
      (append-writer allocator list-writer :json/array nil v))
    (doto writer
      (.endList)
      (advance-writer))))

(defmethod append-writer [:json/array :json/array] [allocator ^BaseWriter$ListWriter writer _ _ [tag x]]
  (let [list-writer (.list writer)]
    (.startList list-writer)
    (doseq [v x]
      (append-writer allocator list-writer :json/array nil v))
    (.endList list-writer)
    writer))

(defmethod append-writer [:json/object :json/array] [allocator ^BaseWriter$StructWriter writer _ k [tag x]]
  (let [list-writer (.list writer (kw-name k))]
    (.startList list-writer)
    (doseq [v x]
      (append-writer allocator list-writer :json/array nil v))
    (.endList list-writer)
    writer))

(defmethod append-writer [nil :json/object] [allocator ^UnionWriter writer _ _ [tag x]]
  (let [struct-writer (.asStruct writer)]
    (.start writer)
    (doseq [[k v] x]
      (append-writer allocator struct-writer :json/object k v))
    (doto writer
      (.end)
      (advance-writer))))

(defmethod append-writer [:json/array :json/object] [allocator ^BaseWriter$ListWriter writer _ _ [tag x]]
  (let [struct-writer (.struct writer)]
    (.start struct-writer)
    (doseq [[k v] x]
      (append-writer allocator struct-writer :json/object k v))
    (.end struct-writer)
    writer))

(defmethod append-writer [:json/object :json/object] [allocator ^BaseWriter$StructWriter writer _ k [tag x]]
  (let [struct-writer (.struct writer (kw-name k))]
    (.start struct-writer)
    (doseq [[k v] x]
      (append-writer allocator struct-writer :json/object k v))
    (.end struct-writer)
    writer))

(comment
  (with-open [a (RootAllocator.)
              v (UnionVector/empty "" a)
              ^UnionWriter writer (.getWriter v)]

    (doseq [x [false
               nil
               2
               3.14
               "Hello"
               []
               [2 3.14 [false]]
               {}
               {:B 2 :C 1}
               {:B 3.14 :D {:E ["hello" -1]} :F nil}]]

      (append-writer a writer nil nil (s/conform :json/value x)))
    (.setValueCount v (.getPosition writer))
    (str v)))
