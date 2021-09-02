(ns core2.types.json
  (:require [clojure.data.json :as json]
            [clojure.spec.alpha :as s])
  (:import [java.nio.charset StandardCharsets]
           [org.apache.arrow.vector.types Types$MinorType]
           [org.apache.arrow.vector.complex.writer BaseWriter BaseWriter$ListWriter BaseWriter$ScalarWriter BaseWriter$StructWriter]
           [org.apache.arrow.vector.complex.impl UnionWriter]
           [org.apache.arrow.memory BufferAllocator]))

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

(defn- kw-name ^String [x]
  (if (keyword? x)
    (subs (str x) 1)
    (str x)))

(defn- advance-writer [^BaseWriter writer]
  (.setPosition writer (inc (.getPosition writer))))

(defmulti append-writer (fn [allocator writer parent-type k [tag x]]
                          [parent-type tag]))

(defmethod append-writer [nil :json/null] [_ ^BaseWriter writer _ _ [tag x]]
  (doto writer
    (.writeNull)
    (advance-writer)))

(defmethod append-writer [Types$MinorType/LIST :json/null] [_ ^BaseWriter$ListWriter writer _ _ [tag x]]
  (doto writer
    (-> (.bit) (.writeNull))))

(defmethod append-writer [Types$MinorType/STRUCT :json/null] [_ ^BaseWriter$StructWriter writer _ k [tag x]]
  (doto writer
    (-> (.bit k) (.writeNull))))

(defmethod append-writer [nil :json/boolean] [_ ^BaseWriter$ScalarWriter writer _ _ [tag x]]
  (doto writer
    (.writeBit (if x 1 0))
    (advance-writer)))

(defmethod append-writer [Types$MinorType/LIST :json/boolean] [_ ^BaseWriter$ListWriter writer _ _ [tag x]]
  (doto writer
    (-> (.bit) (.writeBit (if x 1 0)))))

(defmethod append-writer [Types$MinorType/STRUCT :json/boolean] [_ ^BaseWriter$StructWriter writer _ k [tag x]]
  (doto writer
    (-> (.bit k) (.writeBit (if x 1 0)))))

(defmethod append-writer [nil :json/int] [_ ^BaseWriter$ScalarWriter writer _ _ [tag x]]
  (doto writer
    (.writeBigInt x)
    (advance-writer)))

(defmethod append-writer [Types$MinorType/LIST :json/int] [_ ^BaseWriter$ListWriter writer _ _ [tag x]]
  (doto writer
    (-> (.bigInt) (.writeBigInt x))))

(defmethod append-writer [Types$MinorType/STRUCT :json/int] [_ ^BaseWriter$StructWriter writer _ k [tag x]]
  (doto writer
    (-> (.bigInt k) (.writeBigInt x))))

(defmethod append-writer [nil :json/float] [_ ^BaseWriter$ScalarWriter writer _ _ [tag x]]
  (doto writer
    (.writeFloat8 x)
    (advance-writer)))

(defmethod append-writer [Types$MinorType/LIST :json/float] [_ ^BaseWriter$ListWriter writer _ _ [tag x]]
  (doto writer
    (-> (.float8) (.writeFloat8 x))))

(defmethod append-writer [Types$MinorType/STRUCT :json/float] [_ ^BaseWriter$StructWriter writer _ k [tag x]]
  (doto writer
    (-> (.float8 k) (.writeFloat8 x))))

(defn- append-varchar [^BufferAllocator allocator ^BaseWriter$ScalarWriter writer ^String x]
  (let [bs (.getBytes x StandardCharsets/UTF_8)
        len (alength bs)]
    (with-open [buf (.buffer allocator len)]
      (.setBytes buf 0 bs)
      (.writeVarChar writer 0 len buf))))

(defmethod append-writer [nil :json/string] [^BufferAllocator allocator ^BaseWriter$ScalarWriter writer _ _ [tag x]]
  (append-varchar allocator writer x)
  (doto writer
    (advance-writer)))

(defmethod append-writer [Types$MinorType/LIST :json/string] [^BufferAllocator allocator ^BaseWriter$ListWriter writer _ _ [tag x]]
  (append-varchar allocator (.varChar writer) x)
  writer)

(defmethod append-writer [Types$MinorType/STRUCT :json/string] [^BufferAllocator allocator ^BaseWriter$StructWriter writer _ k [tag x]]
  (append-varchar allocator (.varChar writer k) x)
  writer)

(defn- append-list [allocator ^BaseWriter$ListWriter list-writer x]
  (.startList list-writer)
  (doseq [v x]
    (append-writer allocator list-writer Types$MinorType/LIST nil v))
  (.endList list-writer))

(defmethod append-writer [nil :json/array] [allocator ^UnionWriter writer _ _ [tag x]]
  (append-list allocator (.asList writer) x)
  (doto writer
    (advance-writer)))

(defmethod append-writer [Types$MinorType/LIST :json/array] [allocator ^BaseWriter$ListWriter writer _ _ [tag x]]
  (append-list allocator (.list writer) x)
  writer)

(defmethod append-writer [Types$MinorType/STRUCT :json/array] [allocator ^BaseWriter$StructWriter writer _ k [tag x]]
  (append-list allocator (.list writer k) x)
  writer)

(defn- append-struct [allocator ^BaseWriter$StructWriter struct-writer x]
  (.start struct-writer)
  (doseq [[k v] x]
    (append-writer allocator struct-writer Types$MinorType/STRUCT (kw-name k) v))
  (.end struct-writer))

(defmethod append-writer [nil :json/object] [allocator ^UnionWriter writer _ _ [tag x]]
  (append-struct allocator (.asStruct writer) x)
  (doto writer
      (advance-writer)))

(defmethod append-writer [Types$MinorType/LIST :json/object] [allocator ^BaseWriter$ListWriter writer _ _ [tag x]]
  (append-struct allocator (.struct writer) x)
  writer)

(defmethod append-writer [Types$MinorType/STRUCT :json/object] [allocator ^BaseWriter$StructWriter writer _ k [tag x]]
  (append-struct allocator (.struct writer (kw-name k)) x)
  writer)

;; The below is currently unused, assumes a more manual mapping to
;; Arrow than using UnionWriter directly as above.

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
