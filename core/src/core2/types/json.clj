(ns core2.types.json
  (:require [clojure.data.json :as json]
            [clojure.spec.alpha :as s])
  (:import [org.apache.arrow.vector.types Types$MinorType]))

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


;; PromotableWriter spike
(comment
  (with-open [a (org.apache.arrow.memory.RootAllocator.)
              container (org.apache.arrow.vector.complex.NonNullableStructVector/empty "" a)
              v (.addOrGetStruct container "test")
              writer (org.apache.arrow.vector.complex.impl.PromotableWriter. v container)]
    (.allocateNew container)

    (.start writer)

    (.setPosition writer 0)
    (.writeBit (.bit writer "A") 0)

    (.setPosition writer 1)
    (.writeNull writer)

    (.setPosition writer 2)
    (.writeBigInt (.bigInt writer "A") 2)

    (.setPosition writer 3)
    (.writeFloat8 (.float8 writer "A") 3.14)

    (.setPosition writer 4)
    (let [bs (.getBytes "Hello" "UTF-8")
          len (alength bs)]
      (with-open [buf (.buffer a len)]
        (.setBytes buf 0 bs)
        (.writeVarChar (.varChar writer "A") 0 len buf)))

    (.setPosition writer 5)
    (let [l (.list writer "A")]
      (.startList l)
      (.writeBigInt (.bigInt l) 2)
      (.writeFloat8 (.float8 l) 3.14)
      (.endList l))

    (.setPosition writer 6)
    (let [s (.struct writer "A")]
      (.start s)
      (.writeBigInt (.bigInt s "B") 2)
      (.writeBit (.bit s "C") 1)
      (.end s))

    (.setPosition writer 7)
    (let [s (.struct writer "A")]
      (.start s)
      (.writeFloat8 (.float8 s "B") 3.14)
      (.end s))

    (.end writer)

    (.setValueCount container 8)
    (pr-str (.getChild v "A"))

    ;; Attempt to turn Sparse unions created by the above into dense after the fact (before writing IPC)
    #_(let [^org.apache.arrow.vector.complex.UnionVector a-vec (.getChild v "A")
            field (first (.getFields (org.apache.arrow.vector.types.pojo.Schema/fromJSON
                                      (json/json-str (clojure.walk/postwalk #(if (and (= "Sparse" (get % "mode"))
                                                                                      (= "union" (get % "name")))
                                                                               (assoc % "mode" "Dense")
                                                                               %)
                                                                            (json/read-str (.toJson (org.apache.arrow.vector.types.pojo.Schema. [(.getField a-vec)]))))))))]
        (with-open [^org.apache.arrow.vector.complex.DenseUnionVector copy (.createVector ^org.apache.arrow.vector.types.pojo.Field field a)]
          (dotimes [n (.getValueCount a-vec)]
            (let [type-id (.getByte (.getTypeBuffer ^org.apache.arrow.vector.complex.UnionVector a-vec) n)
                  offset (core2.DenseUnionUtil/writeTypeId copy n type-id)]
              (.copyFromSafe (.getVectorByType copy type-id) n offset (.getVectorByType a-vec type-id))
              (prn (.getObject copy n))))))))


;; UnionWriter spike
(comment
  (with-open [a (org.apache.arrow.memory.RootAllocator.)
              v (org.apache.arrow.vector.complex.UnionVector/empty "" a)
              ^org.apache.arrow.vector.complex.impl.UnionWriter writer (.getWriter v)]
    #_(.allocateNew v)

    (.setPosition writer 0)
    (.writeBit writer 0)

    (.setPosition writer 1)
    (.writeNull writer)

    (.setPosition writer 2)
    (.writeBigInt writer 2)

    (.setPosition writer 3)
    (.writeFloat8 writer 3.14)

    (.setPosition writer 4)
    (let [bs (.getBytes "Hello" "UTF-8")
          len (alength bs)]
      (with-open [buf (.buffer a len)]
        (.setBytes buf 0 bs)
        (.writeVarChar writer 0 len buf)))

    (.setPosition writer 5)
    (let [l (.asList writer)]
      (.startList l)
      (.writeBigInt (.bigInt l) 2)
      (.writeFloat8 (.float8 l) 3.14)
      (.endList l))

    (.setPosition writer 6)
    (let [s (.asStruct writer)]
      (.start s)
      (.writeBigInt (.bigInt s "B") 2)
      (.writeBit (.bit s "C") 1)
      (.end s))

    (.setPosition writer 7)
    (let [s (.asStruct writer)]
      (.start s)
      (.writeFloat8 (.float8 s "B") 3.14)
      (.end s))

    (.setPosition writer 8)
    (let [l (.asList writer)]
      (.startList l)
      (.writeBit (.bit l) 1)
      (.writeFloat8 (.float8 l) 3.14)
      (.writeNull l)
      (.endList l))

    (.setValueCount v 9)
    (pr-str v)))
