(ns xtdb.vector.arrow-generative-test
  (:require [clojure.string :as str]
            [clojure.test :as t]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [xtdb.test-util :as tu]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw])
  (:import (clojure.lang MapEntry)
           (java.math BigDecimal)
           (java.net URI)
           (java.nio ByteBuffer)
           (org.apache.arrow.memory BufferAllocator RootAllocator)
           (org.apache.arrow.vector.types.pojo Field FieldType)
           (xtdb.arrow VectorIndirection VectorReader)
           (xtdb.types ClojureForm)
           (xtdb.vector IndirectMultiVectorReader)))

(t/use-fixtures :each tu/with-allocator)

;; TODO non simple scalar types
(def simple-arrow-types (gen/elements [#xt.arrow/type :i8
                                       #xt.arrow/type :i16
                                       #xt.arrow/type :i32
                                       #xt.arrow/type :i64
                                       ;; #xt.arrow/type :f32
                                       #xt.arrow/type :f64
                                       #xt.arrow/type [:decimal 32 1 128]
                                       #xt.arrow/type :utf8
                                       #xt.arrow/type :varbinary
                                       #xt.arrow/type :keyword
                                       #xt.arrow/type :uuid
                                       #xt.arrow/type :uri
                                       #xt.arrow/type :transit]))

(def composites #{#xt.arrow/type :struct
                  #xt.arrow/type :list
                  #xt.arrow/type :set
                  #xt.arrow/type :union})

(def single-legged-composites #{#xt.arrow/type :list
                                #xt.arrow/type :set})

(def composite-arrow-types (gen/elements composites))

(def arrow-types (gen/frequency [[10 simple-arrow-types] [3 composite-arrow-types]]))

(def field-types (gen/let [nullable? gen/boolean
                           arrow-type arrow-types]
                   (if nullable? (FieldType/nullable arrow-type) (FieldType/notNullable arrow-type))))

(def field-gen (gen/let [field-name (gen/such-that #(pos? (count %)) gen/string-alphanumeric)
                         nullable gen/boolean
                         arrow-type arrow-types]
                 (apply types/->field (str/lower-case field-name) arrow-type nullable
                        (cond
                          (single-legged-composites arrow-type)
                          [(gen/generate field-gen)]

                          (composites arrow-type)
                          (cond->> (gen/generate (gen/vector field-gen 1 10))
                            (= #xt.arrow/type :union arrow-type) (util/distinct-by (fn [^Field f] (.getType f))))))))

(defn generate-fields [n]
  (gen/generate (gen/vector field-gen n)))

(def i8-gen gen/byte)
(def i16-gen (gen/let [v (gen/choose Short/MIN_VALUE Short/MAX_VALUE)]
               (short v)))
(def i32-gen (gen/let [v (gen/choose Integer/MAX_VALUE Integer/MAX_VALUE)]
               (int v)))
(def i62-gen (gen/choose Long/MIN_VALUE Long/MAX_VALUE))
(def numeric-char (gen/let [n (gen/choose 0 9)]
                    (char (+ (int \0) n))))

;; TODO change once #3372 is fixed
(def decimal-gen (gen/let [chs (gen/vector numeric-char 1 38)
                           scale (gen/return 19)]
                   (BigDecimal. (biginteger (apply str chs)) ^int scale))
  #_(gen/let [^double v (gen/double* {:infinite? false :NaN? false})]
      (BigDecimal/valueOf v)))

(def gen-form (gen/frequency [[2 (gen/let [v (gen/vector (gen/one-of [gen/symbol gen/symbol-ns]) 1 5)]
                                   (into '() v))]
                              [1 (gen/one-of [gen/symbol gen/symbol-ns])]]))

(declare value-generator)

(defn map-value-gen [^Field field]
  (let [name (.getName field)]
    (gen/let [v (value-generator field)]
      (MapEntry/create name v))))

(defn value-generator [^Field field]
  (gen/frequency
   (cond-> [[15 (condp = (.getType field)
                  #xt.arrow/type :i8 i8-gen
                  #xt.arrow/type :i16 i16-gen
                  #xt.arrow/type :i32 i32-gen
                  #xt.arrow/type :i64 i62-gen
                  ;; #xt.arrow/type :f32 (throw (UnsupportedOperationException.))
                  #xt.arrow/type :f64 (gen/double* {:NaN? false})
                  #xt.arrow/type [:decimal 32 1 128] decimal-gen
                  #xt.arrow/type :utf8 gen/string
                  #xt.arrow/type :varbinary (gen/let [bs gen/bytes] (ByteBuffer/wrap bs))
                  #xt.arrow/type :keyword (gen/one-of [gen/keyword gen/keyword-ns])
                  #xt.arrow/type :uuid gen/uuid
                  #xt.arrow/type :uri (gen/let [s gen/string-alphanumeric] (URI/create s))
                  #xt.arrow/type :transit (gen/let [form gen-form] (ClojureForm. form))
                  #xt.arrow/type :list (gen/vector (value-generator (first (.getChildren field))) 0 10)
                  #xt.arrow/type :set (gen/set (value-generator (first (.getChildren field))) {:min-elements 0 :max-elements 10})
                  #xt.arrow/type :union (gen/one-of (map value-generator (.getChildren field)))
                  #xt.arrow/type :struct (gen/let [entries (apply gen/tuple (map map-value-gen (.getChildren field)))]
                                           (->> (filter val entries)
                                                (into {}))))]]
     (.isNullable field)
     (conj [1 (gen/return nil)]))))

(defn generate-data
  ([^Field field] (generate-data field 10))
  ([^Field field n]
   (let [val-gen (value-generator field)]
     (gen/generate (gen/vector val-gen n)))))

(defn gen-fields+data [nb-fields nb-entries]
  (let [fields (generate-fields nb-fields)
        data (mapv #(generate-data % nb-entries) fields)]
    {:fields fields
     :data data}))

(comment
  (time (gen-fields+data 10 100)))

(defn field+data-gen [nb-entries]
  (gen/let [field field-gen
            vs (gen/vector (value-generator field) nb-entries)]
    {:field field :vs vs}))

#_
(defspec ^:integration read-what-you-write 100
  (prop/for-all [{:keys [field vs]} (field+data-gen 100)]
    (with-open [al (RootAllocator.)
                vec (vw/open-vec al field vs)]
      (= vs (tu/vec->vals (vr/vec->reader vec) #xt/key-fn :snake-case-string)))))

(defn generate-vector [^BufferAllocator al]
  (gen/let [nb-entries (gen/choose 0 100)
            {:keys [field vs]} (field+data-gen nb-entries)]
    (vw/open-vec al field vs)))

(defn- same-shuffle [coll1 coll2]
  (let [shuffeld (-> (map vector coll1 coll2) shuffle)]
    [(map first shuffeld) (map second shuffeld)]))

(defn multi-vec-reader [^BufferAllocator al]
  (gen/let [n (gen/choose 1 10)
            vecs (gen/vector (generate-vector al) n)]
    (let [rdrs (map vr/vec->reader vecs)
          rdr-indirects (->> (map-indexed #(repeat (.getValueCount ^VectorReader %2) %1) rdrs)
                             (apply concat))
          vec-indirects (mapcat #(range (.getValueCount ^VectorReader %)) rdrs)
          [rdr-indirects vec-indirects] (same-shuffle rdr-indirects vec-indirects)]
      (IndirectMultiVectorReader. "foo" rdrs
                                  (VectorIndirection/selection (int-array rdr-indirects))
                                  (VectorIndirection/selection (int-array vec-indirects))))))

#_
(defspec ^:integration read-multi-vec 20
  (prop/for-all [^VectorReader multi-rdr (multi-vec-reader tu/*allocator*)]
    (let [res (= (.getValueCount multi-rdr) (count (tu/vec->vals multi-rdr)))]
      (.close multi-rdr)
      res)))

(defn gen-rdr [^BufferAllocator al]
  (gen/frequency [[2 (gen/let [v (generate-vector al)]
                       (vr/vec->reader v))]
                  [1 (multi-vec-reader al)]]))

(defn- copy-all ^VectorReader [^VectorReader rdr ^BufferAllocator al]
  (let [v (.createVector (.getField rdr) al)
        wrt (vw/->writer v)
        copier (.rowCopier rdr wrt)]
    (dotimes [i (.getValueCount rdr)]
      (.copyRow copier i))
    (vw/vec-wtr->rdr wrt)))

#_
(defspec ^:integration row-copiers 50
  (prop/for-all [^VectorReader rdr (gen-rdr tu/*allocator*)]
    (with-open [copied-rdr (copy-all rdr tu/*allocator*)]
      (let [res (= (tu/vec->vals rdr) (tu/vec->vals copied-rdr))]
        (.close rdr)
        res))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; interactive versions
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(comment

  (def read-what-you-write-prop
    (prop/for-all [{:keys [field vs]} (field+data-gen 100)]
      (with-open [al (RootAllocator.)
                  vec (vw/open-vec al field vs)]
        (= vs (tu/vec->vals (vr/vec->reader vec) #xt/key-fn :snake-case-string)))))

  (tc/quick-check 100 read-what-you-write-prop)


  (defn- read-multi-vec-prop [^BufferAllocator al]
    (prop/for-all [^VectorReader multi-rdr (multi-vec-reader al)]
      (let [res (= (.getValueCount multi-rdr) (count (tu/vec->vals multi-rdr)))]
        (.close multi-rdr)
        res)))

  (with-open [al (RootAllocator.)]
    (tc/quick-check 10 (read-multi-vec-prop al)))

  (defn- row-copiers-prop  [^BufferAllocator al]
    (prop/for-all [^VectorReader rdr (gen-rdr al)]
      (with-open [copied-rdr (copy-all rdr al)]
        (let [res (= (tu/vec->vals rdr) (tu/vec->vals copied-rdr))]
          (.close rdr)
          res))))

  (with-open [al (RootAllocator.)]
    (tc/quick-check 10 (row-copiers-prop al))))
