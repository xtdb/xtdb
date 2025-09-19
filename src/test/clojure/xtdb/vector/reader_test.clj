(ns xtdb.vector.reader-test
  (:require [clojure.test :as t :refer [deftest]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [xtdb.test-generators :as tg]
            [xtdb.test-util :as tu]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw])
  (:import [org.apache.arrow.memory BufferAllocator]
           [org.apache.arrow.vector.complex DenseUnionVector ListVector StructVector]
           (org.apache.arrow.vector.types.pojo FieldType)
           (xtdb.arrow ValueReader Vector VectorIndirection VectorPosition)
           (xtdb.vector IndirectMultiVectorReader)))

(t/use-fixtures :each tu/with-allocator)

(deftest copying-union-legs-with-different-types-throws
  (with-open [rel-wtr1 (vw/->rel-writer tu/*allocator*)
              rel-wtr2 (vw/->rel-writer tu/*allocator*)
              rel-wtr3 (vw/->rel-writer tu/*allocator*)]
    (-> rel-wtr1
        (.vectorFor "my-column" (FieldType/notNullable #xt.arrow/type :union))
        (.vectorFor "foo" (FieldType/notNullable #xt.arrow/type :i64))
        (.writeLong 42))
    (-> rel-wtr2
        (.vectorFor "my-column" (FieldType/notNullable #xt.arrow/type :union))
        (.vectorFor "foo" (FieldType/notNullable #xt.arrow/type :f64))
        (.writeDouble 42.0))
    (t/is (thrown-with-msg?
           RuntimeException #"Field type mismatch"
           (let [copier1 (.rowCopier (vw/rel-wtr->rdr rel-wtr1) rel-wtr3)
                 copier2 (.rowCopier (vw/rel-wtr->rdr rel-wtr2) rel-wtr3)]
             (.copyRow copier1 0)
             (.copyRow copier2 0))))))

(deftest testing-duv->vec-copying
  (with-open [duv (DenseUnionVector/empty "my-duv" tu/*allocator*)
              int-vec (.createVector (types/->field "my-int" #xt.arrow/type :i64 false) tu/*allocator*)]
    (let [duv-wrt (vw/->writer duv)
          int-wrt (vw/->writer int-vec)]
      (doto duv-wrt
        (.writeObject 42)
        (.writeObject 43))

      (doto (.rowCopier int-wrt duv)
        (.copyRow 0)
        (.copyRow 1))

      (t/is (= [42 43]
               (.getAsList (vw/vec-wtr->rdr int-wrt)))
            "duv to monomorphic base type vector copying")))

  (with-open [duv (DenseUnionVector/empty "my-duv" tu/*allocator*)
              list-vec (ListVector/empty "my-list" tu/*allocator*)]
    (let [duv-wrt (vw/->writer duv)
          list-wrt (vw/->writer list-vec)]
      (.writeObject duv-wrt 42)
      (t/is (thrown-with-msg?
             RuntimeException
             #"illegal copy src vector"
             (.rowCopier list-wrt duv)))))

  (with-open [duv (DenseUnionVector/empty "my-duv" tu/*allocator*)
              list-vec (ListVector/empty "my-list" tu/*allocator*)]
    (let [duv-wrt (vw/->writer duv)
          list-wrt (vw/->writer list-vec)
          duv-list-wrt (.vectorFor duv-wrt "list" (FieldType/notNullable #xt.arrow/type :list))]
      (doto (-> duv-list-wrt
                (.getListElements (FieldType/notNullable #xt.arrow/type :i64)))
        (.writeLong 42)
        (.writeLong 43))

      (.endList duv-list-wrt)
      (let [copier (.rowCopier list-wrt duv)]
        (.copyRow copier 0))
      (t/is (= [[42 43]]
               (.getAsList (vw/vec-wtr->rdr list-wrt)))
            "duv to monomorphic list type vector copying")))

  (with-open [duv (DenseUnionVector/empty "my-duv" tu/*allocator*)
              struct-vec (StructVector/empty "my-struct" tu/*allocator*)]
    (let [duv-wrt (vw/->writer duv)
          struct-wrt (vw/->writer struct-vec)]
      (.writeObject duv-wrt 42)
      (t/is (thrown-with-msg?
             RuntimeException
             #"illegal copy src vector"
             (.rowCopier struct-wrt duv)))))

  (with-open [duv (DenseUnionVector/empty "my-duv" tu/*allocator*)
              struct-vec (StructVector/empty "my-struct" tu/*allocator*)]
    (let [duv-wrt (vw/->writer duv)
          struct-wrt (vw/->writer struct-vec)
          duv-struct-wrt (.vectorFor duv-wrt "struct" (FieldType/notNullable #xt.arrow/type :struct))]
      (-> (.vectorFor duv-struct-wrt "foo" (FieldType/notNullable #xt.arrow/type :i64))
          (.writeLong 42))
      (-> (.vectorFor duv-struct-wrt "bar" (FieldType/notNullable #xt.arrow/type :utf8))
          (.writeObject "forty-two"))
      (.endStruct duv-struct-wrt)
      (let [copier (.rowCopier struct-wrt duv)]
        (.copyRow copier 0))
      (t/is (= [{:foo 42, :bar "forty-two"}]
               (.toList (vw/vec-wtr->rdr struct-wrt) #xt/key-fn :kebab-case-keyword))
            "duv to monomorphic struct type vector copying"))))

(deftest testing-set-writing-reading
  (with-open [set-vec (.createVector (types/->field "my-set" #xt.arrow/type :set false
                                                    (types/col-type->field :i64)) tu/*allocator*)]
    (let [set-wrt (vw/->writer set-vec)]
      (.writeObject set-wrt #{1 2 3})
      (.writeObject set-wrt #{4 5 6})

      (t/is (= [#{1 2 3} #{4 5 6}]
               (.getAsList (vw/vec-wtr->rdr set-wrt))))

      (let [pos (VectorPosition/build)]
        (.setPosition pos 0)
        (t/is (= #{1 2 3}
                 (.readObject (.valueReader (vw/vec-wtr->rdr set-wrt) pos)))
              "valueReader testing for set")))))

(deftest struct-normalisation-testing
  (t/testing "structs"
    (with-open [rel-wtr1 (vw/->rel-writer tu/*allocator*)]
      (let [my-column-wtr1 (.vectorFor rel-wtr1 "my_column" (FieldType/notNullable #xt.arrow/type :struct))]
        (-> (.vectorFor my-column-wtr1 "long_name" (FieldType/notNullable #xt.arrow/type :i64))
            (.writeLong 42))
        (-> (.vectorFor my-column-wtr1 "short_name" (FieldType/notNullable #xt.arrow/type :utf8))
            (.writeObject "forty-two"))
        (.endStruct my-column-wtr1)
        (.endRow rel-wtr1))

      (t/is (= [{:my-column {:short-name "forty-two", :long-name 42}}]
               (.toMaps (vw/rel-wtr->rdr rel-wtr1) #xt/key-fn :kebab-case-keyword)))

      (t/is (= [{:my_column {:short_name "forty-two", :long_name 42}}]
               (.toMaps (vw/rel-wtr->rdr rel-wtr1) #xt/key-fn :snake-case-keyword))))))

(deftest multivec-underlying-monomorphic-vectors-get-leg-test ; see #3343
  (with-open [struct-int-vec (.createVector (types/->field "foo" #xt.arrow/type :struct false
                                                           (types/col-type->field "bar" :i64))
                                            tu/*allocator*)
              struct-str-vec (.createVector (types/->field "foo" #xt.arrow/type :struct false
                                                           (types/col-type->field "bar" :utf8))
                                            tu/*allocator*)]
    (letfn [(read-children [v]
              (if (instance? java.util.Map v)
                (update-vals v #(if (instance? ValueReader %) (read-children (.readObject ^ValueReader %)) %))
                v))

            (get-children-legs [v]
              (if (instance? java.util.Map v)
                (update-vals v #(if (instance? ValueReader %) (.getLeg ^ValueReader %) %))
                v))]

      (let [struct-int-wrt (vw/->writer struct-int-vec)
            struct-str-wrt (vw/->writer struct-str-vec)]

        (dotimes [_ 2]
          (.writeObject struct-int-wrt {:bar 42})
          (.writeObject struct-str-wrt {:bar "forty-two"})))

      (let [int-rdr (vr/vec->reader struct-int-vec)
            str-rdr (vr/vec->reader struct-str-vec)
            rdr-ind (VectorIndirection/selection (int-array (concat (repeat 2 0) (repeat 2 1))))
            vec-ind (VectorIndirection/selection (int-array (concat (range 2) (range 2))))
            indirect-rdr (IndirectMultiVectorReader. "foo" [int-rdr str-rdr] rdr-ind vec-ind)
            vpos (VectorPosition/build)
            value-rdr (.valueReader indirect-rdr vpos)]

        (t/is (= (types/->field "foo" #xt.arrow/type :struct false
                                (types/->field "bar" #xt.arrow/type :union false
                                               (types/col-type->field :i64)
                                               (types/col-type->field :utf8)))
                 (.getField indirect-rdr)))

        (t/is (= [{"bar" 42} {"bar" 42} {"bar" "forty-two"} {"bar" "forty-two"}]
                 (for [i (range 4)]
                   (do
                     (.setPosition vpos i)
                     (read-children (.readObject value-rdr))))))

        (t/is (= [{"bar" "i64"} {"bar" "i64"} {"bar" "utf8"} {"bar" "utf8"}]
                 (for [i (range 4)]
                   (do
                     (.setPosition vpos i)
                     (get-children-legs (.readObject value-rdr))))))))))

(t/deftest ^:property vector-read-what-you-write
  (tu/run-property-test
    {:num-tests tu/property-test-iterations}
    (prop/for-all [{:keys [vs] :as vec-gen} tg/vector-vs-gen]
      (with-open [^Vector src-vec (tg/vec-gen->arrow-vec tu/*allocator* vec-gen)]
        (tg/lists-equal-normalized? vs (.getAsList src-vec))))))

(defn- copy-vector ^Vector
  ([^Vector src-vec ^BufferAllocator al]
   (copy-vector src-vec al 0 (.getValueCount src-vec)))
  ([^Vector src-vec ^BufferAllocator al start-idx end-idx]
   (util/with-close-on-catch [out-vec (Vector/open al (.getField src-vec))]
     (let [copier (.rowCopier src-vec out-vec)]
       (doseq [i (range start-idx end-idx)]
         (.copyRow copier i))
       out-vec))))

(defn- vectors-equal?
  [^Vector src-vec ^Vector out-vec]
  (and (= (.getValueCount src-vec) (.getValueCount out-vec))
       (tg/lists-equal-normalized? (.getAsList src-vec) (.getAsList out-vec))))

(t/deftest ^:property full-vector-copy-preserves-data
  (tu/run-property-test
   {:num-tests tu/property-test-iterations}
   (prop/for-all [vec-gen tg/vector-vs-gen]
                 (with-open [^Vector src-vec (tg/vec-gen->arrow-vec tu/*allocator* vec-gen)
                             ^Vector copied-vec (copy-vector src-vec tu/*allocator*)]
                   (vectors-equal? src-vec copied-vec)))))

(t/deftest ^:property partial-vector-copy-preserves-data
  (tu/run-property-test
   {:num-tests tu/property-test-iterations}
   (prop/for-all [vec-gen (tg/fixed-length-vector-vs-gen 100)
                  start-idx (gen/choose 0 50)
                  end-idx (gen/choose 51 100)]
                 (with-open [^Vector src-vec (tg/vec-gen->arrow-vec tu/*allocator* vec-gen)
                             ^Vector copied-vec (copy-vector src-vec tu/*allocator* start-idx end-idx)]
                   (let [expected-data (subvec (:vs vec-gen) start-idx end-idx)
                         actual-data (.getAsList copied-vec)]
                     (tg/lists-equal-normalized? expected-data actual-data))))))

(defn- merge-vectors-into-duv ^Vector [^BufferAllocator al vectors]
  (util/with-close-on-catch [^Vector duv-vec (Vector/open al (types/->field "mixed" #xt.arrow/type :union true))]
    (doseq [^Vector vec vectors]
      (let [copier (.rowCopier vec duv-vec)]
        (dotimes [i (.getValueCount vec)]
          (.copyRow copier i))))

    duv-vec))

(t/deftest ^:property copy-two-distinct-single-typed-vectors
  (tu/run-property-test
   {:num-tests tu/property-test-iterations}
   (prop/for-all [[vec-gen1 vec-gen2] tg/two-distinct-single-type-vecs-gen]
                 (with-open [^Vector src-vec1 (tg/vec-gen->arrow-vec tu/*allocator* vec-gen1)
                             ^Vector src-vec2 (tg/vec-gen->arrow-vec tu/*allocator* vec-gen2)
                             ^Vector duv (merge-vectors-into-duv tu/*allocator* [src-vec1 src-vec2])]
                   (let [expected-data (concat (:vs vec-gen1) (:vs vec-gen2))
                         actual-data (.getAsList duv)]
                     (tg/lists-equal-normalized? expected-data actual-data))))))

(t/deftest ^:property copy-two-duvs
  (tu/run-property-test
   {:num-tests tu/property-test-iterations}
   (prop/for-all [[vec-gen1 vec-gen2] tg/two-distinct-duvs-gen]
                 (with-open [^Vector src-vec1 (tg/vec-gen->arrow-vec tu/*allocator* vec-gen1)
                             ^Vector src-vec2 (tg/vec-gen->arrow-vec tu/*allocator* vec-gen2)
                             ^Vector duv (merge-vectors-into-duv tu/*allocator* [src-vec1 src-vec2])]
                   (let [expected-data (concat (:vs vec-gen1) (:vs vec-gen2))
                         actual-data (.getAsList duv)]
                     (tg/lists-equal-normalized? expected-data actual-data))))))

(t/deftest ^:property multiple-type-promotions
  (tu/run-property-test
   {:num-tests tu/property-test-iterations} 
   (prop/for-all [vec-gen tg/vector-vs-gen
                  field-types (gen/vector tg/field-type-gen 1 4)]
                 (with-open [^Vector promoted-vec (reduce
                                                   (fn [^Vector old-vec field-type]
                                                     (util/with-close-on-catch [old-vec old-vec]
                                                       (.maybePromote$xtdb_core old-vec tu/*allocator* field-type)))
                                                   (tg/vec-gen->arrow-vec tu/*allocator* vec-gen)
                                                   field-types)]
                   promoted-vec))))
