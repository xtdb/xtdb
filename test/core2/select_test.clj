(ns core2.select-test
  (:require [clojure.test :as t]
            [core2.select :as sel]
            [core2.test-util :as tu])
  (:import java.util.List
           [org.apache.arrow.memory RootAllocator]
           [org.apache.arrow.vector BigIntVector FieldVector VarCharVector VectorSchemaRoot]
           org.apache.arrow.vector.holders.NullableBigIntHolder
           org.apache.arrow.vector.util.Text))

(def ^:dynamic ^org.apache.arrow.memory.BufferAllocator *allocator*)

(t/use-fixtures :once
  (fn [f]
    (with-open [allocator (RootAllocator.)]
      (binding [*allocator* allocator]
        (f)))))

(defn ->bigint-holder [^long value]
  (doto (NullableBigIntHolder.)
    (-> .isSet (set! 1))
    (-> .value (set! value))))

(defn bigint-vec ^org.apache.arrow.vector.BigIntVector [^String vec-name coll]
  (let [res (BigIntVector. vec-name *allocator*)]
    (.setValueCount res (count coll))
    (dotimes [n (count coll)]
      (.setSafe res n ^long (nth coll n)))
    res))

(t/deftest test-filter-query
  (with-open [foo-vec (bigint-vec "foo" [12 52 30])]
    (letfn [(select [pred value]
              (-> (sel/select foo-vec (sel/->vec-pred pred (->bigint-holder value)))
                  vec))]

      (t/is (= [2] (select sel/pred= 30)))
      (t/is (= [] (select sel/pred= 25)))

      (t/testing "range queries"
        (t/is (= [1 2] (select sel/pred> 25)))

        (t/is (= [0 2] (select sel/pred<= 30)))

        (t/is (= [0] (select sel/pred< 30)))))))

(t/deftest test-multiple-filters
  (with-open [foo-vec (bigint-vec "foo" [12 52 30])
              bar-vec (bigint-vec "bar" [10 12 25])]

    (letfn [(select [foo-pred foo-value, bar-pred bar-value]
              (-> (sel/select foo-vec (sel/->vec-pred foo-pred (->bigint-holder foo-value)))
                  (sel/select bar-vec (sel/->vec-pred bar-pred (->bigint-holder bar-value)))
                  vec))]
      (t/is (= [2] (select sel/pred= 30, sel/pred= 25)))
      (t/is (= [] (select sel/pred= 30, sel/pred= 20)))

      (t/testing "range queries"
        (t/is (= [] (select sel/pred> 10, sel/pred> 25)))
        (t/is (= [1] (select sel/pred> 25, sel/pred< 20)))))))

(t/deftest test-search
  (with-open [foo-vec (bigint-vec "foo" [12 12 30 30 52 52 52])]
    (letfn [(search [value]
              (-> (sel/search foo-vec (sel/->vec-compare (->bigint-holder value)))
                  vec))]

      (t/is (= [0 1] (search 12)))
      (t/is (= [2 3] (search 30)))
      (t/is (= [4 5 6] (search 52)))

      (t/is (= [] (search 10)))
      (t/is (= [] (search 20)))
      (t/is (= [] (search 40)))
      (t/is (= [] (search 60))))))

(t/deftest test-align
  (with-open [age-vec (doto (BigIntVector. "age" *allocator*)
                        (.setValueCount 5)
                        (.setSafe 0 12)
                        (.setSafe 1 42)
                        (.setSafe 2 15)
                        (.setSafe 3 83)
                        (.setSafe 4 25))

              age-row-id-vec (doto (BigIntVector. "_row-id" *allocator*)
                               (.setValueCount 5)
                               (.setSafe 0 2)
                               (.setSafe 1 5)
                               (.setSafe 2 9)
                               (.setSafe 3 12)
                               (.setSafe 4 13))

              age-vsr (let [^List vecs [age-row-id-vec age-vec]]
                        (VectorSchemaRoot. vecs))

              name-vec (doto (VarCharVector. "name" *allocator*)
                         (.setValueCount 4)
                         (.setSafe 0 (Text. "Al"))
                         (.setSafe 1 (Text. "Dave"))
                         (.setSafe 2 (Text. "Bob"))
                         (.setSafe 3 (Text. "Steve")))

              name-row-id-vec (doto (BigIntVector. "_row-id" *allocator*)
                                (.setValueCount 4)
                                (.setSafe 0 1)
                                (.setSafe 1 2)
                                (.setSafe 2 9)
                                (.setSafe 3 13))

              name-vsr (let [^List vecs [name-row-id-vec name-vec]]
                         (VectorSchemaRoot. vecs))]

    (let [row-ids (doto (sel/->row-id-bitmap (sel/select age-vec (sel/->vec-pred sel/pred<= (doto (NullableBigIntHolder.)
                                                                                              (-> .isSet (set! 1))
                                                                                              (-> .value (set! 30)))))
                                             age-row-id-vec)
                    (.and (sel/->row-id-bitmap (sel/select name-vec (sel/->str-pred sel/pred<= "Frank"))
                                               name-row-id-vec)))]
      (with-open [^VectorSchemaRoot vsr (sel/align-vectors *allocator* [name-vsr age-vsr] row-ids)]
        (t/is (= [[2 (Text. "Dave") 12]
                  [9 (Text. "Bob") 15]]
                 (tu/vsr->rows vsr)))))))
