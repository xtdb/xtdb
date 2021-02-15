(ns core2.select-test
  (:require [core2.select :as sel]
            [clojure.test :as t])
  (:import org.apache.arrow.memory.RootAllocator
           [org.apache.arrow.vector BigIntVector BitVector]
           org.apache.arrow.vector.holders.NullableBigIntHolder))

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

(defn res->coll [^BitVector res]
  (filterv (fn [idx]
             (pos? (.get res idx)))
           (range (.getValueCount res))))

(t/deftest test-filter-query
  (with-open [foo-vec (bigint-vec "foo" [12 52 30])]
    (letfn [(select [pred value]
              (with-open [res-vec (sel/open-result-vec *allocator* 3)]
                (.select (sel/->selector (sel/->vec-pred pred (->bigint-holder value)))
                         foo-vec
                         res-vec)
                (res->coll res-vec)))]

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
              (with-open [res-vec (sel/open-result-vec *allocator* 3)]
                (.select (sel/->selector (sel/->vec-pred foo-pred (->bigint-holder foo-value)))
                         foo-vec
                         res-vec)
                (.select (sel/->selector (sel/->vec-pred bar-pred (->bigint-holder bar-value)))
                         bar-vec
                         res-vec)
                (res->coll res-vec)))]
      (t/is (= [2] (select sel/pred= 30, sel/pred= 25)))
      (t/is (= [] (select sel/pred= 30, sel/pred= 20)))

      (t/testing "range queries"
        (t/is (= [] (select sel/pred> 10, sel/pred> 25)))

        (t/is (= [1] (select sel/pred> 25, sel/pred< 20)))))))

(t/deftest test-search
  (with-open [foo-vec (bigint-vec "foo" [12 12 30 30 52 52 52])]
    (letfn [(first-index-of [pred value]
              (sel/first-index-of foo-vec pred (->bigint-holder value)))]

      (t/is (= 0 (first-index-of sel/pred= 12)))
      (t/is (= 2 (first-index-of sel/pred= 30)))
      (t/is (= 4 (first-index-of sel/pred= 52)))

      (t/is (= -1 (first-index-of sel/pred= 10)))
      (t/is (= -1 (first-index-of sel/pred= 20)))
      (t/is (= -1 (first-index-of sel/pred= 40)))
      (t/is (= -1 (first-index-of sel/pred= 60)))

      (t/testing "range queries"
        (t/is (= 0 (first-index-of sel/pred>= 10)))
        (t/is (= 0 (first-index-of sel/pred>= 12)))
        (t/is (= 2 (first-index-of sel/pred>= 25)))
        (t/is (= 4 (first-index-of sel/pred>= 52)))
        (t/is (= 7 (first-index-of sel/pred>= 60)))

        (t/is (= 0 (first-index-of sel/pred> 10)))
        (t/is (= 2 (first-index-of sel/pred> 12)))
        (t/is (= 2 (first-index-of sel/pred> 25)))
        (t/is (= 4 (first-index-of sel/pred> 30)))
        (t/is (= 7 (first-index-of sel/pred> 52)))
        (t/is (= 7 (first-index-of sel/pred> 60)))))))
