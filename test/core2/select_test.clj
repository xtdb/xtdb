(ns core2.select-test
  (:require [clojure.test :as t]
            [core2.select :as sel]
            [core2.test-util :as tu])
  (:import org.apache.arrow.vector.BigIntVector
           org.apache.arrow.vector.holders.NullableBigIntHolder))

(t/use-fixtures :each tu/with-allocator)

(defn ->bigint-holder [^long value]
  (doto (NullableBigIntHolder.)
    (-> .isSet (set! 1))
    (-> .value (set! value))))

(defn bigint-vec ^org.apache.arrow.vector.BigIntVector [^String vec-name coll]
  (let [res (BigIntVector. vec-name tu/*allocator*)]
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
