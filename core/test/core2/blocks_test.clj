(ns core2.blocks-test
  (:require [clojure.test :as t]
            [core2.blocks :as blocks]
            [core2.test-util :as tu]
            [core2.types :as ty])
  (:import org.apache.arrow.vector.BigIntVector
           org.apache.arrow.vector.complex.ListVector))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-list-count-blocks
  (letfn [(row-count-seq [el-counts max-el-count]
            (with-open [^ListVector
                        list-vec (.createVector (ty/->field "my-list" ty/list-type false
                                                            (ty/->field "els" ty/bigint-type false))
                                                tu/*allocator*)]
              (.setValueCount list-vec (count el-counts))
              (let [^BigIntVector el-vec (.getDataVector list-vec)]
                (dorun
                 (map-indexed (fn [idx el-count]
                                (let [start-idx (.startNewValue list-vec idx)]
                                  (.setValueCount el-vec (+ start-idx el-count))
                                  (dotimes [n el-count]
                                    (.setSafe el-vec (+ start-idx n) n))
                                  (.endValue list-vec idx el-count)))
                              el-counts)))

              (blocks/list-count-blocks list-vec max-el-count)))]

    (t/is (= [] (row-count-seq [] 10)))
    (t/is (= [1] (row-count-seq [10] 10)))
    (t/is (= [1] (row-count-seq [12] 10)))
    (t/is (= [2 1] (row-count-seq [5 8 5] 10)))
    (t/is (= [1 1 1] (row-count-seq [15 18 15] 10)))
    (t/is (= [1 1 2] (row-count-seq [15 18 5 15] 10)))
    (t/is (= [5 2] (row-count-seq [1 1 2 3 5 8 13] 10)))
    (t/is (= [2] (row-count-seq [0 0] 10)))
    (t/is (= [4 1] (row-count-seq [0 0 5 5 5] 10)))))
