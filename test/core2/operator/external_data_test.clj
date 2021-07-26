(ns core2.operator.external-data-test
  (:require [core2.operator.csv :as csv]
            [core2.util :as util]
            [clojure.test :as t]
            [clojure.java.io :as io]
            [core2.test-util :as tu]
            [core2.operator.arrow :as arrow]))

(t/use-fixtures :once tu/with-allocator)

(def example-data
  [[{:id "foo1", :a-long 10, :a-double 54.2, :an-inst #inst "2021"}
    {:id "foo2", :a-long 82, :a-double 1052.25, :an-inst #inst "2021-01-04"}
    {:id "foo3", :a-long -15, :a-double -1534.23, :an-inst #inst "2021-01-04T12:13"}]
   [{:id "foo4", :a-long 0, :a-double 0.0, :an-inst #inst "2021-05-21T17:30"}
    {:id "foo5", :a-long 53, :a-double 10.0, :an-inst #inst "2022"}]])

(t/deftest test-csv-cursor
  (with-open [cursor (csv/->csv-cursor tu/*allocator*
                                       (-> (io/resource "core2/operator/csv-cursor-test.csv")
                                           .toURI
                                           util/->path)
                                       {"a-long" :bigint
                                        "a-double" :float8
                                        "an-inst" :timestamp-milli}
                                       {:batch-size 3})]
    (t/is (= example-data (into [] (tu/<-cursor cursor))))))

(t/deftest test-arrow-cursor
  (with-open [cursor (arrow/->arrow-cursor tu/*allocator*
                                           (-> (io/resource "core2/operator/arrow-cursor-test.arrow")
                                               .toURI
                                               util/->path))]
    (t/is (= example-data (into [] (tu/<-cursor cursor))))))
