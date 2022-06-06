(ns core2.operator.external-data-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [core2.test-util :as tu]
            [core2.types :as types]
            [core2.util :as util]
            [core2.operator :as op])
  (:import org.apache.arrow.memory.RootAllocator
           org.apache.arrow.vector.types.pojo.Schema
           org.apache.arrow.vector.VectorSchemaRoot))

(t/use-fixtures :once tu/with-allocator)

(def example-data
  [[{:id "foo1", :a-long 10, :a-double 54.2, :an-inst (util/->zdt #inst "2021")}
    {:id "foo2", :a-long 82, :a-double 1052.25, :an-inst (util/->zdt #inst "2021-01-04")}
    {:id "foo3", :a-long -15, :a-double -1534.23, :an-inst (util/->zdt #inst "2021-01-04T12:13")}]
   [{:id "foo4", :a-long 0, :a-double 0.0, :an-inst (util/->zdt #inst "2021-05-21T17:30")}
    {:id "foo5", :a-long 53, :a-double 10.0, :an-inst (util/->zdt #inst "2022")}]])

(t/deftest test-csv-cursor
  (with-open [res (op/open-ra [:csv (-> (io/resource "core2/operator/csv-cursor-test.csv")
                                        .toURI
                                        util/->path)
                               '{id :varchar
                                 a-long :bigint
                                 a-double :float8
                                 an-inst :timestamp}
                               {:batch-size 3}])]
    (t/is (= example-data (into [] (tu/<-cursor res))))))

(def ^:private arrow-path
  (-> (io/resource "core2/operator/arrow-cursor-test.arrow")
      .toURI
      util/->path))

(t/deftest test-arrow-cursor
  (with-open [res (op/open-ra [:arrow arrow-path])]
    (t/is (= example-data (tu/<-cursor res)))))

(comment
  (with-open [al (RootAllocator.)
              root (VectorSchemaRoot/create (Schema. [(types/->field "id" types/varchar-type false)
                                                      (types/->field "a-long" types/bigint-type false)
                                                      (types/->field "a-double" types/float8-type false)
                                                      (types/->field "an-inst" types/timestamp-micro-tz-type false)])
                                            al)]
    (doto (util/build-arrow-ipc-byte-buffer root :file
            (fn [write-batch!]
              (doseq [block example-data]
                (tu/populate-root root block)
                (write-batch!))))
      (util/write-buffer-to-path arrow-path))))
