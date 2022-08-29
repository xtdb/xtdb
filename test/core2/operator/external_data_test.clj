(ns core2.operator.external-data-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [core2.test-util :as tu]
            [core2.types :as types]
            [core2.util :as util])
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
  (t/is (= {:col-types {"id" :utf8, "a-long" :i64, "a-double" :f64, "an-inst" [:timestamp-tz :micro "UTC"]}
            :res example-data}
           (tu/query-ra [:csv (-> (io/resource "core2/operator/csv-cursor-test.csv")
                                  .toURI
                                  util/->path)
                         '{id :utf8
                           a-long :i64
                           a-double :f64
                           an-inst :timestamp}
                         {:batch-size 3}]
                        {:preserve-blocks? true
                         :with-col-types? true}))))

(def ^:private arrow-stream-url
  (io/resource "core2/operator/arrow-cursor-test.arrows"))

(def ^:private arrow-file-url
  (io/resource "core2/operator/arrow-cursor-test.arrow"))

(t/deftest test-arrow-cursor
  (let [expected {:col-types {"id" :utf8, "a-long" :i64, "a-double" :f64, "an-inst" [:timestamp-tz :micro "UTC"]}
                  :res example-data}]
    (t/is (= expected (tu/query-ra [:arrow arrow-file-url]
                                   {:preserve-blocks? true, :with-col-types? true})))
    (t/is (= expected (tu/query-ra [:arrow arrow-stream-url]
                                   {:preserve-blocks? true, :with-col-types? true})))))

(comment
  (let  [arrow-path (-> (io/resource "core2/operator/arrow-cursor-test.arrows")
                        .toURI
                        util/->path)]
    (with-open [al (RootAllocator.)
                root (VectorSchemaRoot/create (Schema. [(types/col-type->field "id" :utf8)
                                                        (types/col-type->field "a-long" :i64)
                                                        (types/col-type->field "a-double" :f64)
                                                        (types/col-type->field "an-inst" [:timestamp-tz :micro "UTC"])])
                                              al)]
      (doto (util/build-arrow-ipc-byte-buffer root :stream
              (fn [write-batch!]
                (doseq [block example-data]
                  (tu/populate-root root block)
                  (write-batch!))))
        (util/write-buffer-to-path arrow-path)))))
