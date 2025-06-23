(ns xtdb.operator.external-data-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [xtdb.test-util :as tu]
            [xtdb.types :as types]
            [xtdb.util :as util])
  (:import org.apache.arrow.memory.RootAllocator
           org.apache.arrow.vector.types.pojo.Schema
           xtdb.arrow.Relation))

(t/use-fixtures :once tu/with-allocator)

(def example-data
  [[{:xt/id "foo1", :a-long 10, :a-double 54.2, :an-inst #xt/zdt "2021-01-01Z[UTC]"}
    {:xt/id "foo2", :a-long 82, :a-double 1052.25, :an-inst #xt/zdt "2021-01-04Z[UTC]"}
    {:xt/id "foo3", :a-long -15, :a-double -1534.23, :an-inst #xt/zdt "2021-01-04T12:13Z[UTC]"}]
   [{:xt/id "foo4", :a-long 0, :a-double 0.0, :an-inst #xt/zdt "2021-05-21T17:30Z[UTC]"}
    {:xt/id "foo5", :a-long 53, :a-double 10.0, :an-inst #xt/zdt "2022-01-01Z[UTC]"}]])

(t/deftest test-csv-cursor
  (t/is (= {:col-types '{_id :utf8, a-long :i64, a-double :f64, an-inst [:timestamp-tz :micro "UTC"]}
            :res example-data}
           (tu/query-ra [:csv (-> (io/resource "xtdb/operator/csv-cursor-test.csv")
                                  .toURI
                                  util/->path)
                         '{_id :utf8
                           a-long :i64
                           a-double :f64
                           an-inst :timestamp}
                         {:batch-size 3}]
                        {:preserve-pages? true
                         :with-col-types? true}))))

(def ^:private arrow-stream-url
  (io/resource "xtdb/operator/arrow-cursor-test.arrows"))

(def ^:private arrow-file-url
  (io/resource "xtdb/operator/arrow-cursor-test.arrow"))

(t/deftest test-arrow-cursor
  (let [expected {:col-types '{_id :utf8, a_long :i64, a_double :f64, an_inst [:timestamp-tz :micro "UTC"]}
                  :res example-data}]
    (t/is (= expected (tu/query-ra [:arrow arrow-file-url]
                                   {:preserve-pages? true, :with-col-types? true})))
    (t/is (= expected (tu/query-ra [:arrow arrow-stream-url]
                                   {:preserve-pages? true, :with-col-types? true})))))

(comment
  (let [arrow-path (-> (io/resource "xtdb/operator/arrow-cursor-test.arrow")
                       .toURI
                       util/->path)]
    (with-open [al (RootAllocator.)
                rel (Relation/open al
                                   (Schema. [(types/col-type->field "_id" :utf8)
                                             (types/col-type->field "a_long" :i64)
                                             (types/col-type->field "a_double" :f64)
                                             (types/col-type->field "an_inst" [:timestamp-tz :micro "UTC"])]))
                ch (util/->file-channel arrow-path util/write-truncate-open-opts)
                unl (.startUnload rel ch xtdb.arrow.Relation$UnloadMode/FILE)]
      (doseq [page example-data]
        (doseq [row page]
          (.writeRow rel row))

        (.writePage unl)
        (.clear rel))

      (.end unl))))
