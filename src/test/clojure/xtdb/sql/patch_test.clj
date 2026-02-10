(ns xtdb.sql.patch-test
  (:require [clojure.test :as t]
            [xtdb.sql :as sql]
            [xtdb.time :as time]
            [xtdb.tx-ops :as tx-ops]))

(t/deftest test-sql->static-ops-patch-null-valid-time-4448
  (t/is (= [(tx-ops/map->PatchDocs {:table-name 'public/foo, :docs [{"_id" 1}]
                                    :valid-from time/start-of-time, :valid-to time/end-of-time})]
           (sql/sql->static-ops "PATCH INTO foo FOR PORTION OF VALID_TIME FROM NULL TO NULL RECORDS {_id: 1}" nil))
        "FROM NULL → start-of-time, TO NULL → end-of-time")

  (t/is (= [(tx-ops/map->PatchDocs {:table-name 'public/foo, :docs [{"_id" 1}]
                                    :valid-from time/start-of-time, :valid-to time/end-of-time})]
           (sql/sql->static-ops "PATCH INTO foo FOR VALID_TIME FROM NULL RECORDS {_id: 1}" nil))
        "FROM NULL without TO leaves valid-to as nil")

  (t/is (= [(tx-ops/map->PatchDocs {:table-name 'public/foo, :docs [{"_id" 1}]
                                    :valid-from "2020-01-05", :valid-to #xt/date "2020-01-07"})]
           (sql/sql->static-ops "PATCH INTO foo FOR VALID_TIME FROM '2020-01-05' TO DATE '2020-01-07' RECORDS {_id: 1}" nil))
        "non-temporal valid-time passes through (caught by indexer assert-timestamp-col-type)"))

(t/deftest test-sql->static-ops-patch-5231
  (t/is (= [(tx-ops/map->PatchDocs {:table-name 'public/foo, :docs [{"_id" 1, "v" 2}]
                                     :valid-from nil, :valid-to nil})]
           (sql/sql->static-ops "PATCH INTO foo RECORDS {_id: 1, v: 2}" nil))
        "basic patch with literal record")

  (t/is (= [(tx-ops/map->PatchDocs {:table-name 'public/foo, :docs [{"_id" 1, "v" 2} {"_id" 3, "v" 4}]
                                     :valid-from nil, :valid-to nil})]
           (sql/sql->static-ops "PATCH INTO foo RECORDS {_id: 1, v: 2}, {_id: 3, v: 4}" nil))
        "multiple records batched together")

  (t/is (= [(tx-ops/map->PatchDocs {:table-name 'public/foo, :docs [{"_id" 1, "v" 2}]
                                     :valid-from #xt/date "2020-08-01", :valid-to time/end-of-time})]
           (sql/sql->static-ops "PATCH INTO foo FOR VALID_TIME FROM DATE '2020-08-01' RECORDS {_id: 1, v: 2}" nil))
        "valid-time FROM literal")

  (t/is (= [(tx-ops/map->PatchDocs {:table-name 'public/foo, :docs [{"_id" 1, "v" 2}]
                                     :valid-from #xt/date "2020-08-01", :valid-to #xt/date "2021-01-01"})]
           (sql/sql->static-ops "PATCH INTO foo FOR VALID_TIME FROM DATE '2020-08-01' TO DATE '2021-01-01' RECORDS {_id: 1, v: 2}" nil))
        "valid-time FROM + TO literals")

  (t/testing "with param records"
    (t/is (= [(tx-ops/map->PatchDocs {:table-name 'public/bar, :docs [{"_id" 0, "value" "hola"} {"_id" 1, "value" "mundo"}]
                                       :valid-from nil, :valid-to nil})]
             (sql/sql->static-ops "PATCH INTO bar RECORDS $1"
                                  [[{"_id" 0, "value" "hola"}]
                                   [{"_id" 1, "value" "mundo"}]]))))

  (t/testing "parameterized valid-time groups across arg-rows"
    (t/is (= [(tx-ops/map->PatchDocs {:table-name 'public/foo, :docs [{"_id" 1}]
                                       :valid-from #xt/date "2020-01-01", :valid-to time/end-of-time})
              (tx-ops/map->PatchDocs {:table-name 'public/foo, :docs [{"_id" 2}]
                                       :valid-from #xt/date "2020-01-02", :valid-to time/end-of-time})]
             (sql/sql->static-ops "PATCH INTO foo FOR VALID_TIME FROM ? RECORDS {_id: ?}"
                                  [[#xt/date "2020-01-01" 1]
                                   [#xt/date "2020-01-02" 2]]))))

  (t/testing "expression in record returns nil (graceful fallback)"
    (t/is (nil? (sql/sql->static-ops "PATCH INTO foo RECORDS {_id: 1 + 2, v: 2}" nil)))))
