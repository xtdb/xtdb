(ns crux.tpch-test
  (:require [clojure.test :as t]
            [crux.db :as db]
            [crux.fixtures :as f]
            [crux.fixtures.api :as fapi :refer [*api*]]
            [crux.fixtures.calcite :as cf :refer [query]]
            [crux.fixtures.tpch :as tf]
            [crux.fixtures.kv :as kvf]
            [crux.fixtures.standalone :as fs]))

(t/use-fixtures :each fs/with-standalone-node cf/with-calcite-module kvf/with-kv-dir fapi/with-node cf/with-calcite-connection)

(t/deftest test-tpch-schema
  (t/is (f/transact! *api* (tf/tpch-tables->crux-sql-schemas)))

  (t/is (= [{:name "Ivan" :age 21}]
           (query "SELECT * FROM CUSTOMER"))))
