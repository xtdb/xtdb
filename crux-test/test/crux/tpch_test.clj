(ns crux.tpch-test
  (:require [clojure.test :as t]
            [crux.fixtures :as f]
            [crux.fixtures.api :as fapi :refer [*api*]]
            [crux.fixtures.calcite :as cf :refer [query]]
            [crux.fixtures.kv :as kvf]
            [crux.fixtures.standalone :as fs]
            [crux.fixtures.tpch :as tf])
  (:import io.airlift.tpch.TpchTable))

(t/use-fixtures :each fs/with-standalone-node cf/with-calcite-module kvf/with-kv-dir fapi/with-node cf/with-calcite-connection)

(t/deftest test-tpch-schema
  (t/is (f/transact! *api* (tf/tpch-tables->crux-sql-schemas)))

  (f/transact! *api* (take 5 (tf/tpch-table->docs (first (TpchTable/getTables)))))

  (t/is (= 5 (count (query "SELECT * FROM CUSTOMER")))))
