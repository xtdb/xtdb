(ns crux.calcite-test
  (:require [clojure.test :as t]
            [crux.codec :as c]
            [crux.fixtures.api :as fapi :refer [*api*]]
            [crux.fixtures.kv :as kvf]
            [crux.api :as api]
            [crux.fixtures.standalone :as fs]
            [crux.kv :as kv]
            [crux.fixtures :as f])
  (:import java.sql.DriverManager))

(t/use-fixtures :each fs/with-standalone-node kvf/with-kv-dir fapi/with-node)

(t/deftest test-hello-world-query
  (f/transact! *api* (f/people [{:crux.db/id :ivan :name "Ivan" :last-name "Ivanov"}
                                {:crux.db/id :petr :name "Petr" :last-name "Petrov"}]))

  (t/testing "Can query value by single field"
    (t/is (= #{["Ivan"]} (api/q (api/db *api*) '{:find [name]
                                                 :where [[e :name "Ivan"]
                                                         [e :name name]]}))))

  (let [conn (DriverManager/getConnection "jdbc:calcite:model=src/juxt/calcite_play/model.json")
        stmt (.createStatement conn)]
    (resultset-seq (.executeQuery stmt "select name from product limit 2"))))
