(ns crux.calcite-test
  (:require [clojure.test :as t]
            [crux.codec :as c]
            [crux.fixtures.api :as fapi :refer [*api*]]
            [crux.fixtures.kv :as kvf]
            [crux.api :as api]
            [crux.fixtures.standalone :as fs]
            [crux.kv :as kv]
            [crux.fixtures :as f])
  (:import java.sql.DriverManager
           crux.calcite.CruxSchemaFactory))

(t/use-fixtures :each fs/with-standalone-node kvf/with-kv-dir fapi/with-node)

(t/deftest test-hello-world-query
  (f/transact! *api* (f/people [{:crux.db/id :ivan :name "Ivan" :homeworld "Earth"}]))

  (t/testing "Can query value by single field"
    (t/is (= #{["Ivan"]} (api/q (api/db *api*) '{:find [name]
                                                 :where [[e :name "Ivan"]
                                                         [e :name name]]}))))

  (let [conn (DriverManager/getConnection "jdbc:calcite:model=crux-calcite/resources/model.json")
        stmt (.createStatement conn)]

    ;; Matches the hardcoded SW
    (t/is (= [{:name "Ivan"}] (resultset-seq
                               (.executeQuery stmt "SELECT PERSON.NAME FROM PERSON"))))))
