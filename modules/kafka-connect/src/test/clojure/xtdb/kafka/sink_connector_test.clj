(ns xtdb.kafka.sink-connector-test
  (:require [clojure.test :as t]
            [xtdb.kafka.test-utils :refer [->config]])
  (:import (org.apache.kafka.common.config ConfigException)))

(t/deftest test-connector-config
  (t/testing "Missing url"
    (t/is (thrown? ConfigException
                   (->config {}))))
  (t/testing "Missing id.mode"
    (t/is (thrown? ConfigException
                   (->config {"jdbcUrl" "jdbc:xtdb://localhost:5432/xtdb"}))))
  (t/testing "Invalid id.mode"
    (t/is (thrown? ConfigException
                   (->config {"jdbcUrl" "jdbc:xtdb://localhost:5432/xtdb"
                              "id.mode" "invalid"}))))
  (t/testing "record_key"
    (t/testing "Valid config"
      (->config {"jdbcUrl" "jdbc:xtdb://localhost:5432/xtdb"
                 "id.mode" "record_key"})))
  (t/testing "record_value"
    (t/testing "Missing id.field"
      (t/is (thrown? ConfigException
                     (->config {"jdbcUrl" "jdbc:xtdb://localhost:5432/xtdb"
                                "id.mode" "record_value"}))))
    (t/testing "Valid config"
      (->config {"jdbcUrl" "jdbc:xtdb://localhost:5432/xtdb"
                 "id.mode" "record_value"
                 "id.field" "xt/id"}))))
