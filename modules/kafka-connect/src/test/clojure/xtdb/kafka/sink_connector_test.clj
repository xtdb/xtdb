(ns xtdb.kafka.sink-connector-test
  (:require [clojure.test :as t])
  (:import (org.apache.kafka.common.config ConfigException)
           (xtdb.kafka.connect XtdbSinkConfig)))

(defn ->config [config]
  (XtdbSinkConfig/parse config))

(t/deftest test-connector-config
  (t/is (thrown-with-msg? ConfigException #"connection.url"
          (->config {})))

  (t/is (thrown-with-msg? ConfigException #"id.mode"
          (->config {"connection.url" "jdbc:xtdb://localhost:5432/xtdb"
                     "id.mode" "invalid"})))

  (->config {"connection.url" "jdbc:xtdb://localhost:5432/xtdb"
             "id.mode" "record_key"}))
