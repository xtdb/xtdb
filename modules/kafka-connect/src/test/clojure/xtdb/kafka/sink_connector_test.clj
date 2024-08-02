(ns xtdb.kafka.sink-connector-test
  (:require [clojure.test :as t])
  (:import (xtdb.kafka.connect XtdbSinkConnector)
           (org.apache.kafka.common.config ConfigException)))

(defn try-config [config]
  (let [connector (XtdbSinkConnector.)]
    (try
      (.start connector config)
      (finally
        (.stop connector)))))

(t/deftest test-connector-config
  (t/testing "Missing url"
    (t/is (thrown? ConfigException
                   (try-config {}))))
  (t/testing "Missing id.mode"
    (t/is (thrown? ConfigException
                   (try-config {XtdbSinkConnector/URL_CONFIG "http://localhost:3000"}))))
  (t/testing "Invalid id.mode"
    (t/is (thrown? ConfigException
                   (try-config {XtdbSinkConnector/URL_CONFIG "http://localhost:3000"
                                XtdbSinkConnector/ID_MODE_CONFIG "invalid"}))))
  (t/testing "record_key"
    (t/testing "Valid config"
      (try-config {XtdbSinkConnector/URL_CONFIG "http://localhost:3000"
                   XtdbSinkConnector/ID_MODE_CONFIG "record_key"})))
  (t/testing "record_value"
    (t/testing "Missing id.field"
      (t/is (thrown? ConfigException
                     (try-config {XtdbSinkConnector/URL_CONFIG "http://localhost:3000"
                                  XtdbSinkConnector/ID_MODE_CONFIG "record_value"}))))
    (t/testing "Valid config"
      (try-config {XtdbSinkConnector/URL_CONFIG "http://localhost:3000"
                   XtdbSinkConnector/ID_MODE_CONFIG "record_value"
                   XtdbSinkConnector/ID_FIELD_CONFIG "xt/id"}))))
