(ns xtdb.kafka.connect-test
  (:require [clojure.test :as t]
            [xtdb.kafka.test-utils :as tu :refer [*node*]]
            [xtdb.kafka.connect :as kc]
            [xtdb.api :as xt])
  (:import (xtdb.kafka.connect XtdbSinkConnector)
           (org.apache.kafka.connect.sink SinkRecord)))

(t/use-fixtures :each tu/with-node)

(defn ->sink-record [{:keys [topic partition
                             key-schema key-value
                             value-schema value-value
                             offset]
                      :or {partition 0 offset 0}}]
  (SinkRecord. topic partition
               key-schema key-value
               value-schema value-value
               offset))

(defn ->config [config]
  (let [connector (XtdbSinkConnector.)]
    (try
      (.start connector config)
      (first (.taskConfigs connector 1))
      (finally
        (.stop connector)))))

(comment
  (->config {XtdbSinkConnector/URL_CONFIG "test"
             XtdbSinkConnector/ID_MODE_CONFIG "record_key"}))

(t/deftest e2e-test
  (let [sink (partial kc/submit-sink-records *node*)]
    (t/testing "basic record_key"
      (let [props (->config {XtdbSinkConnector/URL_CONFIG "url"
                             XtdbSinkConnector/ID_MODE_CONFIG "record_key"})]
        (sink props [(->sink-record
                       {:topic "foo"
                        :key-value 1
                        :value-value {:value {:a 1}}})])
        (t/is
          (= (xt/q *node* "SELECT * FROM foo")
             [{:xt/id 1
               :value {:a 1}}]))))
    (t/testing "full config"
      (let [props (->config {XtdbSinkConnector/URL_CONFIG "url"
                             XtdbSinkConnector/ID_MODE_CONFIG "record_value"
                             XtdbSinkConnector/ID_FIELD_CONFIG "my-id-field"
                             XtdbSinkConnector/VALID_FROM_FIELD_CONFIG "my-valid-from-field"
                             XtdbSinkConnector/VALID_TO_FIELD_CONFIG "my-valid-to-field"
                             XtdbSinkConnector/TABLE_NAME_FORMAT_CONFIG "pre_${topic}_post"})]
        (sink props [(->sink-record
                       {:topic "foo"
                        :value-value {:my-id-field 1
                                      :my-valid-from-field #inst "2021-01-01T00:00:00Z"
                                      :my-valid-to-field #inst "2021-01-02T00:00:00Z"
                                      :value {:a 1}}})])
        (t/is
          (= (xt/q *node* "SELECT foo.*, _valid_from, _valid_to FROM pre_foo_post FOR VALID_TIME ALL AS foo")
             [{:xt/id 1
               :my-id-field 1
               :xt/valid-from #xt.time/zoned-date-time "2021-01-01T00:00:00Z[UTC]"
               :my-valid-from-field #xt.time/zoned-date-time "2021-01-01T00:00:00Z[UTC]"
               :xt/valid-to #xt.time/zoned-date-time "2021-01-02T00:00:00Z[UTC]"
               :my-valid-to-field #xt.time/zoned-date-time "2021-01-02T00:00:00Z[UTC]"
               :value {:a 1}}]))))))
