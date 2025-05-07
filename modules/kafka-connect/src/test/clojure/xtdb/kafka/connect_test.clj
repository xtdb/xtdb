(ns xtdb.kafka.connect-test
  (:require [clojure.test :as t]
            [xtdb.kafka.test-utils :as ktu]
            [xtdb.test-util :as tu]
            [xtdb.kafka.connect :as kc]
            [xtdb.api :as xt])
  (:import (org.apache.kafka.connect.sink SinkRecord)))

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

(t/deftest e2e-test
  (let [sink (partial kc/submit-sink-records tu/*node*)]
    (t/testing "basic record_key"
      (let [props (ktu/->config {"jdbcUrl" "jdbcUrl"
                                 "id.mode" "record_key"})]
        (sink props [(->sink-record
                      {:topic "foo"
                       :key-value 1
                       :value-value {:value {:a 1}}})])
        (t/is
         (= (xt/q tu/*node* "SELECT * FROM foo")
            [{:xt/id 1
              :value {:a 1}}]))))

    (t/testing "full config"
      (let [props (ktu/->config {"jdbcUrl" "jdbcUrl"
                                 "id.mode" "record_value"
                                 "id.field" "my-id-field"
                                 "validFrom.field" "my-valid-from-field"
                                 "validTo.field" "my-valid-to-field"
                                 "table.name.format" "pre_${topic}_post"})]
        (sink props [(->sink-record
                      {:topic "foo"
                       :value-value {:my-id-field 1
                                     :my-valid-from-field #inst "2021-01-01T00:00:00Z"
                                     :my-valid-to-field #inst "2021-01-02T00:00:00Z"
                                     :value {:a 1}}})])
        (t/is
         (= (xt/q tu/*node* "SELECT foo.*, _valid_from, _valid_to FROM pre_foo_post FOR VALID_TIME ALL AS foo")
            [{:xt/id 1
              :my-id-field 1
              :xt/valid-from #xt/zoned-date-time "2021-01-01T00:00:00Z[UTC]"
              :my-valid-from-field #xt/zoned-date-time "2021-01-01T00:00:00Z[UTC]"
              :xt/valid-to #xt/zoned-date-time "2021-01-02T00:00:00Z[UTC]"
              :my-valid-to-field #xt/zoned-date-time "2021-01-02T00:00:00Z[UTC]"
              :value {:a 1}}]))))))
