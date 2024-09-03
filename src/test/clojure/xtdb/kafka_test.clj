(ns xtdb.kafka-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.kafka]
            [xtdb.node :as xtn]
            [xtdb.test-util :as tu])
  (:import [xtdb.api.log Log]))

(t/deftest ^:requires-docker ^:kafka test-kafka
  (let [test-uuid (random-uuid)]
    (with-open [node (xtn/start-node {:log [:kafka {:bootstrap-servers "localhost:9092"
                                                    :tx-topic (str "xtdb.kafka-test.tx-" test-uuid)
                                                    :files-topic (str "xtdb.kafka-test.files-" test-uuid)}]})]
      (t/is (= true
               (:committed? (xt/execute-tx node [[:put-docs :xt_docs {:xt/id :foo}]]))))

      (t/is (= [{:xt/id :foo}]
               (tu/query-ra '[:scan {:table xt_docs} [_id]]
                            {:node node}))))))

(t/deftest ^:requires-docker ^:kafka test-kafka-setup-with-provided-opts
  (let [test-uuid (random-uuid)]
    (with-open [node (xtn/start-node {:log [:kafka {:tx-topic (str "xtdb.kafka-test.tx-" test-uuid)
                                                    :files-topic (str "xtdb.kafka-test.files-" test-uuid)
                                                    :bootstrap-servers "localhost:9092"
                                                    :create-topics? true
                                                    :tx-poll-duration "PT2S"
                                                    :properties-map {}
                                                    :properties-file nil}]})]
      (t/testing "KafkaLog successfully created"
        (let [log (get-in node [:system :xtdb/log])]
          (t/is (instance? Log log)))))))
