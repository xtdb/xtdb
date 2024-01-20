(ns xtdb.kafka-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt] 
            [xtdb.node :as xtn]
            [xtdb.test-util :as tu]
            [xtdb.kafka])
  (:import [java.util UUID]
           [xtdb.api.log Log]))

(t/deftest ^:requires-docker ^:kafka test-kafka
  (let [topic-name (str "xtdb.kafka-test." (UUID/randomUUID))]
    (with-open [node (xtn/start-node {:log [:kafka {:bootstrap-servers "localhost:9092"
                                                    :topic-name topic-name}]})]
      (xt/submit-tx node [[:put :xt_docs {:xt/id :foo}]])

      (t/is (= [{:xt/id :foo}]
               (tu/query-ra '[:scan {:table xt_docs} [xt/id]]
                            {:node node}))))))

(t/deftest ^:requires-docker ^:kafka test-kafka-setup-with-provided-opts
  (let [topic-name (str "xtdb.kafka-test." (UUID/randomUUID))]
    (with-open [node (xtn/start-node {:log [:kafka {:topic-name topic-name
                                                    :bootstrap-servers "localhost:9092"
                                                    :create-topic? true
                                                    :replication-factor 1
                                                    :poll-duration "PT2S"
                                                    :topic-config {}
                                                    :properties-map {}
                                                    :properties-file nil}]})]
      (t/testing "KafkaLog successfully created"
        (let [log (get-in node [:system :xtdb/log])]
          (t/is (instance? Log log)))))))
