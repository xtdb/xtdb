(ns ^:kafka xtdb.tx-sink-kafka-test
  (:require [clojure.test :as t]
            [next.jdbc :as jdbc]
            [xtdb.serde :as serde]
            [xtdb.node :as xtn]
            [xtdb.test-util :as tu]
            [xtdb.util :as util]
            [xtdb.tx-sink :as tx-sink])
  (:import (org.apache.kafka.clients.consumer ConsumerRecord KafkaConsumer)
           org.testcontainers.kafka.ConfluentKafkaContainer
           org.testcontainers.utility.DockerImageName))

(def ^:private ^:dynamic *bootstrap-servers* nil)

(defonce ^ConfluentKafkaContainer kafka-container
  (ConfluentKafkaContainer. (DockerImageName/parse "confluentinc/cp-kafka:7.8.0")))

(t/use-fixtures :once
  (fn [f]
    (tu/with-container kafka-container
      (fn [^ConfluentKafkaContainer c]
        (binding [*bootstrap-servers* (.getBootstrapServers c)]
          (f))))))

(defn consume-messages [bootstrap-servers topic]
  (let [props {"bootstrap.servers" bootstrap-servers
               "group.id" (str "test-consumer-" (random-uuid))
               "key.deserializer" "org.apache.kafka.common.serialization.ByteArrayDeserializer"
               "value.deserializer" "org.apache.kafka.common.serialization.ByteArrayDeserializer"
               "auto.offset.reset" "earliest"}
        ^KafkaConsumer consumer (KafkaConsumer. ^java.util.Map props)
        ^java.util.Collection topics [topic]]
    (try
      (.subscribe consumer topics)
      (let [records (.poll consumer (java.time.Duration/ofSeconds 5))]
        (mapv (fn [^ConsumerRecord record] (.value record)) records))
      (finally
        (.close consumer)))))

(t/deftest ^:integration test-tx-sink-kafka-log
  (let [output-topic (str "xtdb.kafka-test." (random-uuid))]
    (with-open [node (xtn/start-node {:log-clusters {:my-kafka [:kafka {:bootstrap-servers *bootstrap-servers*
                                                                        :poll-duration "PT2S"}]}
                                      :log [:in-memory]
                                      :storage [:in-memory]
                                      :compactor {:threads 0}
                                      :tx-sink {:enable true
                                                :output-log [:kafka {:cluster :my-kafka, :topic output-topic}]}})]
      (jdbc/execute! node ["INSERT INTO docs RECORDS {_id: 1, a: 1, b: {c: [1, 2, 3], d: 'test'}}, {_id: 2, a: 2}, {_id: 3, a: 3}"])
      (jdbc/execute! node ["UPDATE docs SET a = 4 WHERE _id = 1"])
      (jdbc/execute! node ["DELETE FROM docs WHERE _id = 1"])
      (jdbc/execute! node ["ERASE FROM docs WHERE _id = 1"])
      (jdbc/execute! node ["INSERT INTO other RECORDS {_id: 1}"])

      ;; Consume off the topic
      (let [msgs (->> (consume-messages *bootstrap-servers* output-topic)
                      (map serde/read-transit))
            payloads (mapcat :tables msgs)
            tx-payloads (filter #(= (:table %) "txs") payloads)
            docs-payloads (filter #(= (:table %) "docs") payloads)
            other-payloads (filter #(= (:table %) "other") payloads)]
        (t/is (= 5 (count msgs)))
        (t/is (= 5 (count tx-payloads)))
        (t/is (= [:put :put :put :put :delete :erase]
                 (->> docs-payloads (mapcat :ops) (map :op))))
        (t/is (= [:put] (->> other-payloads (mapcat :ops) (map :op))))))))

(t/deftest ^:integration test-tx-sink-restart-behaviour
  (util/with-tmp-dirs #{node-dir}
    (let [log-topic (str "xtdb.kafka-test." (random-uuid))
          output-topic (str "xtdb.kafka-test." (random-uuid))
          node-opts (merge tu/*node-opts*
                           {:log-clusters {:my-kafka [:kafka {:bootstrap-servers *bootstrap-servers*}]}
                            :log [:kafka {:cluster :my-kafka :topic log-topic}]
                            :storage [:local {:path (.resolve node-dir "storage")}]
                            :compactor {:threads 0}
                            :tx-sink {:enable true
                                      :output-log [:kafka {:cluster :my-kafka, :topic output-topic}]
                                      :format :transit+json}})]
      (with-open [node (xtn/start-node node-opts)]
        (jdbc/execute! node ["INSERT INTO docs RECORDS {_id: 1}"])
        (let [msgs (consume-messages *bootstrap-servers* output-topic)]
          (t/is (= 1 (count msgs)))
          ;; NOTE: No flush here, so offset isn't be committed yet
          ,))
      ;; Restart node
      (with-open [node (xtn/start-node node-opts)]
        ;; Flush block to ensure node has processed all messages
        (tu/flush-block! node)
        (let [msgs (consume-messages *bootstrap-servers* output-topic)]
          ; above + duplicate = 2
          (t/is (= 2 (count msgs))))))))

(t/deftest ^:integration test-tx-sink-main-test
  ;; Tests that tx-sink tails the main node
  (util/with-tmp-dirs #{node-dir}
    (let [log-topic (str "xtdb.kafka-test." (random-uuid))
          output-topic (str "xtdb.kafka-test." (random-uuid))
          node-opts (merge tu/*node-opts*
                           {:log-clusters {:my-kafka [:kafka {:bootstrap-servers *bootstrap-servers*}]}
                            :log [:kafka {:cluster :my-kafka :topic log-topic}]
                            :storage [:local {:path (.resolve node-dir "storage")}]
                            :compactor {:threads 0}})]
      (with-open [node (xtn/start-node node-opts)]
        (jdbc/execute! node ["INSERT INTO docs RECORDS {_id: 1}"])
        (let [msgs (consume-messages *bootstrap-servers* output-topic)]
          (t/is (= 0 (count msgs))))
        (with-open [_tx-sink-node (tx-sink/open! (merge node-opts
                                                        {:tx-sink {:output-log [:kafka {:cluster :my-kafka, :topic output-topic}]
                                                                   :format :transit+json}}))]
          (let [msgs (consume-messages *bootstrap-servers* output-topic)]
            (t/is (= 1 (count msgs))))
          (jdbc/execute! node ["INSERT INTO docs RECORDS {_id: 1}"])
          (let [msgs (consume-messages *bootstrap-servers* output-topic)]
            (t/is (= 2 (count msgs)))))
        (jdbc/execute! node ["INSERT INTO docs RECORDS {_id: 1}"])
        (let [msgs (consume-messages *bootstrap-servers* output-topic)]
          (t/is (= 2 (count msgs))))))))
