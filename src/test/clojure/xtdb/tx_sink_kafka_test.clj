(ns ^:kafka xtdb.tx-sink-kafka-test
  (:require [clojure.test :as t]
            [next.jdbc :as jdbc]
            [xtdb.db-catalog :as db]
            [xtdb.garbage-collector :as gc]
            [xtdb.node :as xtn]
            [xtdb.serde :as serde]
            [xtdb.test-util :as tu]
            [xtdb.tx-sink :as tx-sink]
            [xtdb.util :as util]
            [xtdb.api :as xt])
  (:import [org.apache.kafka.clients.consumer ConsumerRecord KafkaConsumer]
           org.testcontainers.kafka.ConfluentKafkaContainer
           org.testcontainers.utility.DockerImageName
           [xtdb.test.log RecordingLog]
           [xtdb.api.storage ObjectStore$StoredObject]))

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
        ^java.util.Collection topics [topic]]
    (with-open [consumer (KafkaConsumer. ^java.util.Map props)]
      (.subscribe consumer topics)
      (let [records (.poll consumer (java.time.Duration/ofSeconds 5))]
        (mapv (fn [^ConsumerRecord record] (.value record)) records)))))

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
                      (map #(serde/read-transit % :json)))
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

(t/deftest ^:integration test-block-deleted-during-backfill
  (util/with-tmp-dirs #{node-dir}
    (let [log-topic (str "xtdb.kafka-test." (random-uuid))]
      ;; Create block 0
      (with-open [node (xtn/start-node {:storage [:local {:path (.resolve node-dir "storage")}]
                                        :log-clusters {:my-kafka [:kafka {:bootstrap-servers *bootstrap-servers*}]}
                                        :log [:kafka {:cluster :my-kafka :topic log-topic}]
                                        :compactor {:threads 0}})]
        (xt/submit-tx node [[:put-docs :docs {:xt/id 0}]])
        (tu/flush-block! node))

      (let [first-block-done (promise)
            resume-backfill (promise)
            result (promise)
            done (promise)]
        ;; Start backfill in background thread then block
        ;; This is to simulate the tx-sink taking so long that other nodes have moved on
        (future
          (try
            (binding [tx-sink/*after-block-hook*
                      (fn [block-idx]
                        (when (= block-idx 0)
                          (deliver first-block-done true)
                          @resume-backfill))]
              (with-open [node (xtn/start-node {:storage [:local {:path (.resolve node-dir "storage")}]
                                                :log-clusters {:my-kafka [:kafka {:bootstrap-servers *bootstrap-servers*}]}
                                                :log [:kafka {:cluster :my-kafka :topic log-topic}]
                                                :compactor {:threads 0}
                                                :tx-sink {:enable true
                                                          :initial-scan true
                                                          :output-log [::tu/recording {}]
                                                          :format :transit+json}})]
                ; Works where sync & await-node don't for some reason
                (xt/submit-tx node [[:put-docs :docs {:xt/id 2}]])
                (deliver result (count (.getMessages ^RecordingLog (tu/get-output-log node))))))
            (catch Exception e
              (deliver result e)))
          (deliver done true))

        (deref first-block-done 5000 :timeout)

        (with-open [node (xtn/start-node {:storage [:local {:path (.resolve node-dir "storage")}]
                                          :log-clusters {:my-kafka [:kafka {:bootstrap-servers *bootstrap-servers*}]}
                                          :log [:kafka {:cluster :my-kafka :topic log-topic}]
                                          :compactor {:threads 0}
                                          :garbage-collector {:enabled true
                                                              :blocks-to-keep 1
                                                              :garbage-lifetime #xt/duration "PT1S"}})]
          ;; Create block 1
          (xt/submit-tx node [[:put-docs :docs {:xt/id 1}]])
          (tu/flush-block! node)

          ;; Run GC
          (let [gc (gc/garbage-collector node)
                db (db/primary-db node)
                bp (.getBufferPool db)
                get-blocks (fn []
                             (->> (.listAllObjects bp (util/->path "blocks"))
                                  (map #(str (.getKey ^ObjectStore$StoredObject %)))))]
            (t/is (= 2 (count (get-blocks)))
                  "Before GC, blocks 0, 1 should exist")
            (.collectAllGarbage gc)

            (t/is (= 1 (count (get-blocks)))
                  "After GC, only block 1 should exist")))

        (deliver resume-backfill true)
        ; TxSink will now resume and move onto indexing

        (t/is (= (deref result 5000 :timeout) 3))
        (t/is (not= (deref done 5000 :timeout) :timeout))))))
