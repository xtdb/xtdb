(ns ^:kafka xtdb.kafka-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.log :as log]
            [xtdb.node :as xtn]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import [java.nio ByteBuffer]
           [java.time Duration]
           [java.util Map]
           [org.apache.kafka.clients.admin AdminClient NewTopic]
           [org.apache.kafka.clients.consumer ConsumerRecord KafkaConsumer]
           [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]
           [org.apache.kafka.common TopicPartition]
           org.apache.kafka.common.KafkaException
           org.testcontainers.containers.GenericContainer
           org.testcontainers.kafka.ConfluentKafkaContainer
           org.testcontainers.utility.DockerImageName
           [xtdb.api.log Log]))

(def ^:private ^:dynamic *bootstrap-servers* nil)

(defonce ^ConfluentKafkaContainer container
  (ConfluentKafkaContainer. (DockerImageName/parse "confluentinc/cp-kafka:7.8.0")))

(comment
  ;; start these once when you're developing,
  ;; save the time of starting the container for each run
  (.start container)

  (.stop container))

(defn with-container [^GenericContainer c, f]
  (if (.getContainerId c)
    (f c)
    (try
      (.start c)
      (f c)
      (finally
        (.stop c)))))

(t/use-fixtures :once
  (fn [f]
    (with-container container
      (fn [^ConfluentKafkaContainer c]
        (binding [*bootstrap-servers* (.getBootstrapServers c)]
          (f))))))

(t/deftest ^:integration test-kafka
  (let [test-uuid (random-uuid)]
    (with-open [node (xtn/start-node {:log [:kafka {:bootstrap-servers *bootstrap-servers*
                                                    :topic (str "xtdb.kafka-test." test-uuid)}]})]
      (t/is (true? (:committed? (xt/execute-tx node [[:put-docs :xt_docs {:xt/id :foo}]]))))

      (t/is (= [{:xt/id :foo}]
               (tu/query-ra '[:scan {:table public/xt_docs} [_id]]
                            {:node node}))))))

(t/deftest ^:integration test-kafka-setup-with-provided-opts
  (let [test-uuid (random-uuid)]
    (with-open [node (xtn/start-node {:log [:kafka {:topic (str "xtdb.kafka-test." test-uuid)
                                                    :bootstrap-servers *bootstrap-servers*
                                                    :create-topic? true
                                                    :poll-duration "PT2S"
                                                    :properties-map {}
                                                    :properties-file nil}]})]
      (t/testing "KafkaLog successfully created"
        (let [log (get-in node [:system :xtdb/log])]
          (t/is (instance? Log log)))))))

(t/deftest ^:integration test-kafka-closes-properly-with-messages-sent
  (let [test-uuid (random-uuid)]
    (util/with-tmp-dirs #{path}
      (with-open [node (xtn/start-node {:log [:kafka {:topic (str "xtdb.kafka-test." test-uuid)
                                                      :bootstrap-servers *bootstrap-servers*
                                                      :create-topic? true
                                                      :poll-duration "PT2S"
                                                      :properties-map {}
                                                      :properties-file nil}]
                                        :storage [:remote {:object-store [:in-memory {}]
                                                           :local-disk-cache path}]})]
        (t/testing "Send a transaction"
          (t/is (= true (:committed? (xt/execute-tx node [[:put-docs :xt_docs {:xt/id :foo}]]))))))

      (t/testing "should be no 'xtdb-tx-subscription' threads remaining"
        (let [all-threads (.keySet (Thread/getAllStackTraces))
              tx-subscription-threads (filter (fn [^Thread thread]
                                                (re-find #"xtdb-tx-subscription" (.getName thread)))
                                              all-threads)]
          (t/is (= 0 (count tx-subscription-threads))))))))

(t/deftest ^:integration test-startup-errors-returned-with-no-system-map
  (t/is (thrown-with-msg? KafkaException
                          #"Failed to create new KafkaAdminClient"
                          (xtn/start-node {:log [:kafka {:topic "topic"
                                                         :bootstrap-servers "nonresolvable:9092"
                                                         :create-topic? false
                                                         :some-secret "foobar"}]}))))

(t/deftest ^:integration test-kafka-topic-cleared
  (let [original-topic (str "xtdb.kafka-test." (random-uuid))
        empty-topic (str "xtdb.kafka-test." (random-uuid))]
    (util/with-tmp-dirs #{local-disk-path}
      ;; Node with storage and log topic 
      (with-open [node (xtn/start-node {:log [:kafka {:topic original-topic
                                                      :bootstrap-servers *bootstrap-servers*
                                                      :create-topic? true
                                                      :poll-duration "PT2S"
                                                      :properties-map {}
                                                      :properties-file nil}]
                                        :storage [:local {:path local-disk-path}]})]
        ;; Submit a few transactions
        (xt/execute-tx node [[:put-docs :xt_docs {:xt/id :foo}]])
        (xt/execute-tx node [[:put-docs :xt_docs {:xt/id :bar}]])
        (t/is (= (set [{:xt/id :foo} {:xt/id :bar}])
                 (set (xt/q node "SELECT _id FROM xt_docs"))))
        ;; Finish the block
        (t/is (nil? (tu/finish-block! node)))

        ;; Submit a few more transactions
        (xt/execute-tx node [[:put-docs :xt_docs {:xt/id :willbe}]])
        (xt/execute-tx node [[:put-docs :xt_docs {:xt/id :lost}]])
        (t/is (= (set [{:xt/id :foo} 
                       {:xt/id :bar}
                       {:xt/id :willbe}
                       {:xt/id :lost}])
                 (set (xt/q node "SELECT _id FROM xt_docs")))))

      ;; Node with intact storage and (now) empty topic
      (t/is
       (thrown-with-msg?
        IllegalStateException
        #"Node failed to start due to an invalid transaction log state \(the log is empty\)"
        (xtn/start-node {:log [:kafka {:topic empty-topic
                                       :bootstrap-servers *bootstrap-servers*
                                       :create-topic? true
                                       :poll-duration "PT2S"
                                       :properties-map {}
                                       :properties-file nil}]
                         :storage [:local {:path local-disk-path}]})))

      ;; Node with intact storage and topic 2 (ie, empty topic) along with setting log offset
      (with-open [node (xtn/start-node {:log [:kafka {:topic empty-topic
                                                      :bootstrap-servers *bootstrap-servers*
                                                      :create-topic? true
                                                      :poll-duration "PT2S"
                                                      :properties-map {}
                                                      :properties-file nil
                                                      :current-epoch 1}]
                                        :storage [:local {:path local-disk-path}]})]
        (t/testing "can query previous indexed values, unindexed values will be lost"
          (t/is (= (set [{:xt/id :foo} {:xt/id :bar}])
                   (set (xt/q node "SELECT _id FROM xt_docs")))))

        (t/testing "can index/query new transactions"
          (t/is (xt/execute-tx node [[:put-docs :xt_docs {:xt/id :new}]]))
          (t/is (xt/execute-tx node [[:put-docs :xt_docs {:xt/id :new2}]])) 
          (t/is (= (set [{:xt/id :foo}
                         {:xt/id :bar}
                         {:xt/id :new}
                         {:xt/id :new2}])
                   (set (xt/q node "SELECT _id FROM xt_docs")))))

        (t/testing "can finish the block"
          (t/is (nil? (tu/finish-block! node)))))

      (with-open [node (xtn/start-node {:log [:kafka {:topic empty-topic
                                                      :bootstrap-servers *bootstrap-servers*
                                                      :create-topic? true
                                                      :poll-duration "PT2S"
                                                      :properties-map {}
                                                      :properties-file nil
                                                      :current-epoch 1}]
                                        :storage [:local {:path local-disk-path}]})]
        (t/testing "can query all previously indexed values, including those after new epoch started"
          (t/is (= (set [{:xt/id :foo} 
                         {:xt/id :bar} 
                         {:xt/id :new} 
                         {:xt/id :new2} ])
                   (set (xt/q node "SELECT _id FROM xt_docs")))))

        (t/testing "can continue to index/query new transactions"
          (t/is (xt/execute-tx node [[:put-docs :xt_docs {:xt/id :new3}]]))
          (t/is (= (set [{:xt/id :foo}
                         {:xt/id :bar} 
                         {:xt/id :new}
                         {:xt/id :new2}
                         {:xt/id :new3}])
                   (set (xt/q node "SELECT _id FROM xt_docs")))))))))

(defn copy-first-n-messages!
  [^String bootstrap-servers ^String source-topic ^String dest-topic ^long n]
  ;; 1. Create the destination topic (always, no check)
  (with-open [admin (AdminClient/create {"bootstrap.servers" bootstrap-servers})]
    (.createTopics admin
                   [(doto (NewTopic. dest-topic 1 (short 1))
                      (.configs {"message.timestamp.type" "LogAppendTime"}))])
    (Thread/sleep 500)) ;; allow for topic propagation

  ;; 2. Use ByteBuffer serializers, Unit keys, and read_committed
  (let [consumer (KafkaConsumer.
                  ^Map {"bootstrap.servers" bootstrap-servers
                        "enable.auto.commit" "false"
                        "isolation.level" "read_committed"
                        "auto.offset.reset" "earliest"
                        "key.deserializer" "org.apache.kafka.common.serialization.ByteArrayDeserializer"
                        "key.serializer" "org.apache.kafka.common.serialization.ByteBufferSerializer"
                        "value.deserializer" "org.apache.kafka.common.serialization.ByteArrayDeserializer"})

        producer (KafkaProducer.
                  ^Map {"bootstrap.servers" bootstrap-servers
                        "enable.idempotence" "true"
                        "acks" "all"
                        "compression.type" "snappy"
                        "key.deserializer" "org.apache.kafka.common.serialization.ByteArrayDeserializer"
                        "key.serializer" "org.apache.kafka.common.serialization.ByteBufferSerializer"
                        "value.serializer" "org.apache.kafka.common.serialization.ByteBufferSerializer"})]

    ;; 3. Begin copying first N messages
    (try
      (let [tp (TopicPartition. source-topic 0)]
        (.assign consumer [tp])
        (.seek consumer tp 0)

        (loop [remaining n]
          (when (pos? remaining)
            (let [records (.poll consumer (Duration/ofSeconds 1))
                  recs (iterator-seq (.iterator records))]
              (doseq [^ConsumerRecord r (take remaining recs)]
                (.send producer (ProducerRecord. dest-topic (.key r) (ByteBuffer/wrap (.value r)))))
              (when (< (count recs) remaining)
                (recur (- remaining (count recs)))))))
        (.flush producer))
      (finally
        (.close consumer)
        (.close producer)))))

  (t/deftest ^:integration test-stale-log-recovery
    (let [original-topic (str "xtdb.kafka-test." (random-uuid))
          stale-topic (str "xtdb.kafka-test." (random-uuid)) 
          empty-topic (str "xtdb.kafka-test." (random-uuid))]

      (util/with-tmp-dirs #{local-disk-path}
        ;; Start a node, write a few transactions to the original topic
        (with-open [node (xtn/start-node {:log [:kafka {:topic original-topic
                                                        :bootstrap-servers *bootstrap-servers*
                                                        :create-topic? true
                                                        :poll-duration "PT2S"}]
                                          :storage [:local {:path local-disk-path}]})]
          (xt/execute-tx node [[:put-docs :xt_docs {:xt/id :foo}]])
          (xt/execute-tx node [[:put-docs :xt_docs {:xt/id :bar}]])
          (xt/execute-tx node [[:put-docs :xt_docs {:xt/id :baz}]])
          (t/is (= (set [{:xt/id :foo} {:xt/id :bar} {:xt/id :baz}])
                   (set (xt/q node "SELECT _id FROM xt_docs"))))

          ;; Finish the block
          (t/is (nil? (tu/finish-block! node)))

          ;; Submit a few more transactions
          (xt/execute-tx node [[:put-docs :xt_docs {:xt/id :willbelost}]])
          (t/is (= (set [{:xt/id :foo} {:xt/id :bar} {:xt/id :baz} {:xt/id :willbelost}])
                   (set (xt/q node "SELECT _id FROM xt_docs")))))

        ;; Copy the first 2 messages from the original topic to the stale topic
        (copy-first-n-messages! *bootstrap-servers* original-topic stale-topic 2)

        ;; Attempt to restart the node with intact storage and the stale topic + original epoch
        (t/is
         (thrown-with-msg?
          IllegalStateException
          #"Node failed to start due to an invalid transaction log state \(epoch=0, offset=1\) that does not correspond with the latest indexed transaction \(epoch=0 and offset=2\)"
          (xtn/start-node {:log [:kafka {:topic stale-topic
                                         :bootstrap-servers *bootstrap-servers*
                                         :create-topic? false
                                         :poll-duration "PT2S"}]
                           :storage [:local {:path local-disk-path}]})))

        ;; Attempt to restart the node with intact storage, a new topic + new epoch
        (with-open [node (xtn/start-node {:log [:kafka {:topic empty-topic
                                                        :bootstrap-servers *bootstrap-servers*
                                                        :create-topic? true
                                                        :poll-duration "PT2S"
                                                        :properties-map {}
                                                        :properties-file nil
                                                        :current-epoch 1}]
                                          :storage [:local {:path local-disk-path}]})]
          (t/testing "can query previous indexed values, unindexed values will be lost"
            (t/is (= (set [{:xt/id :foo} {:xt/id :bar} {:xt/id :baz}])
                     (set (xt/q node "SELECT _id FROM xt_docs")))))

          (t/testing "can index/query new transactions"
            (t/is (xt/execute-tx node [[:put-docs :xt_docs {:xt/id :new}]]))
            (t/is (xt/execute-tx node [[:put-docs :xt_docs {:xt/id :new2}]]))
            (t/is (= (set [{:xt/id :foo}
                           {:xt/id :bar}
                           {:xt/id :baz}
                           {:xt/id :new}
                           {:xt/id :new2}])
                     (set (xt/q node "SELECT _id FROM xt_docs")))))

          (t/testing "can finish the block"
            (t/is (nil? (tu/finish-block! node))))))))
