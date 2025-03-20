(ns ^:kafka xtdb.kafka-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.log :as log]
            [xtdb.node :as xtn]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import org.apache.kafka.common.KafkaException
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

