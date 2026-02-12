(ns ^:kafka xtdb.kafka-test
  (:require [clojure.test :as t]
            [next.jdbc :as jdbc]
            [xtdb.api :as xt]
            [xtdb.db-catalog :as db]
            [xtdb.node :as xtn] 
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import org.apache.kafka.common.KafkaException
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

(t/use-fixtures :once
  (fn [f]
    (tu/with-container container
      (fn [^ConfluentKafkaContainer c]
        (binding [*bootstrap-servers* (.getBootstrapServers c)]
          (f))))))

(t/deftest ^:integration test-kafka
  (let [test-uuid (random-uuid)]
    (with-open [node (xtn/start-node {:log-clusters {:my-kafka [:kafka {:bootstrap-servers *bootstrap-servers*}]}
                                      :log [:kafka {:cluster :my-kafka
                                                    :topic (str "xtdb.kafka-test." test-uuid)}]})]
      (t/is (xt/execute-tx node [[:put-docs :xt_docs {:xt/id :foo}]]))

      (t/is (= [{:xt/id :foo}]
               (tu/query-ra '[:scan {:table #xt/table xt_docs, :columns [_id]}]
                            {:node node}))))))

(t/deftest ^:integration test-multi-db
  (let [test-uuid (random-uuid)]
    (with-open [node (xtn/start-node {:log-clusters {:my-kafka [:kafka {:bootstrap-servers *bootstrap-servers*}]}
                                      :log [:kafka {:cluster :my-kafka
                                                    :topic (str "xtdb.kafka-test." test-uuid)}]})
                xtdb-conn (.build (.createConnectionBuilder node))]
      (jdbc/execute! xtdb-conn [(format "ATTACH DATABASE secondary WITH $$
         log: !Kafka
           cluster: my-kafka
           topic: xtdb.kafka-test-secondary.%s
         $$" test-uuid)])
      (with-open [secondary-conn (.build (-> (.createConnectionBuilder node)
                                             (.database "secondary")))]

        (t/is (xt/submit-tx xtdb-conn [[:put-docs :docs {:xt/id :primary}]]))
        (t/is (xt/submit-tx secondary-conn [[:put-docs :docs {:xt/id :secondary}]]))

        (t/is (= [{:xt/id :primary}] (xt/q xtdb-conn "SELECT _id FROM docs")))
        (t/is (= [{:xt/id :secondary}] (xt/q secondary-conn "SELECT _id FROM docs")))

        (tu/flush-block! node)

        (t/is (= [{:xt/id :primary}] (xt/q xtdb-conn "SELECT _id FROM docs")))
        (t/is (= [{:xt/id :secondary}] (xt/q secondary-conn "SELECT _id FROM docs")))))))

(t/deftest ^:integration test-kafka-setup-with-provided-opts
  (let [test-uuid (random-uuid)]
    (with-open [node (xtn/start-node {:log-clusters {:my-kafka [:kafka {:bootstrap-servers *bootstrap-servers*
                                                                        :poll-duration "PT2S"
                                                                        :properties-map {}
                                                                        :properties-file nil}]}
                                      :log [:kafka {:cluster :my-kafka
                                                    :topic (str "xtdb.kafka-test." test-uuid)
                                                    :create-topic? true}]})]
      (t/testing "KafkaLog successfully created"
        (t/is (instance? Log (.getSourceLog (db/primary-db node))))))))

(t/deftest ^:integration test-kafka-closes-properly-with-messages-sent
  (let [test-uuid (random-uuid)]
    (util/with-tmp-dirs #{path}
      (with-open [node (xtn/start-node {:log-clusters {:my-kafka [:kafka {:bootstrap-servers *bootstrap-servers*
                                                                          :poll-duration "PT2S"
                                                                          :properties-map {}
                                                                          :properties-file nil}]}
                                        :log [:kafka {:cluster :my-kafka
                                                      :topic (str "xtdb.kafka-test." test-uuid)}]
                                        :storage [:remote {:object-store [:in-memory {}]}]
                                        :disk-cache {:path path}})]
        (t/testing "Send a transaction"
          (t/is (xt/execute-tx node [[:put-docs :xt_docs {:xt/id :foo}]]))))

      (t/testing "should be no 'xtdb-tx-subscription' threads remaining"
        (let [all-threads (.keySet (Thread/getAllStackTraces))
              tx-subscription-threads (filter (fn [^Thread thread]
                                                (re-find #"xtdb-tx-subscription" (.getName thread)))
                                              all-threads)]
          (t/is (= 0 (count tx-subscription-threads))))))))

(t/deftest ^:integration test-startup-errors-returned-with-no-system-map
  (t/is (thrown-with-msg? KafkaException
                          #"Failed to construct kafka producer"
                          (xtn/start-node {:log-clusters {:my-kafka [:kafka {:bootstrap-servers "nonresolvable:9092"
                                                                             :some-secret "foobar"}]}
                                           :log [:kafka {:cluster :my-kafka
                                                         :topic "topic"
                                                         :create-topic? false}]}))))

(t/deftest ^:integration test-kafka-topic-cleared
  (let [original-topic (str "xtdb.kafka-test." (random-uuid))
        empty-topic (str "xtdb.kafka-test." (random-uuid))]
    (util/with-tmp-dirs #{local-disk-path}
      ;; Node with storage and log topic
      (with-open [node (xtn/start-node {:log-clusters {:my-kafka [:kafka {:bootstrap-servers *bootstrap-servers*
                                                                          :poll-duration "PT2S"
                                                                          :properties-map {}
                                                                          :properties-file nil}]}
                                        :log [:kafka {:cluster :my-kafka
                                                      :topic original-topic
                                                      :create-topic? true}]
                                        :storage [:local {:path local-disk-path}]})]
        ;; Submit a few transactions
        (xt/execute-tx node [[:put-docs :xt_docs {:xt/id :foo}]])
        (xt/execute-tx node [[:put-docs :xt_docs {:xt/id :bar}]])
        (t/is (= (set [{:xt/id :foo} {:xt/id :bar}])
                 (set (xt/q node "SELECT _id FROM xt_docs"))))
        ;; Finish the block
        (t/is (nil? (tu/flush-block! node)))

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
        (xtn/start-node {:log-clusters {:my-kafka [:kafka {:bootstrap-servers *bootstrap-servers*
                                                           :poll-duration "PT2S"
                                                           :properties-map {}
                                                           :properties-file nil}]}
                         :log [:kafka {:cluster :my-kafka
                                       :topic empty-topic
                                       :create-topic? true}]
                         :storage [:local {:path local-disk-path}]})))

      ;; Node with intact storage and topic 2 (ie, empty topic) along with setting log offset
      (with-open [node (xtn/start-node {:log-clusters {:my-kafka [:kafka {:bootstrap-servers *bootstrap-servers*
                                                                          :poll-duration "PT2S"
                                                                          :properties-map {}
                                                                          :properties-file nil}]}
                                        :log [:kafka {:cluster :my-kafka
                                                      :topic empty-topic
                                                      :create-topic? true
                                                      :epoch 1}]
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
          (t/is (nil? (tu/flush-block! node)))))

      (with-open [node (xtn/start-node {:log-clusters {:my-kafka [:kafka {:bootstrap-servers *bootstrap-servers*
                                                                          :poll-duration "PT2S"
                                                                          :properties-map {}
                                                                          :properties-file nil}]}
                                        :log [:kafka {:cluster :my-kafka
                                                      :topic empty-topic
                                                      :create-topic? true
                                                      :epoch 1}]
                                        :storage [:local {:path local-disk-path}]})]
        (t/testing "can query all previously indexed values, including those after new epoch started"
          (t/is (= (set [{:xt/id :foo}
                         {:xt/id :bar}
                         {:xt/id :new}
                         {:xt/id :new2}])
                   (set (xt/q node "SELECT _id FROM xt_docs")))))

        (t/testing "can continue to index/query new transactions"
          (t/is (xt/execute-tx node [[:put-docs :xt_docs {:xt/id :new3}]]))
          (t/is (= (set [{:xt/id :foo}
                         {:xt/id :bar}
                         {:xt/id :new}
                         {:xt/id :new2}
                         {:xt/id :new3}])
                   (set (xt/q node "SELECT _id FROM xt_docs")))))))))

(t/deftest ^:integration test-stale-log-recovery
  (let [original-topic (str "xtdb.kafka-test." (random-uuid))
        stale-topic (str "xtdb.kafka-test." (random-uuid))
        empty-topic (str "xtdb.kafka-test." (random-uuid))]

    (util/with-tmp-dirs #{local-disk-path}
      ;; Start a node, write a few transactions to the original topic
      (with-open [node (xtn/start-node {:log-clusters {:my-kafka [:kafka {:bootstrap-servers *bootstrap-servers*
                                                                          :poll-duration "PT2S"}]}
                                        :log [:kafka {:cluster :my-kafka, :topic original-topic}]
                                        :storage [:local {:path local-disk-path}]})]
        (xt/execute-tx node [[:put-docs :xt_docs {:xt/id :foo}]])
        (xt/execute-tx node [[:put-docs :xt_docs {:xt/id :bar}]])
        (xt/execute-tx node [[:put-docs :xt_docs {:xt/id :baz}]])
        (t/is (= (set [{:xt/id :foo} {:xt/id :bar} {:xt/id :baz}])
                 (set (xt/q node "SELECT _id FROM xt_docs"))))

        ;; Finish the block
        (t/is (nil? (tu/flush-block! node)))

        ;; Submit a few more transactions
        (xt/execute-tx node [[:put-docs :xt_docs {:xt/id :willbelost}]])
        (t/is (= (set [{:xt/id :foo} {:xt/id :bar} {:xt/id :baz} {:xt/id :willbelost}])
                 (set (xt/q node "SELECT _id FROM xt_docs")))))

      ;; Setup a "stale log":
      ;; - Start a node with a new topic and memory storage.
      ;; - Submit two txes to it.
      (with-open [node (xtn/start-node {:log-clusters {:my-kafka [:kafka {:bootstrap-servers *bootstrap-servers*
                                                                          :poll-duration "PT2S"}]}
                                        :log [:kafka {:cluster :my-kafka
                                                      :topic stale-topic
                                                      :create-topic? true}]
                                        :storage [:in-memory {}]})]
        (xt/execute-tx node [[:put-docs :xt_docs {:xt/id :foo}]])
        (xt/execute-tx node [[:put-docs :xt_docs {:xt/id :bar}]]))

      ;; Attempt to restart the node with intact storage and the stale topic + original epoch
      (t/is
       (thrown-with-msg?
        IllegalStateException
        #"Node failed to start due to an invalid transaction log state \(epoch=0, offset=1\) that does not correspond with the latest indexed transaction \(epoch=0 and offset=2\)"
        (xtn/start-node {:log-clusters {:my-kafka [:kafka {:bootstrap-servers *bootstrap-servers*
                                                           :poll-duration "PT2S"}]}
                         :log [:kafka {:cluster :my-kafka
                                       :topic stale-topic
                                       :create-topic? false}]
                         :storage [:local {:path local-disk-path}]})))

      ;; Attempt to restart the node with intact storage, a new topic + new epoch
      (with-open [node (xtn/start-node {:log-clusters {:my-kafka [:kafka {:bootstrap-servers *bootstrap-servers*
                                                                          :poll-duration "PT2S"
                                                                          :properties-map {}
                                                                          :properties-file nil}]}
                                        :log [:kafka {:cluster :my-kafka
                                                      :topic empty-topic
                                                      :create-topic? true
                                                      :epoch 1}]
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
          (t/is (nil? (tu/flush-block! node)))))


      ;; Restarting the node again with the same new log path and epoch 1
      (with-open [node (xtn/start-node {:log-clusters {:my-kafka [:kafka {:bootstrap-servers *bootstrap-servers*
                                                                          :poll-duration "PT2S"
                                                                          :properties-map {}
                                                                          :properties-file nil}]}
                                        :log [:kafka {:cluster :my-kafka
                                                      :topic empty-topic
                                                      :epoch 1}]
                                        :storage [:local {:path local-disk-path}]})]
        (t/testing "can query same transactions + nothing has been re-indexed"
          (t/is (= (set [{:xt/id :foo}
                         {:xt/id :bar}
                         {:xt/id :baz}
                         {:xt/id :new}
                         {:xt/id :new2}])
                   (set (xt/q node "SELECT _id FROM xt_docs FOR VALID_TIME ALL FOR SYSTEM_TIME ALL")))))

        (t/testing "can index/query new transactions"
          (t/is (xt/execute-tx node [[:put-docs :xt_docs {:xt/id :new3}]]))
          (t/is (= (set [{:xt/id :foo}
                         {:xt/id :bar}
                         {:xt/id :baz}
                         {:xt/id :new}
                         {:xt/id :new2}
                         {:xt/id :new3}])
                   (set (xt/q node "SELECT _id FROM xt_docs")))))

        (t/testing "can finish another block"
          (t/is (nil? (tu/flush-block! node))))))))

(t/deftest ^:integration test-kafka-log-starts-at-correct-point-after-block-cut
  (let [topic (str "xtdb.kafka-test." (random-uuid))]
    (util/with-tmp-dirs #{local-disk-path}
      (t/testing "Start a node, write a number of transactions to the topic - ensure block is cut"
        (with-open [node (xtn/start-node {:log-clusters {:my-kafka [:kafka {:bootstrap-servers *bootstrap-servers*
                                                                            :poll-duration "PT2S"}]}
                                          :log [:kafka {:cluster :my-kafka, :topic topic}]
                                          :storage [:local {:path local-disk-path}]
                                          :indexer {:rows-per-block 20}
                                          :compactor {:threads 0}})]

          (doseq [batch (->> (range 100) (partition-all 10))]
            (xt/execute-tx node (for [i batch] [:put-docs :docs {:xt/id i}])))
          (t/is (= 100 (count (xt/q node "SELECT *, _valid_from, _system_from FROM docs FOR VALID_TIME ALL FOR SYSTEM_TIME ALL"))))
          (t/is (= 10 (count (xt/q node "SELECT * FROM xt.txs"))))
          (t/testing "ensure blocks have been written"
            (Thread/sleep 1000)
            (t/is (= ["l00-rc-b00.arrow" "l00-rc-b01.arrow" "l00-rc-b02.arrow" "l00-rc-b03.arrow" "l00-rc-b04.arrow"]
                     (tu/read-files-from-bp-path node "tables/public$docs/meta/"))))))

      (t/testing "Restart the node, ensure it picks up from the correct position in the log"
        (with-open [node (xtn/start-node {:log-clusters {:my-kafka [:kafka {:bootstrap-servers *bootstrap-servers*
                                                                            :poll-duration "PT2S"}]}
                                          :log [:kafka {:cluster :my-kafka, :topic topic}]
                                          :storage [:local {:path local-disk-path}]
                                          :indexer {:rows-per-block 20}
                                          :compactor {:threads 0}})]

          (t/testing "shouldn't reindex any transactions when starting up"
            (Thread/sleep 1000)
            (t/is (= 100 (count (xt/q node "SELECT *, _valid_from, _system_from FROM docs FOR VALID_TIME ALL FOR SYSTEM_TIME ALL"))))
            (t/is (= 10 (count (xt/q node "SELECT *  FROM xt.txs")))))

          (t/testing "sending a new transaction shouldnt cut a block yet - still ten blocks off"
            (xt/execute-tx node (for [i (range 101 111)] [:put-docs :docs {:xt/id i}]))
            (t/is (= 110 (count (xt/q node "SELECT *, _valid_from, _system_from FROM docs FOR VALID_TIME ALL FOR SYSTEM_TIME ALL"))))
            (t/is (= 11 (count (xt/q node "SELECT *  FROM xt.txs"))))
            (t/is (= ["l00-rc-b00.arrow" "l00-rc-b01.arrow" "l00-rc-b02.arrow" "l00-rc-b03.arrow" "l00-rc-b04.arrow"]
                     (tu/read-files-from-bp-path node "tables/public$docs/meta/")))))))))

(t/deftest ^:integration test-kafka-log-starts-at-correct-point-after-flush-block
  (let [topic (str "xtdb.kafka-test." (random-uuid))]
    (util/with-tmp-dirs #{local-disk-path}
      (t/testing "Start a node, write a number of transactions to the log - ensure block is cut"
        (with-open [node (xtn/start-node {:log-clusters {:my-kafka [:kafka {:bootstrap-servers *bootstrap-servers*
                                                                            :poll-duration "PT2S"}]}
                                          :log [:kafka {:cluster :my-kafka, :topic topic}]
                                          :storage [:local {:path local-disk-path}]
                                          :compactor {:threads 0}})]

          (doseq [batch (->> (range 100) (partition-all 10))]
            (xt/execute-tx node (for [i batch] [:put-docs :docs {:xt/id i}])))
          (t/is (= 100 (count (xt/q node "SELECT *, _valid_from, _system_from FROM docs FOR VALID_TIME ALL FOR SYSTEM_TIME ALL"))))
          (t/is (= 10 (count (xt/q node "SELECT * FROM xt.txs"))))
          (tu/flush-block! node)
          (t/testing "ensure block has been written"
            (t/is (= ["l00-rc-b00.arrow"] (tu/read-files-from-bp-path node "tables/public$docs/meta/"))))))

      (t/testing "Restart the node, ensure it picks up from the correct position in the log"
        (with-open [node (xtn/start-node {:log-clusters {:my-kafka [:kafka {:bootstrap-servers *bootstrap-servers*
                                                                            :poll-duration "PT2S"}]}
                                          :log [:kafka {:cluster :my-kafka, :topic topic}]
                                          :storage [:local {:path local-disk-path}]
                                          :compactor {:threads 0}})]

          (t/testing "shouldn't reindex any transactions when starting up"
            (Thread/sleep 1000)
            (t/is (= 100 (count (xt/q node "SELECT *, _valid_from, _system_from FROM docs FOR VALID_TIME ALL FOR SYSTEM_TIME ALL"))))
            (t/is (= 10 (count (xt/q node "SELECT *  FROM xt.txs")))))

          (t/testing "shouldn't have flushed another block / re-read the flush block"
            (t/is (= ["l00-rc-b00.arrow"] (tu/read-files-from-bp-path node "tables/public$docs/meta/")))))))))
