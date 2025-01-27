(ns ^:kafka xtdb.kafka-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.file-log :as fl]
            [xtdb.node :as xtn]
            [xtdb.object-store :as os]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import org.apache.kafka.common.KafkaException
           org.testcontainers.containers.GenericContainer
           org.testcontainers.kafka.ConfluentKafkaContainer
           org.testcontainers.utility.DockerImageName
           [xtdb.api.log FileLog Log]
           xtdb.buffer_pool.RemoteBufferPool))

(def ^:private ^:dynamic *bootstrap-servers* nil)

(defonce ^ConfluentKafkaContainer container
  (ConfluentKafkaContainer. (DockerImageName/parse "confluentinc/cp-kafka:latest")))

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
                                                    :tx-topic (str "xtdb.kafka-test.tx-" test-uuid)
                                                    :files-topic (str "xtdb.kafka-test.files-" test-uuid)}]})]
      (t/is (= true
               (:committed? (xt/execute-tx node [[:put-docs :xt_docs {:xt/id :foo}]]))))

      (t/is (= [{:xt/id :foo}]
               (tu/query-ra '[:scan {:table public/xt_docs} [_id]]
                            {:node node}))))))

(t/deftest ^:integration test-kafka-setup-with-provided-opts
  (let [test-uuid (random-uuid)]
    (with-open [node (xtn/start-node {:log [:kafka {:tx-topic (str "xtdb.kafka-test.tx-" test-uuid)
                                                    :files-topic (str "xtdb.kafka-test.files-" test-uuid)
                                                    :bootstrap-servers *bootstrap-servers*
                                                    :create-topics? true
                                                    :poll-duration "PT2S"
                                                    :properties-map {}
                                                    :properties-file nil}]})]
      (t/testing "KafkaLog successfully created"
        (let [log (get-in node [:system :xtdb/log])]
          (t/is (instance? Log log)))))))

(t/deftest ^:integration test-kafka-closes-properly-with-messages-sent
  (let [test-uuid (random-uuid)]
    (util/with-tmp-dirs #{path}
      (with-open [node (xtn/start-node {:log [:kafka {:tx-topic (str "xtdb.kafka-test.tx-" test-uuid)
                                                      :files-topic (str "xtdb.kafka-test.files-" test-uuid)
                                                      :bootstrap-servers *bootstrap-servers*
                                                      :create-topics? true
                                                      :poll-duration "PT2S"
                                                      :properties-map {}
                                                      :properties-file nil}]
                                        :storage [:remote {:object-store [:in-memory {}]
                                                           :local-disk-cache path}]})]
        (let [^FileLog file-log (tu/component node :xtdb/file-log)
              ^RemoteBufferPool buffer-pool (tu/component node :xtdb/buffer-pool)]
          (t/testing "Send a transaction"
            (t/is (= true (:committed? (xt/execute-tx node [[:put-docs :xt_docs {:xt/id :foo}]])))))

          (t/testing "Send & receive file change notification"
            @(.appendFileNotification file-log (fl/map->FileNotification {:added [(os/->StoredObject (util/->path "foo1") 12)
                                                                                  (os/->StoredObject (util/->path "foo2") 15)
                                                                                  (os/->StoredObject (util/->path "foo3") 8)]}))
            (Thread/sleep 100)
            (t/is (= #{(util/->path "foo1") (util/->path "foo2") (util/->path "foo3")}
                     (set (.getOsFiles buffer-pool)))))))

      (t/testing "should be no 'xtdb-tx-subscription' threads remaining"
        (let [all-threads (.keySet (Thread/getAllStackTraces))
              tx-subscription-threads (filter (fn [^Thread thread]
                                                (re-find #"xtdb-tx-subscription" (.getName thread)))
                                              all-threads)]
          (t/is (= 0 (count tx-subscription-threads))))))))

(t/deftest ^:integration test-startup-errors-returned-with-no-system-map
  (t/is (thrown-with-msg? KafkaException
                          #"Failed to create new KafkaAdminClient"
                          (xtn/start-node {:log [:kafka {:tx-topic "tx-topic"
                                                         :files-topic "files-topic"
                                                         :bootstrap-servers "nonresolvable:9092"
                                                         :create-topics? false
                                                         :some-secret "foobar"}]}))))
