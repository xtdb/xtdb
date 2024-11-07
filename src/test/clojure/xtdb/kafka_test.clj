(ns xtdb.kafka-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.buffer-pool-test :as bp-test]
            [xtdb.file-list-cache :as flc]
            [xtdb.kafka]
            [xtdb.node :as xtn]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import [xtdb.api.log Log]
           xtdb.buffer_pool.RemoteBufferPool))

(t/deftest ^:requires-docker ^:kafka test-kafka
  (let [test-uuid (random-uuid)]
    (with-open [node (xtn/start-node {:log [:kafka {:bootstrap-servers "localhost:9092"
                                                    :tx-topic (str "xtdb.kafka-test.tx-" test-uuid)
                                                    :files-topic (str "xtdb.kafka-test.files-" test-uuid)}]})]
      (t/is (= true
               (:committed? (xt/execute-tx node [[:put-docs :xt_docs {:xt/id :foo}]]))))

      (t/is (= [{:xt/id :foo}]
               (tu/query-ra '[:scan {:table public/xt_docs} [_id]]
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

(t/deftest ^:requires-docker ^:kafka test-kafka-closes-properly-with-messages-sent
  (let [test-uuid (random-uuid)]
    (util/with-tmp-dirs #{path}
      (with-open [node (xtn/start-node {:log [:kafka {:tx-topic (str "xtdb.kafka-test.tx-" test-uuid)
                                                      :files-topic (str "xtdb.kafka-test.files-" test-uuid)
                                                      :bootstrap-servers "localhost:9092"
                                                      :create-topics? true
                                                      :tx-poll-duration "PT2S"
                                                      :properties-map {}
                                                      :properties-file nil}]
                                        :storage [:remote {:object-store [:in-memory {}]
                                                           :local-disk-cache path}]})
                  ^Log log (get-in node [:system :xtdb/log])
                  ^RemoteBufferPool buffer-pool (bp-test/fetch-buffer-pool-from-node node)]
        (t/testing "Send a transaction"
          (t/is (= true (:committed? (xt/execute-tx node [[:put-docs :xt_docs {:xt/id :foo}]])))))

        (t/testing "Send & receive file change notification"
          (.appendFileNotification log (flc/map->FileNotification {:added [{:k (util/->path "foo1"), :size 12}
                                                                           {:k (util/->path "foo2"), :size 15}
                                                                           {:k (util/->path "foo3"), :size 8}]}))
          (Thread/sleep 1000)
          (t/is (= {(util/->path "foo1") 12
                    (util/->path "foo2") 15
                    (util/->path "foo3") 8}
                   (:!os-files buffer-pool)))))
      
        (t/testing "should be no 'xtdb-tx-subscription' threads remaining"
          (let [all-threads (.keySet (Thread/getAllStackTraces))
                tx-subscription-threads (filter (fn [^Thread thread]
                                                  (re-find #"xtdb-tx-subscription" (.getName thread))) 
                                                all-threads)]
            (t/is (= 0 (count tx-subscription-threads))))))))
