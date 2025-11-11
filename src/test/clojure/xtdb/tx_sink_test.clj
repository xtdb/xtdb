(ns xtdb.tx-sink-test
  (:require [clojure.test :as t]
            [next.jdbc :as jdbc]
            [xtdb.api :as xt]
            [xtdb.node :as xtn]
            xtdb.serde
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import [xtdb.api.log Log Log$Record Log$Message Log$Subscriber]
           [xtdb.database Database$Catalog]))

(t/use-fixtures :each tu/with-mock-clock)

(defn- recording-subscriber [^Log log]
  (let [store (atom [])]
    (.subscribe log
                (reify Log$Subscriber
                  (processRecords [_ records] (swap! store into records))
                  (getLatestProcessedMsgId [_] -1)
                  (getLatestSubmittedMsgId [_] -1))
                (.getLatestSubmittedOffset log))
    store))

(defn decode-record [msg] (-> msg Log$Record/.getMessage Log$Message/.encode xtdb.serde/read-transit))
(defn get-output-log [node]
  (let [^Database$Catalog db-cat (util/component node :xtdb/db-catalog)]
    (-> db-cat
        (.databaseOrNull "xtdb")
        (.getTxSink)
        :output-log)))

(t/deftest test-tx-sink-output
  (with-open [node (xtn/start-node (merge tu/*node-opts*
                                          {:tx-sink {:enable true
                                                     :output-log [:in-memory {}]
                                                     :format :transit+json}}))]
    (let [output-log ^Log (get-output-log node)
          store (recording-subscriber output-log)]
      (t/is (= [] @store))

      (xt/execute-tx node [[:put-docs :docs {:xt/id :doc1, :value "test"}]])
      (t/is (= 2 (count @store)))
      (let [msg (-> @store first decode-record)]
        (t/is (= :put (:op msg)))
        (t/is (= "xtdb.public.docs" (:table msg)))
        (t/is (= {"_id" :doc1, "value" "test"} (:data msg))))

      (jdbc/execute! node ["INSERT INTO docs RECORDS {_id: 1, a: 1, b: {c: [1, 2, 3], d: 'test'}}, {_id: 2, a: 2}, {_id: 3, a: 3}"])
      (jdbc/execute! node ["UPDATE docs SET a = 4 WHERE _id = 1"])
      (jdbc/execute! node ["DELETE FROM docs WHERE _id = 1"])
      (jdbc/execute! node ["ERASE FROM docs WHERE _id = 1"])
      (jdbc/execute! node ["INSERT INTO other RECORDS {_id: 1}"])

      (t/is (= 14 (count @store)))
      (let [msgs (->> @store (map decode-record))
            tx-msgs (filter #(= (:table %) "xtdb.xt.txs") msgs)
            docs-msgs (filter #(= (:table %) "xtdb.public.docs") msgs)
            other-msgs (filter #(= (:table %) "xtdb.public.other") msgs)]
        (t/is (= 6 (count tx-msgs)))
        (t/is (= [:put :put :put :put :put :delete :erase] (map :op docs-msgs)))
        (t/is (= [:put] (map :op other-msgs)))))))

(t/deftest test-tx-sink-output-with-tx
  (with-open [node (xtn/start-node (merge tu/*node-opts*
                                          {:tx-sink {:enable true
                                                     :output-log [:in-memory {}]
                                                     :format :transit+json}}))]
    (let [output-log ^Log (get-output-log node)
          store (recording-subscriber output-log)]
      ;; TODO: Remove once #ticket-to-be-created is solved
      (jdbc/execute! node ["INSERT INTO docs RECORDS {_id: 1}"])
      (reset! store [])

      (jdbc/with-transaction [tx node]
        (jdbc/execute! tx ["INSERT INTO docs RECORDS {_id: 1, a: 1, b: {c: [1, 2, 3], d: 'test'}}, {_id: 2, a: 2}, {_id: 3, a: 3}"])
        (jdbc/execute! tx ["UPDATE docs SET a = 4 WHERE _id = 1"])
        (jdbc/execute! tx ["DELETE FROM docs WHERE _id = 1"])
        (jdbc/execute! tx ["ERASE FROM docs WHERE _id = 1"])
        (jdbc/execute! tx ["INSERT INTO other RECORDS {_id: 1}"]))

      (t/is (= 8 (count @store)))
      (let [msgs (->> @store (map decode-record))
            tx-msgs (filter #(= (:table %) "xtdb.xt.txs") msgs)
            docs-msgs (filter #(= (:table %) "xtdb.public.docs") msgs)
            other-msgs (filter #(= (:table %) "xtdb.public.other") msgs)]
        (t/is (= 1 (count tx-msgs)))
        (t/is (= [:put :put :put :put :delete :erase] (map :op docs-msgs)))
        (t/is (= [:put] (map :op other-msgs)))))))

(t/deftest test-tx-sink-disabled
  (with-open [node (xtn/start-node (merge tu/*node-opts*
                                          {:tx-sink {:enable false
                                                     :output-log [:in-memory {}]
                                                     :format :transit+json}}))]
    (t/is (thrown? java.lang.IllegalStateException
                   "tx sink not initialised"
                   (get-output-log node)))))

(t/deftest test-tx-sink-filtering
  (t/testing "include"
    (with-open [node (xtn/start-node (merge tu/*node-opts*
                                            {:tx-sink {:enable true
                                                       :output-log [:in-memory {}]
                                                       :table-filter {:include ["xtdb.public.docs"]}}}))]
      (let [output-log ^Log (get-output-log node)
            store (recording-subscriber output-log)]
        (xt/execute-tx node [[:put-docs :docs {:xt/id :doc1}]])
        (xt/execute-tx node [[:put-docs :other {:xt/id :doc2}]])
        (xt/execute-tx node [[:put-docs :docs {:xt/id :doc3}]])

        (t/is (= 2 (count @store)))
        (let [msgs (->> @store (map decode-record))]
          (t/is (= [:put :put] (map :op msgs)))))))

  (t/testing "exclude"
    (with-open [node (xtn/start-node (merge tu/*node-opts*
                                            {:tx-sink {:enable true
                                                       :output-log [:in-memory {}]
                                                       :table-filter {:exclude ["xtdb.xt.txs"]}}}))]
      (let [output-log ^Log (get-output-log node)
            store (recording-subscriber output-log)]
        (xt/execute-tx node [[:put-docs :docs {:xt/id :doc1}]])
        (xt/execute-tx node [[:put-docs :other {:xt/id :doc2}]])
        (xt/execute-tx node [[:put-docs :docs {:xt/id :doc3}]])

        (t/is (= 3 (count @store)))
        (let [msgs (->> @store (map decode-record))]
          (t/is (= [:put :put :put] (map :op msgs)))
          (t/is (= ["xtdb.public.docs" "xtdb.public.other" "xtdb.public.docs"] (map :table msgs))))))))
