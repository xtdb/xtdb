(ns xtdb.log-tables-test
  (:require [clojure.edn :as edn]
            [clojure.test :as t :refer [deftest]]
            [xtdb.api :as xt]
            [xtdb.log-tables :as log-tables]
            [xtdb.test-util :as tu])
  (:import (xtdb.api.log ReplicaMessage$BlockUploaded ReplicaMessage$IndexBackfilled ReplicaMessage$IndexReady
                         SourceMessage$IndexBackfilled)
           (xtdb.index IndexTrie)
           (xtdb.trie Trie)))

(t/use-fixtures :each tu/with-mock-clock tu/with-allocator tu/with-node)

(deftest test-source-log-msgs-basic
  (xt/execute-tx tu/*node* [[:put-docs :foo {:xt/id 1, :v "hello"}]])
  (xt/execute-tx tu/*node* [[:put-docs :foo {:xt/id 2, :v "world"}]])

  (t/testing "can read source log messages with BETWEEN bounds"
    (let [rows (xt/q tu/*node*
                     "SELECT msg_id, log_offset, log_timestamp, msg, tx_ops FROM xt.source_log_msgs WHERE msg_id BETWEEN 0 AND 100")]
      (t/is (pos? (count rows)) "should return at least one row")

      (t/testing "msg column contains readable EDN with :type key"
        (t/is (string? (:msg (first rows))))
        (t/is (re-find #":type" (:msg (first rows)))))

      (t/testing "tx_ops column is present for tx messages"
        (let [rows-with-tx-ops (filter :tx-ops rows)]
          (when (seq rows-with-tx-ops)
            (t/is (string? (:tx-ops (first rows-with-tx-ops)))))))))

  (t/testing "msg_id and log_offset are present and numeric"
    (let [rows (xt/q tu/*node*
                     "SELECT msg_id, log_offset FROM xt.source_log_msgs WHERE msg_id BETWEEN 0 AND 100")]
      (doseq [row rows]
        (t/is (number? (:msg-id row)))
        (t/is (number? (:log-offset row))))))

  (t/testing "empty range returns no rows"
    (let [rows (xt/q tu/*node*
                     "SELECT * FROM xt.source_log_msgs WHERE msg_id BETWEEN 99999 AND 99999")]
      (t/is (empty? rows)))))

(deftest test-source-log-msgs-tx-ops-decoded
  (xt/execute-tx tu/*node* [[:put-docs :foo {:xt/id 1, :v "hello"}]])

  (let [rows (xt/q tu/*node*
                   "SELECT msg, tx_ops FROM xt.source_log_msgs WHERE msg_id BETWEEN 0 AND 100")
        tx-rows (filter :tx-ops rows)]
    (t/is (seq tx-rows) "should have at least one tx message")

    (t/testing "tx_ops contains decoded operations"
      (t/is (re-find #"put-docs" (:tx-ops (first tx-rows)))))))

(defn- render-replica-msg [msg]
  [(#'log-tables/replica-msg-type msg)
   (edn/read-string (#'log-tables/replica-msg->edn msg))])

(def ^:private index-id #uuid "6e1f3c2a-0000-4000-8000-000000000001")

(deftest test-replica-block-uploaded-renders-index-tries
  (t/is (= ["block-uploaded"
            {:type :block-uploaded, :block-index 3, :latest-processed-msg-id 42
             :storage-version 6, :storage-epoch 0, :tries []
             :index-tries {(str index-id) {:trie-key "l00-rc-b03", :data-file-size 128}}}]
           (render-replica-msg
            (ReplicaMessage$BlockUploaded. (int 6) (int 0) 3 42 []
                                           {index-id (IndexTrie. (Trie/parseKey "l00-rc-b03") 128)}
                                           nil 0 nil)))))

(deftest test-replica-index-ready-renders
  (t/is (= ["index-ready" {:type :index-ready, :index-id (str index-id)}]
           (render-replica-msg (ReplicaMessage$IndexReady. index-id 0 nil)))))

(deftest test-replica-index-backfilled-renders
  (t/is (= ["index-backfilled"
            {:type :index-backfilled, :index-id (str index-id)
             :block-index 3, :l0-data-file-size 128, :source-msg-id 17}]
           (render-replica-msg
            (ReplicaMessage$IndexBackfilled. (SourceMessage$IndexBackfilled. (int 6) (int 0) index-id 3 128)
                                             17 0 nil)))))
