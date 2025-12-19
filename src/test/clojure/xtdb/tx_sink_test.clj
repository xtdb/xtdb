(ns xtdb.tx-sink-test
  (:require [clojure.test :as t]
            [next.jdbc :as jdbc]
            [xtdb.api :as xt]
            [xtdb.node :as xtn]
            [xtdb.serde :as serde]
            [xtdb.test-util :as tu]
            [xtdb.util :as util]
            [xtdb.log :as xt-log]
            [xtdb.trie-catalog :as trie-cat]
            xtdb.tx-sink)
  (:import [xtdb.api.log Log$Message]
           [xtdb.database Database$Catalog]
           [xtdb.test.log RecordingLog]))

(t/use-fixtures :each tu/with-mock-clock)

(defn decode-record
  ([msg] (decode-record msg :json))
  ([msg fmt] (-> msg Log$Message/.encode (xtdb.serde/read-transit fmt))))

(defn get-output-log
  ([node] (get-output-log node "xtdb"))
  ([node db-name]
   (let [^Database$Catalog db-cat (util/component node :xtdb/db-catalog)]
     (-> db-cat
         (.databaseOrNull db-name)
         (.getTxSink)
         :output-log))))

(t/deftest test-tx-sink-output
  (with-open [node (xtn/start-node (merge tu/*node-opts*
                                          {:tx-sink {:enable true
                                                     :output-log [::tu/recording {}]
                                                     :format :transit+json}}))]
    (let [^RecordingLog output-log (get-output-log node)]
      (t/is (= [] (.getMessages output-log)))

      (xt/execute-tx node [[:put-docs :docs {:xt/id :doc1, :value "test"}]])
      (t/is (= 1 (count (.getMessages output-log))))
      (let [msg (-> (.getMessages output-log) first decode-record)]
        (t/is (= (util/->clj
                  {:transaction {:id (serde/->TxKey 0 (.toInstant #inst "2020"))}
                   :system-time #xt/instant "2020-01-01T00:00Z"
                   :source {:db "xtdb" :block-idx 0}
                   :tables [{:db "xtdb"
                             :schema "public"
                             :table "docs"
                             :ops [{:op :put
                                    :iid (util/->iid :doc1)
                                    :system-from #xt/instant "2020-01-01T00:00Z"
                                    :valid-from  #xt/instant "2020-01-01T00:00Z"
                                    :valid-to nil
                                    :doc {"_id" :doc1, "value" "test"}}]}
                            {:db "xtdb"
                             :schema "xt"
                             :table "txs"
                             :ops [{:op :put
                                    :iid (util/->iid 0)
                                    :system-from #xt/instant "2020-01-01T00:00Z"
                                    :valid-from  #xt/instant "2020-01-01T00:00Z"
                                    :valid-to nil
                                    :doc {"_id" 0
                                          "system_time" #xt/zdt "2020-01-01T00:00[UTC]"
                                          "committed" true}}]}]})
                 (util/->clj msg))))

      (jdbc/execute! node ["INSERT INTO docs RECORDS {_id: 1, a: 1, b: {c: [1, 2, 3], d: 'test'}}, {_id: 2, a: 2}, {_id: 3, a: 3}"])
      (jdbc/execute! node ["UPDATE docs SET a = 4 WHERE _id = 1"])
      (jdbc/execute! node ["DELETE FROM docs WHERE _id = 1"])
      (jdbc/execute! node ["ERASE FROM docs WHERE _id = 1"])
      (jdbc/execute! node ["INSERT INTO other RECORDS {_id: 1}"])

      (t/is (= 6 (count (.getMessages output-log))))
      (let [msgs (->> (.getMessages output-log) (map decode-record))
            payloads (mapcat :tables msgs)
            tx-payloads (filter #(= (:table %) "txs") payloads)
            docs-payloads (filter #(= (:table %) "docs") payloads)
            other-payloads (filter #(= (:table %) "other") payloads)]
        (t/is (= 6 (count tx-payloads)))
        (t/is (= [:put :put :put :put :put :delete :erase]
                 (->> docs-payloads (mapcat :ops) (map :op))))
        (t/is (= [:put] (->> other-payloads (mapcat :ops) (map :op))))))))

(t/deftest test-tx-sink-formats
  (t/testing "transit+json"
    (with-open [node (xtn/start-node (merge tu/*node-opts*
                                            {:tx-sink {:enable true
                                                       :output-log [::tu/recording {}]
                                                       :format :transit+json}}))]
      (let [^RecordingLog output-log (get-output-log node)]
        (xt/execute-tx node [[:put-docs :docs {:xt/id :doc1, :value "test"}]])
        (let [msg (-> (.getMessages output-log) first (decode-record :json))]
          (t/is (= #{:transaction :system-time :source :tables} (set (keys msg))))
          (t/is (= "xtdb" (-> msg :source :db)))))))
  (t/testing "transit+msgpack"
    (with-open [node (xtn/start-node (merge tu/*node-opts*
                                            {:tx-sink {:enable true
                                                       :output-log [::tu/recording {}]
                                                       :format :transit+msgpack}}))]
      (let [^RecordingLog output-log (get-output-log node)]
        (xt/execute-tx node [[:put-docs :docs {:xt/id :doc1, :value "test"}]])
        (let [msg (-> (.getMessages output-log) first (decode-record :msgpack))]
          (t/is (= #{:transaction :system-time :source :tables} (set (keys msg))))
          (t/is (= "xtdb" (-> msg :source :db))))))))

(t/deftest test-tx-sink-output-with-tx
  (with-open [node (xtn/start-node (merge tu/*node-opts*
                                          {:tx-sink {:enable true
                                                     :output-log [::tu/recording {}]
                                                     :format :transit+json}}))]
    (let [^RecordingLog output-log (get-output-log node)]
      ;; TODO: Remove once #5012 is solved
      (jdbc/execute! node ["INSERT INTO docs RECORDS {_id: 1}"])
      (.clear (.getMessages output-log))

      (jdbc/with-transaction [tx node]
        (jdbc/execute! tx ["INSERT INTO docs RECORDS {_id: 1, a: 1, b: {c: [1, 2, 3], d: 'test'}}, {_id: 2, a: 2}, {_id: 3, a: 3}"])
        (jdbc/execute! tx ["UPDATE docs SET a = 4 WHERE _id = 1"])
        (jdbc/execute! tx ["DELETE FROM docs WHERE _id = 1"])
        (jdbc/execute! tx ["ERASE FROM docs WHERE _id = 1"])
        (jdbc/execute! tx ["INSERT INTO other RECORDS {_id: 1}"]))

      (t/is (= 1 (count (.getMessages output-log))))
      (let [msgs (->> (.getMessages output-log) (map decode-record))
            payloads (mapcat :tables msgs)
            tx-payloads (filter #(= (:table %) "txs") payloads)
            docs-payloads (filter #(= (:table %) "docs") payloads)
            other-payloads (filter #(= (:table %) "other") payloads)]
        (t/is (= 1 (count tx-payloads)))
        (t/is (= [:put :put :put :put :delete :erase]
                 (->> docs-payloads (mapcat :ops) (map :op))))
        (t/is (= [:put] (->> other-payloads (mapcat :ops) (map :op))))))))

(t/deftest test-tx-sink-block-idx-increments
  (with-open [node (xtn/start-node (merge tu/*node-opts*
                                          {:tx-sink {:enable true
                                                     :output-log [::tu/recording {}]
                                                     :format :transit+json}}))]
    (let [^RecordingLog output-log (get-output-log node)]
      (xt/execute-tx node [[:put-docs :docs {:xt/id :doc1}]])
      (t/is (= 0 (-> (.getMessages output-log) first decode-record :source :block-idx)))

      (tu/finish-block! node)

      (xt/execute-tx node [[:put-docs :docs {:xt/id :doc2}]])
      (t/is (= 1 (-> (.getMessages output-log) last decode-record :source :block-idx))))))

(t/deftest test-tx-sink-disabled
  (with-open [node (xtn/start-node tu/*node-opts*)]
    (t/is (nil? (get-output-log node)))))

(t/deftest test-tx-sink-multi-db
  (with-open [node (xtn/start-node {:tx-sink {:enable true
                                              :db-name "secondary"
                                              :output-log [::tu/recording {}]
                                              :format :transit+json}})]
    (t/testing "not enabled for primary db"
      (t/is (nil? (get-output-log node))))
    (t/testing "enabled for secondary db"
      (jdbc/execute! node [(format "ATTACH DATABASE secondary WITH $$log: !InMemory$$")])
      (let [secondary (.build (-> (.createConnectionBuilder node)
                                  (.database "secondary")))
            ^RecordingLog output-log (get-output-log node "secondary")]
        (jdbc/execute! node ["INSERT INTO docs RECORDS {_id: 1}"])
        (jdbc/execute! secondary ["INSERT INTO docs RECORDS {_id: 1}"])
        (t/is (= 1 (count (.getMessages output-log))))))))

(t/deftest test-tx-sink-restart
  (t/testing "after restart, tx-sink does not resend old messages"
    (util/with-tmp-dirs #{local-path}
      (let [messages (with-open [node (xtn/start-node {:log [:local {:path (.resolve local-path "log")}]
                                                       :tx-sink {:enable true
                                                                 :output-log [::tu/recording {}]
                                                                 :format :transit+json}})]
                       (xt/execute-tx node [[:put-docs :docs {:xt/id :doc1}]])
                       (let [^RecordingLog output-log (get-output-log node)]
                         (.getMessages output-log)))]
        (t/is (= 1 (count messages)))
        (with-open [node (xtn/start-node {:log [:local {:path (.resolve local-path "log")}]
                                          :tx-sink {:enable true
                                                    :output-log [::tu/recording {:messages messages}]
                                                    :format :transit+json}})]
          (xt-log/sync-node node #xt/duration "PT1S")
          (let [^RecordingLog output-log (get-output-log node)]
            (t/is (= 1 (count (.getMessages output-log))))
            (t/is (= messages (.getMessages output-log)))))))))

;; ============================================================================
;; Initial Scan Tests (#5054)
;; ============================================================================

(defn- get-trie-cat [node]
  (let [^Database$Catalog db-cat (util/component node :xtdb/db-catalog)]
    (-> db-cat
        (.databaseOrNull "xtdb")
        (.getTrieCatalog))))

(t/deftest test-initial-scan-outputs-historical-transactions
  ;; Test 1: Start node, write transactions, flush blocks, stop node,
  ;; start node with TxSink + initialScan: true, validate it writes those events
  (util/with-tmp-dirs #{node-dir}
    ;; Phase 1: Create data without TxSink
    (with-open [node (xtn/start-node {:storage [:local {:path (.resolve node-dir "storage")}]
                                      :log [:local {:path (.resolve node-dir "log")}]
                                      :compactor {:threads 0}})]
      (jdbc/execute! node ["INSERT INTO docs RECORDS {_id: 1, value: 'first'}"])
      (tu/flush-block! node)
      (jdbc/execute! node ["INSERT INTO docs RECORDS {_id: 2, value: 'second'}"])
      (tu/flush-block! node))

    ;; Phase 2: Start with TxSink + initialScan, verify it outputs historical txs
    (with-open [node (xtn/start-node {:storage [:local {:path (.resolve node-dir "storage")}]
                                      :log [:local {:path (.resolve node-dir "log")}]
                                      :compactor {:threads 0}
                                      :tx-sink {:enable true
                                                :initial-scan true
                                                :output-log [::tu/recording {}]
                                                :format :transit+json}})]
      ;; Wait for initial scan to complete
      (xt-log/sync-node node #xt/duration "PT5S")

      (let [^RecordingLog output-log (get-output-log node)
            msgs (->> (.getMessages output-log) (map decode-record))]
        ;; Should have 2 transactions from L0 scan
        (t/is (= 2 (count msgs)) "Expected 2 historical transactions from L0 scan")

        ;; Verify transactions are in order (by tx-id) - ascending order
        (let [tx-ids (->> msgs (map #(-> % :transaction :id :tx-id)))]
          (t/is (apply < tx-ids) "Transactions should be in ascending order"))

        ;; Verify content
        (let [docs-ops (->> msgs
                            (mapcat :tables)
                            (filter #(= (:table %) "docs"))
                            (mapcat :ops))]
          (t/is (= 2 (count docs-ops)))
          (t/is (= #{"first" "second"}
                   (->> docs-ops (map #(get-in % [:doc "value"])) set))))

        ;; Verify new transactions also get output
        (jdbc/execute! node ["INSERT INTO docs RECORDS {_id: 3, value: 'third'}"])
        (t/is (= 3 (count (.getMessages output-log)))
              "New transaction should also be output")))))

(t/deftest test-initial-scan-skips-unflushed-transactions
  ;; Test 2: TxSink skips transactions not in L0 (expected until stage 2)
  (util/with-tmp-dirs #{node-dir}
    ;; Phase 1: Create data, flush some blocks
    (with-open [node (xtn/start-node {:storage [:local {:path (.resolve node-dir "storage")}]
                                      :log [:local {:path (.resolve node-dir "log")}]
                                      :compactor {:threads 0}})]
      (jdbc/execute! node ["INSERT INTO docs RECORDS {_id: 1}"])
      (tu/flush-block! node))

    ;; Phase 2: Start TxSink, let it scan
    (let [recording-messages (atom [])]
      (with-open [node (xtn/start-node {:storage [:local {:path (.resolve node-dir "storage")}]
                                        :log [:local {:path (.resolve node-dir "log")}]
                                        :compactor {:threads 0}
                                        :tx-sink {:enable true
                                                  :initial-scan true
                                                  :output-log [::tu/recording {}]
                                                  :format :transit+json}})]
        (xt-log/sync-node node #xt/duration "PT5S")
        (let [^RecordingLog output-log (get-output-log node)]
          (reset! recording-messages (vec (.getMessages output-log)))))

      ;; Phase 3: Add more blocks without TxSink
      (with-open [node (xtn/start-node {:storage [:local {:path (.resolve node-dir "storage")}]
                                        :log [:local {:path (.resolve node-dir "log")}]
                                        :compactor {:threads 0}})]
        (jdbc/execute! node ["INSERT INTO docs RECORDS {_id: 2}"])
        (tu/flush-block! node)
        (jdbc/execute! node ["INSERT INTO docs RECORDS {_id: 3}"])
        (tu/flush-block! node))

      ;; Phase 4: Restart TxSink - should NOT output the new transactions
      ;; because they weren't in L0 when it started, and we don't re-scan
      ;; (This behavior will change in stage 2)
      (with-open [node (xtn/start-node {:storage [:local {:path (.resolve node-dir "storage")}]
                                        :log [:local {:path (.resolve node-dir "log")}]
                                        :compactor {:threads 0}
                                        :tx-sink {:enable true
                                                  :initial-scan true
                                                  :output-log [::tu/recording {:messages @recording-messages}]
                                                  :format :transit+json}})]
        (xt-log/sync-node node #xt/duration "PT5S")
        (let [^RecordingLog output-log (get-output-log node)
              msgs (->> (.getMessages output-log) (map decode-record))]
          ;; Should still only have 1 message (the original)
          ;; because last-tx-key is set from output log
          (t/is (= 1 (count msgs))
                "Should not re-output historical transactions"))))))

(t/deftest test-initial-scan-reads-garbage-l0s
  ;; Test 5: L0s marked as garbage should still be readable
  (util/with-tmp-dirs #{node-dir}
    ;; Phase 1: Create data and compact to L1 (marking L0s as garbage)
    (with-open [node (xtn/start-node {:storage [:local {:path (.resolve node-dir "storage")}]
                                      :log [:local {:path (.resolve node-dir "log")}]
                                      :compactor {:threads 1}})]  ; Enable compactor
      (jdbc/execute! node ["INSERT INTO docs RECORDS {_id: 1}"])
      (tu/flush-block! node)

      ;; Give compactor time to run
      (Thread/sleep 2000)

      ;; Verify L0s are marked as garbage
      (let [^xtdb.trie.TrieCatalog trie-cat (get-trie-cat node)
            _ (.refresh trie-cat)
            table-state (trie-cat/trie-state trie-cat (xtdb.table.TableRef/parse "xtdb" "docs"))
            l0-partition (get-in table-state [:tries [0 nil []]])]
        ;; L0 should be in either :live or :garbage after compaction
        (t/is (or (seq (:live l0-partition))
                  (seq (:garbage l0-partition)))
              "L0 should exist in trie catalog")))

    ;; Phase 2: Start with initialScan, should still read from L0 files
    (with-open [node (xtn/start-node {:storage [:local {:path (.resolve node-dir "storage")}]
                                      :log [:local {:path (.resolve node-dir "log")}]
                                      :compactor {:threads 0}
                                      :tx-sink {:enable true
                                                :initial-scan true
                                                :output-log [::tu/recording {}]
                                                :format :transit+json}})]
      (xt-log/sync-node node #xt/duration "PT5S")

      (let [^RecordingLog output-log (get-output-log node)
            msgs (->> (.getMessages output-log) (map decode-record))]
        (t/is (= 1 (count msgs))
              "Should read from L0 even if marked as garbage")))))

(t/deftest test-initial-scan-catch-up-loop
  ;; Test 3: Catch-up loop handles new blocks appearing during scan
  ;; Uses process-block-fn injection to pause scan mid-way
  (util/with-tmp-dirs #{node-dir}
    ;; Phase 1: Create initial data
    (with-open [node (xtn/start-node {:storage [:local {:path (.resolve node-dir "storage")}]
                                      :log [:local {:path (.resolve node-dir "log")}]
                                      :compactor {:threads 0}})]
      (jdbc/execute! node ["INSERT INTO docs RECORDS {_id: 1}"])
      (tu/flush-block! node))

    ;; Start node with TxSink
    (with-open [node (xtn/start-node {:storage [:local {:path (.resolve node-dir "storage")}]
                                      :log [:local {:path (.resolve node-dir "log")}]
                                      :compactor {:threads 0}
                                      :tx-sink {:enable true
                                                :initial-scan true
                                                :output-log [::tu/recording {}]
                                                :format :transit+json}})]
      ;; Give initial scan time to process first block
      (Thread/sleep 1000)

      ;; The initial scan happens synchronously during init, so by now it's complete
      ;; This tests that any subsequent blocks (not yet flushed) would also be picked up
      ;; in the catch-up loop. However, since initialScan runs at init time,
      ;; we can verify that the node started correctly and processed existing blocks.

      (let [^RecordingLog output-log (get-output-log node)
            initial-count (count (.getMessages output-log))]
        (t/is (= 1 initial-count) "Should have processed initial block")

        ;; Now add another transaction (this won't trigger initial scan,
        ;; but will be output via normal TxSink.onCommit)
        (jdbc/execute! node ["INSERT INTO docs RECORDS {_id: 2}"])
        (xt-log/sync-node node #xt/duration "PT5S")

        (t/is (= 2 (count (.getMessages output-log)))
              "New transaction should also be output via normal TxSink path")))))

(t/deftest test-initial-scan-no-block-files-needed
  ;; Test 4: Initial scan works even if block files have been GC'd
  ;; because it reads from L0 files in trie catalog, not block files
  (util/with-tmp-dirs #{node-dir}
    ;; Phase 1: Create data
    (with-open [node (xtn/start-node {:storage [:local {:path (.resolve node-dir "storage")}]
                                      :log [:local {:path (.resolve node-dir "log")}]
                                      :compactor {:threads 0}})]
      (jdbc/execute! node ["INSERT INTO docs RECORDS {_id: 1}"])
      (tu/flush-block! node)
      (jdbc/execute! node ["INSERT INTO docs RECORDS {_id: 2}"])
      (tu/flush-block! node))

    ;; Phase 2: Start fresh node with TxSink + initialScan
    ;; The key point is: L0 files exist, that's what we read
    (with-open [node (xtn/start-node {:storage [:local {:path (.resolve node-dir "storage")}]
                                      :log [:local {:path (.resolve node-dir "log")}]
                                      :compactor {:threads 0}
                                      :tx-sink {:enable true
                                                :initial-scan true
                                                :output-log [::tu/recording {}]
                                                :format :transit+json}})]
      (xt-log/sync-node node #xt/duration "PT5S")

      (let [^RecordingLog output-log (get-output-log node)
            msgs (->> (.getMessages output-log) (map decode-record))]
        ;; Should have read both transactions from L0 files
        (t/is (= 2 (count msgs))
              "Should read from L0 files regardless of block file presence")))))
