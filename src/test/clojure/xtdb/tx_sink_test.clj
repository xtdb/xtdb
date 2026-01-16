(ns xtdb.tx-sink-test
  (:require [clojure.test :as t]
            [next.jdbc :as jdbc]
            [xtdb.api :as xt]
            [xtdb.compactor :as c]
            [xtdb.db-catalog :as db]
            xtdb.error
            [xtdb.node :as xtn]
            [xtdb.serde :as serde]
            [xtdb.test-util :as tu]
            [xtdb.util :as util]
            [xtdb.log :as xt-log]
            [xtdb.trie-catalog :as trie-cat]
            [xtdb.tx-sink :as tx-sink])
  (:import [xtdb.api.log Log$Message]
           [xtdb.table TableRef]
           [xtdb.test.log RecordingLog]))

(t/use-fixtures :each tu/with-mock-clock)

(defn decode-record
  ([msg] (decode-record msg :json))
  ([msg fmt] (-> msg Log$Message/.encode (xtdb.serde/read-transit fmt))))

(t/deftest test-tx-sink-output
  (with-open [node (xtn/start-node (merge tu/*node-opts*
                                          {:tx-sink {:enable true
                                                     :output-log [::tu/recording {}]
                                                     :format :transit+json}}))]
    (let [^RecordingLog output-log (tu/get-output-log node)]
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
                                    :doc {:xt/id :doc1, :value "test"}}]}
                            {:db "xtdb"
                             :schema "xt"
                             :table "txs"
                             :ops [{:op :put
                                    :iid (util/->iid 0)
                                    :system-from #xt/instant "2020-01-01T00:00Z"
                                    :valid-from  #xt/instant "2020-01-01T00:00Z"
                                    :valid-to nil
                                    :doc {:xt/id 0
                                          :system-time #xt/zdt "2020-01-01T00:00[UTC]"
                                          :committed true}}]}]})
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
      (let [^RecordingLog output-log (tu/get-output-log node)]
        (xt/execute-tx node [[:put-docs :docs {:xt/id :doc1, :value "test"}]])
        (let [msg (-> (.getMessages output-log) first (decode-record :json))]
          (t/is (= #{:transaction :system-time :source :tables} (set (keys msg))))
          (t/is (= "xtdb" (-> msg :source :db)))))))
  (t/testing "transit+msgpack"
    (with-open [node (xtn/start-node (merge tu/*node-opts*
                                            {:tx-sink {:enable true
                                                       :output-log [::tu/recording {}]
                                                       :format :transit+msgpack}}))]
      (let [^RecordingLog output-log (tu/get-output-log node)]
        (xt/execute-tx node [[:put-docs :docs {:xt/id :doc1, :value "test"}]])
        (let [msg (-> (.getMessages output-log) first (decode-record :msgpack))]
          (t/is (= #{:transaction :system-time :source :tables} (set (keys msg))))
          (t/is (= "xtdb" (-> msg :source :db))))))))

(t/deftest test-tx-sink-output-with-tx
  (with-open [node (xtn/start-node (merge tu/*node-opts*
                                          {:tx-sink {:enable true
                                                     :output-log [::tu/recording {}]
                                                     :format :transit+json}}))]
    (let [^RecordingLog output-log (tu/get-output-log node)]
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
    (let [^RecordingLog output-log (tu/get-output-log node)]
      (xt/execute-tx node [[:put-docs :docs {:xt/id :doc1}]])
      (t/is (= 0 (-> (.getMessages output-log) first decode-record :source :block-idx)))

      (tu/finish-block! node)

      (xt/execute-tx node [[:put-docs :docs {:xt/id :doc2}]])
      (t/is (= 1 (-> (.getMessages output-log) last decode-record :source :block-idx))))))

(t/deftest test-tx-sink-disabled
  (with-open [node (xtn/start-node tu/*node-opts*)]
    (t/is (nil? (tu/get-output-log node)))))

(t/deftest test-tx-sink-multi-db
  (with-open [node (xtn/start-node {:tx-sink {:enable true
                                              :db-name "secondary"
                                              :output-log [::tu/recording {}]
                                              :format :transit+json}})]
    (t/testing "not enabled for primary db"
      (t/is (nil? (tu/get-output-log node))))
    (t/testing "enabled for secondary db"
      (jdbc/execute! node [(format "ATTACH DATABASE secondary WITH $$log: !InMemory$$")])
      (let [secondary (.build (-> (.createConnectionBuilder node)
                                  (.database "secondary")))
            ^RecordingLog output-log (tu/get-output-log node "secondary")]
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
                       (let [^RecordingLog output-log (tu/get-output-log node)]
                         (.getMessages output-log)))]
        (t/is (= 1 (count messages)))
        (with-open [node (xtn/start-node {:log [:local {:path (.resolve local-path "log")}]
                                          :tx-sink {:enable true
                                                    :output-log [::tu/recording {:messages messages}]
                                                    :format :transit+json}})]
          (xt-log/sync-node node #xt/duration "PT1S")
          (let [^RecordingLog output-log (tu/get-output-log node)]
            (t/is (= 1 (count (.getMessages output-log))))
            (t/is (= messages (.getMessages output-log)))))))))

(t/deftest test-read-relation-rows
  (tu/with-tmp-dirs #{node-dir}
    (with-open [node (tu/->local-node {:node-dir node-dir :compactor-threads 0})]
      (xt/execute-tx node [[:put-docs :docs {:xt/id 1 :value "first"}]])
      (xt/execute-tx node [[:put-docs :docs {:xt/id 2 :value "second"}]])
      (xt/execute-tx node [[:delete-docs :docs 1]])

      (let [xtdb-db (db/primary-db node)
            live-index (.getLiveIndex xtdb-db)
            live-table (.liveTable live-index #xt/table docs)
            live-rel (.getLiveRelation live-table)
            rows (tx-sink/read-relation-rows live-rel)]

        (t/is (= 3 (count rows)))

        (t/testing "put operations include doc"
          (let [puts (filter #(= :put (:op %)) rows)]
            (t/is (= 2 (count puts)))
            (t/is (every? :doc puts))
            (t/is (= #{"first" "second"} (->> puts (map #(get-in % [:doc :value])) set)))))

        (t/testing "delete operations"
          (let [deletes (filter #(= :delete (:op %)) rows)]
            (t/is (= 1 (count deletes)))
            (t/is (nil? (:doc (first deletes))))))

        (t/testing "all rows have required temporal fields"
          (t/is (every? :iid rows))
          (t/is (every? :valid-from rows))
          (t/is (every? :system-from rows))
          (t/is (every? #(contains? % :valid-to) rows)))))))

(t/deftest test-read-l0-events
  (tu/with-tmp-dirs #{node-dir}
    (with-open [node (tu/->local-node {:node-dir node-dir :compactor-threads 0})]
      (xt/execute-tx node [[:put-docs :foo {:xt/id 1 :value "first"}]])
      (tu/finish-block! node)
      (xt/execute-tx node [[:put-docs :foo {:xt/id 2 :value "second"}]])
      (xt/execute-tx node [[:put-docs :foo {:xt/id 3 :value "third"}]])
      (tu/finish-block! node)

      (let [allocator (.getAllocator node)
            xtdb-db (db/primary-db node)
            bp (.getBufferPool xtdb-db)
            cat (.getTrieCatalog xtdb-db)
            blocks (trie-cat/l0-blocks cat)
            table-events (fn [table-filter events]
                           (->> events
                                (filter (fn [{:keys [^TableRef table]}]
                                          (= table-filter (.getTableName table))))
                                (map :event)))
            event-values #(get-in % [:doc :value])]
        (t/is (= [0 1] (map :block-idx blocks)))
        (let [{:keys [tables]} (first blocks)
              events (tx-sink/read-l0-events allocator bp tables)
              foo-events (table-events "foo" events)]
          (t/is (= 1 (count foo-events)))
          (t/is (= #{"first"} (->> foo-events (map event-values) set))))
        (let [{:keys [tables]} (second blocks)
              events (tx-sink/read-l0-events allocator bp tables)
              foo-events (table-events "foo" events)]
          (t/is (= 2 (count foo-events)))
          (t/is (= #{"second" "third"} (->> foo-events (map event-values) set))))))))

(t/deftest test-group-events-by-system-time
  (let [t1 #inst "2020-01-01T00:00:00Z"
        t2 #inst "2020-01-01T00:01:00Z"
        t3 #inst "2020-01-01T00:02:00Z"
        events [{:table :table-a :event {:system-from t1 :id 1}}
                {:table :table-a :event {:system-from t2 :id 2}}
                {:table :table-a :event {:system-from t3 :id 3}}
                {:table :table-b :event {:system-from t1 :id 10}}
                {:table :table-b :event {:system-from t2 :id 20}}
                {:table :table-b :event {:system-from t2 :id 30}}]
        grouped (tx-sink/group-events-by-system-time events)]

    (t/testing "returns a seq with system-from as keys"
      (t/is (= [t1 t2 t3] (map first grouped))))

    (t/testing "events at t1 grouped by table"
      (let [[_ tables-at-t1] (nth grouped 0)]
        (t/is (= #{:table-a :table-b} (set (keys tables-at-t1))))
        (t/is (= [{:system-from t1 :id 1}] (get tables-at-t1 :table-a)))
        (t/is (= [{:system-from t1 :id 10}] (get tables-at-t1 :table-b)))))

    (t/testing "events at t2 grouped by table"
      (let [[_ tables-at-t2] (nth grouped 1)]
        (t/is (= #{:table-a :table-b} (set (keys tables-at-t2))))
        (t/is (= [{:system-from t2 :id 2}] (get tables-at-t2 :table-a)))
        (t/is (= [{:system-from t2 :id 20} {:system-from t2 :id 30}] (get tables-at-t2 :table-b)))))

    (t/testing "events at t3 only table-a"
      (let [[_ tables-at-t3] (nth grouped 2)]
        (t/is (= #{:table-a} (set (keys tables-at-t3))))
        (t/is (= [{:system-from t3 :id 3}] (get tables-at-t3 :table-a)))))))

(t/deftest test-events->tx-output-requires-txs-table
  (let [t1 #inst "2020-01-01T00:00:00Z"
        table->events {#xt/table "xtdb/foo" [{:doc {:xt/id 1}}]}]
    (t/is (anomalous? [:fault :xtdb.tx-sink/missing-txs-table
                       "Expected xt.txs table"]
                      (tx-sink/events->tx-output t1 table->events "xtdb" 0 identity)))))

(t/deftest test-events->tx-output-requires-exactly-one-txs-event
  (let [t1 #inst "2020-01-01T00:00:00Z"
        table->events {#xt/table "xt/txs" [{:doc {:xt/id 1}} {:doc {:xt/id 2}}]}]
    (t/is (anomalous? [:fault :xtdb.tx-sink/unexpected-txs-count
                       "Expected exactly 1 xt.txs event"]
                      (tx-sink/events->tx-output t1 table->events "xtdb" 0 identity)))))

(t/deftest test-events->tx-output-produces-valid-tx-key
  (let [t1 #inst "2020-01-01T00:00:00Z"
        table->events {#xt/table "xt/txs" [{:doc {:xt/id 42}
                                            :op :put
                                            :iid (util/->iid 42)
                                            :valid-from t1
                                            :valid-to nil}]}
        encode-fn (tx-sink/->encode-fn :transit+json)
        {:keys [tx-key]} (tx-sink/events->tx-output t1 table->events "xtdb" 0 encode-fn)]
    (t/is (= 42 (:tx-id tx-key)) "tx-key should have correct tx-id")))

(t/deftest test-initial-scan-disabled
  (util/with-tmp-dirs #{node-dir}
    ; Create L0 block
    (with-open [node (xtn/start-node {:storage [:local {:path (.resolve node-dir "storage")}]
                                      :log [:local {:path (.resolve node-dir "log") :instant-src (tu/->mock-clock)}]
                                      :compactor {:threads 0}
                                      :tx-sink {:enable true
                                                :output-log [::tu/recording {}]
                                                :format :transit+json}})]
      (jdbc/execute! node ["INSERT INTO docs RECORDS {_id: 1}"])
      (tu/flush-block! node))

    ; initial-scan disabled
    (with-open [node (xtn/start-node {:storage [:local {:path (.resolve node-dir "storage")}]
                                      :log [:local {:path (.resolve node-dir "log") :instant-src (tu/->mock-clock)}]
                                      :compactor {:threads 0}
                                      :tx-sink {:enable true
                                                :initial-scan false
                                                :output-log [::tu/recording {}]
                                                :format :transit+json}})]
      (xt-log/sync-node node #xt/duration "PT5S")

      (let [^RecordingLog output-log (tu/get-output-log node)
            msgs (->> (.getMessages output-log) (map decode-record))]
        (t/is (= 0 (count msgs)))))

    ; initial-scan enabled
    (with-open [node (xtn/start-node {:storage [:local {:path (.resolve node-dir "storage")}]
                                      :log [:local {:path (.resolve node-dir "log") :instant-src (tu/->mock-clock)}]
                                      :compactor {:threads 0}
                                      :tx-sink {:enable true
                                                :initial-scan true
                                                :output-log [::tu/recording {}]
                                                :format :transit+json}})]
      (xt-log/sync-node node #xt/duration "PT5S")

      (let [^RecordingLog output-log (tu/get-output-log node)
            msgs (->> (.getMessages output-log) (map decode-record))]
        (t/is (= 1 (count msgs)))))))

(t/deftest test-initial-scan-output-matches
  (util/with-tmp-dirs #{node-dir}
    (let [original-messages
          (with-open [node (xtn/start-node {:storage [:local {:path (.resolve node-dir "storage")}]
                                            :log [:local {:path (.resolve node-dir "log") :instant-src (tu/->mock-clock)}]
                                            :compactor {:threads 0}
                                            :tx-sink {:enable true
                                                      :output-log [::tu/recording {}]
                                                      :format :transit+json}})]
            ; Block with single transaction
            (jdbc/execute! node ["INSERT INTO docs RECORDS {_id: 1}"])
            (tu/flush-block! node)
            (jdbc/execute! node ["INSERT INTO docs RECORDS {_id: 1}"])
            ; Block with multi-statement transaction
            (jdbc/with-transaction [tx node]
              (jdbc/execute! tx ["INSERT INTO docs RECORDS {_id: 1, a: 1, b: {c: [1, 2, 3], d: 'test'}}, {_id: 2, a: 2}, {_id: 3, a: 3}"])
              (jdbc/execute! tx ["UPDATE docs SET a = 4 WHERE _id = 1"])
              (jdbc/execute! tx ["DELETE FROM docs WHERE _id = 1"])
              (jdbc/execute! tx ["ERASE FROM docs WHERE _id = 1"])
              (jdbc/execute! tx ["INSERT INTO other RECORDS {_id: 1}"]))
            (tu/flush-block! node)
            ; Block with multiple transactions
            (jdbc/execute! node ["INSERT INTO docs RECORDS {_id: 10}"])
            (jdbc/execute! node ["INSERT INTO docs RECORDS {_id: 20}"])
            (tu/flush-block! node)
            (let [^RecordingLog output-log (tu/get-output-log node)]
              (->> (.getMessages output-log) (map decode-record))))]

      (with-open [node (xtn/start-node {:storage [:local {:path (.resolve node-dir "storage")}]
                                        :log [:local {:path (.resolve node-dir "log") :instant-src (tu/->mock-clock)}]
                                        :compactor {:threads 0}
                                        :tx-sink {:enable true
                                                  :initial-scan true
                                                  :output-log [::tu/recording {}]
                                                  :format :transit+json}})]
        (xt-log/sync-node node #xt/duration "PT5S")

        (let [^RecordingLog output-log (tu/get-output-log node)
              msgs (->> (.getMessages output-log) (map decode-record))]
          (t/is (= 5 (count msgs)))

          (doseq [[idx msg original] (map vector
                                          (range)
                                          (util/->clj msgs)
                                          (util/->clj original-messages))]
            (t/testing (str "transaction " idx)
              (t/is (= (:transaction msg) (:transaction original)))
              (t/is (= (:system-time msg) (:system-time original)))
              (t/is (= (:source msg) (:source original)))
              ; NOTE: We don't guarantee the order of tables *or* ops within tables, except between iids
              (t/is (= (->> (:tables msg) (map #(update % :ops set)) set)
                       (->> (:tables original) (map #(update % :ops set)) set)))))

          (jdbc/execute! node ["INSERT INTO docs RECORDS {_id: 3, value: 'third'}"])
          (t/is (= 6 (count (.getMessages output-log)))
                "New transaction should also be output"))))))

(t/deftest test-initial-scan-outputs-historical-transactions
  (util/with-tmp-dirs #{node-dir}
    (let [; Create some l0 files
          original-messages
          (with-open [node (xtn/start-node {:storage [:local {:path (.resolve node-dir "storage")}]
                                            :log [:local {:path (.resolve node-dir "log") :instant-src (tu/->mock-clock)}]
                                            :compactor {:threads 0}
                                            :tx-sink {:enable true
                                                      :output-log [::tu/recording {}]
                                                      :format :transit+json}})]
            (jdbc/execute! node ["INSERT INTO docs RECORDS {_id: 1, value: 'first'}"])
            (tu/flush-block! node)
            (jdbc/execute! node ["INSERT INTO docs RECORDS {_id: 2, value: 'second'}"])
            (tu/flush-block! node)
            (->> (.getMessages ^RecordingLog (tu/get-output-log node)) (map decode-record)))]
      ; Check backfill behaves the same
      (with-open [node (xtn/start-node {:storage [:local {:path (.resolve node-dir "storage")}]
                                        :log [:local {:path (.resolve node-dir "log") :instant-src (tu/->mock-clock)}]
                                        :compactor {:threads 0}
                                        :tx-sink {:enable true
                                                  :initial-scan true
                                                  :output-log [::tu/recording {}]
                                                  :format :transit+json}})]
        (xt-log/sync-node node #xt/duration "PT5S")

        (let [^RecordingLog output-log (tu/get-output-log node)
              msgs (->> (.getMessages output-log) (map decode-record))]
          (t/is (= (util/->clj msgs) (util/->clj original-messages))))))))

(t/deftest test-initial-scan-catches-up-to-latest-block
  (util/with-tmp-dirs #{node-dir}
    (let [original-messages
          (with-open [node (xtn/start-node {:storage [:local {:path (.resolve node-dir "storage")}]
                                            :log [:local {:path (.resolve node-dir "log")}]
                                            :compactor {:threads 0}
                                            :tx-sink {:enable true
                                                      :initial-scan true
                                                      :output-log [::tu/recording {}]
                                                      :format :transit+json}})]
            (jdbc/execute! node ["INSERT INTO docs RECORDS {_id: 1}"])
            (tu/flush-block! node)
            (.getMessages ^RecordingLog (tu/get-output-log node)))]
      (t/is (= 1 (count original-messages))
            "Should have 1 original message")

      ; While the tx-sink is down, more blocks are added
      (with-open [node (xtn/start-node {:storage [:local {:path (.resolve node-dir "storage")}]
                                        :log [:local {:path (.resolve node-dir "log")}]
                                        :compactor {:threads 0}})]
        (jdbc/execute! node ["INSERT INTO docs RECORDS {_id: 2}"])
        (tu/flush-block! node)
        (jdbc/execute! node ["INSERT INTO docs RECORDS {_id: 3}"])
        (tu/flush-block! node))

      (with-open [node (xtn/start-node {:storage [:local {:path (.resolve node-dir "storage")}]
                                        :log [:local {:path (.resolve node-dir "log")}]
                                        :compactor {:threads 0}
                                        :tx-sink {:enable true
                                                  :initial-scan true
                                                  :output-log [::tu/recording {:messages original-messages}]
                                                  :format :transit+json}})]
        (xt-log/sync-node node #xt/duration "PT5S")
        (let [^RecordingLog output-log (tu/get-output-log node)]
          (t/is (= 3 (count (.getMessages output-log)))))))))

(t/deftest test-initial-scan-reads-garbage-l0s
  (util/with-tmp-dirs #{node-dir}
    ; Create data and compact to L1 (marking L0s as garbage)
    (with-open [node (xtn/start-node {:storage [:local {:path (.resolve node-dir "storage")}]
                                      :log [:local {:path (.resolve node-dir "log")}]
                                      :compactor {:threads 1}})]
      (doseq [_ (range 4)]
        (jdbc/execute! node ["INSERT INTO docs RECORDS {_id: 1}"])
        (tu/flush-block! node))
      (c/compact-all! node #xt/duration "PT5S")

      (let [^xtdb.trie.TrieCatalog trie-cat (tu/get-trie-cat node)
            {:keys [tries]} (trie-cat/trie-state trie-cat #xt/table "docs")
            {:keys [live garbage]} (get tries [0 nil []])]
        (t/is (empty? live))
        (t/is (not (empty? garbage)))))

    (with-open [node (xtn/start-node {:storage [:local {:path (.resolve node-dir "storage")}]
                                      :log [:local {:path (.resolve node-dir "log")}]
                                      :compactor {:threads 0}
                                      :tx-sink {:enable true
                                                :initial-scan true
                                                :output-log [::tu/recording {}]
                                                :format :transit+json}})]
      (xt-log/sync-node node #xt/duration "PT5S")

      (let [^RecordingLog output-log (tu/get-output-log node)
            msgs (->> (.getMessages output-log) (map decode-record))]
        (t/is (= 4 (count msgs))
              "Should read from L0 even if marked as garbage")))))

(t/deftest test-initial-scan-with-no-block-files
  (with-open [node (xtn/start-node {:compactor {:threads 0}
                                    :tx-sink {:enable true
                                              :initial-scan true
                                              :output-log [::tu/recording {}]
                                              :format :transit+json}})]
    (let [^RecordingLog output-log (tu/get-output-log node)
          msgs (->> (.getMessages output-log) (map decode-record))]
      (t/is (empty? msgs) "Should have no messages when no blocks exist"))))

(comment
  (util/with-tmp-dirs #{test-dir}
    (let [test-dir (java.nio.file.Files/createTempDirectory
                    "l0-order-test"
                    (into-array java.nio.file.attribute.FileAttribute []))]
      (with-open [node (xtn/start-node
                        {:storage [:local {:path (.resolve test-dir "storage")}]
                         :log [:local {:path (.resolve test-dir "log")}]
                         :compactor {:threads 0}})]
        (jdbc/with-transaction [tx node]
          (jdbc/execute! tx ["INSERT INTO docs RECORDS {_id: 1, a: 1, b: {c: [1, 2, 3], d: 'test'}}, {_id: 2, a: 2}, {_id: 3, a: 3}"])
          (jdbc/execute! tx ["UPDATE docs SET a = 4 WHERE _id = 1"])
          (jdbc/execute! tx ["DELETE FROM docs WHERE _id = 1"])
          (jdbc/execute! tx ["ERASE FROM docs WHERE _id = 1"])
          (jdbc/execute! tx ["INSERT INTO other RECORDS {_id: 1}"]))

        ;; Flush to L0
        (tu/finish-block! node)

        ;; Read L0 directly
        (let [xtdb-db (db/primary-db node)
              bp (.getBufferPool xtdb-db)
              trie-cat (.getTrieCatalog xtdb-db)
              allocator (.getAllocator node)
              [{:keys [tables]}] (trie-cat/l0-blocks trie-cat)
              events (tx-sink/read-l0-events allocator bp tables)
              fmt-instant #(some-> % str (subs 11 23))]

          (println "\n=== Events as stored in L0 ===")
          (println "(sorted by table, iid, valid-from)\n")
          (doseq [{:keys [^TableRef table event]} events]
            (let [{:keys [op doc system-from iid]} event
                  iid-hex (->> iid (take 4) (map #(format "%02x" (bit-and % 0xff))) (apply str))]
              (printf "%-6s %-7s iid=%s sf=%s doc-id=%s doc=%s\n"
                      (.getTableName table)
                      (name op)
                      iid-hex
                      (fmt-instant system-from)
                      (pr-str (:xt/id doc))
                      (pr-str doc))))

          (println "\n=== After group-events-by-system-time ===\n")
          (let [grouped (tx-sink/group-events-by-system-time events)]
            (doseq [[system-time table->events] grouped]
              (println "TX at" (fmt-instant system-time))
              (doseq [[^TableRef table-ref ops] table->events]
                (printf "  %s: %s\n"
                        (.getTableName table-ref)
                        (mapv #(vector (:op %) (:xt/id (:doc %))) ops)))
              (println))))))))
