(ns xtdb.tx-sink-test
  (:require [clojure.test :as t]
            [next.jdbc :as jdbc]
            [xtdb.api :as xt]
            [xtdb.db-catalog :as db]
            [xtdb.log :as xt-log]
            [xtdb.node :as xtn]
            [xtdb.serde :as serde]
            [xtdb.test-util :as tu]
            [xtdb.tx-sink :as tx-sink]
            [xtdb.util :as util])
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
            (t/is (= #{"first" "second"} (->> puts (map #(get-in % [:doc "value"])) set)))))

        (t/testing "delete operations"
          (let [deletes (filter #(= :delete (:op %)) rows)]
            (t/is (= 1 (count deletes)))
            (t/is (nil? (:doc (first deletes))))))

        (t/testing "all rows have required temporal fields"
          (t/is (every? :iid rows))
          (t/is (every? :valid-from rows))
          (t/is (every? :system-from rows))
          (t/is (every? #(contains? % :valid-to) rows)))))))
