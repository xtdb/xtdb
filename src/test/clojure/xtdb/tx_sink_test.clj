(ns xtdb.tx-sink-test
  (:require [clojure.test :as t]
            [next.jdbc :as jdbc]
            [xtdb.api :as xt]
            [xtdb.node :as xtn]
            [xtdb.serde :as serde]
            [xtdb.test-util :as tu]
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
                    :source {:db "xtdb"}
                    :tables [{:db "xtdb"
                              :schema "public"
                              :table "docs"
                              :ops [{:op :put
                                     :iid (util/->iid :doc1)
                                     :valid-from  #xt/instant "2020-01-01T00:00Z"
                                     :valid-to nil
                                     :doc {"_id" :doc1, "value" "test"}}]}
                             {:db "xtdb"
                              :schema "xt"
                              :table "txs"
                              :ops [{:op :put
                                     :iid (util/->iid 0)
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
