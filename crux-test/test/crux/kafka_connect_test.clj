(ns crux.kafka-connect-test
  (:require [crux.api :as api]
            [crux.kafka :as k]
            [crux.kafka.connect :as cfc]
            [crux.codec :as c]
            [clojure.test :as t]
            [crux.fixtures.http-server :as fh :refer [*api-url*]]
            [crux.fixtures :as fix :refer [*api*]]
            [clojure.edn :as edn])
  (:import [crux.kafka.connect CruxSinkTask CruxSourceTask CruxSourceConnector]
           org.apache.kafka.connect.sink.SinkRecord
           [org.apache.kafka.connect.source SourceTaskContext SourceRecord]
           org.apache.kafka.connect.storage.OffsetStorageReader))

(t/use-fixtures :each fh/with-http-server fix/with-node)

(defn new-sink-record [{:keys [topic partition key-schema key value-schema value kafka-offset]
                        :or {partition 0
                             kafka-offset 0}}]
  (SinkRecord. topic partition key-schema key value-schema value kafka-offset))

(defn get-tx-from-source-task [^CruxSourceTask source-task]
  (some-> (.poll source-task)
          (first)
          (#(.value ^SourceRecord %))
          (c/read-edn-string-with-readers)))

(defn get-docs-from-source-task [^CruxSourceTask source-task]
  (let [docs (.poll source-task)]
    (map
     (fn [record]
       {:doc (c/read-edn-string-with-readers (.value ^SourceRecord record))
        :id (.key ^SourceRecord record)})
     docs)))

(t/deftest test-sink-task
  (let [sink-task (doto (CruxSinkTask.) (.start {"url" *api-url*}))]
    (t/testing "`put` on documents"
      (t/testing "put with key contained in document"
        (.put sink-task [(new-sink-record {:value {:xt/id :foo}})])
        (t/is (api/await-tx *api* {:xt/tx-id 0}))
        (t/is (= {:xt/id (c/new-id :foo)} (api/entity (api/db *api*) :foo))))
      (t/testing "put with key contained in sink record"
        (.put sink-task [(new-sink-record {:key :bar
                                           :value {:hello "world"}})])
        (t/is (api/await-tx *api* {:xt/tx-id 1}))
        (t/is (= {:xt/id (c/new-id :bar) :hello "world"} (api/entity (api/db *api*) :bar)))))
    (t/testing "`delete` on documents - (key with an empty document)"
      (.put sink-task [(new-sink-record {:key :foo})])
      (t/is (api/await-tx *api* {:xt/tx-id 2}))
      (t/is (nil? (api/entity (api/db *api*) :foo))))
    (.stop sink-task))
  (t/testing "testing sinktask with custom id.key config"
    (let [sink-task (doto (CruxSinkTask.) (.start {"url" *api-url*
                                                   "id.key" "kafka/id"}))]
      (.put sink-task [(new-sink-record {:value {:kafka/id :kafka-id}})])
      (t/is (api/await-tx *api* {:xt/tx-id 3}))
      (t/is (= {:kafka/id :kafka-id
                :xt/id (c/new-id :kafka-id)} (api/entity (api/db *api*) :kafka-id)))
      (.stop sink-task))))

(t/deftest test-source-task-tx-mode-edn
  (let [source-props {"url" *api-url*
                      "topic" "crux-tx"
                      "format" "edn"
                      "mode" "tx"
                      "batch.size" "100"}
        source-task (-> (CruxSourceTask.)
                        (doto (.start source-props))
                        (doto (.initialize (reify SourceTaskContext
                                             (configs [_]
                                               source-props)
                                             (offsetStorageReader [_]
                                               (reify OffsetStorageReader
                                                 (offset [_ p] {"offset" nil})
                                                 (offsets [this ps] (map #(.offset this %) ps))))))))]
    (t/testing "CruxSourceTask outputs single operation transactions"
      (t/testing ":crux.tx/put"
        (let [{:xt/keys [tx-time] :as tx} (fix/submit+await-tx *api* [[:crux.tx/put {:xt/id :hello}]])]
          (t/is
           (= [[:crux.tx/put {:xt/id :hello} tx-time]]
              (get-tx-from-source-task source-task)))))
      (t/testing ":crux.tx/match"
        (let [{:xt/keys [tx-time] :as tx} (fix/submit+await-tx *api* [[:crux.tx/match :hello {:xt/id :hello}]])]
          (t/is
           (= [[:crux.tx/match (c/new-id :hello) {:xt/id :hello}]]
              (get-tx-from-source-task source-task)))))
      (t/testing ":crux.tx/delete"
        (let [{:xt/keys [tx-time] :as tx} (fix/submit+await-tx *api* [[:crux.tx/delete :hello]])]
          (t/is
           (= [[:crux.tx/delete (c/new-id :hello) tx-time]]
              (get-tx-from-source-task source-task)))))
      (t/testing ":crux.tx/evict"
        (let [{:xt/keys [tx-time] :as tx} (fix/submit+await-tx *api* [[:crux.tx/evict :hello]])]
          (t/is
           (= [[:crux.tx/evict (c/new-id :hello)]]
              (get-tx-from-source-task source-task))))))

    (t/testing "CruxSourceTask outputs a set of mixed transactions"
      (let [{:xt/keys [tx-time] :as tx} (fix/submit+await-tx *api* [[:crux.tx/put {:xt/id :bar :age 20}]
                                                                         [:crux.tx/put {:xt/id :foo}]
                                                                         [:crux.tx/match :foo {:xt/id :foo}]])]
        (t/is
         (= [[:crux.tx/put {:xt/id :bar :age 20} tx-time]
             [:crux.tx/put {:xt/id :foo} tx-time]
             [:crux.tx/match (c/new-id :foo) {:xt/id :foo}]]
            (get-tx-from-source-task source-task)))))

    (t/testing "CruxSourceTask doesn't break on failed transactions"
      (t/testing "Failed transactions are skipped, outputted as nil"
        (let [{:xt/keys [tx-time] :as tx} (fix/submit+await-tx *api* [[:crux.tx/put {:xt/id :bar2}]
                                                                           [:crux.tx/match :bar2 {:xt/id :bar2 :key "not-found"}]
                                                                           [:crux.tx/put {:xt/id :foo2}]])]
          (t/is
           (= nil
              (get-tx-from-source-task source-task)))))
      (t/testing "Continues to read post a failed transaction"
        (let [{:xt/keys [tx-time] :as tx} (fix/submit+await-tx *api* [[:crux.tx/put {:xt/id :test}]])]
          (t/is
           (= [[:crux.tx/put {:xt/id :test} tx-time]]
              (get-tx-from-source-task source-task))))))
    (.stop source-task)))

(t/deftest test-source-task-doc-mode-edn
  (t/testing "Testing doc mode with EDN"
    (let [source-props {"url" *api-url*
                        "topic" "crux-tx"
                        "format" "edn"
                        "mode" "doc"
                        "batch.size" "100"}
          source-task (-> (CruxSourceTask.)
                          (doto (.start source-props))
                          (doto (.initialize (reify SourceTaskContext
                                               (configs [_]
                                                 source-props)
                                               (offsetStorageReader [_]
                                                 (reify OffsetStorageReader
                                                   (offset [_ p] {"offset" nil})
                                                   (offsets [this ps] (map #(.offset this %) ps))))))))
          hello-doc-id (str (c/new-id :hello-doc))]
      (t/testing ":crux.tx/put"
        (let [{:xt/keys [tx-time] :as tx} (fix/submit+await-tx *api* [[:crux.tx/put {:xt/id :hello-doc}]])]
          (t/is
           (= {:doc {:xt/id :hello-doc}
               :id hello-doc-id}
              (first (get-docs-from-source-task source-task))))))
      (t/testing ":crux.tx/match"
        (let [{:xt/keys [tx-time] :as tx} (fix/submit+await-tx *api* [[:crux.tx/match :hello {:xt/id :hello-doc}]])]
          (t/is (empty? (get-docs-from-source-task source-task)))))
      (t/testing ":crux.tx/delete"
        (let [{:xt/keys [tx-time] :as tx} (fix/submit+await-tx *api* [[:crux.tx/delete :hello-doc]])]
          (t/is
           (= {:doc nil
               :id hello-doc-id}
              (first (get-docs-from-source-task source-task))))))
      (t/testing ":crux.tx/evict"
        (let [{:xt/keys [tx-time] :as tx} (fix/submit+await-tx *api* [[:crux.tx/evict :hello-doc]])]
          (= {:doc nil
               :id hello-doc-id}
              (first (get-docs-from-source-task source-task)))))
      (.stop source-task))))
