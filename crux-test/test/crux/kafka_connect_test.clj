(ns crux.kafka-connect-test
  (:require [clojure.test :as t]
            [crux.api :as api]
            [crux.codec :as c]
            [crux.fixtures :as fix :refer [*api*]]
            [crux.fixtures.http-server :as fh :refer [*api-url*]])
  (:import [crux.kafka.connect CruxSinkTask CruxSourceTask]
           org.apache.kafka.connect.sink.SinkRecord
           [org.apache.kafka.connect.source SourceRecord SourceTaskContext]
           org.apache.kafka.connect.storage.OffsetStorageReader))

(t/use-fixtures :each fh/with-http-server fix/with-node)

(defn new-sink-record [{:keys [topic partition key-schema key value-schema value kafka-offset]
                        :or {partition 0
                             kafka-offset 0}}]
  (SinkRecord. topic partition key-schema key value-schema value kafka-offset))

(t/deftest test-sink-task
  (let [sink-task (doto (CruxSinkTask.) (.start {"url" *api-url*}))]
    (t/testing "`put` on documents"
      (t/testing "put with key contained in document"
        (.put sink-task [(new-sink-record {:value {:crux.db/id :foo}})])
        (t/is (api/await-tx *api* {:crux.tx/tx-id 0}))
        (t/is (= {:crux.db/id (c/new-id :foo)} (api/entity (api/db *api*) :foo))))

      (t/testing "put with key contained in sink record"
        (.put sink-task [(new-sink-record {:key :bar
                                           :value {:hello "world"}})])
        (t/is (api/await-tx *api* {:crux.tx/tx-id 1}))
        (t/is (= {:crux.db/id (c/new-id :bar) :hello "world"} (api/entity (api/db *api*) :bar)))))

    (t/testing "`delete` on documents - (key with an empty document)"
      (.put sink-task [(new-sink-record {:key :foo})])
      (t/is (api/await-tx *api* {:crux.tx/tx-id 2}))
      (t/is (nil? (api/entity (api/db *api*) :foo))))
    (.stop sink-task))

  (t/testing "testing sinktask with custom id.key config"
    (let [sink-task (doto (CruxSinkTask.) (.start {"url" *api-url*
                                                   "id.key" "kafka/id"}))]
      (.put sink-task [(new-sink-record {:value {:kafka/id :kafka-id}})])
      (t/is (api/await-tx *api* {:crux.tx/tx-id 3}))
      (t/is (= {:kafka/id :kafka-id
                :crux.db/id (c/new-id :kafka-id)} (api/entity (api/db *api*) :kafka-id)))
      (.stop sink-task))))

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
        (let [{:crux.tx/keys [tx-time] :as tx} (fix/submit+await-tx *api* [[:crux.tx/put {:crux.db/id :hello}]])]
          (t/is
           (= [[:crux.tx/put {:crux.db/id :hello} tx-time]]
              (get-tx-from-source-task source-task)))))
      (t/testing ":crux.tx/match"
        (let [{:crux.tx/keys [tx-time] :as tx} (fix/submit+await-tx *api* [[:crux.tx/match :hello {:crux.db/id :hello}]])]
          (t/is
           (= [[:crux.tx/match (c/new-id :hello) {:crux.db/id :hello}]]
              (get-tx-from-source-task source-task)))))
      (t/testing ":crux.tx/delete"
        (let [{:crux.tx/keys [tx-time] :as tx} (fix/submit+await-tx *api* [[:crux.tx/delete :hello]])]
          (t/is
           (= [[:crux.tx/delete (c/new-id :hello) tx-time]]
              (get-tx-from-source-task source-task)))))
      (t/testing ":crux.tx/evict"
        (let [{:crux.tx/keys [tx-time] :as tx} (fix/submit+await-tx *api* [[:crux.tx/evict :hello]])]
          (t/is
           (= [[:crux.tx/evict (c/new-id :hello)]]
              (get-tx-from-source-task source-task))))))

    (t/testing "CruxSourceTask outputs a set of mixed transactions"
      (let [{:crux.tx/keys [tx-time] :as tx} (fix/submit+await-tx *api* [[:crux.tx/put {:crux.db/id :bar :age 20}]
                                                                         [:crux.tx/put {:crux.db/id :foo}]
                                                                         [:crux.tx/match :foo {:crux.db/id :foo}]])]
        (t/is
         (= [[:crux.tx/put {:crux.db/id :bar :age 20} tx-time]
             [:crux.tx/put {:crux.db/id :foo} tx-time]
             [:crux.tx/match (c/new-id :foo) {:crux.db/id :foo}]]
            (get-tx-from-source-task source-task)))))

    (t/testing "CruxSourceTask doesn't break on failed transactions"
      (t/testing "Failed transactions are skipped, outputted as nil"
        (let [{:crux.tx/keys [tx-time] :as tx} (fix/submit+await-tx *api* [[:crux.tx/put {:crux.db/id :bar2}]
                                                                           [:crux.tx/match :bar2 {:crux.db/id :bar2 :key "not-found"}]
                                                                           [:crux.tx/put {:crux.db/id :foo2}]])]
          (t/is
           (= nil
              (get-tx-from-source-task source-task)))))
      (t/testing "Continues to read post a failed transaction"
        (let [{:crux.tx/keys [tx-time] :as tx} (fix/submit+await-tx *api* [[:crux.tx/put {:crux.db/id :test}]])]
          (t/is
           (= [[:crux.tx/put {:crux.db/id :test} tx-time]]
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
        (let [{:crux.tx/keys [tx-time] :as tx} (fix/submit+await-tx *api* [[:crux.tx/put {:crux.db/id :hello-doc}]])]
          (t/is
           (= {:doc {:crux.db/id :hello-doc}
               :id hello-doc-id}
              (first (get-docs-from-source-task source-task))))))
      (t/testing ":crux.tx/match"
        (let [{:crux.tx/keys [tx-time] :as tx} (fix/submit+await-tx *api* [[:crux.tx/match :hello {:crux.db/id :hello-doc}]])]
          (t/is (empty? (get-docs-from-source-task source-task)))))
      (t/testing ":crux.tx/delete"
        (let [{:crux.tx/keys [tx-time] :as tx} (fix/submit+await-tx *api* [[:crux.tx/delete :hello-doc]])]
          (t/is
           (= {:doc nil
               :id hello-doc-id}
              (first (get-docs-from-source-task source-task))))))
      (t/testing ":crux.tx/evict"
        (let [{:crux.tx/keys [tx-time] :as tx} (fix/submit+await-tx *api* [[:crux.tx/evict :hello-doc]])]
          (= {:doc nil
               :id hello-doc-id}
              (first (get-docs-from-source-task source-task)))))
      (.stop source-task))))
