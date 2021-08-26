(ns crux.kafka-connect-test
  (:require [crux.api :as xt]
            [crux.codec :as c]
            [clojure.test :as t]
            [crux.fixtures.http-server :as fh :refer [*api-url*]]
            [crux.fixtures :as fix :refer [*api*]])
  (:import [xtdb.kafka.connect XtdbSinkTask XtdbSourceTask]
           org.apache.kafka.connect.sink.SinkRecord
           [org.apache.kafka.connect.source SourceTaskContext SourceRecord]
           org.apache.kafka.connect.storage.OffsetStorageReader))

(t/use-fixtures :each fh/with-http-server fix/with-node)

(defn new-sink-record [{:keys [topic partition key-schema key value-schema value kafka-offset]
                        :or {partition 0
                             kafka-offset 0}}]
  (SinkRecord. topic partition key-schema key value-schema value kafka-offset))

(defn get-tx-from-source-task [^XtdbSourceTask source-task]
  (some-> (.poll source-task)
          (first)
          (#(.value ^SourceRecord %))
          (c/read-edn-string-with-readers)))

(defn get-docs-from-source-task [^XtdbSourceTask source-task]
  (let [docs (.poll source-task)]
    (map
     (fn [record]
       {:doc (c/read-edn-string-with-readers (.value ^SourceRecord record))
        :id (.key ^SourceRecord record)})
     docs)))

(t/deftest test-sink-task
  (let [sink-task (doto (XtdbSinkTask.) (.start {"url" *api-url*}))]
    (t/testing "`put` on documents"
      (t/testing "put with key contained in document"
        (.put sink-task [(new-sink-record {:value {:xt/id :foo}})])
        (t/is (xt/await-tx *api* {:xt/tx-id 0}))
        (t/is (= {:xt/id (c/new-id :foo)} (xt/entity (xt/db *api*) :foo))))
      (t/testing "put with key contained in sink record"
        (.put sink-task [(new-sink-record {:key :bar
                                           :value {:hello "world"}})])
        (t/is (xt/await-tx *api* {:xt/tx-id 1}))
        (t/is (= {:xt/id (c/new-id :bar) :hello "world"} (xt/entity (xt/db *api*) :bar)))))
    (t/testing "`delete` on documents - (key with an empty document)"
      (.put sink-task [(new-sink-record {:key :foo})])
      (t/is (xt/await-tx *api* {:xt/tx-id 2}))
      (t/is (nil? (xt/entity (xt/db *api*) :foo))))
    (.stop sink-task))
  (t/testing "testing sinktask with custom id.key config"
    (let [sink-task (doto (XtdbSinkTask.) (.start {"url" *api-url*
                                                   "id.key" "kafka/id"}))]
      (.put sink-task [(new-sink-record {:value {:kafka/id :kafka-id}})])
      (t/is (xt/await-tx *api* {:xt/tx-id 3}))
      (t/is (= {:kafka/id :kafka-id
                :xt/id (c/new-id :kafka-id)} (xt/entity (xt/db *api*) :kafka-id)))
      (.stop sink-task))))

(t/deftest test-source-task-tx-mode-edn
  (let [source-props {"url" *api-url*
                      "topic" "crux-tx"
                      "format" "edn"
                      "mode" "tx"
                      "batch.size" "100"}
        source-task (-> (XtdbSourceTask.)
                        (doto (.start source-props))
                        (doto (.initialize (reify SourceTaskContext
                                             (configs [_]
                                               source-props)
                                             (offsetStorageReader [_]
                                               (reify OffsetStorageReader
                                                 (offset [_ p] {"offset" nil})
                                                 (offsets [this ps] (map #(.offset this %) ps))))))))]
    (t/testing "XtdbSourceTask outputs single operation transactions"
      (t/testing ":xt/put"
        (let [{:xt/keys [tx-time] :as tx} (fix/submit+await-tx *api* [[:xt/put {:xt/id :hello}]])]
          (t/is
           (= [[:xt/put {:xt/id :hello} tx-time]]
              (get-tx-from-source-task source-task)))))
      (t/testing ":xt/match"
        (let [{:xt/keys [tx-time] :as tx} (fix/submit+await-tx *api* [[:xt/match :hello {:xt/id :hello}]])]
          (t/is
           (= [[:xt/match (c/new-id :hello) {:xt/id :hello}]]
              (get-tx-from-source-task source-task)))))
      (t/testing ":xt/delete"
        (let [{:xt/keys [tx-time] :as tx} (fix/submit+await-tx *api* [[:xt/delete :hello]])]
          (t/is
           (= [[:xt/delete (c/new-id :hello) tx-time]]
              (get-tx-from-source-task source-task)))))
      (t/testing ":xt/evict"
        (let [{:xt/keys [tx-time] :as tx} (fix/submit+await-tx *api* [[:xt/evict :hello]])]
          (t/is
           (= [[:xt/evict (c/new-id :hello)]]
              (get-tx-from-source-task source-task))))))

    (t/testing "XtdbSourceTask outputs a set of mixed transactions"
      (let [{:xt/keys [tx-time] :as tx} (fix/submit+await-tx *api* [[:xt/put {:xt/id :bar :age 20}]
                                                                         [:xt/put {:xt/id :foo}]
                                                                         [:xt/match :foo {:xt/id :foo}]])]
        (t/is
         (= [[:xt/put {:xt/id :bar :age 20} tx-time]
             [:xt/put {:xt/id :foo} tx-time]
             [:xt/match (c/new-id :foo) {:xt/id :foo}]]
            (get-tx-from-source-task source-task)))))

    (t/testing "XtdbSourceTask doesn't break on failed transactions"
      (t/testing "Failed transactions are skipped, outputted as nil"
        (let [{:xt/keys [tx-time] :as tx} (fix/submit+await-tx *api* [[:xt/put {:xt/id :bar2}]
                                                                           [:xt/match :bar2 {:xt/id :bar2 :key "not-found"}]
                                                                           [:xt/put {:xt/id :foo2}]])]
          (t/is
           (= nil
              (get-tx-from-source-task source-task)))))
      (t/testing "Continues to read post a failed transaction"
        (let [{:xt/keys [tx-time] :as tx} (fix/submit+await-tx *api* [[:xt/put {:xt/id :test}]])]
          (t/is
           (= [[:xt/put {:xt/id :test} tx-time]]
              (get-tx-from-source-task source-task))))))
    (.stop source-task)))

(t/deftest test-source-task-doc-mode-edn
  (t/testing "Testing doc mode with EDN"
    (let [source-props {"url" *api-url*
                        "topic" "crux-tx"
                        "format" "edn"
                        "mode" "doc"
                        "batch.size" "100"}
          source-task (-> (XtdbSourceTask.)
                          (doto (.start source-props))
                          (doto (.initialize (reify SourceTaskContext
                                               (configs [_]
                                                 source-props)
                                               (offsetStorageReader [_]
                                                 (reify OffsetStorageReader
                                                   (offset [_ p] {"offset" nil})
                                                   (offsets [this ps] (map #(.offset this %) ps))))))))
          hello-doc-id (str (c/new-id :hello-doc))]
      (t/testing ":xt/put"
        (let [{:xt/keys [tx-time] :as tx} (fix/submit+await-tx *api* [[:xt/put {:xt/id :hello-doc}]])]
          (t/is
           (= {:doc {:xt/id :hello-doc}
               :id hello-doc-id}
              (first (get-docs-from-source-task source-task))))))
      (t/testing ":xt/match"
        (let [{:xt/keys [tx-time] :as tx} (fix/submit+await-tx *api* [[:xt/match :hello {:xt/id :hello-doc}]])]
          (t/is (empty? (get-docs-from-source-task source-task)))))
      (t/testing ":xt/delete"
        (let [{:xt/keys [tx-time] :as tx} (fix/submit+await-tx *api* [[:xt/delete :hello-doc]])]
          (t/is
           (= {:doc nil
               :id hello-doc-id}
              (first (get-docs-from-source-task source-task))))))
      (t/testing ":xt/evict"
        (let [{:xt/keys [tx-time] :as tx} (fix/submit+await-tx *api* [[:xt/evict :hello-doc]])]
          (= {:doc nil
               :id hello-doc-id}
              (first (get-docs-from-source-task source-task)))))
      (.stop source-task))))
