(ns crux.kafka-test
  (:require [clojure.test :as t]
            [crux.api :as api]
            [crux.codec :as c]
            [crux.db :as db]
            [crux.fixtures :as fix :refer [*api* *opts*]]
            [crux.fixtures.kafka :as kf :refer [*ingest-client*]]
            [crux.kafka :as k]
            [crux.rdf :as rdf]
            [crux.tx :as tx])
  (:import java.time.Duration
           [org.apache.kafka.clients.consumer ConsumerRecord KafkaConsumer]
           org.apache.kafka.clients.producer.ProducerRecord))

(t/use-fixtures :once kf/with-embedded-kafka-cluster)
(t/use-fixtures :each kf/with-cluster-node-opts fix/with-kv-dir)

(t/deftest test-ingest-client
  (fix/with-node
    (fn []
      (kf/with-ingest-client
        (fn []
          (let [submitted-tx @(api/submit-tx-async *ingest-client* [[:crux.tx/put {:crux.db/id :ivan :name "Ivan"}]])]
            (api/await-tx *api* submitted-tx nil)
            (t/is (true? (api/tx-committed? *api* submitted-tx)))
            (t/is (= #{[:ivan]} (api/q (api/db *api*)
                                       '{:find [e]
                                         :where [[e :name "Ivan"]]})))

            (with-open [tx-log-iterator (api/open-tx-log *ingest-client* nil false)]
              (let [result (iterator-seq tx-log-iterator)]
                (t/is (not (realized? result)))
                (t/is (= [(assoc submitted-tx
                            :crux.tx.event/tx-events [[:crux.tx/put (c/new-id :ivan) (c/new-id {:crux.db/id :ivan :name "Ivan"})]])]
                         result))
                (t/is (realized? result))))

            (t/is (thrown? IllegalArgumentException (api/open-tx-log *ingest-client* nil true)))))))))

(defn compact-doc-messages [docs]
  (->> docs
       (reverse)
       (reduce (fn [docs doc]
                 (cond-> docs
                   (not (contains? (set (map first docs)) (first doc))) (conj doc)))
               [])
       (reverse)))

(def evicted-doc {:crux.db/id :to-be-evicted :personal "private"})
(def non-evicted-doc {:crux.db/id :not-evicted :personal "private"})
(def after-evict-doc {:crux.db/id :after-evict :personal "private"})

(defn submit-txs-to-compact []
  (fix/with-node
    (fn []
      (with-open [doc-consumer (doto (kf/open-consumer)
                                 (k/subscribe-consumer #{kf/*doc-topic*} {}))]
        (t/testing "transacting and indexing"
          (let [submitted-tx (api/submit-tx *api* [[:crux.tx/put evicted-doc]
                                                   [:crux.tx/put non-evicted-doc]])
                _ (api/await-tx *api* submitted-tx)

                evicted-doc-hash (:crux.db/content-hash (api/entity-tx (api/db *api*) :to-be-evicted))

                _ (.submitTx *api* [[:crux.tx/evict (:crux.db/id evicted-doc)]])
                submitted-tx (.submitTx *api* [[:crux.tx/put after-evict-doc]])
                _ (.awaitTx *api* submitted-tx nil)]

            (t/testing "querying transacted data"
              (t/is (= non-evicted-doc (api/entity (api/db *api*) :not-evicted)))
              (t/is (nil? (api/entity (api/db *api*) (:crux.db/id :to-be-evicted))))
              (t/is (= after-evict-doc (api/entity (api/db *api*) :after-evict))))

            (let [doc-messages (->> (k/consumer-seqs doc-consumer (Duration/ofSeconds 5))
                                    (apply concat)
                                    (map k/doc-record->id+doc))
                  compacted-docs (compact-doc-messages doc-messages)]
              (t/is (= 4 (count doc-messages)))
              (t/is (= 3 (count compacted-docs)))

              {:compacted-docs compacted-docs
               :submitted-tx submitted-tx})))))))

(defn with-compacted-node [{:keys [compacted-docs submitted-tx]} f]
  (t/testing "compaction"
    (fix/with-opts {:crux.node/topology ['crux.kafka/topology 'crux.kv.memdb/kv-store]
                    ::k/doc-topic (str "compacted-" kf/*doc-topic*)
                    ::k/group-id (str "compacted-" (java.util.UUID/randomUUID))}

      (fn []
        (fix/with-kv-dir
          (fn []
            (fix/with-node
              (fn []
                (t/testing "new node can pick-up"
                  (db/submit-docs (:document-store *api*) compacted-docs)
                  (api/await-tx *api* submitted-tx)
                  (f))))))))))

(t/deftest test-can-process-compacted-documents
  ;; when doing a evict a tombstone document will be written to
  ;; replace the original document. The original document will be then
  ;; removed once kafka compacts it away.

  (with-compacted-node (submit-txs-to-compact)
    (fn []
      (t/testing "querying transacted data"
        (t/is (= non-evicted-doc (api/entity (api/db *api*) :not-evicted)))
        (t/is (nil? (api/entity (api/db *api*) :to-be-evicted)))
        (t/is (= after-evict-doc (api/entity (api/db *api*) :after-evict)))))))
