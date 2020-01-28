(ns crux.kafka-test
  (:require [clojure.test :as t]
            [clojure.java.io :as io]
            [crux.io :as cio]
            [clojure.tools.logging :as log]
            [crux.db :as db]
            [crux.index :as idx]
            [crux.fixtures.kafka :as fk]
            [crux.object-store :as os]
            [crux.lru :as lru]
            [crux.fixtures.kv :as kvf]
            [crux.fixtures.kv-only :as fkv :refer [*kv*]]
            [crux.kafka :as k]
            [crux.kafka.consumer :as kc]
            [crux.query :as q]
            [crux.rdf :as rdf]
            [crux.sparql :as sparql]
            [crux.api :as api]
            [crux.tx :as tx]
            [crux.bus :as bus]
            [crux.fixtures.api :refer [*api* *opts*] :as fapi])
  (:import java.time.Duration
           java.util.List
           org.apache.kafka.clients.producer.ProducerRecord
           org.apache.kafka.clients.consumer.ConsumerRecord
           [org.apache.kafka.clients.consumer ConsumerRebalanceListener ConsumerRecord KafkaConsumer]
           org.apache.kafka.common.TopicPartition
           java.io.Closeable))

(t/use-fixtures :once fk/with-embedded-kafka-cluster)
(t/use-fixtures :each fk/with-cluster-node-opts kvf/with-kv-dir)

(defn- consumer-record->value [^ConsumerRecord record]
  (.value record))

(defn- txes-on-topic []
  (with-open [tx-consumer ^KafkaConsumer (fk/with-consumer)]
    (let [tx-offsets (kc/map->IndexedOffsets {:indexer (:indexer *api*) :k ::txes})]
      (kc/subscribe-from-stored-offsets tx-offsets tx-consumer [(:crux.kafka/tx-topic *opts*)]))
    (doall (map consumer-record->value (.poll tx-consumer (Duration/ofMillis 10000))))))

(defn- docs-on-topic []
  (with-open [doc-consumer ^KafkaConsumer (fk/with-consumer)]
    (let [doc-offsets (kc/map->IndexedOffsets {:indexer (:indexer *api*) :k ::docs})]
      (kc/subscribe-from-stored-offsets doc-offsets doc-consumer [(:crux.kafka/doc-topic *opts*)]))
    (map consumer-record->value (.poll doc-consumer (Duration/ofMillis 10000)))))

(t/deftest test-can-transact-entities
  (fapi/with-node
    (fn []
      (let [tx-ops (rdf/->tx-ops (rdf/ntriples "crux/example-data-artists.nt"))
            submitted-tx (.submitTx *api* tx-ops)
            _ (.awaitTx *api* submitted-tx nil)]

        (t/testing "tx-log contains relevant txes"
          (let [txes (txes-on-topic)]
            (t/is (= 7 (count (first txes))))))

        (t/testing "doc-log contains relevant docs"
          (let [docs (docs-on-topic)]
            (t/is (= (rdf/with-prefix {:foaf "http://xmlns.com/foaf/0.1/"}
                       {:foaf/firstName "Pablo"
                        :foaf/surname "Picasso"})
                     (select-keys (first docs)
                                  (rdf/with-prefix {:foaf "http://xmlns.com/foaf/0.1/"}
                                    [:foaf/firstName
                                     :foaf/surname]))))))))))

(t/deftest test-can-transact-and-query-entities
  (fapi/with-node
    (fn []
      (let [tx-ops (rdf/->tx-ops (rdf/ntriples "crux/picasso.nt"))
            {:crux.tx/keys [tx-time tx-id] :as submitted-tx} (.submitTx *api* tx-ops)
            _ (.awaitTx *api* submitted-tx nil)]

        (t/testing "transacting and indexing"
          (t/is (= 3 (count (docs-on-topic))))
          (t/is (= 1 (count (txes-on-topic)))))

        (t/testing "querying transacted data"
          (t/is (= #{[:http://example.org/Picasso]}
                   (q/q (api/db *api*)
                        (rdf/with-prefix {:foaf "http://xmlns.com/foaf/0.1/"}
                          '{:find [e]
                            :where [[e :foaf/firstName "Pablo"]]})))))

        (t/testing "can read tx log"
          (t/testing "tx-log"
            (with-open [tx-log-iterator (.openTxLog *api* nil false)]
              (let [result (iterator-seq tx-log-iterator)]
                (t/is (not (realized? result)))
                (t/is (= {:crux.tx/tx-time tx-time
                          :crux.tx/tx-id tx-id}
                         (dissoc (first result) :crux.tx.event/tx-events)))
                (t/is (= 1 (count result)))
                (t/is (= 3 (count (:crux.tx.event/tx-events (first result)))))
                (t/is (realized? result))))))

        (t/testing "new node can pick-up"
          (kvf/with-kv-dir
            (fn []
              (fapi/with-node
                (fn []
                  (.awaitTx *api* submitted-tx (Duration/ofSeconds 10))
                  (t/is (= #{[:http://example.org/Picasso]}
                           (q/q (api/db *api*)
                                (rdf/with-prefix {:foaf "http://xmlns.com/foaf/0.1/"}
                                  '{:find [e]
                                    :where [[e :foaf/firstName "Pablo"]]}))))))))

          (t/testing "no new txes or docs"
            (t/is (= 3 (count (docs-on-topic))))
            (t/is (= 1 (count (txes-on-topic))))))))))

#_(defn- consume-topics [tx-consume-opts doc-consume-opts]
  (loop []
    (when (or (not= 0 (k/consume-and-index-documents doc-consume-opts fk/*consumer2*))
              (not= 0 (k/consume-and-index-txes tx-consume-opts fk/*consumer*)))
      (recur))))

#_(t/deftest test-can-process-compacted-documents
  ;; when doing a evict a tombstone document will be written to
  ;; replace the original document. The original document will be then
  ;; removed once kafka compacts it away.

  (let [tx-topic "test-can-process-compacted-documents-tx"
        doc-topic "test-can-process-compacted-documents-doc"

        tx-ops (rdf/->tx-ops (rdf/ntriples "crux/picasso.nt"))

        doc-store (k/->KafkaDocumentStore fk/*producer* doc-topic)

        tx-log (k/->KafkaTxLog doc-store fk/*producer* fk/*consumer* tx-topic {"bootstrap.servers" fk/*kafka-bootstrap-servers*})

        object-store  (os/->CachedObjectStore (lru/new-cache os/default-doc-cache-size) (os/->KvObjectStore *kv*))
        indexer (tx/->KvIndexer (os/->KvObjectStore *kv*) *kv* tx-log doc-store (bus/->EventBus (atom #{})) nil)

        node (reify crux.api.ICruxAPI
               (db [this]
                 (q/db *kv* object-store (cio/next-monotonic-date) (cio/next-monotonic-date))))
        tx-offsets (kc/map->IndexedOffsets {:indexer indexer
                                            :k :crux.tx-log/consumer-state})
        doc-offsets (kc/map->IndexedOffsets {:indexer indexer
                                             :k :crux.doc-log/consumer-state})
        tx-consume-opts {:indexer indexer
                         :offsets tx-offsets
                         :pending-txs-state (atom [])
                         :tx-topic tx-topic}
        doc-consume-opts {:indexer indexer
                          :offsets doc-offsets
                          :doc-topic doc-topic}]

    (k/create-topic fk/*admin-client* tx-topic 1 1 k/tx-topic-config)
    (k/create-topic fk/*admin-client* doc-topic 1 1 k/doc-topic-config)
    (kc/subscribe-from-stored-offsets tx-offsets fk/*consumer* [tx-topic])
    (kc/subscribe-from-stored-offsets doc-offsets fk/*consumer2* [doc-topic])

    (t/testing "transacting and indexing"
      (let [evicted-doc {:crux.db/id :to-be-evicted :personal "private"}
            non-evicted-doc {:crux.db/id :not-evicted :personal "private"}
            tx-ops [[:crux.tx/put evicted-doc]
                    [:crux.tx/put non-evicted-doc]]
            evicted-doc-hash
            (do (db/submit-docs doc-store (tx/tx-ops->id-and-docs tx-ops))
                @(db/submit-tx tx-log tx-ops)
                (t/is (= 2 (k/consume-and-index-documents doc-consume-opts fk/*consumer2*)))
                (t/is (= 1 (k/consume-and-index-txes tx-consume-opts fk/*consumer*)))
                (:crux.db/content-hash (q/entity-tx (api/db node) (:crux.db/id evicted-doc))))

            after-evict-doc {:crux.db/id :after-evict :personal "private"}
            {:crux.tx/keys [tx-id tx-time]}
            (do
              @(db/submit-tx tx-log [[:crux.tx/evict (:crux.db/id evicted-doc)]])
              (db/submit-docs doc-store (tx/tx-ops->id-and-docs [[:crux.tx/put after-evict-doc]]))
              @(db/submit-tx tx-log [[:crux.tx/put after-evict-doc]]))]

        (consume-topics tx-consume-opts doc-consume-opts)

        (t/testing "querying transacted data"
          (t/is (= non-evicted-doc (q/entity (api/db node) (:crux.db/id non-evicted-doc))))
          (t/is (nil? (q/entity (api/db node) (:crux.db/id evicted-doc))))
          (t/is (= after-evict-doc (q/entity (api/db node) (:crux.db/id after-evict-doc)))))

        (t/testing "re-indexing the same transactions after doc compaction"
          (binding [fk/*consumer-options* {"max.poll.records" "1"}]
            (fk/with-kafka-client
              (fn []
                (fkv/with-kv-store
                  (fn []
                    (let [object-store (os/->KvObjectStore *kv*)
                          doc-store (k/->KafkaDocumentStore fk/*producer* doc-topic)
                          indexer (tx/->KvIndexer object-store *kv* tx-log doc-store (bus/->EventBus (atom #{})) nil)
                          tx-offsets (kc/map->IndexedOffsets {:indexer indexer
                                                              :k :crux.tx-log/consumer-state})
                          doc-offsets (kc/map->IndexedOffsets {:indexer indexer
                                                               :k :crux.doc-log/consumer-state})
                          tx-consume-opts {:indexer indexer
                                           :offsets tx-offsets
                                           :pending-txs-state (atom [])
                                           :tx-topic tx-topic}
                          doc-consume-opts {:indexer indexer
                                            :offsets doc-offsets
                                            :doc-topic doc-topic}
                          node (reify crux.api.ICruxAPI
                                 (db [this]
                                   (q/db *kv* object-store (cio/next-monotonic-date) (cio/next-monotonic-date))))]

                      (kc/subscribe-from-stored-offsets tx-offsets fk/*consumer* [tx-topic])
                      (kc/subscribe-from-stored-offsets doc-offsets fk/*consumer2* [doc-topic])
                      (consume-topics tx-consume-opts doc-consume-opts)

                      ;; ideally want to test with an evicted document from the log (a gap)
                      ;; run a virtual compaction?
                      ;; take documents in reverse order

                      (t/testing "querying transacted data"
                        (t/is (= non-evicted-doc (q/entity (api/db node) (:crux.db/id non-evicted-doc))))
                        (t/is (nil? (q/entity (api/db node) (:crux.db/id evicted-doc))))
                        (t/is (= after-evict-doc (q/entity (api/db node) (:crux.db/id after-evict-doc))))))))))))))))
