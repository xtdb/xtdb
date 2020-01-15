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
            [crux.fixtures.kv-only :as fkv :refer [*kv*]]
            [crux.kafka :as k]
            [crux.kafka.consumer :as kc]
            [crux.query :as q]
            [crux.rdf :as rdf]
            [crux.sparql :as sparql]
            [crux.api :as api]
            [crux.tx :as tx]
            [crux.bus :as bus])
  (:import java.time.Duration
           java.util.List
           org.apache.kafka.clients.producer.ProducerRecord
           org.apache.kafka.clients.consumer.ConsumerRecord
           org.apache.kafka.common.TopicPartition
           java.io.Closeable))

(t/use-fixtures :once fk/with-embedded-kafka-cluster)
(t/use-fixtures :each fk/with-kafka-client fkv/with-memdb fkv/with-kv-store)

(defn- consumer-record->value [^ConsumerRecord record]
  (.value record))

(t/deftest test-can-produce-and-consume-message-using-embedded-kafka
  (let [topic "test-can-produce-and-consume-message-using-embedded-kafka-topic"
        person {:crux.db/id "foo"}
        partitions [(TopicPartition. topic 0)]]

    (k/create-topic fk/*admin-client* topic 1 1 {})

    @(.send fk/*producer* (ProducerRecord. topic person))

    (.assign fk/*consumer* partitions)
    (let [records (.poll fk/*consumer* (Duration/ofMillis 10000))]
      (t/is (= 1 (count (seq records))))
      (t/is (= person (first (map consumer-record->value records)))))))

(t/deftest test-can-transact-entities
  (let [tx-topic "test-can-transact-entities-tx"
        doc-topic "test-can-transact-entities-doc"
        tx-ops (rdf/->tx-ops (rdf/ntriples "crux/example-data-artists.nt"))
        doc-store (k/->KafkaRemoteDocumentStore fk/*producer* doc-topic)
        tx-log (k/->KafkaTxLog doc-store fk/*producer* fk/*consumer* tx-topic {})
        indexer (tx/->KvIndexer (os/->KvObjectStore *kv*) *kv* tx-log doc-store (bus/->EventBus (atom #{})) nil)
        tx-offsets (kc/map->IndexedOffsets {:indexer indexer
                                            :k :crux.tx-log/consumer-state})
        doc-offsets (kc/map->IndexedOffsets {:indexer indexer
                                             :k :crux.doc-log/consumer-state})]

    (k/create-topic fk/*admin-client* tx-topic 1 1 k/tx-topic-config)
    (k/create-topic fk/*admin-client* doc-topic 1 1 k/doc-topic-config)
    (kc/subscribe-from-stored-offsets tx-offsets fk/*consumer* [tx-topic])
    (kc/subscribe-from-stored-offsets doc-offsets fk/*consumer2* [doc-topic])

    (db/submit-tx tx-log tx-ops)

    (let [docs (map consumer-record->value (.poll fk/*consumer2* (Duration/ofMillis 10000)))]
      (t/is (= 7 (count docs)))
      (t/is (= (rdf/with-prefix {:foaf "http://xmlns.com/foaf/0.1/"}
                 {:foaf/firstName "Pablo"
                  :foaf/surname "Picasso"})
               (select-keys (first docs)
                            (rdf/with-prefix {:foaf "http://xmlns.com/foaf/0.1/"}
                              [:foaf/firstName
                               :foaf/surname])))))

    (let [txes (map consumer-record->value (.poll fk/*consumer* (Duration/ofMillis 10000)))]
      (t/is (= 7 (count (first txes)))))))

(t/deftest test-can-transact-and-query-entities
  (let [tx-topic "test-can-transact-and-query-entities-tx"
        doc-topic "test-can-transact-and-query-entities-doc"
        tx-ops (rdf/->tx-ops (rdf/ntriples "crux/picasso.nt"))
        doc-store (k/->KafkaRemoteDocumentStore fk/*producer* doc-topic)
        tx-log (k/->KafkaTxLog doc-store fk/*producer* fk/*consumer* tx-topic {"bootstrap.servers" fk/*kafka-bootstrap-servers*})
        indexer (tx/->KvIndexer (os/->KvObjectStore *kv*) *kv* tx-log doc-store (bus/->EventBus (atom #{})) nil)
        object-store  (os/->CachedObjectStore (lru/new-cache os/default-doc-cache-size) (os/->KvObjectStore *kv*))
        node (reify crux.api.ICruxAPI
               (db [this]
                 (q/db *kv* object-store (cio/next-monotonic-date) (cio/next-monotonic-date))))
        tx-offsets (kc/map->IndexedOffsets {:indexer indexer
                                            :k :crux.tx-log/consumer-state})
        doc-offsets (kc/map->IndexedOffsets {:indexer indexer
                                             :k :crux.doc-log/consumer-state})
        consume-opts {:indexer indexer
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
      (let [{:crux.tx/keys [tx-id tx-time]} @(db/submit-tx tx-log tx-ops)]
        (t/is (= 3 (k/consume-and-index-documents doc-consume-opts fk/*consumer2*)))
        (t/is (= 1 (k/consume-and-index-txes consume-opts fk/*consumer*)))
        (t/is (empty? (.poll fk/*consumer* (Duration/ofMillis 1000))))

        (t/testing "restoring to stored offsets"
          (.seekToBeginning fk/*consumer* (.assignment fk/*consumer*))
          (kc/seek-to-stored-offsets tx-offsets fk/*consumer* (.assignment fk/*consumer*))
          (t/is (empty? (.poll fk/*consumer* (Duration/ofMillis 1000)))))

        (t/testing "querying transacted data"
          (t/is (= #{[:http://example.org/Picasso]}
                   (q/q (api/db node)
                        (rdf/with-prefix {:foaf "http://xmlns.com/foaf/0.1/"}
                          '{:find [e]
                            :where [[e :foaf/firstName "Pablo"]]})))))

        (t/testing "can read tx log"
          (with-open [tx-log-iterator (db/open-tx-log tx-log nil)]
            (let [log (iterator-seq tx-log-iterator)]
              (t/is (not (realized? log)))
              ;; Cannot compare the tx-ops as they contain blank nodes
              ;; with random ids.
              (t/is (= {:crux.tx/tx-time tx-time
                        :crux.tx/tx-id tx-id}
                       (dissoc (first log) :crux.tx.event/tx-events)))
              (t/is (= 1 (count log)))
              (t/is (= 3 (count (:crux.tx.event/tx-events (first log))))))))))))

(defn- consume-topics [tx-consume-opts doc-consume-opts]
  (loop []
    (when (or (not= 0 (k/consume-and-index-documents doc-consume-opts fk/*consumer2*))
              (not= 0 (k/consume-and-index-txes tx-consume-opts fk/*consumer*)))
      (recur))))

(t/deftest test-can-process-compacted-documents
  ;; when doing a evict a tombstone document will be written to
  ;; replace the original document. The original document will be then
  ;; removed once kafka compacts it away.

  (let [tx-topic "test-can-process-compacted-documents-tx"
        doc-topic "test-can-process-compacted-documents-doc"

        tx-ops (rdf/->tx-ops (rdf/ntriples "crux/picasso.nt"))

        doc-store (k/->KafkaRemoteDocumentStore fk/*producer* doc-topic)

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
            evicted-doc-hash
            (do @(db/submit-tx tx-log [[:crux.tx/put evicted-doc]
                                       [:crux.tx/put non-evicted-doc]])
                (t/is (= 2 (k/consume-and-index-documents doc-consume-opts fk/*consumer2*)))
                (t/is (= 1 (k/consume-and-index-txes tx-consume-opts fk/*consumer*)))
                (:crux.db/content-hash (q/entity-tx (api/db node) (:crux.db/id evicted-doc))))

            after-evict-doc {:crux.db/id :after-evict :personal "private"}
            {:crux.tx/keys [tx-id tx-time]}
            (do
              @(db/submit-tx tx-log [[:crux.tx/evict (:crux.db/id evicted-doc)]])
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
                          doc-store (k/->KafkaRemoteDocumentStore fk/*producer* doc-topic)
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
                                            :doc-topic doc-topic}]
                      (kc/subscribe-from-stored-offsets tx-offsets fk/*consumer* [tx-topic])
                      (kc/subscribe-from-stored-offsets doc-offsets fk/*consumer2* [doc-topic])
                      (consume-topics tx-consume-opts doc-consume-opts)

                      ;; delete the object that would have been compacted away
                      (db/delete-objects object-store [evicted-doc-hash])

                      (t/testing "querying transacted data"
                        (t/is (= non-evicted-doc (q/entity (api/db node) (:crux.db/id non-evicted-doc))))
                        (t/is (nil? (q/entity (api/db node) (:crux.db/id evicted-doc))))
                        (t/is (= after-evict-doc (q/entity (api/db node) (:crux.db/id after-evict-doc))))))))))))))))
