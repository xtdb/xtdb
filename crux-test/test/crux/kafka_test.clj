(ns crux.kafka-test
  (:require [clojure.test :as t]
            [clojure.java.io :as io]
            [crux.io :as cio]
            [clojure.tools.logging :as log]
            [crux.db :as db]
            [crux.index :as idx]
            [crux.fixtures.kafka :as fk]
            [crux.fixtures.indexer :as fi]
            [crux.object-store :as os]
            [crux.lru :as lru]
            [crux.fixtures.kv-only :as fkv :refer [*kv*]]
            [crux.kafka :as k]
            [crux.query :as q]
            [crux.rdf :as rdf]
            [crux.sparql :as sparql]
            [crux.api :as api]
            [crux.tx :as tx]
            [crux.node :as n]
            [clojure.set :as set]
            [crux.codec :as c]
            [crux.fixtures.kafka :as kf])
  (:import java.time.Duration
           [java.util List UUID]
           org.apache.kafka.clients.producer.ProducerRecord
           org.apache.kafka.clients.consumer.ConsumerRecord
           org.apache.kafka.common.TopicPartition
           java.io.Closeable))

(def ^:dynamic ^{:arglists '([tx-ops])} *submit-tx*)
(def ^:dynamic ^{:arglists '([tx timeout-ms])} *await-tx*)

(defn with-topology [topology f]
  (let [[modules close-fn] (n/start-modules (merge topology
                                                   (select-keys k/topology [::n/tx-log
                                                                            ::k/producer
                                                                            ::k/indexing-consumer]))
                                            (let [test-id (UUID/randomUUID)]
                                              {::k/bootstrap-servers fk/*kafka-bootstrap-servers*
                                               ::k/doc-topic (str "test-doc-topic-" test-id)
                                               ::k/tx-topic (str "test-tx-topic-" test-id)}))]

    (binding [*submit-tx* (fn [tx-ops]
                            @(db/submit-tx (::n/tx-log modules) tx-ops))

              *await-tx* (fn [tx timeout-ms]
                           (tx/await-tx (::n/indexer modules) tx timeout-ms))]
      (try
        (f)
        (finally
          (close-fn))))))

(t/use-fixtures :once fk/with-embedded-kafka-cluster)
(t/use-fixtures :each fi/with-indexer)

(t/deftest test-coordination
  (let [!agent (agent nil)
        config {:indexer fi/*indexer*, :!agent !agent}]

    (t/testing "tx-consumer getting ahead"
      (let [!latch (k/doc-latch #{:hash1 :hash2} config)]

        ;; the tx-consumer is now ahead of the doc-consumers,
        ;; we want it to wait
        (await !agent)
        (t/is (= {:awaited-hashes #{:hash1 :hash2}, :!latch !latch} @!agent))
        (t/is (= ::not-yet (deref !latch 50 ::not-yet)))

        ;; doc-consumers done the first doc, tx-consumer still waits
        (send-off !agent k/docs-indexed {:indexed-hashes #{:hash1}, :doc-partition-offsets {1 1}} config)
        (await !agent)
        (t/is (= {:awaited-hashes #{:hash2}, :!latch !latch} @!agent))
        (t/is (= ::still-no (deref !latch 50 ::still-no)))
        (t/is (= {1 1} (get @fi/*!index-meta* ::k/doc-partition-offsets)))

        ;; now the doc-consumer's slightly ahead, the tx-consumer can continue
        (send-off !agent k/docs-indexed {:indexed-hashes #{:hash2 :hash3}, :doc-partition-offsets {1 2}} config)
        (await !agent)
        (t/is (not= ::still-waiting (deref !latch 50 ::still-waiting)))
        (t/is (nil? @!agent))
        (t/is (= {1 2} (get @fi/*!index-meta* ::k/doc-partition-offsets)))))

    (t/testing "doc-consumer getting ahead"
      (reset! fi/*!docs* {:hash3 :doc3})

      (let [!latch (k/doc-latch #{:hash3} config)]
        ;; tx-consumer can continue, doc-consumer's already got :hash3
        (t/is (not= ::blocked (deref !latch 50 ::blocked)))

        ;; won't be anything submitted, but we check regardless
        (await !agent)
        (t/is (nil? @!agent))))

    (t/testing "tx-consumer continues if the docs are not initially in but are by the time its send-off runs"
      (let [!continue (promise)]
        (with-redefs [send-off (let [send-off send-off] ; otherwise we see the redef'd version
                                 (fn [!a f & args]
                                   (apply send-off !a
                                          (fn [& inner-args]
                                            @!continue
                                            (reset! fi/*!docs* {:hash1 :doc1})
                                            (apply f inner-args))
                                          args)))]
          (let [!latch (k/doc-latch #{:hash1} config)]
            (t/is (= ::not-yet (deref !latch 50 ::not-yet)))
            (deliver !continue nil)
            (await !agent)
            (t/is (not= ::not-yet (deref !latch 50 ::not-yet)))
            (t/is (nil? @!agent))))))))

(t/deftest test-can-transact-entities
  (with-topology fi/topology
    (fn []
      (-> (*submit-tx* (rdf/->tx-ops (rdf/ntriples "crux/example-data-artists.nt")))
          (*await-tx* 10000))

      (let [docs @fi/*!docs*]
        (t/is (= 7 (count docs)))
        (t/is (= (rdf/with-prefix {:foaf "http://xmlns.com/foaf/0.1/"}
                   {:foaf/firstName "Pablo"
                    :foaf/surname "Picasso"})
                 (-> (vals docs)
                     (->> (filter (comp #{:http://example.org/Picasso} :crux.db/id))
                          first)
                     (select-keys (rdf/with-prefix {:foaf "http://xmlns.com/foaf/0.1/"}
                                    [:foaf/firstName :foaf/surname])))))))))

(def with-full-node
  (t/join-fixtures [kf/with-cluster-node-opts fkv/with-memdb apif/with-node]))

#_(t/deftest test-can-transact-and-query-entities
  (let [tx-topic "test-can-transact-and-query-entities-tx"
        doc-topic "test-can-transact-and-query-entities-doc"
        tx-ops (rdf/->tx-ops (rdf/ntriples "crux/picasso.nt"))
        tx-log (k/->KafkaTxLog fk/*producer* tx-topic doc-topic {"bootstrap.servers" fk/*kafka-bootstrap-servers*})
        indexer (tx/->KvIndexer *kv* tx-log (os/->KvObjectStore *kv*) nil)
        object-store  (os/->CachedObjectStore (lru/new-cache os/default-doc-cache-size) (os/->KvObjectStore *kv*))
        node (reify crux.api.ICruxAPI
               (db [this]
                 (q/db *kv* object-store (cio/next-monotonic-date) (cio/next-monotonic-date))))]

    (k/create-topic fk/*admin-client* tx-topic 1 1 k/tx-topic-config)
    (k/create-topic fk/*admin-client* doc-topic 1 1 k/doc-topic-config)
    (k/subscribe-topic fk/*tx-consumer* tx-topic (constantly nil))
    (k/subscribe-topic fk/*doc-consumer* doc-topic (constantly nil))

    (t/testing "transacting and indexing"
      (let [{:crux.tx/keys [tx-id tx-time]} @(db/submit-tx tx-log tx-ops)
            consume-opts {:indexer indexer :consumer fk/*consumer*
                          :pending-txs-state (atom [])
                          :tx-topic tx-topic
                          :doc-topic doc-topic}]

        (t/is (= {:txs 1 :docs 3}
                 (k/consume-and-index-entities consume-opts)))
        (t/is (empty? (.poll fk/*consumer* (Duration/ofMillis 1000))))

        (t/testing "restoring to stored offsets"
          (.seekToBeginning fk/*consumer* (.assignment fk/*consumer*))
          (k/seek-to-stored-offsets indexer fk/*consumer* (.assignment fk/*consumer*))
          (t/is (empty? (.poll fk/*consumer* (Duration/ofMillis 1000)))))

        (t/testing "querying transacted data"
          (t/is (= #{[:http://example.org/Picasso]}
                   (q/q (api/db node)
                        (rdf/with-prefix {:foaf "http://xmlns.com/foaf/0.1/"}
                          '{:find [e]
                            :where [[e :foaf/firstName "Pablo"]]})))))

        (t/testing "can read tx log"
          (with-open [consumer (db/new-tx-log-context tx-log)]
            (let [log (db/tx-log tx-log consumer nil)]
              (t/is (not (realized? log)))
              ;; Cannot compare the tx-ops as they contain blank nodes
              ;; with random ids.
              (t/is (= {:crux.tx/tx-time tx-time
                        :crux.tx/tx-id tx-id}
                       (dissoc (first log) :crux.tx.event/tx-events)))
              (t/is (= 1 (count log)))
              (t/is (= 3 (count (:crux.tx.event/tx-events (first log))))))))))))

#_(t/deftest test-can-process-compacted-documents
  ;; when doing a evict a tombstone document will be written to
  ;; replace the original document. The original document will be then
  ;; removed once kafka compacts it away.

  (let [tx-topic "test-can-process-compacted-documents-tx"
        doc-topic "test-can-process-compacted-documents-doc"

        tx-ops (rdf/->tx-ops (rdf/ntriples "crux/picasso.nt"))

        tx-log (k/->KafkaTxLog fk/*producer* tx-topic doc-topic {"bootstrap.servers" fk/*kafka-bootstrap-servers*})

        object-store  (os/->CachedObjectStore (lru/new-cache os/default-doc-cache-size) (os/->KvObjectStore *kv*))
        indexer (tx/->KvIndexer *kv* tx-log (os/->KvObjectStore *kv*) nil)

        node (reify crux.api.ICruxAPI
               (db [this]
                 (q/db *kv* object-store (cio/next-monotonic-date) (cio/next-monotonic-date))))]

    (k/create-topic fk/*admin-client* tx-topic 1 1 k/tx-topic-config)
    (k/create-topic fk/*admin-client* doc-topic 1 1 k/doc-topic-config)
    (k/subscribe-from-stored-offsets indexer fk/*consumer* [tx-topic doc-topic])

    (t/testing "transacting and indexing"
      (let [consume-opts {:indexer indexer
                          :consumer fk/*consumer*
                          :pending-txs-state (atom [])
                          :tx-topic tx-topic
                          :doc-topic doc-topic}

            evicted-doc {:crux.db/id :to-be-eviceted :personal "private"}
            non-evicted-doc {:crux.db/id :not-evicted :personal "private"}
            evicted-doc-hash
            (do @(db/submit-tx
                  tx-log
                  [[:crux.tx/put evicted-doc]
                   [:crux.tx/put non-evicted-doc]])

                (k/consume-and-index-entities consume-opts)
                (while (not= {:txs 0 :docs 0} (k/consume-and-index-entities consume-opts)))
                (:crux.db/content-hash (q/entity-tx (api/db node) (:crux.db/id evicted-doc))))

            after-evict-doc {:crux.db/id :after-evict :personal "private"}
            {:crux.tx/keys [tx-id tx-time]}
            (do
              @(db/submit-tx tx-log [[:crux.tx/evict (:crux.db/id evicted-doc)]])
              @(db/submit-tx
                tx-log
                [[:crux.tx/put after-evict-doc]]))]

        (while (not= {:txs 0 :docs 0} (k/consume-and-index-entities consume-opts)))

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
                          indexer (tx/->KvIndexer *kv* tx-log object-store nil)
                          consume-opts {:indexer indexer
                                        :consumer fk/*consumer*
                                        :pending-txs-state (atom [])
                                        :tx-topic tx-topic
                                        :doc-topic doc-topic}]
                      (k/subscribe-from-stored-offsets indexer fk/*consumer* [tx-topic doc-topic])
                      (k/consume-and-index-entities consume-opts)
                      (t/is (= {:txs 0, :docs 1} (k/consume-and-index-entities consume-opts)))
                      ;; delete the object that would have been compacted away
                      (db/delete-objects object-store [evicted-doc-hash])

                      (while (not= {:txs 0 :docs 0} (k/consume-and-index-entities consume-opts)))
                      (t/is (empty? (.poll fk/*consumer* (Duration/ofMillis 1000))))

                      (t/testing "querying transacted data"
                        (t/is (= non-evicted-doc (q/entity (api/db node) (:crux.db/id non-evicted-doc))))
                        (t/is (nil? (q/entity (api/db node) (:crux.db/id evicted-doc))))
                        (t/is (= after-evict-doc (q/entity (api/db node) (:crux.db/id after-evict-doc))))))))))))))))
