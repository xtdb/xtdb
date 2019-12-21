(ns crux.kafka-test
  (:require [clojure.test :as t]
            [crux.node :as n]
            [crux.fixtures.api :as apif :refer [*api*]]
            [crux.fixtures.kafka :as fk]
            [crux.fixtures.indexer :as fi]
            [crux.kafka :as k]
            [crux.rdf :as rdf]
            [crux.api :as crux]
            [crux.node :as n]
            [crux.tx :as tx]
            [crux.db :as db])
  (:import [java.util UUID]
           [java.time Duration]
           java.io.Closeable
           java.util.concurrent.locks.StampedLock))

(def ^:dynamic *topics*)

(defn with-fresh-topics [f]
  (apif/with-opts (let [test-id (UUID/randomUUID)]
                    {::k/doc-topic (str "test-doc-topic-" test-id)
                     ::k/tx-topic (str "test-tx-topic-" test-id)})
    f))

(defn with-consumer* [f]
  (let [[modules close-fn] (n/start-modules (merge fi/topology
                                                   (select-keys k/topology [::n/tx-log
                                                                            ::k/producer
                                                                            ::k/indexing-consumer]))
                                            {::k/bootstrap-servers fk/*kafka-bootstrap-servers*})]

    (with-open [api ^crux.node.CruxNode (n/map->CruxNode {:close-fn close-fn
                                                          :tx-log (::n/tx-log modules)
                                                          :indexer (::n/indexer modules)
                                                          :closed? (atom false)
                                                          :lock (StampedLock.)})]
      (binding [*api* api]
        (try
          (f)
          (finally
            (close-fn)))))))

(defmacro with-consumer [& body] `(with-consumer* (fn [] ~@body)))

(t/use-fixtures :once fk/with-embedded-kafka-cluster)
(t/use-fixtures :each fi/with-indexer with-fresh-topics)

(t/deftest test-coordination
  (with-consumer
    (let [!agent (agent nil)
          config {:indexer (:indexer *api*), :!agent !agent}]

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
              (t/is (nil? @!agent)))))))))

(t/deftest test-can-transact-entities
  (with-consumer
    (let [tx (crux/submit-tx *api* (rdf/->tx-ops (rdf/ntriples "crux/example-data-artists.nt")))]
      (tx/await-tx (:indexer *api*) tx 10000)

      (let [docs @fi/*!docs*]
        (t/is (= 7 (count docs)))
        (t/is (= (rdf/with-prefix {:foaf "http://xmlns.com/foaf/0.1/"}
                   {:foaf/firstName "Pablo"
                    :foaf/surname "Picasso"})
                 (-> (vals docs)
                     (->> (filter (comp #{:http://example.org/Picasso} :crux.db/id))
                          first)
                     (select-keys (rdf/with-prefix {:foaf "http://xmlns.com/foaf/0.1/"}
                                    [:foaf/firstName :foaf/surname]))))))

      (t/testing "can read tx log"
        (with-open [ctx (crux/new-tx-log-context *api*)]
          (let [log (db/tx-log (:tx-log *api*) ctx nil)]
            (t/is (not (realized? log)))
            ;; Cannot compare the tx-ops as they contain blank nodes
            ;; with random ids.
            (t/is (= tx (dissoc (first log) :crux.tx.event/tx-events)))
            (t/is (= 1 (count log)))
            (t/is (= 7 (count (:crux.tx.event/tx-events (first log)))))))))))

(t/deftest test-can-resubscribe
  (with-consumer
    (let [tx (crux/submit-tx *api* [[:crux.tx/put {:crux.db/id :foo}]])]
      (tx/await-tx (:indexer *api*) tx 10000)

      (t/is (= #{{:crux.db/id :foo}} (set (vals @fi/*!docs*))))))

  (reset! fi/*!docs* {})

  (with-consumer
    (let [tx (crux/submit-tx *api* [[:crux.tx/put {:crux.db/id :bar}]])]
      (tx/await-tx (:indexer *api*) tx 10000)

      ;; no :foo here.
      (t/is (= #{{:crux.db/id :bar}} (set (vals @fi/*!docs*)))))))

#_(t/deftest test-can-process-compacted-documents
  ;; when doing a evict a tombstone document will be written to
  ;; replace the original document. The original document will be then
  ;; removed once kafka compacts it away.

  (let [tx-ops (rdf/->tx-ops (rdf/ntriples "crux/picasso.nt"))]

    (t/testing "transacting and indexing"
      (let [consume-opts {:indexer indexer
                          :consumer fk/*consumer*
                          :pending-txs-state (atom [])
                          :tx-topic tx-topic
                          :doc-topic doc-topic}

            evicted-doc {:crux.db/id :to-be-evicted :personal "private"}
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
