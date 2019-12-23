(ns crux.kafka-test
  (:require [clojure.test :as t]
            [crux.node :as n]
            [crux.fixtures.api :as fapi :refer [*api*]]
            [crux.fixtures.kafka :as fk]
            [crux.fixtures.indexer :as fi]
            [crux.kafka :as k]
            [crux.rdf :as rdf]
            [crux.api :as crux]
            [crux.node :as n]
            [crux.tx :as tx]
            [crux.db :as db]
            [crux.codec :as c])
  (:import [java.util UUID]
           [java.time Duration]
           java.io.Closeable
           java.util.concurrent.locks.StampedLock
           org.apache.kafka.common.TopicPartition))

(defn with-fresh-topics [f]
  (fapi/with-opts (let [test-id (UUID/randomUUID)]
                    {::k/doc-topic (str "test-doc-topic-" test-id)
                     ::k/tx-topic (str "test-tx-topic-" test-id)})
    f))

(defn with-consumer* [f]
  (let [[modules close-fn] (n/start-modules (merge fi/topology
                                                   (select-keys k/topology [::n/tx-log
                                                                            ::k/producer
                                                                            ::k/indexing-consumer]))
                                            (merge fapi/*opts*
                                                   {::k/bootstrap-servers fk/*kafka-bootstrap-servers*
                                                    ::k/doc-partitions 5}))]

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
  (let [!agent (agent nil)
        config {:indexer fi/*indexer*, :!agent !agent}
        tp (TopicPartition. "doc-topic" 1)]

    (t/testing "tx-consumer getting ahead"
      (let [!latch (k/doc-latch #{:hash1 :hash2} config)]

        ;; the tx-consumer is now ahead of the doc-consumers,
        ;; we want it to wait
        (await !agent)
        (t/is (= {:awaited-hashes #{:hash1 :hash2}, :!latch !latch} @!agent))
        (t/is (= ::not-yet (deref !latch 50 ::not-yet)))

        ;; doc-consumers done the first doc, tx-consumer still waits
        (send-off !agent k/docs-indexed {:indexed-hashes #{:hash1},
                                         :partition-states {tp 1}}
                  config)
        (await !agent)
        (t/is (= {:awaited-hashes #{:hash2}, :!latch !latch} @!agent))
        (t/is (= ::still-no (deref !latch 50 ::still-no)))
        (t/is (= {:crux.kafka.topic-partition/doc-topic-1 1}
                 (get @fi/*!index-meta* :crux.tx-log/consumer-state)))

        ;; now the doc-consumer's slightly ahead, the tx-consumer can continue
        (send-off !agent k/docs-indexed {:indexed-hashes #{:hash2 :hash3},
                                         :partition-states {tp 2}}
                  config)
        (await !agent)
        (t/is (not= ::still-waiting (deref !latch 50 ::still-waiting)))
        (t/is (nil? @!agent))
        (t/is (= {:crux.kafka.topic-partition/doc-topic-1 2}
                 (get @fi/*!index-meta* :crux.tx-log/consumer-state)))))

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

(t/deftest test-can-process-updated-documents
  ;; when doing a evict a tombstone document will be written to
  ;; replace the original document. The original document will be then
  ;; removed once kafka compacts it away.

  (with-consumer
    (t/testing "handling updated documents"
      (let [evicted-doc {:crux.db/id :to-be-evicted :personal "private"}
            non-evicted-doc {:crux.db/id :not-evicted :personal "private"}
            _ (crux/submit-tx *api* [[:crux.tx/put evicted-doc]
                                     [:crux.tx/put non-evicted-doc]])

            evict-tx (crux/submit-tx *api* [[:crux.tx/evict (:crux.db/id evicted-doc)]])

            _ (tx/await-tx (:indexer *api*) evict-tx 10000)

            ;; normally the indexer would do this, but we've swapped the normal impl out
            _ @(db/submit-doc (:tx-log *api*) (c/new-id evicted-doc) {:crux.db/id :to-be-evicted
                                                                      :crux.db/evicted? true})

            after-evict-tx (crux/submit-tx *api* [[:crux.tx/put {:crux.db/id :after-evict
                                                                 :personal "private"}]])]

        (tx/await-tx (:indexer *api*) after-evict-tx 10000)

        (t/is (= #{{:crux.db/id :after-evict, :personal "private"}
                   {:crux.db/id :not-evicted, :personal "private"}
                   {:crux.db/id :to-be-evicted, :crux.db/evicted? true}}
                 (set (vals @fi/*!docs*))))))))

(t/deftest test-can-process-compacted-documents
  ;; when documents are compacted, the earlier messages are removed,
  ;; so documents might arrive a long time after their transaction

  (let [!agent (agent nil)
        config {:indexer fi/*indexer*, :!agent !agent}
        !tx-consumer (future
                       (k/index-tx-record {:content-hashes #{:evicted-hash :not-evicted-hash}
                                           :crux.tx/tx-time #inst "2019-12-23"
                                           :crux.tx/tx-id 0}

                                          config))]

    (await !agent)
    (t/is (= ::not-yet (deref !tx-consumer 50 ::not-yet)))

    ;; normally the 'evicted' document would be here
    (k/index-docs [{:content-hash :not-evicted-hash,
                    :doc {:crux.db/id :foo}
                    :offset 0
                    :topic-partition "doc-tp"}]
                  {"doc-tp" 2}
                  config)
    (await !agent)
    (t/is (= ::not-yet (deref !tx-consumer 50 ::not-yet)))

    ;; some more documents
    (k/index-docs [{:content-hash :something-else,
                    :doc {:crux.db/id :bar}
                    :offset 1
                    :topic-partition "doc-tp"}]
                  {"doc-tp" 2}
                  config)
    (await !agent)
    (t/is (= ::not-yet (deref !tx-consumer 50 ::not-yet)))

    ;; now we got it, and the tx-indexer continues
    (k/index-docs [{:content-hash :evicted-hash,
                    :doc {:crux.db/id :private, :crux.db/evicted? true}
                    :offset 2
                    :topic-partition "doc-tp"}]
                  {"doc-tp" 3}
                  config)
    (await !agent)
    (t/is (not= ::not-yet (deref !tx-consumer 50 ::not-yet)))

    (t/is (= {:evicted-hash {:crux.db/id :private, :crux.db/evicted? true}
              :something-else {:crux.db/id :bar}
              :not-evicted-hash {:crux.db/id :foo}}
             @fi/*!docs*))

    (t/is (= [{:crux.tx.event/tx-events []
               :crux.tx/tx-id 0
               :crux.tx/tx-time #inst "2019-12-23"}]
             @fi/*!txs*))

    (t/is (= {:crux.tx-log/consumer-state {:crux.kafka.topic-partition/doc-tp {:next-offset 3, :end-offset 3}},
              :crux.tx/latest-completed-tx #:crux.tx{:tx-id 0, :tx-time #inst "2019-12-23"}}
             @fi/*!index-meta*))))
