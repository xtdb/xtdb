(ns crux.kafka-test
  (:require [clojure.test :as t]
            [crux.api :as api]
            [crux.codec :as c]
            [crux.db :as db]
            [crux.fixtures :as fix :refer [*api* *opts*]]
            [crux.fixtures.kafka :as fk]
            [crux.kafka :as k]
            [crux.rdf :as rdf]
            [crux.tx :as tx]
            [crux.kv :as kv]
            [clojure.java.io :as io])
  (:import java.time.Duration
           [org.apache.kafka.clients.consumer ConsumerRecord KafkaConsumer]
           org.apache.kafka.clients.producer.ProducerRecord))

(t/use-fixtures :once fk/with-embedded-kafka-cluster)

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

          (with-open [doc-consumer (doto (fk/open-consumer)
                                     (#'k/subscribe-consumer #{fk/*doc-topic*} {}))]
            (let [doc-messages (->> (#'k/consumer-seqs doc-consumer (Duration/ofSeconds 5))
                                    (apply concat)
                                    (map k/doc-record->id+doc))
                  compacted-docs (compact-doc-messages doc-messages)]
              (t/is (= 4 (count doc-messages)))
              (t/is (= 3 (count compacted-docs)))

              {:compacted-docs compacted-docs
               :submitted-tx submitted-tx})))))))

(defn with-compacted-node [{:keys [compacted-docs submitted-tx]} f]
  (t/testing "compaction"
    (let [with-fixtures (t/join-fixtures [(fix/with-opts {::fk/doc-topic-opts {:topic-name (str "compacted-" fk/*doc-topic*)}})
                                          fix/with-node])]
      (with-fixtures
        (fn []
          (t/testing "new node can pick-up"
            (db/submit-docs (:document-store *api*) compacted-docs)
            (api/await-tx *api* submitted-tx)
            (f)))))))

(t/deftest test-can-process-compacted-documents
  ;; when doing a evict a tombstone document will be written to
  ;; replace the original document. The original document will be then
  ;; removed once kafka compacts it away.

  (let [with-fixtures (t/join-fixtures [fk/with-cluster-tx-log-opts fk/with-cluster-doc-store-opts])]
    (with-fixtures
      (fn []
        (with-compacted-node (submit-txs-to-compact)
          (fn []
            (t/testing "querying transacted data"
              (t/is (= non-evicted-doc (api/entity (api/db *api*) :not-evicted)))
              (t/is (nil? (api/entity (api/db *api*) :to-be-evicted)))
              (t/is (= after-evict-doc (api/entity (api/db *api*) :after-evict))))))))))

(t/deftest test-consumer-seeks-after-restart
  (fix/with-tmp-dir "crux-tmp" [tmp]
    (let [with-fixtures (t/join-fixtures [fk/with-cluster-tx-log-opts
                                          fk/with-cluster-doc-store-opts
                                          (fix/with-opts {:crux/indexer {:kv-store {:crux/module `crux.rocksdb/->kv-store, :db-dir (io/file tmp "indexes")}}
                                                          :crux/document-store {:local-document-store {:kv-store {:crux/module `crux.rocksdb/->kv-store, :db-dir (io/file tmp "docs")}}}})])]
      (with-fixtures
        (fn []
          (fix/with-node
            (fn []
              (fix/submit+await-tx [[:crux.tx/put {:crux.db/id :foo}]])))
          (fix/with-node
            (fn []
              (doto (api/submit-tx *api* [[:crux.tx/put {:crux.db/id :bar}]])
                (as-> tx (api/await-tx *api* tx (Duration/ofSeconds 5))))
              (t/is (api/entity (api/db *api*) :foo))
              (t/is (api/entity (api/db *api*) :bar)))))))))
