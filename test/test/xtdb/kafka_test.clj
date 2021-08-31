(ns xtdb.kafka-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.db :as db]
            [xtdb.fixtures :as fix :refer [*api*]]
            [xtdb.fixtures.kafka :as fk]
            [xtdb.kafka :as k])
  (:import java.time.Duration))

(t/use-fixtures :once fk/with-embedded-kafka-cluster)

(defn compact-doc-messages [docs]
  (->> docs
       (reverse)
       (reduce (fn [docs doc]
                 (cond-> docs
                   (not (contains? (set (map first docs)) (first doc))) (conj doc)))
               [])
       (reverse)))

(def evicted-doc {:xt/id :to-be-evicted :personal "private"})
(def non-evicted-doc {:xt/id :not-evicted :personal "private"})
(def after-evict-doc {:xt/id :after-evict :personal "private"})

(defn submit-txs-to-compact []
  (fix/with-node
    (fn []
      (t/testing "transacting and indexing"
        (let [submitted-tx (xt/submit-tx *api* [[::xt/put evicted-doc]
                                                 [::xt/put non-evicted-doc]])
              _ (xt/await-tx *api* submitted-tx)
              _ (xt/submit-tx *api* [[::xt/evict (:xt/id evicted-doc)]])
              submitted-tx (xt/submit-tx *api* [[::xt/put after-evict-doc]])
              _ (xt/await-tx *api* submitted-tx nil)]

          (t/testing "querying transacted data"
            (t/is (= non-evicted-doc (xt/entity (xt/db *api*) :not-evicted)))
            (t/is (nil? (xt/entity (xt/db *api*) (:xt/id :to-be-evicted))))
            (t/is (= after-evict-doc (xt/entity (xt/db *api*) :after-evict))))

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
            (xt/await-tx *api* submitted-tx)
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
              (t/is (= non-evicted-doc (xt/entity (xt/db *api*) :not-evicted)))
              (t/is (nil? (xt/entity (xt/db *api*) :to-be-evicted)))
              (t/is (= after-evict-doc (xt/entity (xt/db *api*) :after-evict))))))))))

(t/deftest test-consumer-seeks-after-restart
  (fix/with-tmp-dir "xtdb-tmp" [tmp]
    (let [with-fixtures (t/join-fixtures [fk/with-cluster-tx-log-opts
                                          fk/with-cluster-doc-store-opts
                                          (fix/with-opts {:xtdb/index-store {:kv-store {:xtdb/module `xtdb.rocksdb/->kv-store, :db-dir (io/file tmp "indexes")}}
                                                          :xtdb/document-store {:local-document-store {:kv-store {:xtdb/module `xtdb.rocksdb/->kv-store, :db-dir (io/file tmp "docs")}}}})])]
      (with-fixtures
        (fn []
          (fix/with-node
            (fn []
              (fix/submit+await-tx [[::xt/put {:xt/id :foo}]])))
          (fix/with-node
            (fn []
              (doto (xt/submit-tx *api* [[::xt/put {:xt/id :bar}]])
                (as-> tx (xt/await-tx *api* tx (Duration/ofSeconds 5))))
              (t/is (xt/entity (xt/db *api*) :foo))
              (t/is (xt/entity (xt/db *api*) :bar)))))))))

(t/deftest submit-oversized-doc
  (let [with-fixtures (t/join-fixtures [(partial
                                         fk/with-kafka-config
                                         {:properties-map {"max.partition.fetch.bytes" "1024"
                                                           "max.request.size" "1024"}})
                                        fk/with-cluster-tx-log-opts
                                        fk/with-cluster-doc-store-opts
                                        fix/with-node])]
    (with-fixtures
      (fn []
        (t/testing "submitting an oversized document returns proper exception"
          (t/is
           (thrown-with-msg?
            java.util.concurrent.ExecutionException
            #"org.apache.kafka.common.errors.RecordTooLargeException"
            (xt/submit-tx
             *api*
             [[::xt/put
               (into {:xt/id :test}
                     (for [n (range 1000)]
                       [(keyword (str "key-" n))
                        (str "value-" n)]))]]))))))))
