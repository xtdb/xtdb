(ns crux.follower-test
  (:require [clojure.test :as t :refer [deftest is]]
            [crux.db :as db]
            [crux.doc :as doc]
            [crux.embedded-kafka :as ek]
            [crux.fixtures :as f]
            [crux.kafka :as k]
            [crux.tx :as tx]))


(def ^:dynamic *following-topic*)

(defn with-lubm-data [f]
  (let [tx-topic "follower-tx"
        doc-topic "follower-doc"
        other-topic "other-topic"
        tx-ops []
        tx-log (k/->KafkaTxLog ek/*producer* tx-topic doc-topic)
        object-store (doc/new-cached-object-store f/*kv*)
        indexer (tx/->DocIndexer f/*kv* tx-log object-store)]

    (k/create-topic ek/*admin-client* tx-topic 1 1 k/tx-topic-config)
    (k/create-topic ek/*admin-client* doc-topic 1 1 k/doc-topic-config)
    (k/create-topic ek/*admin-client* other-topic 1 1 k/doc-topic-config)
    (k/subscribe-from-stored-offsets indexer ek/*consumer* [tx-topic doc-topic
                                                            other-topic])
    (binding [*following-topic* other-topic]
      (f))))

(t/use-fixtures :once
  ek/with-embedded-kafka-cluster
  ek/with-kafka-client
  f/with-kv-store
  with-lubm-data)

(defn write-to-following-topic
  [])

(deftest test-follower

  (is (= 1 1)))
