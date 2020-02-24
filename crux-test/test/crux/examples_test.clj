(ns crux.examples-test
  (:require [clojure.test :as t]
            [crux.kafka.embedded :as ek]
            [crux.api :as crux]
            [clojure.java.io :as io]
            [docs.examples :as ex]
            [crux.io :as cio]
            [crux.fixtures.api :as fapi]
            [crux.fixtures :as f])
  (:import (java.io Closeable)))

(def ^:dynamic *storage-dir*)

(defn with-storage-dir [f]
  (f/with-tmp-dir "storage" [storage-dir]
    (binding [*storage-dir* storage-dir]
      (f))))

(t/use-fixtures :each with-storage-dir)

(t/deftest test-example-standalone-node
  (let [node (ex/start-standalone-node *storage-dir*)
        submitted (ex/example-submit-tx node)]
    (t/testing "example standalone node is created properly"
      (t/is (not= nil node)))

    (t/testing "example submit-tx works properly (and wait for it to complete)"
      (t/is (not= nil submitted))
      (crux/await-tx node submitted nil))

    (t/testing "'getting started' example queries"
      (t/is (= {:crux.db/id :dbpedia.resource/Pablo-Picasso
                :name "Pablo"
                :last-name "Picasso"}
               (ex/example-query-entity node)))
      (t/is (= #{[:dbpedia.resource/Pablo-Picasso]} (ex/example-query node)))
      (t/is (not (empty? (ex/example-query-valid-time node)))))

    (t/testing "example standalone node is closed properly"
      (t/is (nil? (ex/close-node node))))))

(t/deftest test-example-http-node
  (let [port (cio/free-port)]
    (with-open [underlying-node ^Closeable (ex/start-standalone-http-node port *storage-dir*)
                node ^Closeable (ex/start-http-client port)]
      (t/testing "example standalone node is created properly"
        (t/is node))

      (let [submitted (ex/example-submit-tx node)]
        (crux/await-tx node submitted nil)
        (t/is (= {:crux.db/id :dbpedia.resource/Pablo-Picasso
                  :name "Pablo"
                  :last-name "Picasso"}
                 (crux/entity (crux/db node) :dbpedia.resource/Pablo-Picasso)))
        (t/is (= #{[:dbpedia.resource/Pablo-Picasso]} (ex/example-query node)))
        (t/is (not (empty? (ex/example-query-valid-time node))))))))

(t/deftest test-example-kafka-node
  (let [kafka-port (cio/free-port)
        embedded-kafka (ex/start-embedded-kafka kafka-port *storage-dir*)
        node (ex/start-cluster kafka-port)]
    (t/testing "example embedded kafka is created properly"
      (t/is (not= nil embedded-kafka)))

    (t/testing "example cluster node is created properly"
      (t/is (not= nil node)))

    (t/testing "example cluster node is closed properly"
      (t/is (nil? (ex/close-node node))))

    (t/testing "example embedded kafka node is closed properly"
      (t/is (nil? (ex/stop-embedded-kafka embedded-kafka))))))

(t/deftest test-example-rocks-node
  (let [node (ex/start-rocks-node *storage-dir*)]
    (t/testing "example node with RocksDB is created properly"
      (t/is (not= nil node)))

    (t/testing "example node with RocksDB is closed properly"
      (t/is (nil? (ex/close-node node))))))

(t/deftest test-example-lmdb-node
  (let [node (ex/start-lmdb-node *storage-dir*)]
    (t/testing "example node with LMDB is created properly"
      (t/is (not= nil node)))

    (t/testing "example node with LMDB is closed properly"
      (t/is (nil? (ex/close-node node))))))

(t/deftest test-example-basic-queries
  (with-open [node (ex/start-standalone-node *storage-dir*)]
    (crux/await-tx node (ex/query-example-setup node) nil)
    (t/is (= #{[:smith]} (ex/query-example-basic-query node)))
    (t/is (= #{["Ivan"]} (ex/query-example-with-arguments-1 node)))
    (t/is (= #{[:petr] [:ivan]} (ex/query-example-with-arguments-2 node)))
    (t/is (= #{[:petr] [:ivan]} (ex/query-example-with-arguments-3 node)))
    (t/is (= #{["Ivan"]} (ex/query-example-with-arguments-4 node)))
    (t/is (= #{[22]} (ex/query-example-with-arguments-5 node)))
    (t/is (= #{[21]} (ex/query-example-with-predicate-1 node)))

    (let [!results (atom [])]
      (ex/query-example-lazy node #(swap! !results conj %))
      (t/is (= [[:smith]] @!results)))))

(t/deftest test-example-time-queries
  (with-open [node (ex/start-standalone-node *storage-dir*)]
    (crux/await-tx node (ex/query-example-at-time-setup node) nil)
    (t/is (= #{} (ex/query-example-at-time-q1 node)))
    (t/is (= #{[:malcolm]} (ex/query-example-at-time-q2 node)))))

(t/deftest test-example-join-queries
  (with-open [node (ex/start-standalone-node *storage-dir*)]
    (crux/await-tx node (ex/query-example-join-q1-setup node) nil)

    (t/is (= #{[:ivan :ivan]
               [:petr :petr]
               [:sergei :sergei]
               [:denis-a :denis-a]
               [:denis-b :denis-b]
               [:denis-a :denis-b]
               [:denis-b :denis-a]}
             (ex/query-example-join-q1 node)))

    (crux/await-tx node (ex/query-example-join-q2-setup node) nil)
    (t/is (= #{[:petr]}
             (ex/query-example-join-q2 node)))))
