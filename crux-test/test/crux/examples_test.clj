(ns crux.examples-test
  (:require [clojure.test :as t]
            [crux.kafka.embedded :as ek]
            [crux.api :as crux]
            [clojure.java.io :as io]
            [docs.examples :as ex]
            [crux.tx :as tx]))

(t/deftest test-example-standalone-and-queries
  (let [node (ex/example-start-standalone)
        submitted (ex/example-submit-tx node)]
    ;; Testing example standalone node is created properly
    (t/is (not= nil node))
    ;; Testing example submit-tx works properly (and wait for it to complete)
    (t/is (not= nil submitted))
    (crux/sync node (:crux.tx/tx-time submitted) nil)
    ;; Testing example queries
    (t/is (= {:crux.db/id :dbpedia.resource/Pablo-Picasso
              :name "Pablo"
              :last-name "Picasso"}
           (ex/example-query-entity node)))
    (t/is (= #{[:dbpedia.resource/Pablo-Picasso]} (ex/example-query node)))
    (t/is (not (empty? (ex/example-query-valid-time node))))
    ;; Testing example standalone node is closed properly
    (t/is (nil? (ex/example-close-node node)))))
q
(t/deftest test-example-kafka
  (let [embedded-kafka (ex/example-start-embedded-kafka)
        node (ex/example-start-cluster)]
    ;; Testing example embedded kafka is created properly
    (t/is (not= nil embedded-kafka))
    ;; Testing example cluster node is created properly
    (t/is (not= nil node))
    ;; Testing example cluster node is closed properly
    (t/is (nil? (ex/example-close-node node)))
    ;; Testing example embedded kafka node is closed properly
    (t/is (nil? (ex/example-stop-embedded-kafka embedded-kafka)))))

(t/deftest test-example-rocks
  (let [node (ex/example-start-rocks)]
    ;; Testing example node with RocksDB is created properly
    (t/is (not= nil node))
    ;; Testing example node with RocksDB is closed properly
    (t/is (nil? (ex/example-close-node node)))))
