(ns crux.examples-test
  (:require [clojure.test :as t]
            [crux.kafka.embedded :as ek]
            [crux.api :as crux]
            [clojure.java.io :as io]
            [docs.examples :as ex]
            [crux.tx :as tx]))

(defn start-node-for-queries [event-log-name]
  (let [^crux.api.ICruxAPI node (crux/start-node {:crux.node/topology :crux.standalone/topology
                                                  :crux.node/kv-store "crux.kv.memdb/kv"
                                                  :crux.standalone/db-dir "data/db-dir-1"
                                                  :crux.standalone/event-log-kv "crux.kv.memdb/kv"
                                                  :crux.standalone/event-log-dir "data/event"})]
    node))


(t/deftest test-example-standalone-node
  (let [node (ex/example-start-standalone)
        submitted (ex/example-submit-tx node)]
    ;; Testing example standalone node is created properly
    (t/is (not= nil node))
    ;; Testing example submit-tx works properly (and wait for it to complete)
    (t/is (not= nil submitted))
    (crux/sync node (:crux.tx/tx-time submitted) nil)

    ;; Testing 'getting started' example queries
    (t/is (= {:crux.db/id :dbpedia.resource/Pablo-Picasso
              :name "Pablo"
              :last-name "Picasso"}
           (ex/example-query-entity node)))
    (t/is (= #{[:dbpedia.resource/Pablo-Picasso]} (ex/example-query node)))
    (t/is (not (empty? (ex/example-query-valid-time node))))

    ;; Testing example standalone node is closed properly
    (t/is (nil? (ex/example-close-node node)))))

(t/deftest test-example-kafka-node
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

(t/deftest test-example-rocks-node
  (let [node (ex/example-start-rocks)]
    ;; Testing example node with RocksDB is created properly
    (t/is (not= nil node))
    ;; Testing example node with RocksDB is closed properly
    (t/is (nil? (ex/example-close-node node)))))

(t/deftest test-example-basic-queries
  (with-open [node (start-node-for-queries "data/event-log-basic")]
    (crux/sync node (:crux.tx/tx-time (ex/query-example-setup node)) nil)
    (t/is (= #{[:smith]} (ex/query-example-basic-query node)))
    (t/is (= #{["Ivan"]} (ex/query-example-with-arguments-1 node)))
    (t/is (= #{[:petr] [:ivan]} (ex/query-example-with-arguments-2 node)))
    (t/is (= #{[:petr] [:ivan]} (ex/query-example-with-arguments-3 node)))
    (t/is (= #{["Ivan"]} (ex/query-example-with-arguments-4 node)))
    (t/is (= #{[22]} (ex/query-example-with-arguments-5 node)))))

(t/deftest test-example-time-queries
  (with-open [node (start-node-for-queries "data/event-log-queries")]
    (crux/sync node (:crux.tx/tx-time (ex/query-example-at-time-setup node)) nil)
    (t/is (= #{} (ex/query-example-at-time-q1 node)))
    (t/is (= #{[:malcolm]} (ex/query-example-at-time-q2 node)))))

(t/deftest test-example-join-queries
  (with-open [node (start-node-for-queries "data/event-log-join")]
    (crux/sync node (:crux.tx/tx-time (ex/query-example-join-q1-setup node)) nil)
    (t/is (= #{[:ivan :ivan]
               [:petr :petr]
               [:sergei :sergei]
               [:denis-a :denis-a]
               [:denis-b :denis-b]
               [:denis-a :denis-b]
               [:denis-b :denis-a]}
             (ex/query-example-join-q1 node)))
    (crux/sync node (:crux.tx/tx-time (ex/query-example-join-q2-setup node)) nil)
    (t/is (= #{[:petr]}
             (ex/query-example-join-q2 node)))))
