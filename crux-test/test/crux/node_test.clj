(ns crux.node-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [crux.api :as api]
            [crux.api.java :as java]
            [crux.bus :as bus]
            [crux.fixtures :as f]
            [crux.io :as cio]
            [crux.jdbc :as j]
            [crux.kv :as kv]
            [crux.node :as n]
            [crux.rocksdb :as rocks]
            [crux.tx :as tx]
            [crux.tx.event :as txe]
            [crux.db :as db])
  (:import [crux.api Crux ICruxAPI CruxDocument]
           [crux.api.tx Transaction]
           java.time.Duration
           [java.util Date HashMap Map]
           java.util.concurrent.TimeoutException))

(t/deftest test-calling-shutdown-node-fails-gracefully
  (f/with-tmp-dir "data" [data-dir]
    (try
      (let [n ^ICruxAPI (java/->JCruxNode (api/start-node {}))]
        (t/is (.status n))
        (.close n)
        (.status n)
        (t/is false))
      (catch IllegalStateException e
        (t/is (= "Crux node is closed" (.getMessage e)))))))

(t/deftest test-start-node-should-throw-missing-argument-exception
  (t/is (thrown-with-msg? IllegalArgumentException
                          #"Error parsing opts"
                          (api/start-node {:crux/document-store {:crux/module `j/->document-store}})))
  (t/is (thrown-with-msg? IllegalArgumentException
                          #"Missing module .+ :dialect"
                          (api/start-node {:crux/document-store {:crux/module `j/->document-store
                                                                 :connection-pool {:db-spec {}}}}))))

(t/deftest test-can-start-JDBC-node
  (f/with-tmp-dir "data" [data-dir]
    (with-open [n (api/start-node {:crux/tx-log {:crux/module `j/->tx-log, :connection-pool ::j/connection-pool}
                                   :crux/document-store {:crux/module `j/->document-store, :connection-pool ::j/connection-pool}
                                   ::j/connection-pool {:dialect `crux.jdbc.h2/->dialect,
                                                        :db-spec {:dbname (str (io/file data-dir "cruxtest"))}}})]
      (t/is n))))

(t/deftest test-can-set-indexes-kv-store
  (f/with-tmp-dir "data" [data-dir]
    (with-open [n (api/start-node {:crux/tx-log {:kv-store {:crux/module `rocks/->kv-store, :db-dir (io/file data-dir "tx-log")}}
                                   :crux/document-store {:kv-store {:crux/module `rocks/->kv-store, :db-dir (io/file data-dir "doc-store")}}
                                   :crux/index-store {:kv-store {:crux/module `rocks/->kv-store, :db-dir (io/file data-dir "indexes")}}})]
      (t/is n))))

(t/deftest start-node-from-java
  (f/with-tmp-dir "data" [data-dir]
    (with-open [node (Crux/startNode
                      (doto (HashMap.)
                        (.put "crux/tx-log"
                              (doto (HashMap.)
                                (.put "kv-store"
                                      (doto (HashMap.)
                                        (.put "crux/module" "crux.rocksdb/->kv-store")
                                        (.put "db-dir" (io/file data-dir "txs"))))))
                        (.put "crux/document-store"
                              (doto (HashMap.)
                                (.put "kv-store"
                                      (doto (HashMap.)
                                        (.put "crux/module" "crux.rocksdb/->kv-store")
                                        (.put "db-dir" (io/file data-dir "docs"))))))
                        (.put "crux/index-store"
                              (doto (HashMap.)
                                (.put "kv-store"
                                      (doto (HashMap.)
                                        (.put "crux/module" "crux.rocksdb/->kv-store")
                                        (.put "db-dir" (io/file data-dir "indexes"))))))))]
      (t/is (= "crux.rocksdb.RocksKv"
               (kv/kv-name (get-in node [:node :tx-log :kv-store]))
               (kv/kv-name (get-in node [:node :document-store :document-store :kv-store]))
               (kv/kv-name (get-in node [:node :index-store :kv-store]))))
      (t/is (= (.toPath (io/file data-dir "txs"))
               (get-in node [:node :tx-log :kv-store :db-dir]))))))

(t/deftest test-start-up-2-nodes
  (f/with-tmp-dir "data" [data-dir]
    (with-open [n ^ICruxAPI (Crux/startNode (let [^Map m {:crux/tx-log {:crux/module `j/->tx-log, :connection-pool ::j/connection-pool}
                                                          :crux/document-store {:crux/module `j/->document-store, :connection-pool ::j/connection-pool}
                                                          :crux/index-store {:kv-store {:crux/module `rocks/->kv-store, :db-dir (io/file data-dir "kv")}}
                                                          ::j/connection-pool {:dialect `crux.jdbc.h2/->dialect
                                                                               :db-spec {:dbname (str (io/file data-dir "cruxtest"))}}}]
                                              m))]
      (t/is n)

      (let [valid-time (Date.)
            submitted-tx (.submitTx n
                                    (-> (Transaction/builder)
                                        (.put (CruxDocument/factory {:crux.db/id :ivan :name "Ivan"}) valid-time)
                                        (.build)))]
        (t/is (= submitted-tx (.awaitTx n submitted-tx nil)))
        (t/is (= #{[:ivan]} (.query (.db n)
                                    '{:find [e]
                                      :where [[e :name "Ivan"]]}
                                    (object-array 0)))))

      (with-open [^ICruxAPI n2 (Crux/startNode (let [^Map m {:crux/tx-log {:crux/module `j/->tx-log, :connection-pool ::j/connection-pool}
                                                             :crux/document-store {:crux/module `j/->document-store, :connection-pool ::j/connection-pool}
                                                             :crux/index-store {:kv-store {:crux/module `rocks/->kv-store, :db-dir (io/file data-dir "kv2")}}
                                                             ::j/connection-pool {:dialect `crux.jdbc.h2/->dialect
                                                                                  :db-spec {:dbname (str (io/file data-dir "cruxtest2"))}}}]
                                                 m))]

        (t/is (= #{} (.query (.db n2)
                             '{:find [e]
                               :where [[e :name "Ivan"]]}
                             (object-array 0))))

        (let [valid-time (Date.)
              submitted-tx (.submitTx
                             n2
                             (-> (Transaction/builder)
                                 (.put (CruxDocument/factory {:crux.db/id :ivan :name "Iva"}) valid-time)
                                 (.build)))]
          (t/is (= submitted-tx (.awaitTx n2 submitted-tx nil)))
          (t/is (= #{[:ivan]} (.query (.db n2)
                                      '{:find [e]
                                        :where [[e :name "Iva"]]}
                                      (object-array 0)))))

        (t/is n2))

      (t/is (= #{[:ivan]} (.query (.db n)
                                  '{:find [e]
                                    :where [[e :name "Ivan"]]}
                                  (object-array 0)))))))
