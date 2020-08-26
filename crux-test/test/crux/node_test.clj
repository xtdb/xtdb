(ns crux.node-test
  (:require [clojure.test :as t]
            [crux.config :as cc]
            [crux.io :as cio]
            [crux.jdbc :as j]
            crux.kv.memdb
            [crux.rocksdb :as rocks]
            [crux.node :as n]
            [clojure.spec.alpha :as s]
            [crux.fixtures :as f]
            [clojure.java.io :as io]
            [crux.tx :as tx]
            [crux.tx.event :as txe]
            [crux.api :as api]
            [crux.bus :as bus]
            [crux.kv :as kv])
  (:import java.util.Date
           crux.api.Crux
           (java.util HashMap ArrayList)
           (clojure.lang Keyword)
           (java.time Duration)
           (java.util.concurrent TimeoutException)))

(t/deftest test-calling-shutdown-node-fails-gracefully
  (f/with-tmp-dir "data" [data-dir]
    (try
      (let [n (api/start-node {})]
        (t/is (.status n))
        (.close n)
        (.status n)
        (t/is false))
      (catch IllegalStateException e
        (t/is (= "Crux node is closed" (.getMessage e)))))))

(t/deftest test-start-node-should-throw-missing-argument-exception
  (t/is (thrown-with-msg? IllegalArgumentException
                          #"Missing module.+:dialect"
                          (api/start-node {:crux/document-store {:crux/module `j/->document-store}}))))

(t/deftest test-can-start-JDBC-node
  (f/with-tmp-dir "data" [data-dir]
    (with-open [n (api/start-node {:crux/tx-log {:crux/module `j/->tx-log, :data-source ::data-source, :dbtype "h2"}
                                   ::j/tx-consumer {}
                                   :crux/document-store {:crux/module `j/->document-store, :data-source ::data-source, :dbtype "h2"}
                                   ::data-source {:crux/module `crux.jdbc.h2/->data-source, :db-file (io/file data-dir "cruxtest")}})]
      (t/is n))))

(t/deftest test-can-set-indexes-kv-store
  (f/with-tmp-dir "data" [data-dir]
    (with-open [n (api/start-node {:crux/tx-log {:crux/module `rocks/->kv-store, :db-dir (io/file data-dir "tx-log")}
                                   :crux/document-store {:crux/module `rocks/->kv-store, :db-dir (io/file data-dir "doc-store")}
                                   :crux/indexer {:crux/module `rocks/->kv-store, :db-dir (io/file data-dir "indexes")}})]
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
                        (.put "crux/indexer"
                              (doto (HashMap.)
                                (.put "kv-store"
                                      (doto (HashMap.)
                                        (.put "crux/module" "crux.rocksdb/->kv-store")
                                        (.put "db-dir" (io/file data-dir "indexes"))))))))]
      (t/is (= "crux.rocksdb.RocksKv"
               (kv/kv-name (get-in node [:tx-log :kv-store]))
               (kv/kv-name (get-in node [:document-store :document-store :kv]))
               (kv/kv-name (get-in node [:indexer :kv-store]))))
      (t/is (= (.toPath (io/file data-dir "txs"))
               (get-in node [:tx-log :kv-store :db-dir]))))))

(t/deftest test-start-up-2-nodes
  (f/with-tmp-dir "data" [data-dir]
    (with-open [n (api/start-node {:crux/tx-log {:crux/module `j/->tx-log, :data-source ::data-source, :dbtype "h2"}
                                   ::j/tx-consumer {}
                                   :crux/document-store {:crux/module `j/->document-store, :data-source ::data-source, :dbtype "h2"}
                                   :crux/indexer {:kv-store {:crux/module `rocks/->kv-store, :db-dir (io/file data-dir "kv1")}}
                                   ::data-source {:crux/module `crux.jdbc.h2/->data-source, :db-file (io/file data-dir "cruxtest1")}})]
      (t/is n)

      (let [valid-time (Date.)
            submitted-tx (.submitTx n [[:crux.tx/put {:crux.db/id :ivan :name "Ivan"} valid-time]])]
        (t/is (= submitted-tx (.awaitTx n submitted-tx nil)))
        (t/is (= #{[:ivan]} (.query (.db n)
                                    '{:find [e]
                                      :where [[e :name "Ivan"]]}))))

      (t/is (= #{[:ivan]} (.query (.db n)
                                  '{:find [e]
                                    :where [[e :name "Ivan"]]})))

      (with-open [n2 (api/start-node {:crux/tx-log {:crux/module `j/->tx-log, :data-source ::data-source, :dbtype "h2"}
                                      ::j/tx-consumer {}
                                      :crux/document-store {:crux/module `j/->document-store, :data-source ::data-source, :dbtype "h2"}
                                      :crux/indexer {:kv-store {:crux/module `rocks/->kv-store, :db-dir (io/file data-dir "kv2")}}
                                      ::data-source {:crux/module `crux.jdbc.h2/->data-source, :db-file (io/file data-dir "cruxtest2")}})]

        (t/is (= #{} (.query (.db n2)
                             '{:find [e]
                               :where [[e :name "Ivan"]]})))

        (let [valid-time (Date.)
              submitted-tx (.submitTx n2 [[:crux.tx/put {:crux.db/id :ivan :name "Iva"} valid-time]])]
          (t/is (= submitted-tx (.awaitTx n2 submitted-tx nil)))
          (t/is (= #{[:ivan]} (.query (.db n2)
                                      '{:find [e]
                                        :where [[e :name "Iva"]]}))))

        (t/is n2))

      (t/is (= #{[:ivan]} (.query (.db n)
                                  '{:find [e]
                                    :where [[e :name "Ivan"]]}))))))

(defmacro with-latest-tx [latest-tx & body]
  `(with-redefs [api/latest-completed-tx (fn [node#]
                                           ~latest-tx)]
     ~@body))

(t/deftest test-await-tx
  (let [bus (bus/->bus {})
        await-tx (fn [tx timeout]
                   (#'n/await-tx {:bus bus} ::tx/tx-id tx timeout))
        tx1 {::tx/tx-id 1
             ::tx/tx-time (Date.)}
        tx-evt {:crux/event-type ::tx/indexed-tx
                ::tx/submitted-tx tx1
                ::txe/tx-events []
                :committed? true}]

    (t/testing "ready already"
      (with-latest-tx tx1
        (t/is (= tx1 (await-tx tx1 nil)))))

    (t/testing "times out"
      (with-latest-tx nil
        (t/is (thrown? TimeoutException (await-tx tx1 (Duration/ofMillis 10))))))

    (t/testing "eventually works"
      (future
        (Thread/sleep 100)
        (bus/send bus tx-evt))

      (with-latest-tx nil
        (t/is (= tx1 (await-tx tx1 nil)))))

    (t/testing "times out if it's not quite ready"
      (let [!latch (promise)]
        (future
          (Thread/sleep 100)
          (bus/send bus (assoc-in tx-evt [::tx/submitted-tx ::tx/tx-id] 0)))

        (with-latest-tx nil
          (t/is (thrown? TimeoutException (await-tx tx1 (Duration/ofMillis 500)))))))

    (t/testing "throws on ingester error"
      (future
        (Thread/sleep 100)
        (bus/send bus {:crux/event-type ::tx/ingester-error
                       ::tx/ingester-error (ex-info "Ingester error" {})}))

      (with-latest-tx nil
        (t/is (thrown-with-msg? Exception #"Transaction ingester aborted."
                                (await-tx tx1 nil)))))

    (t/testing "throws if node closed"
      (future
        (Thread/sleep 100)
        (bus/send bus {:crux/event-type ::n/node-closed}))

      (with-latest-tx nil
        (t/is (thrown? InterruptedException (await-tx tx1 nil)))))))
