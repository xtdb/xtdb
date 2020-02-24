(ns crux.node-test
  (:require [clojure.test :as t]
            [crux.config :as cc]
            [crux.io :as cio]
            crux.jdbc
            crux.kv.memdb
            crux.kv.rocksdb
            [crux.node :as n]
            [clojure.spec.alpha :as s]
            [crux.fixtures :as f]
            [clojure.java.io :as io]
            crux.standalone
            [crux.api :as crux])
  (:import java.util.Date
           crux.api.Crux
           (java.util HashMap)
           (clojure.lang Keyword)
           (java.time Duration)))

(t/deftest test-calling-shutdown-node-fails-gracefully
  (f/with-tmp-dir "data" [data-dir]
    (try
      (let [n (n/start {:crux.node/topology ['crux.standalone/topology]
                        :crux.kv/db-dir (str (io/file data-dir "db"))})]
        (t/is (.status n))
        (.close n)
        (.status n)
        (t/is false))
      (catch IllegalStateException e
        (t/is (= "Crux node is closed" (.getMessage e)))))))

(t/deftest test-start-node-complain-if-no-topology
  (try
    (with-open [n (n/start {})]
      (t/is false))
    (catch IllegalArgumentException e
      (t/is (re-find #"Please specify :crux.node/topology" (.getMessage e))))))

(t/deftest test-start-node-should-throw-missing-argument-exception
  (f/with-tmp-dir "data" [data-dir]
    (try
      (with-open [n (n/start {:crux.node/topology '[crux.jdbc/topology]
                              :crux.kv/db-dir (str (io/file data-dir "db"))})]
        (t/is false))
      (catch Throwable e
        (t/is (re-find #"Arg :crux.jdbc/dbtype required" (.getMessage e))))
      (finally
        (cio/delete-dir data-dir)))))

(t/deftest test-can-start-JDBC-node
  (f/with-tmp-dir "data" [data-dir]
    (with-open [n (n/start {:crux.node/topology ['crux.jdbc/topology]
                            :crux.kv/db-dir (str (io/file data-dir "kv-store"))
                            :crux.jdbc/dbtype "h2"
                            :crux.jdbc/dbname (str (io/file data-dir "cruxtest"))})]
      (t/is n))))

(t/deftest test-can-set-standalone-kv-store
  (f/with-tmp-dir "data" [data-dir]
    (with-open [n (n/start {:crux.node/topology ['crux.standalone/topology]
                            :crux.kv/kv-store :crux.kv.memdb/kv
                            :crux.kv/db-dir (str (io/file data-dir "db"))})]
      (t/is n))))

(t/deftest test-properties-file-to-node
  (f/with-tmp-dir "data" [data-dir]
    (with-open [n (n/start (assoc (cc/load-properties (clojure.java.io/resource "sample.properties"))
                             :crux.db/db-dir (str (io/file data-dir "db"))))]
      (t/is (instance? crux.standalone.StandaloneTxLog (-> n :tx-log)))
      (t/is (= (Duration/ofSeconds 20) (-> n :options :crux.tx-log/await-tx-timeout))))))

(t/deftest topology-resolution-from-java
  (f/with-tmp-dir "data" [data-dir]
    (let [mem-db-node-options
          (doto (HashMap.)
            (.put :crux.node/topology 'crux.standalone/topology)
            (.put :crux.node/kv-store 'crux.kv.memdb/kv)
            (.put :crux.kv/db-dir (str (io/file data-dir "db-dir"))))
          memdb-node (Crux/startNode mem-db-node-options)]
      (t/is memdb-node)
      (t/is (not (.close memdb-node))))))

(t/deftest test-start-up-2-nodes
  (f/with-tmp-dir "data" [data-dir]
    (with-open [n (n/start {:crux.node/topology ['crux.jdbc/topology]
                            :crux.kv/db-dir (str (io/file data-dir "kv1"))
                            :crux.jdbc/dbtype "h2"
                            :crux.jdbc/dbname (str (io/file data-dir "cruxtest1"))})]
      (t/is n)

      (let [valid-time (Date.)
            submitted-tx (crux/submit-tx n [[:crux.tx/put {:crux.db/id :ivan :name "Ivan"} valid-time]])]
        (t/is (= #{[:ivan]} (crux/q (crux/at n submitted-tx)
                                    '{:find [e]
                                      :where [[e :name "Ivan"]]}))))

      (with-open [n2 (n/start {:crux.node/topology ['crux.jdbc/topology]
                               :crux.kv/db-dir (str (io/file data-dir "kv2"))
                               :crux.jdbc/dbtype "h2"
                               :crux.jdbc/dbname (str (io/file data-dir "cruxtest2"))})]

        (t/is (= #{} (crux/q (crux/at n2)
                             '{:find [e]
                               :where [[e :name "Ivan"]]})))

        (let [valid-time (Date.)
              submitted-tx (crux/submit-tx n2 [[:crux.tx/put {:crux.db/id :ivan :name "Iva"} valid-time]])]
          (t/is (= #{[:ivan]} (crux/q (crux/at n2 submitted-tx)
                                  '{:find [e]
                                    :where [[e :name "Iva"]]}))))))))
