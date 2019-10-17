(ns crux.node-test
  (:require [clojure.test :as t]
            [crux.config :as cc]
            [crux.io :as cio]
            [crux.moberg]
            crux.jdbc
            crux.kv.memdb
            crux.kv.rocksdb
            [crux.node :as n])
  (:import crux.moberg.MobergTxLog
           java.util.Date
           crux.api.Crux
           (java.util HashMap)
           (clojure.lang Keyword)))

(t/deftest test-properties-to-topology
  (let [t (n/options->topology {:crux.node/topology :crux.jdbc/topology})]

    (t/is (= (-> t :crux.node/tx-log) (-> crux.jdbc/topology :crux.node/tx-log)))
    (t/is (= (-> t :crux.node/kv-store) 'crux.kv.rocksdb/kv)))

  (t/testing "override module in topology"
    (let [t (n/options->topology {:crux.node/topology :crux.jdbc/topology
                                  :crux.node/kv-store :crux.kv.memdb/kv})]

      (t/is (= (-> t :crux.node/tx-log) (-> crux.jdbc/topology :crux.node/tx-log)))
      (t/is (= (-> t :crux.node/kv-store) crux.kv.memdb/kv)))))

(t/deftest test-start-node-complain-if-no-topology
  (try
    (with-open [n (n/start {})]
      (t/is false))
    (catch IllegalArgumentException e
      (t/is (re-find #"Please specify :crux.node/topology" (.getMessage e))))))

(t/deftest test-start-node-should-throw-missing-argument-exception
  (let [data-dir (cio/create-tmpdir "kv-store")]
    (try
      (with-open [n (n/start {:crux.node/topology :crux.jdbc/topology})]
        (t/is false))
      (catch Throwable e
        (t/is (re-find #"Arg :crux.jdbc/dbtype required" (.getMessage e))))
      (finally
        (cio/delete-dir data-dir)))))

(t/deftest test-option-parsing
  (with-open [n (n/start {:foo 2
                          :boo false
                          :crux.node/topology {:a {:args {:foo {:crux.config/type :crux.config/int
                                                                :doc "An argument"
                                                                :default 3}
                                                          :boo {:crux.config/type :crux.config/boolean
                                                                :doc "An argument"
                                                                :default true}}
                                                   :start-fn (fn [_ {:keys [foo boo]}]
                                                               (t/is (= 2 foo))
                                                               (t/is (= false boo)))}}})]
    (t/is true)))

(t/deftest test-nodes-shutdown-in-order
  (let [started (atom [])
        stopped (atom [])]
    (with-open [n (n/start {:crux.node/topology {:a {:deps [:b]
                                                     :start-fn (fn [{:keys [b]} _]
                                                                 (assert b)
                                                                 (swap! started conj :a)
                                                                 (reify java.io.Closeable
                                                                   (close [_]
                                                                     (swap! stopped conj :a))))}
                                                 :b {:start-fn (fn [_ _]
                                                                 (swap! started conj :b)
                                                                 (reify java.io.Closeable
                                                                   (close [_]
                                                                     (swap! stopped conj :b))))}}})]
      (t/is (= [:b :a] @started)))
    (t/is (= [:a :b] @stopped)))

  (t/testing "With Exception"
    (let [started (atom [])
          stopped (atom [])]
      (try
        (with-open [n (n/start {:crux.node/topology {:a {:deps [:b]
                                                         :start-fn (fn [_ _]
                                                                     (throw (Exception.)))}
                                                     :b {:start-fn (fn [_ _]
                                                                     (swap! started conj :b)
                                                                     (reify java.io.Closeable
                                                                       (close [_]
                                                                         (swap! stopped conj :b))))}}})]
          (t/is false))
        (catch Exception e))
      (t/is (= [:b] @started))
      (t/is (= [:b] @stopped)))))

(t/deftest test-can-start-JDBC-node
  (let [data-dir (cio/create-tmpdir "kv-store")]
    (try
      (with-open [n (n/start {:crux.node/topology :crux.jdbc/topology
                              :crux.kv/db-dir (str data-dir)
                              :crux.jdbc/dbtype "h2"
                              :crux.jdbc/dbname "cruxtest"})]
        (t/is n))
      (finally
        (cio/delete-dir data-dir)))))

(t/deftest test-can-set-standalone-kv-store
  (let [event-log-dir (cio/create-tmpdir "kv-store")]
    (try
      (with-open [n (n/start {:crux.node/topology :crux.standalone/topology
                              :crux.kv/kv-store :crux.kv.memdb/kv
                              :crux.standalone/event-log-dir (str event-log-dir)
                              :crux.standalone/event-log-kv-store :crux.kv.memdb/kv})]
        (t/is n))
      (finally
        (cio/delete-dir event-log-dir)))))

(t/deftest test-properties-file-to-node
  (let [event-log-dir (cio/create-tmpdir "kv-store")]
    (try
      (with-open [n (n/start (assoc (cc/load-properties (clojure.java.io/resource "sample.properties"))
                                    :crux.standalone/event-log-dir (str event-log-dir)))]
        (t/is (instance? MobergTxLog (-> n :tx-log)))
        (t/is (= 20000 (-> n :options :crux.tx-log/await-tx-timeout))))
      (finally
        (cio/delete-dir event-log-dir)))))

(t/deftest test-conflicting-standalone-props
  (let [event-log-dir (cio/create-tmpdir "kv-store")]
    (try
      (with-open [n (n/start {:crux.node/topology :crux.standalone/topology
                              :crux.kv/kv-store :crux.kv.memdb/kv
                              :crux.standalone/event-log-sync-interval-ms 1000
                              :crux.standalone/event-log-sync? true
                              :crux.standalone/event-log-dir (str event-log-dir)
                              :crux.standalone/event-log-kv-store :crux.kv.memdb/kv})]
        (t/is false))
      (catch java.lang.AssertionError e
        (t/is true))
      (finally
        (cio/delete-dir event-log-dir)))))

(t/deftest topology-resolution-from-java
  (let [mem-db-node-options
        (doto (HashMap.)
          (.put (Keyword/intern "crux.node/topology") (Keyword/intern "crux.standalone/topology"))
          (.put (Keyword/intern "crux.node/kv-store") "crux.kv.memdb/kv")
          (.put (Keyword/intern "crux.standalone/event-log-kv-store") "crux.kv.memdb/kv")
          (.put (Keyword/intern "crux.standalone/event-log-dir") "data/eventlog")
          (.put (Keyword/intern "crux.kv/db-dir") "data/db-dir"))
        memdb-node (Crux/startNode mem-db-node-options)]
    (t/is memdb-node)
    (t/is (not (.close memdb-node)))))

(t/deftest test-start-up-2-nodes
  (let [kv-data-dir-1 (cio/create-tmpdir "kv-store1")
        kv-data-dir-2 (cio/create-tmpdir "kv-store2")]
    (try
      (with-open [n (n/start {:crux.node/topology :crux.jdbc/topology
                              :crux.kv/db-dir (str kv-data-dir-1)
                              :crux.jdbc/dbtype "h2"
                              :crux.jdbc/dbname "cruxtest1"})]
        (t/is n)

        (let [valid-time (Date.)
              {:keys [crux.tx/tx-time
                      crux.tx/tx-id]
               :as submitted-tx} (.submitTx n [[:crux.tx/put {:crux.db/id :ivan :name "Ivan"} valid-time]])]
          (t/is (= tx-time (.sync n (:crux.tx/tx-time submitted-tx) nil)))
          (t/is (= #{[:ivan]} (.q (.db n)
                                  '{:find [e]
                                    :where [[e :name "Ivan"]]}))))

        (t/is (= #{[:ivan]} (.q (.db n)
                                '{:find [e]
                                  :where [[e :name "Ivan"]]})))

        (with-open [n2 (n/start {:crux.node/topology :crux.jdbc/topology
                                 :crux.kv/db-dir (str kv-data-dir-2)
                                 :crux.jdbc/dbtype "h2"
                                 :crux.jdbc/dbname "cruxtest2"})]

          (t/is (= #{} (.q (.db n2)
                           '{:find [e]
                             :where [[e :name "Ivan"]]})))

          (let [valid-time (Date.)
                {:keys [crux.tx/tx-time
                        crux.tx/tx-id]
                 :as submitted-tx} (.submitTx n2 [[:crux.tx/put {:crux.db/id :ivan :name "Iva"} valid-time]])]
            (t/is (= tx-time (.sync n2 (:crux.tx/tx-time submitted-tx) nil)))
            (t/is (= #{[:ivan]} (.q (.db n2)
                                    '{:find [e]
                                      :where [[e :name "Iva"]]}))))

          (t/is n2))

        (t/is (= #{[:ivan]} (.q (.db n)
                                '{:find [e]
                                  :where [[e :name "Ivan"]]}))))
      (finally
        (cio/delete-dir kv-data-dir-1)
        (cio/delete-dir kv-data-dir-2)))))
