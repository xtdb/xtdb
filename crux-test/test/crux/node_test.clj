(ns crux.node-test
  (:require [clojure.test :as t]
            [crux.config :as cc]
            [crux.io :as cio]
            [crux.moberg]
            crux.jdbc
            crux.kv.memdb
            crux.kv.rocksdb
            [crux.node :as n]
            [clojure.spec.alpha :as s]
            [crux.fixtures :as f]
            [clojure.java.io :as io])
  (:import crux.moberg.MobergTxLog
           java.util.Date
           crux.api.Crux
           (java.util HashMap)
           (clojure.lang Keyword)))

(t/deftest test-properties-to-topology
  (let [t (n/options->topology {:crux.node/topology ['crux.jdbc/topology]})]

    (t/is (= (-> crux.jdbc/topology :crux.node/tx-log)
             (-> t :crux.node/tx-log)))
    (t/is (= (s/conform ::n/component crux.kv.rocksdb/kv)
             (-> t :crux.node/kv-store))))

  (t/testing "override module in topology"
    (let [t (n/options->topology {:crux.node/topology ['crux.jdbc/topology]
                                  :crux.node/kv-store :crux.kv.memdb/kv})]

      (t/is (= (-> crux.jdbc/topology :crux.node/tx-log)
               (-> t :crux.node/tx-log)))
      (t/is (= (s/conform ::n/component crux.kv.memdb/kv)
               (-> t :crux.node/kv-store))))))

(t/deftest test-calling-shutdown-node-fails-gracefully
  (let [data-dir (cio/create-tmpdir "kv-store")
        event-log-dir (cio/create-tmpdir "kv-store")]
    (try
      (let [n (n/start {:crux.node/topology ['crux.standalone/topology]
                        :crux.kv/db-dir (str data-dir)
                        :crux.standalone/event-log-dir (str event-log-dir)})]
        (t/is (.status n))
        (.close n)
        (.status n)
        (t/is false))
      (catch IllegalStateException e
        (t/is (= "Crux node is closed" (.getMessage e))))
      (finally
        (cio/delete-dir data-dir)
        (cio/delete-dir event-log-dir)))))

(t/deftest test-start-node-complain-if-no-topology
  (try
    (with-open [n (n/start {})]
      (t/is false))
    (catch IllegalArgumentException e
      (t/is (re-find #"Please specify :crux.node/topology" (.getMessage e))))))

(t/deftest test-start-node-should-throw-missing-argument-exception
  (let [data-dir (cio/create-tmpdir "kv-store")]
    (try
      (with-open [n (n/start {:crux.node/topology ['crux.jdbc/topology]})]
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
                            :crux.standalone/event-log-dir (str (io/file data-dir "event-log"))
                            :crux.standalone/event-log-kv-store :crux.kv.memdb/kv})]
      (t/is n))))

(t/deftest test-properties-file-to-node
  (f/with-tmp-dir "data" [data-dir]
    (with-open [n (n/start (assoc (cc/load-properties (clojure.java.io/resource "sample.properties"))
                                  :crux.standalone/event-log-dir (str (io/file data-dir "event-log"))))]
      (t/is (instance? MobergTxLog (-> n :tx-log)))
      (t/is (= 20000 (-> n :options :crux.tx-log/await-tx-timeout))))))

(t/deftest test-conflicting-standalone-props
  (f/with-tmp-dir "data" [data-dir]
    (try
      (with-open [n (n/start {:crux.node/topology ['crux.standalone/topology]
                              :crux.kv/kv-store :crux.kv.memdb/kv
                              :crux.standalone/event-log-sync-interval-ms 1000
                              :crux.standalone/event-log-sync? true
                              :crux.standalone/event-log-dir (str (io/file data-dir "event-log"))
                              :crux.standalone/event-log-kv-store :crux.kv.memdb/kv})]
        (t/is false))
      (catch java.lang.AssertionError e
        (t/is true)))))

(t/deftest topology-resolution-from-java
  (f/with-tmp-dir "data" [data-dir]
    (let [mem-db-node-options
          (doto (HashMap.)
            (.put :crux.node/topology 'crux.standalone/topology)
            (.put :crux.node/kv-store 'crux.kv.memdb/kv)
            (.put :crux.standalone/event-log-kv-store 'crux.kv.memdb/kv)
            (.put :crux.standalone/event-log-dir (str (io/file data-dir "eventlog")))
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
            submitted-tx (.submitTx n [[:crux.tx/put {:crux.db/id :ivan :name "Ivan"} valid-time]])]
        (t/is (= submitted-tx (.awaitTx n submitted-tx nil)))
        (t/is (= #{[:ivan]} (.q (.db n)
                                '{:find [e]
                                  :where [[e :name "Ivan"]]}))))

      (t/is (= #{[:ivan]} (.q (.db n)
                              '{:find [e]
                                :where [[e :name "Ivan"]]})))

      (with-open [n2 (n/start {:crux.node/topology ['crux.jdbc/topology]
                               :crux.kv/db-dir (str (io/file data-dir "kv2"))
                               :crux.jdbc/dbtype "h2"
                               :crux.jdbc/dbname (str (io/file data-dir "cruxtest2"))})]

        (t/is (= #{} (.q (.db n2)
                         '{:find [e]
                           :where [[e :name "Ivan"]]})))

        (let [valid-time (Date.)
              submitted-tx (.submitTx n2 [[:crux.tx/put {:crux.db/id :ivan :name "Iva"} valid-time]])]
          (t/is (= submitted-tx (.awaitTx n2 submitted-tx nil)))
          (t/is (= #{[:ivan]} (.q (.db n2)
                                  '{:find [e]
                                    :where [[e :name "Iva"]]}))))

        (t/is n2))

      (t/is (= #{[:ivan]} (.q (.db n)
                              '{:find [e]
                                :where [[e :name "Ivan"]]}))))))
