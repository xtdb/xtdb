(ns crux.node-test
  (:require [clojure.test :as t]
            [crux.io :as cio]
            crux.jdbc
            crux.kv.memdb
            crux.kv.rocksdb
            crux.moberg
            [crux.node :as n])
  (:import crux.moberg.MobergTxLog
           java.util.Date))

(t/deftest test-properties-to-topology
  (let [t (n/options->topology {:crux.node/topology :crux.jdbc/topology})]

    (t/is (= (-> t :crux.node/tx-log) (-> crux.jdbc/topology :crux.node/tx-log)))
    (t/is (= (-> t :crux.node/kv-store) 'crux.kv.rocksdb/kv)))

  (t/testing "override module in topology"
    (let [t (n/options->topology {:crux.node/topology :crux.jdbc/topology
                                  :crux.node/kv-store :crux.kv.memdb/kv})]

      (t/is (= (-> t :crux.node/tx-log) (-> crux.jdbc/topology :crux.node/tx-log)))
      (t/is (= (-> t :crux.node/kv-store) crux.kv.memdb/kv)))))

(t/deftest test-start-node-should-throw-missing-argument-exception
  (let [data-dir (cio/create-tmpdir "kv-store")]
    (try
      (with-open [n (n/start {:crux.node/topology :crux.jdbc/topology})]
        (t/is false))
      (catch Throwable e
        (t/is (re-find #"Arg :dbtype required by module" (.getMessage e))))
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
                              :dbtype "h2"
                              :dbname "cruxtest"})]
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

(defn- load-props [f]
  (let [props (java.util.Properties.)]
    (.load props (clojure.java.io/reader f))
    (into {}
          (for [[k v] props]
            [(keyword k) v]))))

(t/deftest test-properties-file-to-node
  (let [event-log-dir (cio/create-tmpdir "kv-store")]
    (try
      (with-open [n (n/start (assoc (load-props (clojure.java.io/resource "sample.properties"))
                                    :crux.standalone/event-log-dir (str event-log-dir)))]
        (t/is (instance? MobergTxLog (-> n :tx-log)))
        (t/is (= 20000 (-> n :options :crux.tx-log/await-tx-timeout))))
      (finally
        (cio/delete-dir event-log-dir)))))

(t/deftest test-can-set-rocks-options
  ;; TODO write.
  (t/is false))

(t/deftest test-start-up-2-nodes
  (let [kv-data-dir-1 (cio/create-tmpdir "kv-store1")
        kv-data-dir-2 (cio/create-tmpdir "kv-store2")]
    (try
      (with-open [n (n/start {:crux.node/topology :crux.jdbc/topology
                              :crux.kv/db-dir (str kv-data-dir-1)
                              :dbtype "h2"
                              :dbname "cruxtest1"})]
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
                                 :dbtype "h2"
                                 :dbname "cruxtest2"})]

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
