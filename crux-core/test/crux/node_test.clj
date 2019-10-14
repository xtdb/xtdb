(ns crux.node-test
  (:require [clojure.test :as t]
            [crux.io :as cio]
            crux.jdbc
            crux.kv.memdb
            crux.kv.rocksdb
            [crux.node :as n])
  (:import crux.moberg.MobergTxLog
           java.util.Date))

(t/deftest test-properties-to-topology
  (let [t (n/options->topology {:crux.node/topology :crux.jdbc/topology
                                :crux.node/kv-store :crux.kv.rocksdb/kv})]

    (t/is (= (-> t :crux.node/tx-log) (-> crux.jdbc/topology :crux.node/tx-log)))
    (t/is (= (-> t :crux.node/kv-store) crux.kv.rocksdb/kv))))

(t/deftest test-can-set-standalone-kv-store
  (let [event-log-dir (cio/create-tmpdir "kv-store")]
    (try
      (with-open [n (n/start {:crux.node/topology :crux.standalone/topology
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
        (t/is (instance? MobergTxLog (-> n :tx-log))))
      (finally
        (cio/delete-dir event-log-dir)))))

(t/deftest test-start-up-2-nodes
  (let [kv-data-dir-1 (cio/create-tmpdir "kv-store1")
        kv-data-dir-2 (cio/create-tmpdir "kv-store2")
        event-log-dir-1 (cio/create-tmpdir "event-log-1")
        event-log-dir-2 (cio/create-tmpdir "event-log-2")]
    (try
      (with-open [n (n/start {:crux.node/topology :crux.standalone/topology
                              :crux.kv/db-dir (str kv-data-dir-1)
                              :crux.standalone/event-log-dir (str event-log-dir-1)})]
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

        (with-open [n2 (n/start {:crux.node/topology :crux.standalone/topology
                                 :crux.kv/db-dir (str kv-data-dir-2)
                                 :crux.standalone/event-log-dir (str event-log-dir-2)})]
          (let [valid-time (Date.)
                {:keys [crux.tx/tx-time
                        crux.tx/tx-id]
                 :as submitted-tx} (.submitTx n [[:crux.tx/put {:crux.db/id :ivan :name "Iva"} valid-time]])]
            (t/is (= tx-time (.sync n (:crux.tx/tx-time submitted-tx) nil)))
            (t/is (= #{[:ivan]} (.q (.db n)
                                    '{:find [e]
                                      :where [[e :name "Iva"]]}))))

          (t/is n2)
          (println (.status n2)))

        (t/is (= #{[:ivan]} (.q (.db n)
                                '{:find [e]
                                  :where [[e :name "Ivan"]]}))))
      (finally
        (cio/delete-dir event-log-dir-1)
        (cio/delete-dir event-log-dir-2)
        (cio/delete-dir kv-data-dir-1)
        (cio/delete-dir kv-data-dir-2)))))
