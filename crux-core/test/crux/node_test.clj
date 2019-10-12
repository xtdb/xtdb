(ns crux.node-test
  (:require [clojure.test :as t]
            crux.jdbc
            crux.kv.rocksdb
            [crux.node :as n]
            [crux.io :as cio]))

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
