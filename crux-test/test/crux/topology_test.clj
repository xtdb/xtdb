(ns crux.topology-test
  (:require [crux.topology :as topo]
            [clojure.spec.alpha :as s]
            [clojure.test :as t]))

(t/deftest test-properties-to-topology
  (let [t (topo/options->topology {:crux.node/topology ['crux.jdbc/topology]})]

    (t/is (= (-> crux.jdbc/topology :crux.node/tx-log)
             (-> t :crux.node/tx-log)))
    (t/is (= (s/conform ::topo/component crux.kv.rocksdb/kv)
             (-> t :crux.node/kv-store))))

  (t/testing "override module in topology"
    (let [t (topo/options->topology {:crux.node/topology ['crux.jdbc/topology]
                                     :crux.node/kv-store :crux.kv.memdb/kv})]

      (t/is (= (-> crux.jdbc/topology :crux.node/tx-log)
               (-> t :crux.node/tx-log)))
      (t/is (= (s/conform ::topo/component crux.kv.memdb/kv)
               (-> t :crux.node/kv-store))))))
