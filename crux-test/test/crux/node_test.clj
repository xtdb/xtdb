(ns crux.node-test
  (:require [clojure.test :as t]
            [crux.io :as cio]
            crux.jdbc
            crux.kv.memdb
            crux.kv.rocksdb
            [crux.api :as api])
  (:import crux.moberg.MobergTxLog
           java.util.Date))

(t/deftest test-start-up-2-nodes
  (let [kv-data-dir-1 (cio/create-tmpdir "kv-store1")
        kv-data-dir-2 (cio/create-tmpdir "kv-store2")
        event-log-dir-1 (cio/create-tmpdir "event-log-1")
        event-log-dir-2 (cio/create-tmpdir "event-log-2")]
    (try
      (with-open [n (api/start-standalone-node {:db-dir (str kv-data-dir-1)
                                                :event-log-dir (str event-log-dir-1)})]
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

        (with-open [n2 (api/start-standalone-node {:db-dir (str kv-data-dir-2)
                                                   :event-log-dir (str event-log-dir-2)})]

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
        (cio/delete-dir event-log-dir-1)
        (cio/delete-dir event-log-dir-2)
        (cio/delete-dir kv-data-dir-1)
        (cio/delete-dir kv-data-dir-2)))))
