(ns xtdb.submit-client-test
  (:require [xtdb.fixtures :as fix]
            [xtdb.api :as xt]
            [xtdb.rocksdb :as rocks]
            [clojure.java.io :as io]
            [clojure.test :as t]
            [xtdb.db :as db]
            [xtdb.codec :as c]))

(t/deftest test-submit-client
  (fix/with-tmp-dir "db" [db-dir]
    (let [submit-opts {:xtdb/tx-log {:kv-store {:xtdb/module `rocks/->kv-store
                                              :db-dir (io/file db-dir "tx-log")}}
                       :xtdb/document-store {:kv-store {:xtdb/module `rocks/->kv-store
                                                      :db-dir (io/file db-dir "doc-store")}}}

          submitted-tx
          (with-open [submit-client (xt/new-submit-client submit-opts)]
            (let [submitted-tx (xt/submit-tx submit-client [[:xt/put {:xt/id :ivan :name "Ivan"}]])]
              (with-open [tx-log-iterator (db/open-tx-log (:tx-log submit-client) nil)]
                (let [result (iterator-seq tx-log-iterator)]
                  (t/is (not (realized? result)))
                  (t/is (= [(assoc submitted-tx
                                   :xtdb.tx.event/tx-events [[:crux.tx/put (c/new-id :ivan) (c/hash-doc {:xt/id :ivan :name "Ivan"})]])]
                           result))
                  (t/is (realized? result))))
              submitted-tx))]

      (with-open [node (xt/start-node (merge submit-opts
                                             {:xtdb/index-store {:kv-store {:xtdb/module `rocks/->kv-store, :db-dir (io/file db-dir "indexes")}}}))]
        (xt/await-tx node submitted-tx)
        (t/is (true? (xt/tx-committed? node submitted-tx)))
        (t/is (= #{[:ivan]} (xt/q (xt/db node)
                                  '{:find [e]
                                    :where [[e :name "Ivan"]]})))))))
