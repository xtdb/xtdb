(ns crux.ingest-client-test
  (:require [crux.fixtures :as fix]
            [crux.api :as crux]
            [crux.rocksdb :as rocks]
            [clojure.java.io :as io]
            [clojure.test :as t]
            [crux.db :as db]
            [crux.codec :as c]))

(t/deftest test-ingest-client
  (fix/with-tmp-dir "db" [db-dir]
    (let [ingest-opts {:crux/tx-log {:kv-store {:crux/module `rocks/->kv-store
                                                :db-dir (io/file db-dir "tx-log")}}
                       :crux/document-store {:kv-store {:crux/module `rocks/->kv-store
                                                        :db-dir (io/file db-dir "doc-store")}}}

          submitted-tx
          (with-open [ingest-client (crux/new-ingest-client ingest-opts)]
            (let [submitted-tx (crux/submit-tx ingest-client [[:crux.tx/put {:xt/id :ivan :name "Ivan"}]])]
              (with-open [tx-log-iterator (db/open-tx-log (:tx-log ingest-client) nil)]
                (let [result (iterator-seq tx-log-iterator)]
                  (t/is (not (realized? result)))
                  (t/is (= [(assoc submitted-tx
                                   :crux.tx.event/tx-events [[:crux.tx/put (c/new-id :ivan) (c/hash-doc {:xt/id :ivan :name "Ivan"})]])]
                           result))
                  (t/is (realized? result))))
              submitted-tx))]

      (with-open [node (crux/start-node (merge ingest-opts
                                               {:crux/index-store {:kv-store {:crux/module `rocks/->kv-store, :db-dir (io/file db-dir "indexes")}}}))]
        (crux/await-tx node submitted-tx)
        (t/is (true? (crux/tx-committed? node submitted-tx)))
        (t/is (= #{[:ivan]} (crux/q (crux/db node)
                                    '{:find [e]
                                      :where [[e :name "Ivan"]]})))
))))
