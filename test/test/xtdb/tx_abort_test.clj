(ns xtdb.tx-abort-test
  (:require [clojure.test :as t]
            [clojure.tools.logging :as log]
            [xtdb.api :as xt]
            [xtdb.io :as xio]))

(t/deftest aborted-transactions-remove-events-test
  (let [inc-tx-fn
        '(fn [ctx fail]
           (if fail
             false
             [[::xt/put (-> (xtdb.api/entity (xtdb.api/db ctx) 0)
                            (or {:xt/id 0, :n 0})
                            (update :n inc))]]))
        pre-reqs
        [[::xt/put {:xt/id :inc, :xt/fn inc-tx-fn}]]

        fail-tx1
        [[::xt/fn :inc false]
         [::xt/match 0 {:xt/id 0, :n 0}]
         [::xt/fn :inc false]]

        fail-tx2
        [[::xt/fn :inc false]
         [::xt/fn :inc true]
         [::xt/fn :inc false]]

        success-tx
        [[::xt/fn :inc false]
         [::xt/match 0 {:xt/id 0, :n 1}]
         [::xt/fn :inc false]]

        tx-log-dir (xio/create-tmpdir "log")
        doc-dir (xio/create-tmpdir "doc")

        node-opts
        {:xtdb/tx-log {:kv-store {:xtdb/module 'xtdb.rocksdb/->kv-store, :db-dir tx-log-dir}}
         :xtdb/document-store {:kv-store {:xtdb/module 'xtdb.rocksdb/->kv-store, :db-dir doc-dir}}}]

    (try

      (with-open [node (xt/start-node node-opts)]
        (doto node
          (xt/submit-tx pre-reqs)
          (xt/submit-tx fail-tx1)
          (xt/submit-tx fail-tx2)
          (xt/submit-tx success-tx)
          (xt/sync))
        (t/is (= {:xt/id 0, :n 2} (xt/entity (xt/db node) 0))))

      (with-open [node (xt/start-node node-opts)]
        (xt/sync node)
        (t/is (= {:xt/id 0, :n 2} (xt/entity (xt/db node) 0))))

      (finally
        (xio/delete-dir tx-log-dir)
        (xio/delete-dir doc-dir)))))
