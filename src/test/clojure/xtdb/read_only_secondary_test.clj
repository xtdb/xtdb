(ns xtdb.read-only-secondary-test
  (:require [clojure.test :as t]
            [next.jdbc :as jdbc]
            [xtdb.api :as xt]
            [xtdb.log :as xt-log]
            [xtdb.node :as xtn]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import [xtdb.database Database]))

(t/deftest attach-read-write-database-explicit
  (with-open [node (xtn/start-node)]
    (jdbc/execute! node ["ATTACH DATABASE rw_db WITH $$ mode: read-write $$"])

    (xt/submit-tx node [[:put-docs :foo {:xt/id "test"}]]
                  {:database :rw_db})

    (t/is (= [{:xt/id "test"}]
             (xt/q node "SELECT * FROM rw_db.foo"))
          "read-write database allows writes")))

(t/deftest attach-read-only-database
  (with-open [node (xtn/start-node)]
    (jdbc/execute! node ["ATTACH DATABASE ro_db WITH $$ mode: read-only $$"])

    (t/is (= [{:xt/id 0, :committed true}]
             (xt/q node "SELECT _id, committed FROM xt.txs"))
          "attach-db transaction committed")

    (t/is (thrown-with-msg? Exception #"read-only database log"
                            (xt/submit-tx node [[:put-docs :foo {:xt/id "test"}]]
                                          {:database :ro_db}))
          "read-only database rejects writes via log")

    (t/is (= [] (xt/q node "SELECT * FROM ro_db.foo"))
          "read-only database allows reads")))

(t/deftest read-only-secondary-follows-primary
  (let [node-dir (util/->path "target/read-only-secondary/follows-primary")]
    (util/delete-dir node-dir)

    (with-open [primary-node (xtn/start-node {:log [:local {:path (.resolve node-dir "log")}]
                                              :storage [:local {:path (.resolve node-dir "objects")}]})]

      (xt/submit-tx primary-node [[:put-docs :foo {:xt/id "from-primary"}]])

      (with-open [secondary-node (xtn/start-node)]

        (jdbc/execute! secondary-node ["
ATTACH DATABASE shared_db WITH $$
  log: !Local
    path: 'target/read-only-secondary/follows-primary/log'
  storage: !Local
    path: 'target/read-only-secondary/follows-primary/objects'
  mode: read-only
$$"])

        (t/is (= [{:xt/id "from-primary"}]
                 (xt/q secondary-node "SELECT * FROM shared_db.foo"))
              "read-only secondary can read data written by primary")

        (xt/submit-tx primary-node [[:put-docs :foo {:xt/id "from-primary", :v 2}]])

        (xt-log/sync-node secondary-node)

        (t/is (= [{:xt/id "from-primary", :v 2}]
                 (xt/q secondary-node "SELECT * FROM shared_db.foo"))
              "read-only secondary can read updated data")

        (t/is (thrown-with-msg? Exception #"read-only"
                                (xt/submit-tx secondary-node [[:put-docs :foo {:xt/id "from-secondary"}]]
                                              {:database :shared_db}))
              "read-only secondary rejects writes")))))

(t/deftest read-only-secondary-detects-external-log-writes
  (let [node-dir (util/->path "target/read-only-secondary/external-writes")]
    (util/delete-dir node-dir)

    ;; Phase 1: Start a "submit-only" node
    (with-open [submit-node (xtn/start-node {:log [:local {:path (.resolve node-dir "log")}]
                                             :storage [:local {:path (.resolve node-dir "objects")}]
                                             :indexer {:enabled? false}
                                             :compactor {:threads 0}})]

      (xt/submit-tx submit-node [[:put-docs :foo {:xt/id "tx1"}]])
      (xt/submit-tx submit-node [[:put-docs :foo {:xt/id "tx2"}]])

      ;; Phase 2: Start a read-only secondary that should detect log changes via file watcher
      (with-open [secondary-node (xtn/start-node)]

        (jdbc/execute! secondary-node ["
ATTACH DATABASE shared_db WITH $$
  log: !Local
    path: 'target/read-only-secondary/external-writes/log'
  storage: !Local
    path: 'target/read-only-secondary/external-writes/objects'
  mode: read-only
$$"])

        (t/is (= #{{:xt/id "tx1"} {:xt/id "tx2"}}
                 (set (xt/q secondary-node "SELECT * FROM shared_db.foo")))
              "read-only secondary indexes transactions from log")

        ;; Phase 3: Submit more transactions from the submit-only node
        (xt/submit-tx submit-node [[:put-docs :foo {:xt/id "tx3"}]])

        ;; Give the file watcher time to detect the change
        (Thread/sleep 100)
        (xt-log/sync-node secondary-node)

        (t/is (= #{{:xt/id "tx1"} {:xt/id "tx2"} {:xt/id "tx3"}}
                 (set (xt/q secondary-node "SELECT * FROM shared_db.foo")))
              "read-only secondary detects new transactions written by external process")

        (tu/flush-block! submit-node)
        (xt/submit-tx submit-node [[:put-docs :foo {:xt/id "tx4"}]])

        (let [!fut (future
                     (Thread/sleep 100)
                     (xt-log/sync-node secondary-node))]
          (Thread/sleep 200)
          (t/is (not (realized? !fut))
                "blocks til the primary writes the block")

          ;; Phase 4: Start a full primary node
          (with-open [primary-node (xtn/start-node {:log [:local {:path (.resolve node-dir "log")}]
                                                    :storage [:local {:path (.resolve node-dir "objects")}]})]

            (xt/submit-tx primary-node [[:put-docs :foo {:xt/id "tx5"}]])

            (Thread/sleep 100)
            @!fut

            (t/is (= #{{:xt/id "tx1"} {:xt/id "tx2"} {:xt/id "tx3"} {:xt/id "tx4"} {:xt/id "tx5"}}
                     (set (xt/q secondary-node "SELECT * FROM shared_db.foo")))
                  "read-only secondary sees tx written by full primary node")))))))

(t/deftest read-only-secondary-survives-block-flush
  (let [node-dir (util/->path "target/read-only-secondary/block-flush")]
    (util/delete-dir node-dir)

    (with-open [primary-node (xtn/start-node {:log [:local {:path (.resolve node-dir "log")}]
                                              :storage [:local {:path (.resolve node-dir "objects")}]})]

      ;; Write some data to primary before block flush
      (xt/submit-tx primary-node [[:put-docs :foo {:xt/id "before-block-1"}]])
      (xt/submit-tx primary-node [[:put-docs :foo {:xt/id "before-block-2"}]])

      (with-open [secondary-node (xtn/start-node)]

        (jdbc/execute! secondary-node ["
ATTACH DATABASE shared_db WITH $$
  log: !Local
    path: 'target/read-only-secondary/block-flush/log'
  storage: !Local
    path: 'target/read-only-secondary/block-flush/objects'
  mode: read-only
$$"])

        (t/is (= #{{:xt/id "before-block-1"} {:xt/id "before-block-2"}}
                 (set (xt/q secondary-node "SELECT * FROM shared_db.foo")))
              "read-only secondary sees data before block flush")

        (tu/flush-block! primary-node)
        (xt-log/sync-node primary-node)
        (xt/submit-tx primary-node [[:put-docs :foo {:xt/id "after-block-1"}]])

        (Thread/sleep 100)
        (xt-log/sync-node secondary-node)

        (t/is (= #{{:xt/id "before-block-1"} {:xt/id "before-block-2"} {:xt/id "after-block-1"}}
                 (set (xt/q secondary-node "SELECT * FROM shared_db.foo")))
              "read-only secondary sees data after block flush (including block data)")))))
