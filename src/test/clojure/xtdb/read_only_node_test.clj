(ns xtdb.read-only-node-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.db-catalog :as db]
            [xtdb.log :as xt-log]
            [xtdb.node :as xtn]
            [xtdb.test-util :as tu]
            [xtdb.util :as util]))

(t/deftest read-only-node-rejects-writes
  (with-open [node (-> (xtn/->config {})
                       (.readOnlyDatabases true)
                       (.open))]
    (t/is (thrown-with-msg? Exception #"read-only"
                            (xt/submit-tx node [[:put-docs :foo {:xt/id "test"}]]))
          "read-only node rejects writes to primary database")))

(t/deftest read-only-node-follows-read-write-node
  (util/with-tmp-dirs #{node-dir}
    (with-open [rw-node (xtn/start-node {:log [:local {:path (.resolve node-dir "log")}]
                                         :storage [:local {:path (.resolve node-dir "objects")}]})]

      (xt/submit-tx rw-node [[:put-docs :foo {:xt/id "from-rw"}]])

      (with-open [ro-node (-> (xtn/->config {:log [:local {:path (.resolve node-dir "log")}]
                                             :storage [:local {:path (.resolve node-dir "objects")}]})
                              (.readOnlyDatabases true)
                              (.open))]

        (xt-log/sync-node ro-node #xt/duration "PT5S")

        (t/is (= [{:xt/id "from-rw"}]
                 (xt/q ro-node "SELECT * FROM foo"))
              "read-only node can read data written by read-write node")

        (t/is (thrown-with-msg? Exception #"read-only"
                                (xt/submit-tx ro-node [[:put-docs :foo {:xt/id "from-ro"}]]))
              "read-only node rejects writes")

        (xt/submit-tx rw-node [[:put-docs :foo {:xt/id "from-rw", :v 2}]])
        (xt-log/sync-node ro-node #xt/duration "PT5S")

        (t/is (= [{:xt/id "from-rw", :v 2}]
                 (xt/q ro-node "SELECT * FROM foo"))
              "read-only node sees updated data from read-write node")))))

(t/deftest read-only-node-sees-data-after-block-flush
  (util/with-tmp-dirs #{node-dir}
    (let [cfg {:log [:local {:path (.resolve node-dir "log")}]
               :storage [:local {:path (.resolve node-dir "objects")}]}]
      (with-open [rw-node (xtn/start-node cfg)
                  ro-node (-> (xtn/->config cfg)
                              (.readOnlyDatabases true)
                              (.open))]

        (xt/submit-tx rw-node [[:put-docs :foo {:xt/id "a", :x 1}]])
        (tu/flush-block! rw-node)
        (xt-log/sync-node ro-node #xt/duration "PT5S")

        (t/is (= [{:xt/id "a", :x 1}]
                 (xt/q ro-node "SELECT * FROM foo"))
              "ro node sees data after first block flush")

        (xt/submit-tx rw-node [[:put-docs :foo {:xt/id "b", :x 2, :y "hello"}]])
        (tu/flush-block! rw-node)
        (xt-log/sync-node ro-node #xt/duration "PT5S")

        (t/is (= #{{:xt/id "a", :x 1} {:xt/id "b", :x 2, :y "hello"}}
                 (set (xt/q ro-node "SELECT * FROM foo")))
              "ro node sees all data after second block flush")))))

(t/deftest read-only-node-updates-table-catalog-on-block-flush
  (util/with-tmp-dirs #{node-dir}
    (let [cfg {:log [:local {:path (.resolve node-dir "log")}]
               :storage [:local {:path (.resolve node-dir "objects")}]}]
      (with-open [rw-node (xtn/start-node cfg)
                  ro-node (-> (xtn/->config cfg)
                              (.readOnlyDatabases true)
                              (.open))]

        (xt/execute-tx rw-node [[:put-docs :foo {:xt/id "a", :x 1}]])
        (tu/flush-block! rw-node)

        (xt/execute-tx rw-node [[:put-docs :bar {:xt/id "b", :y 2}]])

        (t/is (= [{:xt/id "b", :y 2}]
                 (xt/q ro-node "SELECT * FROM bar"))
              "table in live index is visible")
        (t/is (= [{:xt/id "a", :x 1}]
                 (xt/q ro-node "SELECT * FROM foo"))
              "table flushed to block while ro node was running is visible")))))

(t/deftest read-only-node-loads-table-catalog-at-startup
  (util/with-tmp-dirs #{node-dir}
    (let [cfg {:log [:local {:path (.resolve node-dir "log")}]
               :storage [:local {:path (.resolve node-dir "objects")}]}]
      (with-open [rw-node (xtn/start-node cfg)]

        (xt/execute-tx rw-node [[:put-docs :foo {:xt/id "a", :x 1}]])
        (tu/flush-block! rw-node)

        (xt/execute-tx rw-node [[:put-docs :bar {:xt/id "b", :y 2}]])

        (with-open [ro-node (-> (xtn/->config cfg)
                                (.readOnlyDatabases true)
                                (.open))] 
          (t/is (= [{:xt/id "a", :x 1}]
                   (xt/q ro-node "SELECT * FROM foo"))
                "table from block flushed before startup is visible")
          (t/is (= [{:xt/id "b", :y 2}]
                   (xt/q ro-node "SELECT * FROM bar"))
                "table in live index is visible"))))))

(t/deftest read-only-node-handles-is-full-block-boundary
  ;; Exercises block processing when isFull() triggers the block (not FlushBlock).
  ;; Exercises the FollowerLogProcessor's stale check which previously dropped
  ;; BlockBoundary/BlockUploaded when latestProcessedMsgId == latestSourceMsgId.
  (util/with-tmp-dirs #{node-dir}
    (let [cfg {:log [:local {:path (.resolve node-dir "log")}]
               :storage [:local {:path (.resolve node-dir "objects")}]
               :indexer {:rows-per-block 3}
               :compactor {:threads 0}}]
      (with-open [rw-node (xtn/start-node cfg)
                  ro-node (-> (xtn/->config cfg)
                              (.readOnlyDatabases true)
                              (.open))]

        ;; 2 docs + 1 xt.txs row = 3 rows, triggers isFull() with rows-per-block=3
        (xt/execute-tx rw-node [[:put-docs :foo {:xt/id "a", :x 1}]
                                [:put-docs :foo {:xt/id "b", :x 2}]])

        ;; this tx lands after the block messages on the replica/source log,
        ;; so syncing on it guarantees the ro node has processed the block.
        (xt/execute-tx rw-node [[:put-docs :foo {:xt/id "c", :x 3}]])
        (xt-log/sync-node ro-node #xt/duration "PT5S")

        (t/is (= #{{:xt/id "a", :x 1} {:xt/id "b", :x 2} {:xt/id "c", :x 3}}
                 (set (xt/q ro-node "SELECT * FROM foo")))
              "ro node sees all data after isFull()-triggered block")

        (let [ro-block-cat (.getBlockCatalog (db/primary-db ro-node))]
          (t/is (= 0 (.getCurrentBlockIndex ro-block-cat))
                "follower's block catalog should advance after isFull()-triggered block"))))))