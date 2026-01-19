(ns xtdb.read-only-node-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.log :as xt-log]
            [xtdb.node :as xtn]
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

        (xt-log/sync-node ro-node)

        (t/is (= [{:xt/id "from-rw"}]
                 (xt/q ro-node "SELECT * FROM foo"))
              "read-only node can read data written by read-write node")

        (t/is (thrown-with-msg? Exception #"read-only"
                                (xt/submit-tx ro-node [[:put-docs :foo {:xt/id "from-ro"}]]))
              "read-only node rejects writes")

        (xt/submit-tx rw-node [[:put-docs :foo {:xt/id "from-rw", :v 2}]])
        (xt-log/sync-node ro-node)

        (t/is (= [{:xt/id "from-rw", :v 2}]
                 (xt/q ro-node "SELECT * FROM foo"))
              "read-only node sees updated data from read-write node")))))
