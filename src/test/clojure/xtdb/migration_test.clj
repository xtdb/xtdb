(ns xtdb.migration-test
  (:require [clojure.test :as t :refer [deftest]]
            [xtdb.util :as util]
            [xtdb.node :as xtn]
            [xtdb.api :as xt]
            [xtdb.compactor :as c]
            [xtdb.test-util :as tu]
            ))

(comment
  ;; to see the ignored messages
  (require '[xtdb.logging :refer [set-log-level!]])

  (set-log-level! "xtdb.indexer" :trace)
  )

(deftest test-v06-log-is-readable-from-beta6
  (let [node-root (util/->path "src/test/resources/xtdb/migration-test/v05-v06")]
    (util/delete-dir node-root)
    (util/copy-dir
     (util/->path "src/test/resources/xtdb/migration-test/v06-log/log/")
     (util/->path "src/test/resources/xtdb/migration-test/v05-v06/log/"))

    (util/with-open [v5-node (xtn/start-node {:log [:local {:path (.resolve node-root "log")}]
                                              :storage [:local {:path (.resolve node-root "objects")}]})]

      ;; once compacted, once hopefully uncompacted
      (dotimes [_ 2]

        (t/is (= [{:readings 2500}]
                 (xt/q v5-node "SELECT COUNT(*) AS readings FROM readings FOR ALL VALID_TIME")))

        (t/is (= [{:el-readings 50}]
                 (xt/q v5-node "SELECT COUNT(*) AS el_readings FROM readings FOR ALL VALID_TIME WHERE _id IN (5, 10)"))
              "check ID hashing")

        (t/is (= [{:matching-readings 100}]
                 (xt/q v5-node "SELECT COUNT(*) matching_readings FROM readings FOR ALL VALID_TIME WHERE reading = 21"))
              "check value hashing")

        (t/is (= [{:prices 100}]
                 (xt/q v5-node "SELECT COUNT(*) AS prices FROM prices")))

        (t/is (= [{:prices 2500}]
                 (xt/q v5-node "SELECT COUNT(*) AS prices FROM prices FOR ALL VALID_TIME")))

        (tu/finish-block! v5-node)
        (c/compact-all! v5-node))

      ;; making sure we can still submit + read new data
      (xt/execute-tx v5-node
                     [(into [:put-docs :readings] (for [x (range 100)]
                                                    {:xt/id (+ 2500 x) , :reading 0}))])

      (dotimes [_ 2]
        (t/is (= [{:readings 2600}]
                 (xt/q v5-node "SELECT COUNT(*) AS readings FROM readings FOR ALL VALID_TIME")))
        (tu/finish-block! v5-node)
        (c/compact-all! v5-node)))))
