(ns xtdb.migration-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.compactor :as c]
            [xtdb.migration :as mig]
            [xtdb.node :as xtn]
            [xtdb.table-catalog :as table-cat]
            [xtdb.trie-catalog :as trie-cat]
            [xtdb.util :as util])
  (:import [java.io File]
           [java.nio.file Path]
           (xtdb.util HyperLogLog)))

;; to regenerate the test files, run this against 2.0.0-beta6
(comment
  (require '[xtdb.test-util :as tu]
           '[xtdb.time :as time])

  (let [node-dir (util/->path "src/test/resources/xtdb/migration-test/v05-v06/")]
    (util/delete-dir node-dir)

    (binding [c/*page-size* 32
              c/*ignore-signal-block?* true
              c/*l1-file-size-rows* 250]
      (util/with-open [node (tu/->local-node {:node-dir node-dir, :rows-per-block 120})]
        (doseq [tick (range 25)
                batch (->> (range 100)
                           (partition-all 64))
                :let [tick-at (-> (time/->zdt #inst "2020-01-01") (.plusHours (* tick 12)))]]
          (xt/execute-tx node [(into [:put-docs {:into :readings, :valid-from tick-at, :valid-to (.plusHours tick-at 12)}]
                                     (for [x batch]
                                       {:xt/id x, :reading tick}))

                               (into [:put-docs {:into :prices, :valid-from tick-at}]
                                     (for [x batch]
                                       {:xt/id x, :price tick}))]

                         {:system-time tick-at}))

        (c/compact-all! node #xt/duration "PT1S")))))

(t/deftest test-mig
  (let [node-root (util/->path "src/test/resources/xtdb/migration-test/v05-v06")]
    (util/delete-dir (.resolve node-root "new-log"))
    (util/delete-dir (.resolve node-root "objects/v06"))
    (util/copy-dir (.resolve node-root "log") (.resolve node-root "new-log"))

    (mig/migrate-from 5 {:storage [:local {:path (.resolve node-root "objects")}]})

    (letfn [(count-files [^Path key]
              (let [full-path (-> node-root (.resolve "objects/v06") (.resolve key))]
                (->> (.toFile full-path)
                     (file-seq)
                     (filter #(.exists ^File %))
                     (count))))]
      (t/is (= 26 (count-files (util/->path "blocks"))))
      (t/is (= 26 (count-files (util/->path "tables/public$prices/data"))))
      (t/is (= 26 (count-files (util/->path "tables/public$prices/blocks"))))
      (t/is (= 238 (count-files (util/->path "tables")))))

    (util/with-open [v6-node (xtn/start-node {:log [:local {:path (.resolve node-root "new-log")}]
                                              :storage [:local {:path (.resolve node-root "objects")}]})]

      (c/compact-all! v6-node #xt/duration "PT1S")

      (let [table-cat (table-cat/<-node v6-node)]
        (t/is (< 95 (HyperLogLog/estimate (table-cat/get-hll table-cat "public/prices" "_id")) 105))
        (t/is (< 20 (HyperLogLog/estimate (table-cat/get-hll table-cat "public/prices" "price")) 30))

        (t/is (< 95 (HyperLogLog/estimate (table-cat/get-hll table-cat "public/readings" "_id")) 105))
        (t/is (< 20 (HyperLogLog/estimate (table-cat/get-hll table-cat "public/readings" "reading")) 30)))

      (t/is (= [{:readings 2500}]
               (xt/q v6-node "SELECT COUNT(*) AS readings FROM readings FOR ALL VALID_TIME")))

      (t/is (= [{:el-readings 50}]
               (xt/q v6-node "SELECT COUNT(*) AS el_readings FROM readings FOR ALL VALID_TIME WHERE _id IN (5, 10)"))
            "check ID hashing")

      (t/is (= [{:matching-readings 100}]
               (xt/q v6-node "SELECT COUNT(*) matching_readings FROM readings FOR ALL VALID_TIME WHERE reading = 21"))
            "check value hashing")

      (t/is (= [{:prices 100}]
               (xt/q v6-node "SELECT COUNT(*) AS prices FROM prices")))

      (t/is (= [{:prices 2500}]
               (xt/q v6-node "SELECT COUNT(*) AS prices FROM prices FOR ALL VALID_TIME"))))))
