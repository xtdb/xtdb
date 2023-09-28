(ns xtdb.compactor-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.compactor :as c]
            [xtdb.indexer.live-index :as li]
            [xtdb.test-json :as tj]
            [xtdb.test-util :as tu]
            [xtdb.util :as util]))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-compaction-jobs
  (letfn [(f [table-tries]
            (c/compaction-jobs "foo"
                               (for [[level rf rt] table-tries]
                                 {:level level, :row-from rf, :row-to rt})))]
    (t/is (= [] (f [])))

    (t/is (= []
             (f [[0 0 1] [0 1 2] [0 2 3]])))

    (t/is (= [{:table-name "foo",
               :table-tries [{:level 0, :row-from 0, :row-to 1}
                             {:level 0, :row-from 1, :row-to 2}
                             {:level 0, :row-from 2, :row-to 3}
                             {:level 0, :row-from 3, :row-to 4}],
               :out-trie-key "l01-cf00-ct04"}]
             (f [[0 0 1] [0 1 2] [0 2 3] [0 3 4]])))

    (t/is (= []
             (f [[1 0 2] [1 2 4] [1 4 6]
                 [0 0 1] [0 1 2] [0 2 3] [0 3 4] [0 4 5] [0 5 6] [0 6 7] [0 7 8]])))

    (t/is (= [{:table-name "foo",
               :table-tries [{:level 1, :row-from 0, :row-to 2}
                             {:level 1, :row-from 2, :row-to 4}
                             {:level 1, :row-from 4, :row-to 6}
                             {:level 1, :row-from 6, :row-to 8}],
               :out-trie-key "l02-cf00-ct08"}]
             (f [[1 0 2] [1 2 4] [1 4 6] [1 6 8]
                 [0 0 1] [0 1 2] [0 2 3] [0 3 4] [0 4 5] [0 5 6] [0 6 7] [0 7 8]])))

    (t/is (= []
             (f [[2 0 4]
                 [1 0 2] [1 2 4] [1 4 6] [1 6 8]
                 [0 0 1] [0 1 2] [0 2 3] [0 3 4] [0 4 5] [0 5 6] [0 6 7] [0 7 8]])))))

(t/deftest test-merges-tries
  (let [expected-dir (.toPath (io/as-file (io/resource "xtdb/compactor-test/test-merges-tries")))
        tmp-dir (doto (util/->path "target/compactor/test-merges-tries")
                  util/delete-dir
                  util/mkdirs)]

    (util/with-open [lt0 (tu/open-live-table "foo")
                     lt1 (tu/open-live-table "foo")
                     leaf-out-ch (util/->file-channel (.resolve tmp-dir "leaf.arrow") util/write-truncate-open-opts)
                     trie-out-ch (util/->file-channel (.resolve tmp-dir "trie.arrow") util/write-truncate-open-opts)]

      (tu/index-tx! lt0 #xt/tx-key {:tx-id 0, :system-time #time/instant "2020-01-01T00:00:00Z"}
                    [{:xt/id "foo", :v 0}
                     {:xt/id "bar", :v 0}])

      (tu/index-tx! lt0 #xt/tx-key {:tx-id 1, :system-time #time/instant "2021-01-01T00:00:00Z"}
                    [{:xt/id "bar", :v 1}])

      (tu/index-tx! lt1 #xt/tx-key {:tx-id 2, :system-time #time/instant "2022-01-01T00:00:00Z"}
                    [{:xt/id "foo", :v 1}])

      (tu/index-tx! lt1 #xt/tx-key {:tx-id 3, :system-time #time/instant "2023-01-01T00:00:00Z"}
                    [{:xt/id "foo", :v 2}
                     {:xt/id "bar", :v 2}])

      (c/merge-tries! tu/*allocator*
                      [(.compactLogs (li/live-trie lt0)) (.compactLogs (li/live-trie lt1))]
                      [(tu/->live-leaf-loader lt0) (tu/->live-leaf-loader lt1)]
                      leaf-out-ch trie-out-ch))

    (tj/check-json expected-dir tmp-dir)))

(t/deftest test-e2e
  (let [expected-dir (.toPath (io/as-file (io/resource "xtdb/compactor-test/test-e2e")))
        node-dir (util/->path "target/compactor/test-e2e")]
    (util/delete-dir node-dir)

    (with-open [node (tu/->local-node {:node-dir node-dir, :rows-per-chunk 10})]
      (letfn [(submit! [xs]
                (doseq [batch (partition-all 8 xs)]
                  (xt/submit-tx node (for [x batch]
                                       [:put :foo {:xt/id x}]))))

              (q []
                (->> (xt/q node
                           '{:find [id]
                             :where [($ :foo {:xt/id id})]
                             :order-by [[id]]})
                     (map :id)))]

        (submit! (range 100))
        (tu/then-await-tx node)
        (c/compact-all! node)
        (t/is (= (range 100) (q)))

        (submit! (range 100 200))
        (tu/then-await-tx node)
        (c/compact-all! node)
        (t/is (= (range 200) (q)))

        (tj/check-json expected-dir (.resolve node-dir "objects/tables/foo") #"(trie|leaf)-l01-(.+)\.arrow")

        (t/testing "second level"
          (submit! (range 200 500))
          (tu/then-await-tx node)
          (c/compact-all! node)

          (t/is (= (range 500) (q)))

          (tj/check-json expected-dir
                         (.resolve node-dir "objects/tables/foo")
                         #"(trie|leaf)-l02-(.+)\.arrow"))))))
