(ns xtdb.compactor-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.compactor :as c]
            [xtdb.indexer.live-index :as li]
            [xtdb.test-json :as tj]
            [xtdb.test-util :as tu]
            [xtdb.time :as time]
            [xtdb.trie :as trie]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw]))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-l0->l1-compaction-job
  (letfn [(f [tries]
            (c/compaction-jobs (for [[level nr rows] tries]
                                 (trie/->table-meta-file-path (util/->path "tables/foo")
                                                              (trie/->log-trie-key level nr rows)))
                               {:l1-file-size-rows 16}))]
    (t/is (nil? (f [])))

    (t/is (= [{:trie-keys ["log-l00-nr01-rsa" "log-l00-nr02-rsa"]
               :out-trie-key "log-l01-nr02-rs14"}]
             (f [[0 1 10] [0 2 10] [0 3 10]]))
          "no L1s yet, merge L0s up to limit and stop")

    (t/is (= [{:trie-keys ["log-l01-nr01-rsa" "log-l00-nr02-rsa"]
               :out-trie-key "log-l01-nr02-rs14"}]
             (f [[0 1 10] [0 2 10] [0 3 10]
                 [1 1 10]]))
          "have a partial L1, merge into that until it's full")

    (t/is (nil? (f [[0 1 10] [0 2 10]
                    [1 2 10]]))
          "all merged, nothing to do")

    (t/is (= [{:trie-keys ["log-l00-nr03-rsa" "log-l00-nr04-rsa"],
               :out-trie-key "log-l01-nr04-rs14"}]
             (f [[0 1 10] [0 2 10] [0 3 10] [0 4 10] [0 5 10]
                 [1 2 20]]))
          "have a full L1, start a new L1 til that's full")

    (t/is (= [{:trie-keys ["log-l00-nr03-rsa" "log-l00-nr04-rsa"],
               :out-trie-key "log-l01-nr04-rs14"}]
             (f [[0 1 10] [0 2 10] [0 3 10] [0 4 10] [0 5 10]
                 [1 2 20]]))
          "have a full L1, start a new L1 til that's full")

    (t/is (= [{:trie-keys ["log-l01-nr03-rsa" "log-l00-nr04-rsa"],
               :out-trie-key "log-l01-nr04-rs14"}]
             (f [[0 1 10] [0 2 10] [0 3 10] [0 4 10] [0 5 10]
                 [1 2 20] [1 3 10]]))
          "have a full and a partial L1, merge into that til it's full")

    (t/is (nil? (f [[0 1 10] [0 2 10] [0 3 10] [0 4 10] [0 5 10]
                    [1 2 20] [1 4 20] [1 5 10]]))
          "all merged, nothing to do")

    #_#_
    (t/is (= [{:table-path (util/->path "tables/foo"),
               :trie-keys ["log-l01-nr02-rs2"
                           "log-l01-nr04-rs2"
                           "log-l01-nr06-rs2"
                           "log-l01-nr08-rs2"],
               :out-trie-key "log-l02-nr08-rs8"}]
             (f [[1 2] [1 4] [1 6] [1 8]
                 [0 1] [0 2] [0 3] [0 4] [0 5] [0 6] [0 7] [0 8]])))

    (t/is (= []
             (f [[2 4]
                 [1 2] [1 4] [1 6] [1 8]
                 [0 1] [0 2] [0 3] [0 4] [0 5] [0 6] [0 7] [0 8]])))))

(t/deftest test-merges-segments
  (util/with-open [lt0 (tu/open-live-table "foo")
                   lt1 (tu/open-live-table "foo")]

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

    (let [segments [{:trie (.compactLogs (li/live-trie lt0)), :data-rel (tu/->live-data-rel lt0)}
                    {:trie (.compactLogs (li/live-trie lt1)), :data-rel (tu/->live-data-rel lt1)}]]

      (util/with-open [data-rel-wtr (trie/open-log-data-wtr tu/*allocator* (c/->log-data-rel-schema (map :data-rel segments)))
                       recency-wtr (c/open-recency-wtr tu/*allocator*)]

        (c/merge-segments-into data-rel-wtr recency-wtr segments)

        (t/is (= [{:xt/iid #uuid "9e3f856e-6899-8313-827f-f18dd4d88e78",
                   :xt/system-from (time/->zdt #inst "2023")
                   :xt/valid-from (time/->zdt #inst "2023")
                   :xt/valid-to nil,
                   :op {:v 2, :xt/id "bar"}}
                  {:xt/iid #uuid "9e3f856e-6899-8313-827f-f18dd4d88e78",
                   :xt/system-from (time/->zdt #inst "2021")
                   :xt/valid-from (time/->zdt #inst "2021")
                   :xt/valid-to nil,
                   :op {:v 1, :xt/id "bar"}}
                  {:xt/iid #uuid "9e3f856e-6899-8313-827f-f18dd4d88e78",
                   :xt/system-from (time/->zdt #inst "2020")
                   :xt/valid-from (time/->zdt #inst "2020")
                   :xt/valid-to nil,
                   :op {:v 0, :xt/id "bar"}}
                  {:xt/iid #uuid "d9c7fae2-a04e-0471-6493-6265ba33cf80",
                   :xt/system-from (time/->zdt #inst "2023")
                   :xt/valid-from (time/->zdt #inst "2023")
                   :xt/valid-to nil,
                   :op {:v 2, :xt/id "foo"}}
                  {:xt/iid #uuid "d9c7fae2-a04e-0471-6493-6265ba33cf80",
                   :xt/system-from (time/->zdt #inst "2022")
                   :xt/valid-from (time/->zdt #inst "2022")
                   :xt/valid-to nil,
                   :op {:v 1, :xt/id "foo"}}
                  {:xt/iid #uuid "d9c7fae2-a04e-0471-6493-6265ba33cf80",
                   :xt/system-from (time/->zdt #inst "2020")
                   :xt/valid-from (time/->zdt #inst "2020")
                   :xt/valid-to nil,
                   :op {:v 0, :xt/id "foo"}}]

                 (-> (vw/rel-wtr->rdr data-rel-wtr)
                     (vr/rel->rows)
                     (->> (mapv #(update % :xt/iid util/byte-buffer->uuid))))))

        (t/is (= [nil (time/->zdt #inst "2023") (time/->zdt #inst "2021")
                  nil (time/->zdt #inst "2023") (time/->zdt #inst "2022")]
                 (-> recency-wtr vw/vec-wtr->rdr tu/vec->vals)))))))

(t/deftest test-l1-compaction
  (let [node-dir (util/->path "target/compactor/test-l1-compaction")]
    (util/delete-dir node-dir)

    (binding [c/*page-size* 32
              c/*l1-file-size-rows* 256]
      (util/with-open [node (tu/->local-node {:node-dir node-dir, :rows-per-chunk 10})]
        (letfn [(submit! [xs]
                  (doseq [batch (partition-all 8 xs)]
                    (xt/submit-tx node [(into [:put-docs :foo]
                                              (for [x batch]
                                                {:xt/id x}))])))

                (q []
                  (->> (xt/q node
                             '(-> (from :foo [{:xt/id id}])
                                  (order-by id)))
                       (map :id)))]

          (submit! (range 100))
          (tu/then-await-tx node)
          (c/compact-all! node)

          (t/is (= (range 100) (q)))

          (submit! (range 100 200))
          (tu/then-await-tx node)
          (c/compact-all! node)

          (t/is (= (range 200) (q)))

          (submit! (range 200 500))
          (tu/then-await-tx node)
          (c/compact-all! node)

          (t/is (= (range 500) (q)))

          (tj/check-json (.toPath (io/as-file (io/resource "xtdb/compactor-test/test-l1-compaction")))
                         (.resolve node-dir "objects/v01/tables/foo") #"log-l01-(.+)\.arrow")

          #_ ; temporarily disabled
          (t/testing "second level"
            (submit! (range 200 500))
            (tu/then-await-tx node)
            (c/compact-all! node)

            (t/is (= (range 500) (q)))

            (tj/check-json (.toPath (io/as-file (io/resource "xtdb/compactor-test/test-e2e-level-2")))
                           (.resolve node-dir "objects/v01/tables/foo")
                           #"log-l0(?:1|2)-(.+)\.arrow")))))))
