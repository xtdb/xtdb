(ns xtdb.compactor-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.compactor :as c]
            [xtdb.indexer.live-index :as li]
            [xtdb.node :as xtn]
            [xtdb.test-json :as tj]
            [xtdb.test-util :as tu]
            [xtdb.time :as time]
            [xtdb.trie :as trie]
            [xtdb.trie-test :refer [->trie-file-name]]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw])
  (:import [java.time Duration]))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-compaction-jobs
  (letfn [(f [tries]
            (->> (c/compaction-jobs (map ->trie-file-name tries)
                                    {:l1-file-size-rows 16})

                 (mapv (fn [{:keys [path] :as job}]
                         (cond-> job
                           path (update :path vec))))))]

    (t/testing "l0 -> l1"
      (t/is (= [] (f [])))

      (t/is (= [{:trie-keys ["log-l00-nr01-rsa" "log-l00-nr02-rsa"]
                 :out-trie-key "log-l01-nr02-rs14"}]
               (f [[0 1 10] [0 2 10] [0 3 10]]))
            "no L1s yet, merge L0s up to limit and stop")

      (t/is (= [{:trie-keys ["log-l01-nr01-rsa" "log-l00-nr02-rsa"]
                 :out-trie-key "log-l01-nr02-rs14"}]
               (f [[0 1 10] [0 2 10] [0 3 10]
                   [1 1 10]]))
            "have a partial L1, merge into that until it's full")

      (t/is (= [] (f [[0 1 10] [0 2 10]
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

      (t/is (= [] (f [[0 1 10] [0 2 10] [0 3 10] [0 4 10] [0 5 10]
                      [1 2 20] [1 4 20] [1 5 10]]))
            "all merged, nothing to do"))

    (t/testing "l1 -> l2"
      (t/is (= (let [l1-trie-keys ["log-l01-nr02-rs14" "log-l01-nr04-rs14" "log-l01-nr06-rs14" "log-l01-nr08-rs14"]]
                 [{:trie-keys l1-trie-keys,
                   :path [0],
                   :out-trie-key "log-l02-p0-nr08"}
                  {:trie-keys l1-trie-keys,
                   :path [1],
                   :out-trie-key "log-l02-p1-nr08"}
                  {:trie-keys l1-trie-keys,
                   :path [2],
                   :out-trie-key "log-l02-p2-nr08"}
                  {:trie-keys l1-trie-keys,
                   :path [3],
                   :out-trie-key "log-l02-p3-nr08"}])
               (f [[1 2 20] [1 4 20] [1 6 20] [1 8 20]]))

            "empty L2")

      (t/is (= [{:trie-keys ["log-l01-nr02-rs14" "log-l01-nr04-rs14" "log-l01-nr06-rs14" "log-l01-nr08-rs14"],
                 :path [1],
                 :out-trie-key "log-l02-p1-nr08"}]
               (f [[2 [0] 8] [2 [2] 8] [2 [3] 8]
                   [1 2 20] [1 4 20] [1 6 20] [1 8 20]
                   [0 1 10] [0 2 10] [0 3 10] [0 4 10] [0 5 10] [0 6 10] [0 7 10] [0 8 10]]))
            "still needs L2 [1]"))

    (t/testing "L2+"
      (t/is (= [;; L2 [0] is full, compact L3 [0 2] and [0 3]
                {:trie-keys ["log-l02-p0-nr08" "log-l02-p0-nr110" "log-l02-p0-nr118" "log-l02-p0-nr120"],
                 :path [0 0],
                 :out-trie-key "log-l03-p00-nr120"}
                {:trie-keys ["log-l02-p0-nr08" "log-l02-p0-nr110" "log-l02-p0-nr118" "log-l02-p0-nr120"],
                 :path [0 1],
                 :out-trie-key "log-l03-p01-nr120"}

                ;; L2 [0] has loads, merge from 0x24 onwards (but only 4)
                {:trie-keys ["log-l01-nr124-rs14" "log-l01-nr128-rs14" "log-l01-nr12c-rs14" "log-l01-nr130-rs14"],
                 :path [0],
                 :out-trie-key "log-l02-p0-nr130"}

                ;; L2 [1] has nothing, choose the first four
                {:trie-keys ["log-l01-nr08-rs14" "log-l01-nr0c-rs14" "log-l01-nr110-rs14" "log-l01-nr114-rs14"],
                 :path [1],
                 :out-trie-key "log-l02-p1-nr114"}

                ;; fill in the gaps in [2] and [3]
                {:trie-keys ["log-l01-nr11c-rs14" "log-l01-nr120-rs14" "log-l01-nr124-rs14" "log-l01-nr128-rs14"],
                 :path [2],
                 :out-trie-key "log-l02-p2-nr128"}
                {:trie-keys ["log-l01-nr114-rs14" "log-l01-nr118-rs14" "log-l01-nr11c-rs14" "log-l01-nr120-rs14"],
                 :path [3],
                 :out-trie-key "log-l02-p3-nr120"}]

               (f [[3 [0 2] 32]
                   [3 [0 3] 32]
                   [2 [0] 8] [2 [2] 8] [2 [3] 8]
                   [2 [0] 16] [2 [2] 16] [2 [3] 16]
                   [2 [0] 24] [2 [2] 24]
                   [2 [0] 32]
                   [1 8 20] [1 12 20] [1 16 20] [1 20 20] [1 24 20]
                   [1 28 20] [1 32 20] [1 36 20] [1 40 20]
                   [1 44 20] [1 48 20] [1 50 20] [1 52 20]]))
            "up to L3")

      (t/is (= [{:trie-keys ["log-l03-p03-nr120" "log-l03-p03-nr140" "log-l03-p03-nr160" "log-l03-p03-nr180"],
                 :path [0 3 0],
                 :out-trie-key "log-l04-p030-nr180"}
                {:trie-keys ["log-l03-p03-nr120" "log-l03-p03-nr140" "log-l03-p03-nr160" "log-l03-p03-nr180"],
                 :path [0 3 1],
                 :out-trie-key "log-l04-p031-nr180"}
                {:trie-keys ["log-l03-p03-nr120" "log-l03-p03-nr140" "log-l03-p03-nr160" "log-l03-p03-nr180"],
                 :path [0 3 2],
                 :out-trie-key "log-l04-p032-nr180"}
                {:trie-keys ["log-l03-p03-nr120" "log-l03-p03-nr140" "log-l03-p03-nr160" "log-l03-p03-nr180"],
                 :path [0 3 3],
                 :out-trie-key "log-l04-p033-nr180"}]

               (f [[3 [0 2] 32]
                   [3 [0 3] 32] [3 [0 3] 64] [3 [0 3] 96] [3 [0 3] 128]]))
            "L3 -> L4"))))

(t/deftest test-merges-segments
  (util/with-open [lt0 (tu/open-live-table "foo")
                   lt1 (tu/open-live-table "foo")]

    (tu/index-tx! lt0 #xt/tx-key {:tx-id 0, :system-time #xt.time/instant "2020-01-01T00:00:00Z"}
                  [{:xt/id "foo", :v 0}
                   {:xt/id "bar", :v 0}])

    (tu/index-tx! lt0 #xt/tx-key {:tx-id 1, :system-time #xt.time/instant "2021-01-01T00:00:00Z"}
                  [{:xt/id "bar", :v 1}])

    (tu/index-tx! lt1 #xt/tx-key {:tx-id 2, :system-time #xt.time/instant "2022-01-01T00:00:00Z"}
                  [{:xt/id "foo", :v 1}])

    (tu/index-tx! lt1 #xt/tx-key {:tx-id 3, :system-time #xt.time/instant "2023-01-01T00:00:00Z"}
                  [{:xt/id "foo", :v 2}
                   {:xt/id "bar", :v 2}])

    (let [segments [(-> (trie/->Segment (.compactLogs (li/live-trie lt0)))
                        (assoc :data-rel (tu/->live-data-rel lt0)))
                    (-> (trie/->Segment (.compactLogs (li/live-trie lt1)))
                        (assoc :data-rel (tu/->live-data-rel lt1)))]]

      (t/testing "merge segments"
        (util/with-open [data-rel-wtr (trie/open-log-data-wtr tu/*allocator* (c/->log-data-rel-schema (map :data-rel segments)))
                         recency-wtr (c/open-recency-wtr tu/*allocator*)]

          (c/merge-segments-into data-rel-wtr recency-wtr segments nil)

          (t/is (= [{:xt/iid #uuid "9e3f856e-6899-8313-827f-f18dd4d88e78",
                     :xt/system-from (time/->zdt #inst "2023")
                     :xt/valid-from (time/->zdt #inst "2023")
                     :op {:v 2, :xt/id "bar"}}
                    {:xt/iid #uuid "9e3f856e-6899-8313-827f-f18dd4d88e78",
                     :xt/system-from (time/->zdt #inst "2021")
                     :xt/valid-from (time/->zdt #inst "2021")
                     :op {:v 1, :xt/id "bar"}}
                    {:xt/iid #uuid "9e3f856e-6899-8313-827f-f18dd4d88e78",
                     :xt/system-from (time/->zdt #inst "2020")
                     :xt/valid-from (time/->zdt #inst "2020")
                     :op {:v 0, :xt/id "bar"}}
                    {:xt/iid #uuid "d9c7fae2-a04e-0471-6493-6265ba33cf80",
                     :xt/system-from (time/->zdt #inst "2023")
                     :xt/valid-from (time/->zdt #inst "2023")
                     :op {:v 2, :xt/id "foo"}}
                    {:xt/iid #uuid "d9c7fae2-a04e-0471-6493-6265ba33cf80",
                     :xt/system-from (time/->zdt #inst "2022")
                     :xt/valid-from (time/->zdt #inst "2022")
                     :op {:v 1, :xt/id "foo"}}
                    {:xt/iid #uuid "d9c7fae2-a04e-0471-6493-6265ba33cf80",
                     :xt/system-from (time/->zdt #inst "2020")
                     :xt/valid-from (time/->zdt #inst "2020")
                     :op {:v 0, :xt/id "foo"}}]

                   (-> (vw/rel-wtr->rdr data-rel-wtr)
                       (vr/rel->rows)
                       (->> (mapv #(update % :xt/iid util/byte-buffer->uuid))))))

          (t/is (= [nil (time/->zdt #inst "2023") (time/->zdt #inst "2021")
                    nil (time/->zdt #inst "2023") (time/->zdt #inst "2022")]
                   (-> recency-wtr vw/vec-wtr->rdr tu/vec->vals)))))

      (t/testing "merge segments with path predicate"
        (util/with-open [data-rel-wtr (trie/open-log-data-wtr tu/*allocator* (c/->log-data-rel-schema (map :data-rel segments)))
                         recency-wtr (c/open-recency-wtr tu/*allocator*)]

          (c/merge-segments-into data-rel-wtr recency-wtr segments (byte-array [2]))

          (t/is (= [{:xt/iid #uuid "9e3f856e-6899-8313-827f-f18dd4d88e78",
                     :xt/system-from (time/->zdt #inst "2023")
                     :xt/valid-from (time/->zdt #inst "2023")
                     :op {:v 2, :xt/id "bar"}}
                    {:xt/iid #uuid "9e3f856e-6899-8313-827f-f18dd4d88e78",
                     :xt/system-from (time/->zdt #inst "2021")
                     :xt/valid-from (time/->zdt #inst "2021")
                     :op {:v 1, :xt/id "bar"}}
                    {:xt/iid #uuid "9e3f856e-6899-8313-827f-f18dd4d88e78",
                     :xt/system-from (time/->zdt #inst "2020")
                     :xt/valid-from (time/->zdt #inst "2020")
                     :op {:v 0, :xt/id "bar"}}]

                   (-> (vw/rel-wtr->rdr data-rel-wtr)
                       (vr/rel->rows)
                       (->> (mapv #(update % :xt/iid util/byte-buffer->uuid))))))

          (t/is (= [nil (time/->zdt #inst "2023") (time/->zdt #inst "2021")]
                   (-> recency-wtr vw/vec-wtr->rdr tu/vec->vals))))))))

(t/deftest test-l1-compaction
  (let [node-dir (util/->path "target/compactor/test-l1-compaction")]
    (util/delete-dir node-dir)

    (binding [c/*page-size* 32
              c/*l1-file-size-rows* 256
              c/*ignore-signal-block?* true]
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
          (c/compact-all! node (Duration/ofSeconds 5))

          (t/is (= (range 100) (q)))

          (submit! (range 100 200))
          (tu/then-await-tx node)
          (c/compact-all! node (Duration/ofSeconds 5))

          (t/is (= (range 200) (q)))

          (submit! (range 200 500))
          (tu/then-await-tx node)
          (c/compact-all! node (Duration/ofSeconds 5))

          (t/is (= (range 500) (q)))

          (tj/check-json (.toPath (io/as-file (io/resource "xtdb/compactor-test/test-l1-compaction")))
                         (.resolve node-dir "objects/v02/tables/foo") #"log-l01-(.+)\.arrow"))))))

(t/deftest test-l2+-compaction
  (let [node-dir (util/->path "target/compactor/test-l2+-compaction")]
    (util/delete-dir node-dir)

    (binding [c/*page-size* 8
              c/*l1-file-size-rows* 32]
      (util/with-open [node (tu/->local-node {:node-dir node-dir, :rows-per-chunk 10})]
        (letfn [(submit! [xs]
                  (doseq [batch (partition-all 6 xs)]
                    (xt/submit-tx node [(into [:put-docs :foo]
                                              (for [x batch]
                                                {:xt/id x}))])))

                (q []
                  (->> (xt/q node
                             '(-> (from :foo [{:xt/id id}])
                                  (order-by id)))
                       (map :id)))]

          (submit! (range 500))
          (tu/then-await-tx node)
          (c/compact-all! node (Duration/ofSeconds 5))

          (t/is (= (set (range 500)) (set (q))))

          (submit! (range 500 1000))
          (tu/then-await-tx node)
          (c/compact-all! node (Duration/ofSeconds 5))

          (t/is (= (set (range 1000)) (set (q))))

          (submit! (range 1000 2000))
          (tu/then-await-tx node)
          (c/compact-all! node (Duration/ofSeconds 5))

          (t/is (= (set (range 2000)) (set (q))))

          (tj/check-json (.toPath (io/as-file (io/resource "xtdb/compactor-test/test-l2+-compaction")))
                         (.resolve node-dir "objects/v02/tables/foo") #"log-l(?!00|01)\d\d-(.+)\.arrow"))))))

(t/deftest test-l2-compaction-badly-distributed
  (let [node-dir (util/->path "target/compactor/test-l1-compaction-badly-distributed")]
    (util/delete-dir node-dir)

    (binding [c/*page-size* 8
              c/*l1-file-size-rows* 32]
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

          (submit! (tu/bad-uuid-seq 100))
          (tu/then-await-tx node)
          (c/compact-all! node (Duration/ofSeconds 5))

          (t/is (= (tu/bad-uuid-seq 100) (q)))

          (submit! (tu/bad-uuid-seq 100 200))
          (tu/then-await-tx node)
          (c/compact-all! node (Duration/ofSeconds 5))

          (t/is (= (tu/bad-uuid-seq 200) (q)))

          (submit! (tu/bad-uuid-seq 200 500))
          (tu/then-await-tx node)
          (c/compact-all! node (Duration/ofSeconds 5))

          (t/is (= (tu/bad-uuid-seq 500) (q))))))))

(t/deftest test-more-than-a-page-of-versions
  (let [node-dir (util/->path "target/compactor/test-more-than-a-page-of-versions")]
    (util/delete-dir node-dir)

    (binding [c/*page-size* 8
              c/*l1-file-size-rows* 32
              c/*ignore-signal-block?* true]
      (util/with-open [node (tu/->local-node {:node-dir node-dir, :rows-per-chunk 10})]
        (dotimes [n 100]
          (xt/submit-tx node [[:put-docs :foo {:xt/id "foo", :v n}]]))
        (tu/then-await-tx node)
        (c/compact-all! node (Duration/ofSeconds 5))

        (t/is (= [{:foo-count 100}] (xt/q node "SELECT COUNT(*) foo_count FROM foo FOR ALL VALID_TIME")))))

    (tj/check-json (.toPath (io/as-file (io/resource "xtdb/compactor-test/test-more-than-a-page-of-versions")))
                   (.resolve node-dir "objects/v02/tables/foo") #"log-l01-(.+)\.arrow")))

(t/deftest losing-data-when-compacting-3459
  (binding [c/*page-size* 8
            c/*l1-file-size-rows* 32]
    (let [node-dir (util/->path "target/compactor/lose-data-on-compaction")]
      (util/delete-dir node-dir)

      (util/with-open [node (tu/->local-node {:node-dir node-dir
                                              :rows-per-chunk 32
                                              :page-limit 8
                                              :log-limit 4})]

        (dotimes [v 12]
          (xt/execute-tx node [[:put-docs :docs {:xt/id 0, :v v} {:xt/id 1, :v v}]]))

        (c/compact-all! node)

        (t/is (= #{{:xt/id 0, :count 12} {:xt/id 1, :count 12}}
                 (set (xt/q node "SELECT _id, count(*) count
                             FROM docs FOR ALL VALID_TIME
                             GROUP BY _id"))))

        (tj/check-json (.toPath (io/as-file (io/resource "xtdb/compactor-test/lose-data-on-compaction")))
                       (.resolve node-dir "objects/v02/tables/docs") #"log-(.+)\.arrow")))))
