(ns xtdb.compactor-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.buffer-pool :as bp]
            [xtdb.compactor :as c]
            [xtdb.indexer.live-index :as li]
            [xtdb.test-json :as tj]
            [xtdb.test-util :as tu]
            [xtdb.time :as time]
            [xtdb.trie :as trie]
            [xtdb.trie-test :refer [->trie-file-name]]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw])
  (:import java.lang.AutoCloseable
           [java.time Duration]
           org.apache.arrow.vector.types.pojo.Schema
           [xtdb.trie LiveHashTrie$Leaf IDataRel]
           [xtdb.vector IVectorReader RelationReader]))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-compaction-jobs
  (letfn [(f [tries & {:keys [l1-file-size-rows]}]
            (->> (c/compaction-jobs (map ->trie-file-name tries)
                                    {:l1-file-size-rows (or l1-file-size-rows 16)})

                 (mapv (fn [{:keys [path] :as job}]
                         (cond-> job
                           path (update :path vec))))))]

    (->trie-file-name [0 10 20 10])

    (t/testing "l0 -> l1"
      (t/is (= [] (f [])))

      (t/is (= [{:trie-keys ["log-l00-fr00-nr0a-rsa" "log-l00-fr0a-nr114-rsa"]
                 :out-trie-key "log-l01-fr00-nr114-rs14",} ]
               (f [[0 0 10 10] [0 10 20 10] [0 20 30 10]]))
            "no L1s yet, merge L0s up to limit and stop")

      (t/is (= [{:trie-keys ["log-l01-fr00-nr0a-rsa" "log-l00-fr0a-nr114-rsa"],
                 :out-trie-key "log-l01-fr00-nr114-rs14"            }]
               (f [[0 0 10 10] [0 10 20 10] [0 20 30 10]
                   [1 0 10 10]]))
            "have a partial L1, merge into that until it's full")

      (t/is (= [] (f [[0 0 10 10] [0 10 20 10]
                      [1 0 20 20]]))
            "all merged, nothing to do")

      (t/is (= [{:out-trie-key "log-l01-fr114-nr128-rs14",
                 :trie-keys ["log-l00-fr114-nr11e-rsa" "log-l00-fr11e-nr128-rsa"]}
                ]
               (f [[0 0 10 10] [0 10 20 10] [0 20 30 10] [0 30 40 10] [0 40 50 10]
                   [1 0 20 20]]))
            "have a full L1, start a new L1 til that's full")

      (t/is (= [{:trie-keys ["log-l01-fr114-nr11e-rsa" "log-l00-fr11e-nr128-rsa"],
                 :out-trie-key "log-l01-fr114-nr128-rs14"} ]
               (f [[0 0 10 10] [0 10 20 10] [0 20 30 10] [0 30 40 10] [0 40 50 10]
                   [1 0 20 20] [1 20 30 10]]))
            "have a full and a partial L1, merge into that til it's full")

      (t/is (= [] (f [[0 0 10 10] [0 10 20 10] [0 20 30 10] [0 30 40 10] [0 40 50 10]
                      [1 0 20 20] [1 20 40 20] [1 40 50 10]]))
            "all merged, nothing to do"))

    (t/testing "l1 -> l2"
      (t/is (= (let [l1-trie-keys ["log-l01-fr00-nr114-rs14" "log-l01-fr114-nr128-rs14"
                                   "log-l01-fr128-nr13c-rs14" "log-l01-fr13c-nr150-rs14"]]
                 [{:trie-keys l1-trie-keys,
                   :path [0],
                   :out-trie-key "log-l02-p0-nr150"}
                  {:trie-keys l1-trie-keys,
                   :path [1],
                   :out-trie-key "log-l02-p1-nr150"}
                  {:trie-keys l1-trie-keys,
                   :path [2],
                   :out-trie-key "log-l02-p2-nr150"}
                  {:trie-keys l1-trie-keys,
                   :path [3],
                   :out-trie-key "log-l02-p3-nr150"}])
               (f [[1 0 10 10] [1 0 20 20] [1 20 30 10] [1 20 40 20] [1 40 50 10] [1 40 60 20] [1 60 70 10] [1 60 80 20]]))

            "empty L2 and superseded L1 files get ignored")

      (t/is (= [{:trie-keys ["log-l01-fr00-nr114-rs14" "log-l01-fr114-nr128-rs14"
                             "log-l01-fr128-nr13c-rs14" "log-l01-fr13c-nr150-rs14"],
                 :path [1],
                 :out-trie-key "log-l02-p1-nr150"}]
               (f [[2 [0] 80] [2 [2] 80] [2 [3] 80]
                   [1 0 20 20] [1 20 40 20] [1 40 60 20] [1 60 80 20]
                   [0 0 10 10] [0 10 20 10] [0 20 30 10] [0 30 40 10] [0 40 50 10] [0 50 60 10] [0 60 70 10] [0 70 80 10]]))
            "still needs L2 [1]"))

    (t/testing "L2+"
      (t/is (= [ ;; L2 [0] is full, compact L3 [0 2] and [0 3]
                {:trie-keys ["log-l02-p0-nr08" "log-l02-p0-nr110" "log-l02-p0-nr118" "log-l02-p0-nr120"],
                 :path [0 0],
                 :out-trie-key "log-l03-p00-nr120"}
                {:trie-keys ["log-l02-p0-nr08" "log-l02-p0-nr110" "log-l02-p0-nr118" "log-l02-p0-nr120"],
                 :path [0 1],
                 :out-trie-key "log-l03-p01-nr120"}

                ;; L2 [0] has loads, merge from 0x24 onwards (but only 4)
                {:trie-keys ["log-l01-fr120-nr122-rs2" "log-l01-fr122-nr124-rs2" "log-l01-fr124-nr126-rs2" "log-l01-fr126-nr128-rs2"],
                 :path [0],
                 :out-trie-key "log-l02-p0-nr128"}

                ;; L2 [1] has nothing, choose the first four
                {:trie-keys ["log-l01-fr00-nr02-rs2" "log-l01-fr02-nr04-rs2" "log-l01-fr04-nr06-rs2" "log-l01-fr06-nr08-rs2"],
                 :path [1],
                 :out-trie-key "log-l02-p1-nr08"}

                ;; fill in the gaps in [2] and [3]
                {:trie-keys ["log-l01-fr118-nr11a-rs2" "log-l01-fr11a-nr11c-rs2" "log-l01-fr11c-nr11e-rs2" "log-l01-fr11e-nr120-rs2"],
                 :path [2],
                 :out-trie-key "log-l02-p2-nr120"}
                {:trie-keys ["log-l01-fr110-nr112-rs2" "log-l01-fr112-nr114-rs2" "log-l01-fr114-nr116-rs2" "log-l01-fr116-nr118-rs2"],
                 :path [3],
                 :out-trie-key "log-l02-p3-nr118"}]

               (f [[3 [0 2] 32]
                   [3 [0 3] 32]
                   [2 [0] 8] [2 [2] 8] [2 [3] 8] ; missing [1]
                   [2 [0] 16] [2 [2] 16] [2 [3] 16] ; missing [1]
                   [2 [0] 24] [2 [2] 24] ; missing [1] + [3]
                   [2 [0] 32] ; missing [1], [2], and [3]
                   [1 0 2 2] [1 2 4 2] [1 4 6 2] [1 6 8 2]
                   [1 8 10 2] [1 10 12 2] [1 12 14 2] [1 14 16 2]
                   [1 16 18 2] [1 18 20 2] [1 20 22 2] [1 22 24 2]
                   [1 24 26 2] [1 26 28 2] [1 28 30 2] [1 30 32 2]
                   [1 32 34 2] [1 34 36 2] [1 36 38 2] [1 38 40 2]
                   ;; superseded ones
                   [1 0 1 1]
                   [1 4 5 1]
                   [1 18 19 1]
                   [1 26 27 1]]
                  {:l1-file-size-rows 2}))
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
                   [3 [0 3] 32] [3 [0 3] 64] [3 [0 3] 96] [3 [0 3] 128]]
                  {:l1-file-size-rows 32}))
            "L3 -> L4"))))

(deftype LiveDataRel [^RelationReader live-rel]
  IDataRel
  (getSchema [_]
    (Schema. (for [^IVectorReader rdr live-rel]
               (.getField rdr))))

  (loadPage [_ leaf]
    (.select live-rel (.getData ^LiveHashTrie$Leaf leaf)))

  AutoCloseable
  (close [_]))

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
                        (assoc :data-rel (->LiveDataRel (vw/rel-wtr->rdr (li/live-rel lt0)))))
                    (-> (trie/->Segment (.compactLogs (li/live-trie lt1)))
                        (assoc :data-rel (->LiveDataRel (vw/rel-wtr->rdr (li/live-rel lt1)))))]]

      (t/testing "merge segments"
        (util/with-open [data-rel-wtr (trie/open-log-data-wtr tu/*allocator* (c/->log-data-rel-schema (map :data-rel segments)))
                         recency-wtr (c/open-recency-wtr tu/*allocator*)]

          (c/merge-segments-into data-rel-wtr recency-wtr segments nil)

          (t/is (= [{:xt/iid #uuid "9e3f856e-6899-8313-827f-f18dd4d88e78",
                     :xt/system-from (time/->zdt #inst "2023")
                     :xt/valid-from (time/->zdt #inst "2023")
                     :xt/valid-to (time/->zdt time/end-of-time)
                     :op {:v 2, :xt/id "bar"}}
                    {:xt/iid #uuid "9e3f856e-6899-8313-827f-f18dd4d88e78",
                     :xt/system-from (time/->zdt #inst "2021")
                     :xt/valid-from (time/->zdt #inst "2021")
                     :xt/valid-to (time/->zdt time/end-of-time)
                     :op {:v 1, :xt/id "bar"}}
                    {:xt/iid #uuid "9e3f856e-6899-8313-827f-f18dd4d88e78",
                     :xt/system-from (time/->zdt #inst "2020")
                     :xt/valid-from (time/->zdt #inst "2020")
                     :xt/valid-to (time/->zdt time/end-of-time)
                     :op {:v 0, :xt/id "bar"}}
                    {:xt/iid #uuid "d9c7fae2-a04e-0471-6493-6265ba33cf80",
                     :xt/system-from (time/->zdt #inst "2023")
                     :xt/valid-from (time/->zdt #inst "2023")
                     :xt/valid-to (time/->zdt time/end-of-time)
                     :op {:v 2, :xt/id "foo"}}
                    {:xt/iid #uuid "d9c7fae2-a04e-0471-6493-6265ba33cf80",
                     :xt/system-from (time/->zdt #inst "2022")
                     :xt/valid-from (time/->zdt #inst "2022")
                     :xt/valid-to (time/->zdt time/end-of-time)
                     :op {:v 1, :xt/id "foo"}}
                    {:xt/iid #uuid "d9c7fae2-a04e-0471-6493-6265ba33cf80",
                     :xt/system-from (time/->zdt #inst "2020")
                     :xt/valid-from (time/->zdt #inst "2020")
                     :xt/valid-to (time/->zdt time/end-of-time)
                     :op {:v 0, :xt/id "foo"}}]

                   (-> (vw/rel-wtr->rdr data-rel-wtr)
                       (vr/rel->rows)
                       (->> (mapv #(update % :xt/iid util/byte-buffer->uuid))))))

          (t/is (= [(time/->zdt time/end-of-time) (time/->zdt #inst "2023") (time/->zdt #inst "2021")
                    (time/->zdt time/end-of-time) (time/->zdt #inst "2023") (time/->zdt #inst "2022")]
                   (-> recency-wtr vw/vec-wtr->rdr tu/vec->vals)))))

      (t/testing "merge segments with path predicate"
        (util/with-open [data-rel-wtr (trie/open-log-data-wtr tu/*allocator* (c/->log-data-rel-schema (map :data-rel segments)))
                         recency-wtr (c/open-recency-wtr tu/*allocator*)]

          (c/merge-segments-into data-rel-wtr recency-wtr segments (byte-array [2]))

          (t/is (= [{:xt/iid #uuid "9e3f856e-6899-8313-827f-f18dd4d88e78",
                     :xt/system-from (time/->zdt #inst "2023")
                     :xt/valid-from (time/->zdt #inst "2023")
                     :xt/valid-to (time/->zdt time/end-of-time)
                     :op {:v 2, :xt/id "bar"}}
                    {:xt/iid #uuid "9e3f856e-6899-8313-827f-f18dd4d88e78",
                     :xt/system-from (time/->zdt #inst "2021")
                     :xt/valid-from (time/->zdt #inst "2021")
                     :xt/valid-to (time/->zdt time/end-of-time)
                     :op {:v 1, :xt/id "bar"}}
                    {:xt/iid #uuid "9e3f856e-6899-8313-827f-f18dd4d88e78",
                     :xt/system-from (time/->zdt #inst "2020")
                     :xt/valid-from (time/->zdt #inst "2020")
                     :xt/valid-to (time/->zdt time/end-of-time)
                     :op {:v 0, :xt/id "bar"}}]

                   (-> (vw/rel-wtr->rdr data-rel-wtr)
                       (vr/rel->rows)
                       (->> (mapv #(update % :xt/iid util/byte-buffer->uuid))))))

          (t/is (= [(time/->zdt time/end-of-time) (time/->zdt #inst "2023") (time/->zdt #inst "2021")]
                   (-> recency-wtr vw/vec-wtr->rdr tu/vec->vals))))))))

(defn tables-key ^String [table] (str "objects/" bp/version "/tables/" table))

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
                         (.resolve node-dir (tables-key "foo")) #"log-l01-(.+)\.arrow"))))))

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
                         (.resolve node-dir (tables-key "foo")) #"log-l(?!00|01)\d\d-(.+)\.arrow"))))))

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
                   (.resolve node-dir (tables-key "foo")) #"log-l01-(.+)\.arrow")))

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
                       (.resolve node-dir (tables-key "docs")) #"log-(.+)\.arrow")))))
