(ns xtdb.compactor-test
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.compactor :as c]
            [xtdb.metadata :as meta]
            [xtdb.node :as xtn]
            [xtdb.object-store :as os]
            [xtdb.table-catalog :as table-cat]
            [xtdb.table-catalog-test :as table-test]
            [xtdb.test-json :as tj]
            [xtdb.test-util :as tu]
            [xtdb.trie :as trie]
            [xtdb.trie-catalog :as cat]
            [xtdb.util :as util])
  (:import (java.time Duration)
           (xtdb BufferPool)
           xtdb.api.storage.Storage
           (xtdb.arrow RelationReader)
           [xtdb.block.proto TableBlock]
           (xtdb.trie DataRel Trie)))

(t/use-fixtures :each tu/with-allocator tu/with-node)

(defn- job
  ([out in] (job out in []))
  ([out in part] {:trie-keys in, :out-trie-key out, :part part}))

(defn calc-jobs [& tries]
  (let [opts {:file-size-target cat/*file-size-target*}]
    (-> tries
        (->> (transduce (map (fn [[trie-key size]]
                               (-> (trie/parse-trie-key trie-key)
                                   (assoc :data-file-size (or size -1)))))
                        (completing (partial cat/apply-trie-notification opts))
                        {}))
        (as-> table-cat (c/compaction-jobs "foo" table-cat opts))
        (->> (into #{} (map (fn [job]
                              (-> job
                                  (update :part vec)
                                  (dissoc :table-name)))))))))

(t/deftest test-l0->l1-compaction-jobs
  (binding [cat/*file-size-target* 16]
    (t/is (= #{} (calc-jobs)))

    (t/is (= #{(job "l01-rc-b00" ["l00-rc-b00"])}
             (calc-jobs ["l00-rc-b00" 10] ["l00-rc-b01" 10] ["l00-rc-b02" 10]))
          "no L1s yet, take one L0")

    (t/is (= #{(job "l01-rc-b01" ["l01-rc-b00" "l00-rc-b01"])}
             (calc-jobs ["l00-rc-b00" 10] ["l00-rc-b01" 5] ["l00-rc-b02" 5]
                        ["l01-rc-b00" 10]))
          "have a partial L1, merge a single L0 into that")

    (t/is (empty? (calc-jobs ["l00-rc-b00" 10] ["l00-rc-b01" 10]
                             ["l01-rc-b01" 20]))
          "all merged, nothing to do")

    (t/is (= #{(job "l01-rc-b02" ["l00-rc-b02"])}
             (calc-jobs ["l00-rc-b00" 10] ["l00-rc-b01" 10] ["l00-rc-b02" 10] ["l00-rc-b03" 10] ["l00-rc-b04" 10]
                        ["l01-rc-b01" 20]))
          "have a full L1, start a new L1")

    (t/is (= #{(job "l01-rc-b03" ["l01-rc-b02" "l00-rc-b03"])}
             (calc-jobs ["l00-rc-b00" 10] ["l00-rc-b01" 10] ["l00-rc-b02" 10] ["l00-rc-b03" 10] ["l00-rc-b04" 10]
                        ["l01-rc-b01" 20] ["l01-rc-b02" 10]))
          "have a full and a partial L1, merge a single file into the partial")

    (t/is (empty? (calc-jobs ["l00-rc-b00" 10] ["l00-rc-b01" 10] ["l00-rc-b02" 10] ["l00-rc-b03" 10] ["l00-rc-b04" 10]
                             ["l01-rc-b01" 20] ["l01-rc-b03" 20] ["l01-rc-b04" 10]))
          "all merged, nothing to do")))

(t/deftest test-levelling-compaction-jobs
  (binding [cat/*file-size-target* 16]
    (t/testing "L1C"
      (t/is (= (let [l1-trie-keys ["l01-rc-b01" "l01-rc-b03" "l01-rc-b05" "l01-rc-b07"]]
                 #{(job "l02-rc-p0-b07" l1-trie-keys [0])
                   (job "l02-rc-p1-b07" l1-trie-keys [1])
                   (job "l02-rc-p2-b07" l1-trie-keys [2])
                   (job "l02-rc-p3-b07" l1-trie-keys [3])})
               (calc-jobs ["l01-rc-b00" 10] ["l01-rc-b01" 20] ["l01-rc-b02" 10] ["l01-rc-b03" 20] ["l01-rc-b04" 10] ["l01-rc-b05" 20] ["l01-rc-b06" 10] ["l01-rc-b07" 20]))

            "empty L2 and superseded L1 files get ignored")

      (t/is (= #{(job "l02-rc-p1-b07" ["l01-rc-b01" "l01-rc-b03" "l01-rc-b05" "l01-rc-b07"] [1])}
               (calc-jobs ["l00-rc-b00" 10] ["l00-rc-b01" 10] ["l00-rc-b02" 10] ["l00-rc-b03" 10] ["l00-rc-b04" 10] ["l00-rc-b05" 10] ["l00-rc-b06" 10] ["l00-rc-b07" 10]
                          ["l01-rc-b01" 20] ["l01-rc-b03" 20] ["l01-rc-b05" 20] ["l01-rc-b07" 20]
                          ["l02-rc-p0-b07"] ["l02-rc-p2-b07"] ["l02-rc-p3-b07"]))
            "still needs L2 [1]"))

    (t/testing "L2H"
      (let [full-keys ["l02-r20200101-b04" "l02-r20200101-b06" "l02-r20200101-b0f" "l02-r20200101-b114"]]
        (t/is (= #{}
                 (calc-jobs ["l02-r20200101-b03" 10] ["l02-r20200101-b04" 20]
                            ["l02-r20200101-b06" 20]
                            ["l02-r20200101-b0a" 10] ["l02-r20200101-b0f" 20]

                            ;; different recency
                            ["l02-r20200102-b110" 20]))
              "only 3 full keys")

        (t/is (= #{(job "l03-r20200101-p0-b114" full-keys [0])
                   (job "l03-r20200101-p1-b114" full-keys [1])
                   (job "l03-r20200101-p2-b114" full-keys [2])
                   (job "l03-r20200101-p3-b114" full-keys [3])}
                 (calc-jobs ["l02-r20200101-b03" 10] ["l02-r20200101-b04" 20]
                            ["l02-r20200101-b06" 20]
                            ["l02-r20200101-b0a" 10] ["l02-r20200101-b0f" 20]
                            ["l02-r20200101-b114" 20])))

        (t/is (= #{(job "l03-r20200101-p1-b114" full-keys [1])
                   (job "l03-r20200101-p3-b114" full-keys [3])}
                 (calc-jobs ["l02-r20200101-b03" 10] ["l02-r20200101-b04" 20]
                            ["l02-r20200101-b06" 20]
                            ["l02-r20200101-b0a" 10] ["l02-r20200101-b0f" 20]
                            ["l02-r20200101-b114" 20]
                            ["l03-r20200101-p0-b114"] ["l03-r20200101-p2-b114"]))
              "two L3Hs already present")))))

(t/deftest test-l1h-jobs
  (binding [cat/*file-size-target* 16]
    (t/is (= #{}
             (calc-jobs ["l01-r20200101-b00" 3] ["l01-r20200101-b01" 3] ["l01-r20200101-b02" 3]
                        ["l01-r20200102-b00" 3] ["l01-r20200102-b01" 3]
                        ;; to mark the historical ones 'live'
                        ["l01-rc-b00" 0] ["l01-rc-b01" 0] ["l01-rc-b02" 0] ["l01-rc-b03" 0]))

          "fewer than 4 L1H files in all partitions, no L2H")

    (t/is (= #{(job "l02-r20200102-b03" ["l01-r20200102-b00" "l01-r20200102-b01" "l01-r20200102-b02" "l01-r20200102-b03"] [])}
             (calc-jobs ["l01-r20200101-b00" 3] ["l01-r20200101-b01" 3] ["l01-r20200101-b02" 3]
                        ["l01-r20200102-b00" 3] ["l01-r20200102-b01" 3] ["l01-r20200102-b02" 3] ["l01-r20200102-b03" 3]
                        ;; to mark the historical ones 'live'
                        ["l01-rc-b00" 0] ["l01-rc-b01" 0] ["l01-rc-b02" 0] ["l01-rc-b03" 0]))
          "4 L1H files in one of the partitions, create L2H")

    (t/is (= #{(job "l02-r20200101-b03" ["l01-r20200101-b00" "l01-r20200101-b01" "l01-r20200101-b02" "l01-r20200101-b03"] [])
               (job "l02-r20200102-b03" ["l01-r20200102-b00" "l01-r20200102-b01" "l01-r20200102-b02" "l01-r20200102-b03"] [])}
             (calc-jobs ["l01-r20200101-b00" 3] ["l01-r20200101-b01" 3] ["l01-r20200101-b02" 3] ["l01-r20200101-b03" 3]
                        ["l01-r20200102-b00" 3] ["l01-r20200102-b01" 3] ["l01-r20200102-b02" 3] ["l01-r20200102-b03" 3]
                        ["l01-r20200103-b00" 3] ["l01-r20200102-b03" 3]
                        ;; to mark the historical ones 'live'
                        ["l01-rc-b00" 0] ["l01-rc-b01" 0] ["l01-rc-b02" 0] ["l01-rc-b03" 0]))
          "4 L1H files in multiple partitions, create L2Hs")

    (t/is (= #{(job "l02-r20200101-b02" ["l01-r20200101-b00" "l01-r20200101-b01" "l01-r20200101-b02"] [])}
             (calc-jobs ["l01-r20200101-b00" 6] ["l01-r20200101-b01" 6] ["l01-r20200101-b02" 6] ["l01-r20200101-b03" 5]
                        ["l01-r20200102-b00" 3] ["l01-r20200102-b01" 3]
                        ;; to mark the historical ones 'live'
                        ["l01-rc-b00" 0] ["l01-rc-b01" 0] ["l01-rc-b02" 0] ["l01-rc-b03" 0]))

          "fewer than 4 L1H files take us over the limit, so we compact early")))

(t/deftest test-l2+-compaction-jobs
  (binding [cat/*file-size-target* 16]
    (t/is (= #{ ;; L2 [0] is full, compact L3 [0 0] and [0 1]
               (job "l03-rc-p00-b0f" ["l02-rc-p0-b03" "l02-rc-p0-b07" "l02-rc-p0-b0b" "l02-rc-p0-b0f"] [0 0])
               (job "l03-rc-p01-b0f" ["l02-rc-p0-b03" "l02-rc-p0-b07" "l02-rc-p0-b0b" "l02-rc-p0-b0f"] [0 1])

               ;; L2 [0] has loads, merge from 0x10 onwards (but only 4)
               (job "l02-rc-p0-b113" ["l01-rc-b110" "l01-rc-b111" "l01-rc-b112" "l01-rc-b113"] [0])

               ;; L2 [1] has nothing, choose the first four
               (job "l02-rc-p1-b03" ["l01-rc-b00" "l01-rc-b01" "l01-rc-b02" "l01-rc-b03"] [1])

               ;; fill in the gaps in [2] and [3]
               (job "l02-rc-p2-b0f" ["l01-rc-b0c" "l01-rc-b0d" "l01-rc-b0e" "l01-rc-b0f"] [2])
               (job "l02-rc-p3-b0b" ["l01-rc-b08" "l01-rc-b09" "l01-rc-b0a" "l01-rc-b0b"] [3])}

             (binding [cat/*file-size-target* 2]
               (calc-jobs ["l03-rc-p02-b0f"]
                          ["l03-rc-p03-b0f"]
                          ["l02-rc-p0-b03"] ["l02-rc-p2-b03"] ["l02-rc-p3-b03"] ; missing [1]
                          ["l02-rc-p0-b07"] ["l02-rc-p2-b07"] ["l02-rc-p3-b07"] ; missing [1]
                          ["l02-rc-p0-b0b"] ["l02-rc-p2-b0b"]                ; missing [1] + [3]
                          ["l02-rc-p0-b0f"]                               ; missing [1], [2], and [3]
                          ["l01-rc-b00" 2] ["l01-rc-b01" 2] ["l01-rc-b02" 2] ["l01-rc-b03" 2]
                          ["l01-rc-b04" 2] ["l01-rc-b05" 2] ["l01-rc-b06" 2] ["l01-rc-b07" 2]
                          ["l01-rc-b08" 2] ["l01-rc-b09" 2] ["l01-rc-b0a" 2] ["l01-rc-b0b" 2]
                          ["l01-rc-b0c" 2] ["l01-rc-b0d" 2] ["l01-rc-b0e" 2] ["l01-rc-b0f" 2]
                          ["l01-rc-b110" 2] ["l01-rc-b111" 2] ["l01-rc-b112" 2] ["l01-rc-b113" 2]
                          ;; superseded ones
                          ["l01-rc-b00" 1] ["l01-rc-b02" 1] ["l01-rc-b09" 1] ["l01-rc-b0d" 1])))
          "up to L3")

    (t/testing "L3 -> L4"
      (let [l3-keys ["l03-rc-p03-b0f" "l03-rc-p03-b11f" "l03-rc-p03-b12f" "l03-rc-p03-b13f"]]
        (t/is (= #{(job "l04-rc-p030-b13f" l3-keys [0 3 0])
                   (job "l04-rc-p031-b13f" l3-keys [0 3 1])
                   (job "l04-rc-p032-b13f" l3-keys [0 3 2])
                   (job "l04-rc-p033-b13f" l3-keys [0 3 3])}

                 (calc-jobs ["l03-rc-p02-b0f"]
                            ["l03-rc-p03-b0f"] ["l03-rc-p03-b11f"] ["l03-rc-p03-b12f"] ["l03-rc-p03-b13f"])))))

    (t/testing "L3H -> L4H"
      (let [in-keys ["l03-r20200101-p0-b03" "l03-r20200101-p0-b07" "l03-r20200101-p0-b0b" "l03-r20200101-p0-b0f"]]
        (t/is (= #{(job "l04-r20200101-p00-b0f" in-keys [0 0])
                   (job "l04-r20200101-p01-b0f" in-keys [0 1])
                   (job "l04-r20200101-p02-b0f" in-keys [0 2])
                   (job "l04-r20200101-p03-b0f" in-keys [0 3])}
                 (calc-jobs ["l03-r20200101-p0-b03"] ["l03-r20200101-p0-b07"] ["l03-r20200101-p0-b0b"] ["l03-r20200101-p0-b0f"]

                            ;; only 3 tries
                            ["l03-r20200101-p1-b03"] ["l03-r20200101-p1-b07"] ["l03-r20200101-p1-b0b"]

                            ;; N.B. different recency in the first one
                            ["l03-r20200102-p2-b03"] ["l03-r20200101-p2-b07"] ["l03-r20200101-p2-b0b"] ["l03-r20200101-p2-b0f"]
                            )))))))

(defn tables-key ^String [table] (str "objects/" Storage/version "/tables/" table))

(t/deftest test-l1-compaction
  (let [node-dir (util/->path "target/compactor/test-l1-compaction")]
    (util/delete-dir node-dir)

    (binding [c/*page-size* 32
              cat/*file-size-target* (* 16 1024)
              c/*ignore-signal-block?* true]
      (util/with-open [node (tu/->local-node {:node-dir node-dir, :rows-per-block 50})]
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
          (c/compact-all! node (Duration/ofSeconds 1))

          (t/is (= (range 100) (q)))

          (submit! (range 100 200))
          (tu/then-await-tx node)
          (c/compact-all! node (Duration/ofSeconds 1))

          (t/is (= (range 200) (q)))

          (submit! (range 200 500))
          (tu/then-await-tx node)
          (c/compact-all! node (Duration/ofSeconds 1))

          (t/is (= (range 500) (q)))

          (tj/check-json (.toPath (io/as-file (io/resource "xtdb/compactor-test/test-l1-compaction")))
                         (.resolve node-dir (tables-key "public$foo")) #"l01-rc-(.+)\.arrow"))))))

(t/deftest test-l2+-compaction
  (let [node-dir (util/->path "target/compactor/test-l2+-compaction")]
    (util/delete-dir node-dir)

    (binding [c/*page-size* 8
              cat/*file-size-target* (* 16 1024)
              c/*ignore-signal-block?* true]
      (util/with-open [node (tu/->local-node {:node-dir node-dir, :rows-per-block 10})]
        (letfn [(submit! [xs]
                  (last (for [batch (partition-all 6 xs)]
                          (xt/submit-tx node [(into [:put-docs :foo]
                                                    (for [x batch]
                                                      {:xt/id x}))]))))

                (q []
                  (->> (xt/q node
                             '(-> (from :foo [{:xt/id id}])
                                  (order-by id)))
                       (map :id)))]

          (let [tx-id (submit! (range 500))]
            (tu/then-await-tx tx-id node #xt/duration "PT2S")
            (c/compact-all! node #xt/duration "PT2S"))

          (t/is (= (set (range 500)) (set (q))))

          (let [tx-id (submit! (range 500 1000))]
            (tu/then-await-tx tx-id node #xt/duration "PT2S")
            (c/compact-all! node #xt/duration "PT2S"))

          (t/is (= (set (range 1000)) (set (q))))

          (let [tx-id (submit! (range 1000 2000))]
            (tu/then-await-tx tx-id node #xt/duration "PT5S")
            (c/compact-all! node #xt/duration "PT5S"))

          (t/is (= (set (range 2000)) (set (q))))

          (tj/check-json (.toPath (io/as-file (io/resource "xtdb/compactor-test/test-l2+-compaction")))
                         (.resolve node-dir (tables-key "public$foo")) #"l(?!00|01)\d\d-(.+)\.arrow"))))))

(t/deftest test-no-empty-pages-3580
  (let [node-dir (util/->path "target/compactor/test-badly-distributed")]
    (util/delete-dir node-dir)

    (binding [c/*page-size* 8
              cat/*file-size-target* (* 16 1024)
              c/*ignore-signal-block?* true]
      (util/with-open [node (tu/->local-node {:node-dir node-dir, :rows-per-block 10})]
        (let [^BufferPool bp (tu/component node :xtdb/buffer-pool)
              meta-mgr (meta/<-node node)]
          (letfn [(submit! [xs]
                    (last (for [batch (partition-all 8 xs)]
                            (xt/submit-tx node [(into [:put-docs :foo]
                                                      (for [x batch]
                                                        {:xt/id x}))]))))]

            (let [tx-id (submit! (take 512 (cycle (tu/bad-uuid-seq 8))))]
              (tu/then-await-tx tx-id node)
              (c/compact-all! node (Duration/ofSeconds 5)))

            (let [table-name "foo"
                  meta-files (->> (.listAllObjects bp (trie/->table-meta-dir table-name))
                                  (mapv (comp :key os/<-StoredObject)))]

              ;; TODO this doseq seems to return nothing, so nothing gets tested?
              (doseq [{:keys [trie-key]} (map trie/parse-trie-file-path meta-files)]
                (util/with-open [page-meta (.openPageMetadata meta-mgr (Trie/metaFilePath table-name trie-key))
                                 ^DataRel data-rel (first (DataRel/openRels tu/*allocator* bp table-name [trie-key]))]

                  ;; checking that every page relation has a positive row count
                  (t/is (empty? (->> (mapv #(.loadPage data-rel %) (.getLeaves (.getTrie page-meta)))
                                     (map #(.getRowCount ^RelationReader %))
                                     (filter zero?)))))))))))))

(t/deftest test-l2-compaction-badly-distributed
  (let [node-dir (util/->path "target/compactor/test-l2-compaction-badly-distributed")]
    (util/delete-dir node-dir)

    (binding [c/*page-size* 8
              cat/*file-size-target* (* 16 1024)
              c/*ignore-signal-block?* true]
      (util/with-open [node (tu/->local-node {:node-dir node-dir, :rows-per-block 10})]
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
          (c/compact-all! node (Duration/ofSeconds 1))

          (t/is (= (tu/bad-uuid-seq 100) (q)))

          (submit! (tu/bad-uuid-seq 100 200))
          (tu/then-await-tx node)
          (c/compact-all! node (Duration/ofSeconds 1))

          (t/is (= (tu/bad-uuid-seq 200) (q)))

          (submit! (tu/bad-uuid-seq 200 500))
          (tu/then-await-tx node)
          (c/compact-all! node (Duration/ofSeconds 1))

          (t/is (= (tu/bad-uuid-seq 500) (q))))))))

(t/deftest losing-data-when-compacting-3459
  (binding [c/*page-size* 8
            cat/*file-size-target* (* 16 1024)
            c/*ignore-signal-block?* true]
    (let [node-dir (util/->path "target/compactor/lose-data-on-compaction")]
      (util/delete-dir node-dir)

      (util/with-open [node (tu/->local-node {:node-dir node-dir
                                              :rows-per-block 32
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
                       (.resolve node-dir (tables-key "public$docs")) #"(.+)\.arrow")))))

(t/deftest test-compaction-promotion-bug-3673
  (xt/submit-tx tu/*node* [[:put-docs :foo {:xt/id 0, :foo 12.0} {:xt/id 1}]])
  (tu/finish-block! tu/*node*)
  (c/compact-all! tu/*node* #xt/duration "PT0.5S")

  (xt/submit-tx tu/*node* [[:put-docs :foo {:xt/id 2, :foo 24} {:xt/id 3, :foo 28.1} {:xt/id 4}]])
  (tu/finish-block! tu/*node*)
  (c/compact-all! tu/*node* #xt/duration "PT0.5S")

  (t/is (= #{12.0 nil 24 28.1}
           (->> (xt/q tu/*node* "SELECT foo FROM foo")
                (into #{} (map :foo)))))

  (util/with-open [node (xtn/start-node tu/*node-opts*)]
    (xt/submit-tx node [[:put-docs :foo {:xt/id 1} {:foo "foo", :xt/id 0} {:foo 3, :xt/id 2}]])
    (tu/finish-block! node)
    (c/compact-all! node #xt/duration "PT0.5S")

    (t/is (= #{"foo" nil 3}
             (->> (xt/q node "SELECT foo FROM foo")
                  (into #{} (map :foo)))))))

(t/deftest test-compaction-with-erase-4017
  (let [node-dir (util/->path "target/compactor/compaction-with-erase")]
    (util/delete-dir node-dir)

    (util/with-open [node (tu/->local-node {:node-dir node-dir})]

      (let [[id-before id id-after] [#uuid "00000000-0000-0000-0000-000000000000"
                                     #uuid "40000000-0000-0000-0000-000000000000"
                                     #uuid "80000000-0000-0000-0000-000000000000"]]

        (xt/execute-tx node [[:put-docs :foo {:xt/id id-before} {:xt/id id} {:xt/id id-after}]])
        (tu/finish-block! node)
        (c/compact-all! node #xt/duration "PT0.5S")

        (xt/execute-tx node [[:erase {:from :foo :bind [{:xt/id id}]}]])
        (tu/finish-block! node)
        (c/compact-all! node #xt/duration "PT0.5S")

        (t/is (= [{:xt/id id-before} {:xt/id id-after}]
                 (xt/q node "SELECT _id FROM foo FOR ALL VALID_TIME FOR ALL SYSTEM_TIME")))

        (xt/execute-tx node [[:put-docs :foo {:xt/id id}]])

        (t/testing "an id where there is no previous data shouldn't show up in the compacted files"
          (xt/execute-tx node [[:erase {:from :foo :bind [{:xt/id #uuid "20000000-0000-0000-0000-000000000000"}]}]]))

        (tu/finish-block! node)
        (c/compact-all! node #xt/duration "PT0.5S")

        (t/is (= [{:xt/id id-before} {:xt/id id} {:xt/id id-after}]
                 (xt/q node "SELECT _id FROM foo FOR ALL VALID_TIME FOR ALL SYSTEM_TIME"))))

      (tj/check-json (.toPath (io/as-file (io/resource "xtdb/compactor-test/compaction-with-erase")))
                     (.resolve node-dir (tables-key "public$foo")) #"l01-rc-(.+)\.arrow"))))

(t/deftest compactor-trie-metadata
  (let [node-dir (util/->path "target/compactor/compactor-metadata-test")]
    (util/delete-dir node-dir)

    (with-open [node (tu/->local-node {:node-dir node-dir, :compactor-threads 1})]
      (let [^BufferPool bp (tu/component node :xtdb/buffer-pool)]
        (xt/execute-tx node [[:put-docs {:into :foo
                                         :valid-from #xt/instant "2010-01-01T00:00:00Z"
                                         :valid-to #xt/instant "2011-01-01T00:00:00Z"}
                              {:xt/id 1}]])
        (tu/finish-block! node)

        (xt/execute-tx node [[:put-docs {:into :foo
                                         :valid-from #xt/instant "2015-01-01T00:00:00Z"
                                         :valid-to #xt/instant "2016-01-01T00:00:00Z"}
                              {:xt/id 2}]])
        (tu/finish-block! node)

        (c/compact-all! node)
        ;; to artifically create a new table block
        (tu/finish-block! node)


        (t/is (= (os/->StoredObject "tables/public$foo/blocks/b02.binpb" 696)
                 (last (.listAllObjects bp (table-cat/->table-block-dir "public/foo")))))

        (let [current-tries (->> (.getByteArray bp (util/->path "tables/public$foo/blocks/b02.binpb"))
                                 TableBlock/parseFrom
                                 table-cat/<-table-block
                                 :current-tries
                                 (mapv table-test/trie-details->edn))]
          (t/is (= ["l00-rc-b00" "l01-rc-b00" "l00-rc-b01" "l01-rc-b01"]
                   (map :trie-key current-tries)))

          (t/is (= [{:min-valid-from #xt/instant "2010-01-01T00:00:00Z",
                     :max-valid-from #xt/instant "2010-01-01T00:00:00Z",
                     :min-valid-to #xt/instant "2011-01-01T00:00:00Z",
                     :max-valid-to #xt/instant "2011-01-01T00:00:00Z",
                     :min-system-from #xt/instant "2020-01-01T00:00:00Z",
                     :max-system-from #xt/instant "2020-01-01T00:00:00Z",
                     :row-count 1}
                    {:min-valid-from #xt/instant "2010-01-01T00:00:00Z",
                     :max-valid-from #xt/instant "2015-01-01T00:00:00Z",
                     :min-valid-to #xt/instant "2011-01-01T00:00:00Z",
                     :max-valid-to #xt/instant "2016-01-01T00:00:00Z",
                     :min-system-from #xt/instant "2020-01-01T00:00:00Z",
                     :max-system-from #xt/instant "2020-01-02T00:00:00Z",
                     :row-count 2}]
                   (->> current-tries
                        (filter #(str/starts-with? (:trie-key %) "l01"))
                        (map (comp #(dissoc % :iid-bloom) :trie-metadata))))))))))
