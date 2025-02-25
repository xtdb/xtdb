(ns xtdb.compactor-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.compactor :as c]
            [xtdb.indexer.live-index :as li]
            [xtdb.node :as xtn]
            [xtdb.object-store :as os]
            [xtdb.test-json :as tj]
            [xtdb.test-util :as tu]
            [xtdb.time :as time]
            [xtdb.trie :as trie]
            [xtdb.trie-catalog :as cat]
            [xtdb.util :as util])
  (:import [java.nio ByteBuffer]
           [java.time Duration]
           [xtdb BufferPool]
           xtdb.api.storage.Storage
           (xtdb.arrow Relation RelationReader)
           [xtdb.metadata IMetadataManager]
           [xtdb.trie DataRel HashTrie]))

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

    (t/is (= #{(job "l01-rc-b01" ["l00-rc-b00" "l00-rc-b01"])}
             (calc-jobs ["l00-rc-b00" 10] ["l00-rc-b01" 10] ["l00-rc-b02" 10]))
          "no L1s yet, merge L0s up to limit and stop")

    (t/is (= #{(job "l01-rc-b01" ["l01-rc-b00" "l00-rc-b01"])}
             (calc-jobs ["l00-rc-b00" 10] ["l00-rc-b01" 10] ["l00-rc-b02" 10]
                        ["l01-rc-b00" 10]))
          "have a partial L1, merge into that until it's full")

    (t/is (empty? (calc-jobs ["l00-rc-b00" 10] ["l00-rc-b01" 10]
                             ["l01-rc-b01" 20]))
          "all merged, nothing to do")

    (t/is (= #{(job "l01-rc-b03" ["l00-rc-b02" "l00-rc-b03"])}
             (calc-jobs ["l00-rc-b00" 10] ["l00-rc-b01" 10] ["l00-rc-b02" 10] ["l00-rc-b03" 10] ["l00-rc-b04" 10]
                        ["l01-rc-b01" 20]))
          "have a full L1, start a new L1 til that's full")

    (t/is (= #{(job "l01-rc-b03" ["l01-rc-b02" "l00-rc-b03"])}
             (calc-jobs ["l00-rc-b00" 10] ["l00-rc-b01" 10] ["l00-rc-b02" 10] ["l00-rc-b03" 10] ["l00-rc-b04" 10]
                        ["l01-rc-b01" 20] ["l01-rc-b02" 10]))
          "have a full and a partial L1, merge into that til it's full")

    (t/is (empty? (calc-jobs ["l00-rc-b00" 10] ["l00-rc-b01" 10] ["l00-rc-b02" 10] ["l00-rc-b03" 10] ["l00-rc-b04" 10]
                             ["l01-rc-b01" 20] ["l01-rc-b03" 20] ["l01-rc-b04" 10]))
          "all merged, nothing to do")))

(t/deftest test-l1c->l2c-compaction-jobs
  (binding [cat/*file-size-target* 16]
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
          "still needs L2 [1]")))

(t/deftest test-l2+-compaction-jobs
  (binding [cat/*file-size-target* 16]
    (t/testing "L2+"
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

      (let [l2-keys ["l03-rc-p03-b0f" "l03-rc-p03-b11f" "l03-rc-p03-b12f" "l03-rc-p03-b13f"]]
        (t/is (= #{(job "l04-rc-p030-b13f" l2-keys [0 3 0])
                   (job "l04-rc-p031-b13f" l2-keys [0 3 1])
                   (job "l04-rc-p032-b13f" l2-keys [0 3 2])
                   (job "l04-rc-p033-b13f" l2-keys [0 3 3])}

                 (calc-jobs ["l03-rc-p02-b0f"]
                            ["l03-rc-p03-b0f"] ["l03-rc-p03-b11f"] ["l03-rc-p03-b12f"] ["l03-rc-p03-b13f"]))
              "L3 -> L4")))))

(t/deftest test-merges-segments
  (util/with-open [lt0 (tu/open-live-table "foo")
                   lt1 (tu/open-live-table "foo")]

    (tu/index-tx! lt0 #xt/tx-key {:tx-id 0, :system-time #xt/instant "2020-01-01T00:00:00Z"}
                  [{:xt/id "foo", :v 0}
                   {:xt/id "bar", :v 0}])

    (tu/index-tx! lt0 #xt/tx-key {:tx-id 1, :system-time #xt/instant "2021-01-01T00:00:00Z"}
                  [{:xt/id "bar", :v 1}])

    (tu/index-tx! lt1 #xt/tx-key {:tx-id 2, :system-time #xt/instant "2022-01-01T00:00:00Z"}
                  [{:xt/id "foo", :v 1}])

    (tu/index-tx! lt1 #xt/tx-key {:tx-id 3, :system-time #xt/instant "2023-01-01T00:00:00Z"}
                  [{:xt/id "foo", :v 2}
                   {:xt/id "bar", :v 2}])

    (with-open [live-rel0 (.openAsRelation (.getLiveRelation lt0))
                live-rel1 (.openAsRelation (.getLiveRelation lt1))]

      (let [segments [(-> (trie/->Segment (.compactLogs (li/live-trie lt0)))
                          (assoc :data-rel (DataRel/live live-rel0)))
                      (-> (trie/->Segment (.compactLogs (li/live-trie lt1)))
                          (assoc :data-rel (DataRel/live live-rel1)))]]

        (t/testing "merge segments"
          (util/with-open [data-rel (Relation. tu/*allocator* (c/->log-data-rel-schema (map :data-rel segments)))
                           recency-wtr (c/open-recency-wtr tu/*allocator*)]

            (c/merge-segments-into data-rel recency-wtr segments nil)

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

                     (->> (.toMaps data-rel)
                          (mapv #(update % :xt/iid (comp util/byte-buffer->uuid ByteBuffer/wrap))))))

            (t/is (= [(time/->zdt time/end-of-time) (time/->zdt #inst "2023") (time/->zdt #inst "2021")
                      (time/->zdt time/end-of-time) (time/->zdt #inst "2023") (time/->zdt #inst "2022")]
                     (-> recency-wtr .getAsList)))))

        (t/testing "merge segments with path predicate"
          (util/with-open [data-rel (Relation. tu/*allocator* (c/->log-data-rel-schema (map :data-rel segments)))
                           recency-wtr (c/open-recency-wtr tu/*allocator*)]
            (c/merge-segments-into data-rel recency-wtr segments (byte-array [2]))

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

                     (->> (.toMaps data-rel)
                          (mapv #(update % :xt/iid (comp util/byte-buffer->uuid ByteBuffer/wrap))))))

            (t/is (= [(time/->zdt time/end-of-time) (time/->zdt #inst "2023") (time/->zdt #inst "2021")]
                     (.getAsList recency-wtr)))))))))

(defn tables-key ^String [table] (str "objects/" Storage/version "/tables/" table))

(t/deftest test-l1-compaction
  (let [node-dir (util/->path "target/compactor/test-l1-compaction")]
    (util/delete-dir node-dir)

    (binding [c/*page-size* 32
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
              ^IMetadataManager meta-mgr (tu/component node :xtdb.metadata/metadata-manager)]
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
              (doseq [{:keys [trie-key]} (map trie/parse-trie-file-path meta-files)]
                (util/with-open [{:keys [^HashTrie trie] :as _table-metadata} (.openTableMetadata meta-mgr (trie/->table-meta-file-path table-name trie-key))
                                 ^DataRel data-rel (first (DataRel/openRels tu/*allocator* bp table-name [trie-key]))]

                  ;; checking that every page relation has a positive row count
                  (t/is (empty? (->> (mapv #(.loadPage data-rel %) (.getLeaves trie))
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

(t/deftest recency-bucketing-bug
  (let [node-dir (util/->path "target/compactor/recency-bucketing-bug")]
    (util/delete-dir node-dir)

    (binding [c/*page-size* 2
              c/*ignore-signal-block?* true]

      (util/with-open [node (tu/->local-node {:node-dir node-dir})]

        (dotimes [x 2]
          (xt/submit-tx node
                        [[:put-docs {:into :docs
                                     :valid-from #xt/zoned-date-time "2024-01-01T00:00Z[UTC]" ,
                                     :valid-to #xt/zoned-date-time "2025-01-01T00:00Z[UTC]" }
                          {:xt/id x :col1 "yes"}]])
          (xt/submit-tx node [[:put-docs {:into :docs
                                          :valid-from #xt/zoned-date-time "2024-01-03T00:00Z[UTC]" ,
                                          :valid-to #xt/zoned-date-time "2024-01-04T00:00Z[UTC]"}
                               {:xt/id x :col1 "no"}]]))
        (tu/finish-block! node)
        (c/compact-all! node)

        (t/is (= [{:xt/id 0,
                   :col1 "yes",
                   :xt/valid-time
                   #xt/tstz-range [#xt/zoned-date-time "2024-01-01T00:00Z" #xt/zoned-date-time "2024-01-03T00:00Z"]}
                  {:xt/id 0,
                   :col1 "yes",
                   :xt/valid-time
                   #xt/tstz-range [#xt/zoned-date-time "2024-01-04T00:00Z" #xt/zoned-date-time "2025-01-01T00:00Z"]}]
                 (xt/q node "SELECT *, _valid_time FROM docs FOR ALL VALID_TIME WHERE _id = 0 AND col1 = 'yes'")))))))
