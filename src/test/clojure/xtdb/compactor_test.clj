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
            [xtdb.trie-catalog :as cat]
            [xtdb.trie-catalog-test :as cat-test]
            [xtdb.util :as util]
            [xtdb.object-store :as os])
  (:import java.lang.AutoCloseable
           [java.nio ByteBuffer]
           [java.time Duration]
           [xtdb BufferPool]
           xtdb.api.storage.Storage
           (xtdb.arrow Relation RelationReader)
           [xtdb.metadata IMetadataManager]
           [xtdb.trie HashTrie IDataRel MemoryHashTrie$Leaf]))

(t/use-fixtures :each tu/with-allocator tu/with-node)

(t/deftest test-compaction-jobs
  (letfn [(f [tries & {:keys [l1-row-count-limit]}]
            (let [opts {:l1-row-count-limit (or l1-row-count-limit 16)}]
              (-> tries
                  (->> (transduce (map cat-test/->added-trie)
                                  (partial cat/apply-trie-notification opts)))
                  (as-> trie-state (c/compaction-jobs "foo" trie-state opts))
                  (->> (into #{} (map (fn [{:keys [part] :as job}]
                                        (-> job
                                            (->> (into {} (filter val)))
                                            (cond-> part (update :part vec))
                                            (dissoc :table-name)))))))))]

    (t/testing "l0 -> l1"
      (t/is (= #{} (f [])))

      (t/is (= #{{:trie-keys ["l00-b00-rsa" "l00-b01-rsa"],
                  :out-trie-key "l01-b01-rs14"}}
               (f [[0 0 10] [0 1 10] [0 2 10]]))
            "no L1s yet, merge L0s up to limit and stop")

      (t/is (= #{{:trie-keys ["l01-b00-rsa" "l00-b01-rsa"],
                  :out-trie-key "l01-b01-rs14"}}
               (f [[0 0 10] [0 1 10] [0 2 10]
                   [1 0 10]]))
            "have a partial L1, merge into that until it's full")

      (t/is (empty? (f [[0 0 10] [0 1 10]
                        [1 1 20]]))
            "all merged, nothing to do")

      (t/is (= #{{:trie-keys ["l00-b02-rsa" "l00-b03-rsa"],
                  :out-trie-key "l01-b03-rs14"}}
               (f [[0 0 10] [0 1 10] [0 2 10] [0 3 10] [0 4 10]
                   [1 1 20]]))
            "have a full L1, start a new L1 til that's full")

      (t/is (= #{{:trie-keys ["l01-b02-rsa" "l00-b03-rsa"],
                  :out-trie-key "l01-b03-rs14"}}
               (f [[0 0 10] [0 1 10] [0 2 10] [0 3 10] [0 4 10]
                   [1 1 20] [1 2 10]]))
            "have a full and a partial L1, merge into that til it's full")

      (t/is (empty? (f [[0 0 10] [0 1 10] [0 2 10] [0 3 10] [0 4 10]
                        [1 1 20] [1 3 20] [1 4 10]]))
            "all merged, nothing to do"))

    (t/testing "l1 -> l2"
      (t/is (= (let [l1-trie-keys ["l01-b01-rs14" "l01-b03-rs14" "l01-b05-rs14" "l01-b07-rs14"]]
                 #{{:trie-keys l1-trie-keys, :part [0], :out-trie-key "l02-p0-b07"}
                   {:trie-keys l1-trie-keys, :part [1], :out-trie-key "l02-p1-b07"}
                   {:trie-keys l1-trie-keys, :part [2], :out-trie-key "l02-p2-b07"}
                   {:trie-keys l1-trie-keys, :part [3], :out-trie-key "l02-p3-b07"}})
               (f [[1 0 10] [1 1 20] [1 2 10] [1 3 20] [1 4 10] [1 5 20] [1 6 10] [1 7 20]]))

            "empty L2 and superseded L1 files get ignored")

      (t/is (= #{{:trie-keys ["l01-b01-rs14" "l01-b03-rs14" "l01-b05-rs14" "l01-b07-rs14"],
                  :part [1],
                  :out-trie-key "l02-p1-b07"}}
               (f [[2 [0] 7] [2 [2] 7] [2 [3] 7]
                   [1 1 20] [1 3 20] [1 5 20] [1 7 20]
                   [0 0 10] [0 1 10] [0 2 10] [0 3 10] [0 4 10] [0 5 10] [0 6 10] [0 7 10]]))
            "still needs L2 [1]"))

    (t/testing "L2+"
      (t/is (= #{ ;; L2 [0] is full, compact L3 [0 0] and [0 1]
                 {:trie-keys ["l02-p0-b03" "l02-p0-b07" "l02-p0-b0b" "l02-p0-b0f"],
                  :part [0 0], :out-trie-key "l03-p00-b0f"}

                 {:trie-keys ["l02-p0-b03" "l02-p0-b07" "l02-p0-b0b" "l02-p0-b0f"],
                  :part [0 1], :out-trie-key "l03-p01-b0f"}

                 ;; L2 [0] has loads, merge from 0x10 onwards (but only 4)
                 {:trie-keys ["l01-b110-rs2" "l01-b111-rs2" "l01-b112-rs2" "l01-b113-rs2"],
                  :part [0], :out-trie-key "l02-p0-b113"}

                 ;; L2 [1] has nothing, choose the first four
                 {:trie-keys ["l01-b00-rs2" "l01-b01-rs2" "l01-b02-rs2" "l01-b03-rs2"],
                  :part [1], :out-trie-key "l02-p1-b03"}

                 ;; fill in the gaps in [2] and [3]
                 {:trie-keys ["l01-b0c-rs2" "l01-b0d-rs2" "l01-b0e-rs2" "l01-b0f-rs2"],
                  :part [2], :out-trie-key "l02-p2-b0f"}

                 {:trie-keys ["l01-b08-rs2" "l01-b09-rs2" "l01-b0a-rs2" "l01-b0b-rs2"],
                  :part [3], :out-trie-key "l02-p3-b0b"}}

               (f [[3 [0 2] 0xf]
                   [3 [0 3] 0xf]
                   [2 [0] 3] [2 [2] 3] [2 [3] 3] ; missing [1]
                   [2 [0] 7] [2 [2] 7] [2 [3] 7] ; missing [1]
                   [2 [0] 0xb] [2 [2] 0xb] ; missing [1] + [3]
                   [2 [0] 0xf] ; missing [1], [2], and [3]
                   [1 0 2] [1 1 2] [1 2 2] [1 3 2]
                   [1 4 2] [1 5 2] [1 6 2] [1 7 2]
                   [1 8 2] [1 9 2] [1 0xa 2] [1 0xb 2]
                   [1 0xc 2] [1 0xd 2] [1 0xe 2] [1 0xf 2]
                   [1 0x10 2] [1 0x11 2] [1 0x12 2] [1 0x13 2]
                   ;; superseded ones
                   [1 0 1] [1 2 1] [1 9 1] [1 0xd 1]]

                  {:l1-row-count-limit 2}))
            "up to L3")

      (t/is (= (let [l2-keys ["l03-p03-b0f" "l03-p03-b11f" "l03-p03-b12f" "l03-p03-b13f"]]
                 #{{:trie-keys l2-keys, :part [0 3 0], :out-trie-key "l04-p030-b13f"}
                   {:trie-keys l2-keys, :part [0 3 1], :out-trie-key "l04-p031-b13f"}
                   {:trie-keys l2-keys, :part [0 3 2], :out-trie-key "l04-p032-b13f"}
                   {:trie-keys l2-keys, :part [0 3 3], :out-trie-key "l04-p033-b13f"}})

               (f [[3 [0 2] 0x0f]
                   [3 [0 3] 0x0f] [3 [0 3] 0x1f] [3 [0 3] 0x2f] [3 [0 3] 0x3f]]))
            "L3 -> L4"))))

(deftype LiveDataRel [^RelationReader live-rel]
  IDataRel
  (getSchema [_] (.getSchema live-rel))

  (loadPage [_ leaf]
    (.select live-rel (.getData ^MemoryHashTrie$Leaf leaf)))

  AutoCloseable
  (close [_]))

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
                          (assoc :data-rel (->LiveDataRel live-rel0)))
                      (-> (trie/->Segment (.compactLogs (li/live-trie lt1)))
                          (assoc :data-rel (->LiveDataRel live-rel1)))]]

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
              cat/*l1-row-count-limit* 256
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

          #_
          (t/is (= (range 100) (q)))

          (submit! (range 100 200))
          (tu/then-await-tx node)
          (c/compact-all! node (Duration/ofSeconds 1))

          #_
          (t/is (= (range 200) (q)))

          (submit! (range 200 500))
          (tu/then-await-tx node)
          (c/compact-all! node (Duration/ofSeconds 1))

          (t/is (= (range 500) (q)))

          (tj/check-json (.toPath (io/as-file (io/resource "xtdb/compactor-test/test-l1-compaction")))
                         (.resolve node-dir (tables-key "public$foo")) #"l01-(.+)\.arrow"))))))

(t/deftest test-l2+-compaction
  (let [node-dir (util/->path "target/compactor/test-l2+-compaction")]
    (util/delete-dir node-dir)

    (binding [c/*page-size* 8
              cat/*l1-row-count-limit* 32]
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
              cat/*l1-row-count-limit* 32]
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
                                 ^IDataRel data-rel (first (trie/open-data-rels bp table-name [trie-key]))]

                  ;; checking that every page relation has a positive row count
                  (t/is (empty? (->> (mapv #(.loadPage data-rel %) (.getLeaves trie))
                                     (map #(.getRowCount ^RelationReader %))
                                     (filter zero?)))))))))))))

(t/deftest test-l2-compaction-badly-distributed
  (let [node-dir (util/->path "target/compactor/test-l2-compaction-badly-distributed")]
    (util/delete-dir node-dir)

    (binding [c/*page-size* 8
              cat/*l1-row-count-limit* 32]
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

(t/deftest test-more-than-a-page-of-versions
  (let [node-dir (util/->path "target/compactor/test-more-than-a-page-of-versions")]
    (util/delete-dir node-dir)

    (binding [c/*page-size* 8
              cat/*l1-row-count-limit* 32
              c/*ignore-signal-block?* true]
      (util/with-open [node (tu/->local-node {:node-dir node-dir, :rows-per-block 10})]
        (dotimes [n 100]
          (xt/submit-tx node [[:put-docs :foo {:xt/id "foo", :v n}]]))
        (tu/then-await-tx node)
        (c/compact-all! node (Duration/ofSeconds 5))

        (t/is (= [{:foo-count 100}] (xt/q node "SELECT COUNT(*) foo_count FROM foo FOR ALL VALID_TIME")))))

    (tj/check-json (.toPath (io/as-file (io/resource "xtdb/compactor-test/test-more-than-a-page-of-versions")))
                   (.resolve node-dir (tables-key "public$foo")) #"l01-(.+)\.arrow")))

(t/deftest losing-data-when-compacting-3459
  (binding [c/*page-size* 8
            cat/*l1-row-count-limit* 32]
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

        (xt/submit-tx node [[:put-docs :foo {:xt/id id-before} {:xt/id id} {:xt/id id-after}]])
        (tu/finish-block! node)
        (c/compact-all! node #xt/duration "PT0.5S")

        (xt/submit-tx node [[:erase {:from :foo :bind [{:xt/id id}]}]])
        (tu/finish-block! node)
        (c/compact-all! node #xt/duration "PT0.5S")

        (t/is (= [{:xt/id id-before} {:xt/id id-after}]
                 (xt/q node "SELECT _id FROM foo FOR ALL VALID_TIME FOR ALL SYSTEM_TIME")))

        (xt/submit-tx node [[:put-docs :foo {:xt/id id}]])
        (tu/finish-block! node)
        (c/compact-all! node #xt/duration "PT0.5S")

        (t/is (= [{:xt/id id-before} {:xt/id id} {:xt/id id-after}]
                 (xt/q node "SELECT _id FROM foo FOR ALL VALID_TIME FOR ALL SYSTEM_TIME"))))

      (t/testing "an id where there is no previous data shouldn't show up in the compacted files"
        (xt/submit-tx node [[:erase {:from :foo :bind [{:xt/id #uuid "20000000-0000-0000-0000-000000000000"}]}]]))

      (tj/check-json (.toPath (io/as-file (io/resource "xtdb/compactor-test/compaction-with-erase")))
                     (.resolve node-dir (tables-key "public$foo")) #"l01-(.+)\.arrow"))))
