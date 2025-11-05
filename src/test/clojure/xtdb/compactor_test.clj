(ns xtdb.compactor-test
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :as t]
            [clojure.math :as math]
            [xtdb.api :as xt]
            [xtdb.arrow-edn-test :as aet]
            [xtdb.check-pbuf :as cpb]
            [xtdb.compactor :as c]
            [xtdb.db-catalog :as db]
            [xtdb.log :as xt-log]
            [xtdb.node :as xtn]
            [xtdb.object-store :as os]
            [xtdb.table-catalog :as table-cat]
            [xtdb.table-catalog-test :as table-test]
            [xtdb.test-util :as tu]
            [xtdb.time :as time]
            [xtdb.trie :as trie]
            [xtdb.trie-catalog :as cat]
            [xtdb.util :as util])
  (:import (java.time Duration)
           [xtdb.block.proto TableBlock]
           (xtdb.compactor RecencyPartition)
           xtdb.segment.BufferPoolSegment
           xtdb.storage.LocalStorage
           (xtdb.trie Trie)))

(t/use-fixtures :each tu/with-allocator tu/with-mock-clock tu/with-node)

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
        (as-> table-cat (c/compaction-jobs #xt/table foo, table-cat opts))
        (->> (into #{} (map (fn [job]
                              (-> job
                                  (update :part vec)
                                  (update :out-trie-key str)
                                  (dissoc :table :partitioned-by-recency?)))))))))

(defn- all-parts-rc-level [level]
  (if (= level 1)
    [[]]
    (for [smaller-part (all-parts-rc-level (dec level))
          i (range cat/branch-factor)]
      (conj smaller-part i))))

(defn level-seq
  "Creates a sequence of files for level up to block n. Currently only supports recency current and supposes every file is always full according to size."
  ([level n] (level-seq level n nil))
  ([level n recency] (level-seq level n recency 20))
  ([level n recency size]
   (cond
     (= level 0) (map (fn [i] [(trie/->l0-trie-key i) size]) (range n))

     (= level 1) (map (fn [i] [(trie/->l1-trie-key recency i) size]) (range n))

     :else (let [increments (math/pow 4 (cond-> (dec level)
                                          recency dec))
                 parts (all-parts-rc-level (cond-> level
                                             recency dec))]
             (mapcat (fn [i]
                       (for [part parts]
                         [(trie/->trie-key level recency (byte-array part) i) size])) (range (dec increments) n increments))))))

(t/deftest only-use-live-tries-for-available-jobs-4930
  (binding [cat/*file-size-target* 16]
    (t/is (= #{{:trie-keys ["l01-rc-b00" "l01-rc-b01" "l01-rc-b02" "l01-rc-b03"],
                :part [3],
                :out-trie-key "l02-rc-p3-b03"}
               {:trie-keys ["l01-rc-b00" "l01-rc-b01" "l01-rc-b02" "l01-rc-b03"],
                :part [2],
                :out-trie-key "l02-rc-p2-b03"}
               {:trie-keys ["l01-rc-b00" "l01-rc-b01" "l01-rc-b02" "l01-rc-b03"],
                :part [1],
                :out-trie-key "l02-rc-p1-b03"}}
             (apply calc-jobs (concat (level-seq 1 16)
                                      [["l02-rc-p0-b03" 20] ["l02-rc-p0-b07" 20] ["l02-rc-p0-b0b" 20] ["l02-rc-p0-b0f" 20]]))))))

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

    (t/testing "L1H -> L2H"
      (t/is (= #{}
               (calc-jobs ["l01-r20200101-b00" 2] ["l01-r20200101-b01" 2] ["l01-r20200101-b02" 2]
                          ["l01-rc-b00" 0] ["l01-rc-b01" 0] ["l01-rc-b02" 0]))
            "L1H only -> L2H. files too small and not enough")

      (t/is (= #{(job "l02-r20200101-b01" ["l01-r20200101-b00" "l01-r20200101-b01"] [])}
               (calc-jobs ["l01-r20200101-b00" 10] ["l01-r20200101-b01" 10] ["l01-r20200101-b02" 10]
                          ["l01-rc-b00" 0] ["l01-rc-b01" 0] ["l01-rc-b02" 0]))
            "L1H only -> L2H. files > threshold")

      (t/is (= #{}
               (calc-jobs ["l01-r20200101-b00" 2] ["l01-r20200101-b01" 2] ["l01-r20200101-b02" 2] ["l01-r20200101-b03" 2]
                          ["l01-rc-b00" 0] ["l01-rc-b01" 0] ["l01-rc-b02" 0]))
            "L1H only -> L2H - enough files but last one missing its L1C")

      (t/is (= #{(job "l02-r20200101-b03" ["l01-r20200101-b00" "l01-r20200101-b01" "l01-r20200101-b02" "l01-r20200101-b03"] [])}
               (calc-jobs ["l01-r20200101-b00" 2] ["l01-r20200101-b01" 2] ["l01-r20200101-b02" 2] ["l01-r20200101-b03" 2]
                          ["l01-rc-b00" 0] ["l01-rc-b01" 0] ["l01-rc-b02" 0] ["l01-rc-b03" 0]))
            "L1H only -> L2H - enough files")

      (t/is (= #{(job "l02-r20200101-b02" ["l02-r20200101-b01" "l01-r20200101-b02"] [])}
               (calc-jobs ["l01-r20200101-b00" 2] ["l01-r20200101-b01" 2] ["l01-r20200101-b02" 2] ["l01-r20200101-b03" 2]
                          ["l01-rc-b00" 0] ["l01-rc-b01" 0] ["l01-rc-b02" 0] ["l01-rc-b03" 0]
                          ["l02-r20200101-b01" 14]))
            "merge into partial L2H til it's full")

      (t/is (= #{(job "l02-r20200101-b03" ["l02-r20200101-b00" "l01-r20200101-b01" "l01-r20200101-b02" "l01-r20200101-b03"] [])}
               (calc-jobs ["l01-r20200101-b00" 2] ["l01-r20200101-b01" 2] ["l01-r20200101-b02" 2] ["l01-r20200101-b03" 2]
                          ["l01-rc-b00" 0] ["l01-rc-b01" 0] ["l01-rc-b02" 0] ["l01-rc-b03" 0]
                          ["l02-r20200101-b00" 2]))
            "merge into partial L2H when there are 3 L1Hs, even if it doesn't fill it up")

      (t/is (= #{(job "l02-r20200101-b03" ["l01-r20200101-b02" "l01-r20200101-b03"] [])}
               (calc-jobs ["l01-r20200101-b00" 10] ["l01-r20200101-b01" 10] ["l01-r20200101-b02" 10] ["l01-r20200101-b03" 10]
                          ["l01-rc-b00" 0] ["l01-rc-b01" 0] ["l01-rc-b02" 0] ["l01-rc-b03" 0]
                          ["l02-r20200101-b01" 20]))
            "full L2H so we start again")

      (t/is (= #{(job "l02-r20200101-b03" ["l02-r20200101-b01" "l01-r20200101-b02" "l01-r20200101-b03"] [])}
               (calc-jobs ["l01-r20200101-b00" 2] ["l01-r20200101-b01" 2] ["l01-r20200101-b02" 2] ["l01-r20200101-b03" 2]  ["l01-r20200101-b04" 2]  ["l01-r20200101-b05" 2]
                          ["l01-rc-b00" 0] ["l01-rc-b01" 0] ["l01-rc-b02" 0] ["l01-rc-b03" 0] ["l01-rc-b04" 0] ["l01-rc-b05" 0]
                          ["l02-r20200101-b00" 16] ["l02-r20200101-b01" 12]))
            "one full L2H, one partial - merge into the partial (#4679)"))

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

          "fewer than 4 L1H files take us over the limit, so we compact early")

    (t/is (= #{(job "l02-r20200102-b03" ["l02-r20200102-b00" "l01-r20200102-b01" "l01-r20200102-b02" "l01-r20200102-b03"] [])}
             (calc-jobs ["l02-r20200101-b00" 3] ["l01-r20200101-b01" 3] ["l01-r20200101-b02" 3]
                        ["l02-r20200102-b00" 3] ["l01-r20200102-b01" 3] ["l01-r20200102-b02" 3] ["l01-r20200102-b03" 3]
                        ;; to mark the historical ones 'live'
                        ["l01-rc-b00" 0] ["l01-rc-b01" 0] ["l01-rc-b02" 0] ["l01-rc-b03" 0]))
          "include existing L2H")

    (t/is (= #{(job "l02-r20200101-b02" ["l02-r20200101-b01" "l01-r20200101-b02"] [])}
             (calc-jobs ["l02-r20200101-b01" 12] ["l01-r20200101-b02" 6] ["l01-r20200101-b03" 5]
                        ;; to mark the historical ones 'live'
                        ["l01-rc-b02" 0] ["l01-rc-b03" 0]))

          "L2H + L1H take us over the limit, compact early")))

(defn- filter-result [required-level part-prefix res]
  (filter (comp (fn [{:keys [level part]}]
                  (and (= level required-level) (= part-prefix (take (count part-prefix) (vec part)))))
                trie/parse-trie-key
                :out-trie-key)
          res))

(t/deftest test-l2+-compaction-jobs
  (binding [cat/*file-size-target* 16]
    (t/is (= #{ ;; L2 [0] has loads, merge from 0x10 onwards (but only 4)
               (job "l02-rc-p0-b113" ["l01-rc-b110" "l01-rc-b111" "l01-rc-b112" "l01-rc-b113"] [0])

               ;; L2 [1] has nothing, choose the first four
               (job "l02-rc-p1-b03" ["l01-rc-b00" "l01-rc-b01" "l01-rc-b02" "l01-rc-b03"] [1])

               ;; fill in the gaps in [2] and [3]
               (job "l02-rc-p2-b0f" ["l01-rc-b0c" "l01-rc-b0d" "l01-rc-b0e" "l01-rc-b0f"] [2])
               (job "l02-rc-p3-b0b" ["l01-rc-b08" "l01-rc-b09" "l01-rc-b0a" "l01-rc-b0b"] [3])}

             (binding [cat/*file-size-target* 2]
               (calc-jobs ["l02-rc-p0-b03"] ["l02-rc-p2-b03"] ["l02-rc-p3-b03"] ; missing [1]
                          ["l02-rc-p0-b07"] ["l02-rc-p2-b07"] ["l02-rc-p3-b07"] ; missing [1]
                          ["l02-rc-p0-b0b"] ["l02-rc-p2-b0b"] ; missing [1] + [3]
                          ["l02-rc-p0-b0f"] ; missing [1], [2], and [3]
                          ["l01-rc-b00" 2] ["l01-rc-b01" 2] ["l01-rc-b02" 2] ["l01-rc-b03" 2]
                          ["l01-rc-b04" 2] ["l01-rc-b05" 2] ["l01-rc-b06" 2] ["l01-rc-b07" 2]
                          ["l01-rc-b08" 2] ["l01-rc-b09" 2] ["l01-rc-b0a" 2] ["l01-rc-b0b" 2]
                          ["l01-rc-b0c" 2] ["l01-rc-b0d" 2] ["l01-rc-b0e" 2] ["l01-rc-b0f" 2]
                          ["l01-rc-b110" 2] ["l01-rc-b111" 2] ["l01-rc-b112" 2] ["l01-rc-b113" 2]
                          ;; superseded ones
                          ["l01-rc-b00" 1] ["l01-rc-b02" 1] ["l01-rc-b09" 1] ["l01-rc-b0d" 1])))
          "up to L3")

    (t/is (= #{ ;; for part group [0] only 2 are missing
               (job "l03-rc-p00-b0f" ["l02-rc-p0-b03" "l02-rc-p0-b07" "l02-rc-p0-b0b" "l02-rc-p0-b0f"] [0 0])
               (job "l03-rc-p01-b0f" ["l02-rc-p0-b03" "l02-rc-p0-b07" "l02-rc-p0-b0b" "l02-rc-p0-b0f"] [0 1])}

             (binding [cat/*file-size-target* 2]
               (->> (calc-jobs ["l03-rc-p02-b0f"]
                               ["l03-rc-p03-b0f"]
                               ;; The L2 part groups need to be full to be live and considered for l03 compaction
                               ["l02-rc-p0-b03"] ["l02-rc-p1-b03"] ["l02-rc-p2-b03"] ["l02-rc-p3-b03"]
                               ["l02-rc-p0-b07"] ["l02-rc-p1-b07"] ["l02-rc-p2-b07"] ["l02-rc-p3-b07"]
                               ["l02-rc-p0-b0b"] ["l02-rc-p1-b0b"] ["l02-rc-p2-b0b"] ["l02-rc-p3-b0b"]
                               ["l02-rc-p0-b0f"] ["l02-rc-p1-b0f"] ["l02-rc-p2-b0f"] ["l02-rc-p3-b0f"]
                               ["l01-rc-b00" 2] ["l01-rc-b01" 2] ["l01-rc-b02" 2] ["l01-rc-b03" 2]
                               ["l01-rc-b04" 2] ["l01-rc-b05" 2] ["l01-rc-b06" 2] ["l01-rc-b07" 2]
                               ["l01-rc-b08" 2] ["l01-rc-b09" 2] ["l01-rc-b0a" 2] ["l01-rc-b0b" 2]
                               ["l01-rc-b0c" 2] ["l01-rc-b0d" 2] ["l01-rc-b0e" 2] ["l01-rc-b0f" 2]
                               ;; superseded ones
                               ["l01-rc-b00" 1] ["l01-rc-b02" 1] ["l01-rc-b09" 1] ["l01-rc-b0d" 1])
                    (filter-result 3 [0])
                    set)))
          "L2 -> L3 current recency")

    (t/testing "L2H -> L3H"
      (t/is (= #{}
               (calc-jobs ["l02-r20250101-b01" 10] ["l02-r20250101-b02" 25]
                          ["l02-r20250101-b03" 10] ["l02-r20250101-b04" 13] ["l02-r20250101-b05" 18]
                          ["l02-r20250101-b06" 25]
                          ["l02-r20250101-b07" 10] ["l02-r20250101-b08" 14])))

      (t/is (= (let [l2-keys ["l02-r20250101-b02" "l02-r20250101-b05" "l02-r20250101-b06" "l02-r20250101-b09"]]
                 #{(job "l03-r20250101-p0-b09" l2-keys [0])
                   (job "l03-r20250101-p1-b09" l2-keys [1])
                   (job "l03-r20250101-p2-b09" l2-keys [2])
                   (job "l03-r20250101-p3-b09" l2-keys [3])})

               (calc-jobs ["l02-r20250101-b01" 10] ["l02-r20250101-b02" 25]
                          ["l02-r20250101-b03" 10] ["l02-r20250101-b04" 13] ["l02-r20250101-b05" 18]
                          ["l02-r20250101-b06" 25]
                          ["l02-r20250101-b07" 10] ["l02-r20250101-b08" 14] ["l02-r20250101-b09" 19]))))

    (t/testing "L3 -> L4"
      (let [l3-keys ["l03-rc-p03-b0f" "l03-rc-p03-b11f" "l03-rc-p03-b12f" "l03-rc-p03-b13f"]]
        (t/is (= #{(job "l04-rc-p030-b13f" l3-keys [0 3 0])
                   (job "l04-rc-p031-b13f" l3-keys [0 3 1])
                   (job "l04-rc-p032-b13f" l3-keys [0 3 2])
                   (job "l04-rc-p033-b13f" l3-keys [0 3 3])}

                 (->> (apply calc-jobs (level-seq 3 64))
                      (filter-result 4 [0 3])
                      set)))))

    (t/testing "L3H -> L4H"
      (let [in-keys ["l03-r20200101-p0-b03" "l03-r20200101-p0-b07" "l03-r20200101-p0-b0b" "l03-r20200101-p0-b0f"]]
        (t/is (= #{(job "l04-r20200101-p00-b0f" in-keys [0 0])
                   (job "l04-r20200101-p01-b0f" in-keys [0 1])
                   (job "l04-r20200101-p02-b0f" in-keys [0 2])
                   (job "l04-r20200101-p03-b0f" in-keys [0 3])}
                 (->> (apply calc-jobs (level-seq 3 16 #xt/date "2020-01-01"))
                      (filter-result 4 [0])
                      set)))))))


(defn table-path ^java.nio.file.Path [node table]
  (-> (db/primary-db node)
      ^LocalStorage (.getBufferPool)
      (.getRootPath)
      (.resolve (str "tables/" table))))

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
          (xt-log/sync-node node)
          (c/compact-all! node (Duration/ofSeconds 1))

          (t/is (= (range 100) (q)))

          (submit! (range 100 200))
          (xt-log/sync-node node)
          (c/compact-all! node (Duration/ofSeconds 1))

          (t/is (= (range 200) (q)))

          (submit! (range 200 500))
          (xt-log/sync-node node)
          (c/compact-all! node (Duration/ofSeconds 1))

          (t/is (= (range 500) (q)))

          (aet/check-arrow-edn-dir (.toPath (io/as-file (io/resource "xtdb/compactor-test/test-l1-compaction")))
                                   (table-path node "public$foo")
                                   #"l01-rc-(.+)\.arrow"))))))

(t/deftest test-l1-compaction-by-recency
  (let [node-dir (util/->path "target/compactor/test-l1-compaction-by-recency")]
    (util/delete-dir node-dir)

    (binding [c/*page-size* 32
              cat/*file-size-target* (* 1024 16)
              c/*ignore-signal-block?* true]
      (util/with-open [node (tu/->local-node {:node-dir node-dir})]
        (xt/execute-tx node [(into [:put-docs {:into :readings, :valid-from #inst "2020-01-01", :valid-to #inst "2020-01-05"}]
                                   (for [x (range 100)]
                                     {:xt/id x, :reading 8.3}))

                             (into [:put-docs :prices]
                                   (for [x (range 100)]
                                     {:xt/id x, :price 12.4}))])

        (t/is (= [{:row-count 100}] (xt/q node "SELECT COUNT(*) row_count FROM readings FOR ALL VALID_TIME")))
        (t/is (= [{:row-count 100}] (xt/q node "SELECT COUNT(*) row_count FROM prices")))
        (t/is (= [{:row-count 100}] (xt/q node "SELECT COUNT(*) row_count FROM prices FOR ALL VALID_TIME")))

        (tu/flush-block! node)
        (c/compact-all! node #xt/duration "PT1S")

        (t/is (= [{:row-count 100}] (xt/q node "SELECT COUNT(*) row_count FROM readings FOR ALL VALID_TIME")))
        (t/is (= [{:row-count 100}] (xt/q node "SELECT COUNT(*) row_count FROM prices")))
        (t/is (= [{:row-count 100}] (xt/q node "SELECT COUNT(*) row_count FROM prices FOR ALL VALID_TIME")))

        (xt/execute-tx node [(into [:put-docs {:into :readings, :valid-from #inst "2020-01-05", :valid-to #inst "2020-01-09"}]
                                   (for [x (range 100)]
                                     {:xt/id x, :reading 19.0}))

                             (into [:put-docs :prices]
                                   (for [x (range 100)]
                                     {:xt/id x, :price 6.2}))])

        (t/is (= [{:row-count 200}] (xt/q node "SELECT COUNT(*) row_count FROM readings FOR ALL VALID_TIME")))
        (t/is (= [{:row-count 100}] (xt/q node "SELECT COUNT(*) row_count FROM prices")))
        (t/is (= [{:row-count 200}] (xt/q node "SELECT COUNT(*) row_count FROM prices FOR ALL VALID_TIME")))

        (tu/flush-block! node)
        (c/compact-all! node #xt/duration "PT1S")

        (t/is (= [{:row-count 200}] (xt/q node "SELECT COUNT(*) row_count FROM readings FOR ALL VALID_TIME")))
        (t/is (= [{:row-count 200}] (xt/q node "SELECT COUNT(*) row_count FROM prices FOR ALL VALID_TIME")))

        (aet/check-arrow-edn-dir (.toPath (io/as-file (io/resource "xtdb/compactor-test/test-l1-compaction-by-recency/readings")))
                                 (table-path node "public$readings") #"l01-(.+)\.arrow")

        (aet/check-arrow-edn-dir (.toPath (io/as-file (io/resource "xtdb/compactor-test/test-l1-compaction-by-recency/prices")))
                                 (table-path node "public$prices") #"l01-(.+)\.arrow")))))

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

          (submit! (range 500))
          (xt-log/sync-node node #xt/duration "PT2S")
          (c/compact-all! node #xt/duration "PT2S")

          (t/is (= (set (range 500)) (set (q))))

          (submit! (range 500 1000))
          (xt-log/sync-node node #xt/duration "PT2S")
          (c/compact-all! node #xt/duration "PT2S")

          (t/is (= (set (range 1000)) (set (q))))

          (submit! (range 1000 2000))
          (xt-log/sync-node node #xt/duration "PT5S")
          (c/compact-all! node #xt/duration "PT5S")

          (t/is (= (set (range 2000)) (set (q))))

          (aet/check-arrow-edn-dir (.toPath (io/as-file (io/resource "xtdb/compactor-test/test-l2+-compaction")))
                                   (table-path node "public$foo")
                                   #"l(?!00|01)\d\d-(.+)\.arrow"))))))

(t/deftest test-l2+-compaction-by-recency
  (let [node-dir (util/->path "target/compactor/test-l2+-compaction-by-recency")]
    (util/delete-dir node-dir)

    (binding [c/*page-size* 32
              cat/*file-size-target* (* 1024 16)
              c/*ignore-signal-block?* true]
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

        (c/compact-all! node #xt/duration "PT1S")

        (aet/check-arrow-edn-dir (.toPath (io/as-file (io/resource "xtdb/compactor-test/test-l2+-compaction-by-recency/readings")))
                                 (table-path node "public$readings")
                                 #"l(?!00|01)(.+)\.arrow")

        (aet/check-arrow-edn-dir (.toPath (io/as-file (io/resource "xtdb/compactor-test/test-l2+-compaction-by-recency/prices")))
                                 (table-path node "public$prices")
                                 #"l(?!00|01)(.+)\.arrow")))))

(defn bad-uuid-seq
  ([n] (bad-uuid-seq 0 n))
  ([start end]
   (letfn [(new-uuid [n]
             (java.util.UUID. 0 n))]
     (map new-uuid (range start end)))))

(t/deftest test-no-empty-pages-3580
  (let [node-dir (util/->path "target/compactor/test-badly-distributed")]
    (util/delete-dir node-dir)

    (binding [c/*page-size* 8
              cat/*file-size-target* (* 16 1024)
              c/*ignore-signal-block?* true]
      (util/with-open [node (tu/->local-node {:node-dir node-dir, :rows-per-block 10})]
        (let [db (db/primary-db node)
              bp (.getBufferPool db)
              meta-mgr (.getMetadataManager db)]
          (letfn [(submit! [xs]
                    (last (for [batch (partition-all 8 xs)]
                            (xt/submit-tx node [(into [:put-docs :foo]
                                                      (for [x batch]
                                                        {:xt/id x}))]))))]

            (submit! (take 512 (cycle (bad-uuid-seq 8))))
            (xt-log/sync-node node)
            (c/compact-all! node (Duration/ofSeconds 5))

            (let [^String table-name "foo"
                  meta-files (->> (.listAllObjects bp (trie/->table-meta-dir table-name))
                                  (mapv (comp :key os/<-StoredObject)))]

              ;; TODO this doseq seems to return nothing, so nothing gets tested?
              (doseq [{:keys [^String trie-key]} (map trie/parse-trie-file-path meta-files)]
                (util/with-open [seg (BufferPoolSegment. tu/*allocator* bp meta-mgr table-name trie-key nil)
                                 seg-meta (.openMetadata seg)]
                  (doseq [leaf (.getLeaves (.getTrie seg-meta))
                          :let [page (.page seg-meta leaf)
                                rel (.loadDataPage (.getPage page) tu/*allocator*)]]
                    (t/is (pos? (.getRowCount rel)))))))))))))

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

          (submit! (bad-uuid-seq 100))
          (xt-log/sync-node node)
          (c/compact-all! node (Duration/ofSeconds 1))

          (t/is (= (bad-uuid-seq 100) (q)))

          (submit! (bad-uuid-seq 100 200))
          (xt-log/sync-node node)
          (c/compact-all! node (Duration/ofSeconds 1))

          (t/is (= (bad-uuid-seq 200) (q)))

          (submit! (bad-uuid-seq 200 500))
          (xt-log/sync-node node)
          (c/compact-all! node (Duration/ofSeconds 1))

          (t/is (= (bad-uuid-seq 500) (q))))))))

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

        (c/compact-all! node #xt/duration "PT2S")

        (t/is (= #{{:xt/id 0, :count 12} {:xt/id 1, :count 12}}
                 (set (xt/q node "SELECT _id, count(*) count
                                  FROM docs FOR ALL VALID_TIME
                                  GROUP BY _id"))))

        (aet/check-arrow-edn-dir (.toPath (io/as-file (io/resource "xtdb/compactor-test/lose-data-on-compaction")))
                                 (table-path node "public$docs")
                                 #"(.+)\.arrow")))))

(t/deftest test-compaction-promotion-bug-3673
  (xt/submit-tx tu/*node* [[:put-docs :foo {:xt/id 0, :foo 12.0} {:xt/id 1}]])
  (tu/flush-block! tu/*node*)
  (c/compact-all! tu/*node* #xt/duration "PT0.5S")

  (xt/submit-tx tu/*node* [[:put-docs :foo {:xt/id 2, :foo 24} {:xt/id 3, :foo 28.1} {:xt/id 4}]])
  (tu/flush-block! tu/*node*)
  (c/compact-all! tu/*node* #xt/duration "PT0.5S")

  (t/is (= #{12.0 nil 24 28.1}
           (->> (xt/q tu/*node* "SELECT foo FROM foo")
                (into #{} (map :foo)))))

  (util/with-open [node (xtn/start-node tu/*node-opts*)]
    (xt/submit-tx node [[:put-docs :foo {:xt/id 1} {:foo "foo", :xt/id 0} {:foo 3, :xt/id 2}]])
    (tu/flush-block! node)
    (c/compact-all! node #xt/duration "PT0.5S")

    (t/is (= #{"foo" nil 3}
             (->> (xt/q node "SELECT foo FROM foo")
                  (into #{} (map :foo)))))))

(t/deftest test-compaction-with-erase-4017
  (binding [c/*ignore-signal-block?* true]
    (let [node-dir (util/->path "target/compactor/compaction-with-erase")]
      (util/delete-dir node-dir)

      (util/with-open [node (tu/->local-node {:node-dir node-dir})]

        (let [[id-before id id-after] [#uuid "00000000-0000-0000-0000-000000000000"
                                       #uuid "40000000-0000-0000-0000-000000000000"
                                       #uuid "80000000-0000-0000-0000-000000000000"]]

          (xt/execute-tx node [[:put-docs :foo {:xt/id id-before} {:xt/id id} {:xt/id id-after}]])
          (tu/flush-block! node)
          (c/compact-all! node #xt/duration "PT0.5S")

          (xt/execute-tx node [["ERASE FROM foo WHERE _id = ?" id]])
          (tu/flush-block! node)
          (c/compact-all! node #xt/duration "PT0.5S")

          (t/is (= [{:xt/id id-before} {:xt/id id-after}]
                   (xt/q node "SELECT _id FROM foo FOR ALL VALID_TIME FOR ALL SYSTEM_TIME")))

          (xt/execute-tx node [[:put-docs :foo {:xt/id id}]])

          (t/testing "an id where there is no previous data shouldn't show up in the compacted files"
            (xt/execute-tx node [["ERASE FROM foo WHERE _id = ?" #uuid "20000000-0000-0000-0000-000000000000"]]))

          (tu/flush-block! node)
          (c/compact-all! node #xt/duration "PT0.5S")

          (t/is (= [{:xt/id id-before} {:xt/id id} {:xt/id id-after}]
                   (xt/q node "SELECT _id FROM foo FOR ALL VALID_TIME FOR ALL SYSTEM_TIME"))))

        (aet/check-arrow-edn-dir (.toPath (io/as-file (io/resource "xtdb/compactor-test/compaction-with-erase")))
                                 (table-path node "public$foo")
                                 #"l01-rc-(.+)\.arrow")))))

(t/deftest compactor-trie-metadata
  (let [node-dir (util/->path "target/compactor/compactor-metadata-test")]
    (util/delete-dir node-dir)

    (with-open [node (tu/->local-node {:node-dir node-dir, :compactor-threads 1})]
      (let [bp (.getBufferPool (db/primary-db node))]
        (xt/execute-tx node [[:put-docs {:into :foo
                                         :valid-from #xt/instant "2010-01-01T00:00:00Z"
                                         :valid-to #xt/instant "2011-01-01T00:00:00Z"}
                              {:xt/id 1}]]
                       {:default-tz #xt/zone "Europe/London"})
        (tu/flush-block! node)
        ;; to have consistent block files
        (c/compact-all! node #xt/duration "PT5S")

        (xt/execute-tx node [[:put-docs {:into :foo
                                         :valid-from #xt/instant "2015-01-01T00:00:00Z"
                                         :valid-to #xt/instant "2016-01-01T00:00:00Z"}
                              {:xt/id 2}]]
                       {:default-tz #xt/zone "Europe/London"})
        (tu/flush-block! node)

        (c/compact-all! node #xt/duration "PT5S")
        ;; to artifically create a new table block
        (tu/flush-block! node)

        (cpb/check-pbuf (.toPath (io/as-file (io/resource "xtdb/compactor-test/compactor-metadata-test")))
                        (.resolve node-dir "objects"))

        (let [tries (->> (.getByteArray bp (util/->path "tables/public$foo/blocks/b02.binpb"))
                         TableBlock/parseFrom
                         table-cat/<-table-block
                         :tries
                         (into #{} (map table-test/trie-details->edn)))]
          (t/is (= #{"l00-rc-b00" "l01-r20110103-b00" "l01-rc-b00" "l00-rc-b01" "l01-r20160104-b01" "l01-rc-b01"}
                   (into #{} (map :trie-key) tries)))

          (t/is (= {"l01-rc-b00" nil,
                    "l01-r20110103-b00" {:min-valid-from #xt/instant "2010-01-01T00:00:00Z",
                                         :max-valid-from #xt/instant "2010-01-01T00:00:00Z",
                                         :min-valid-to #xt/instant "2011-01-01T00:00:00Z",
                                         :max-valid-to #xt/instant "2011-01-01T00:00:00Z",
                                         :min-system-from #xt/instant "2020-01-01T00:00:00Z",
                                         :max-system-from #xt/instant "2020-01-01T00:00:00Z",
                                         :row-count 1},
                    "l01-rc-b01" nil,
                    "l01-r20160104-b01" {:min-valid-from #xt/instant "2015-01-01T00:00:00Z",
                                         :max-valid-from #xt/instant "2015-01-01T00:00:00Z",
                                         :min-valid-to #xt/instant "2016-01-01T00:00:00Z",
                                         :max-valid-to #xt/instant "2016-01-01T00:00:00Z",
                                         :min-system-from #xt/instant "2020-01-02T00:00:00Z",
                                         :max-system-from #xt/instant "2020-01-02T00:00:00Z",
                                         :row-count 1}}
                   (->> tries
                        (filter #(str/starts-with? (:trie-key %) "l01"))
                        (into {} (map (juxt :trie-key
                                            (fn [{:keys [trie-metadata]}]
                                              (dissoc trie-metadata :iid-bloom)))))))))))))

(t/deftest different-recency-partitioning
  (binding [c/*recency-partition* RecencyPartition/YEAR]
    (with-open [node (xtn/start-node (assoc-in tu/*node-opts* [:log]
                                               [:in-memory {:instant-src (tu/->mock-clock (tu/->instants :year))}]))]
      (let [tc (.getTrieCatalog (db/primary-db node))]
        (xt/execute-tx node [[:put-docs :docs {:xt/id 1 :version 1}]])
        (xt/execute-tx node [[:put-docs :docs {:xt/id 1 :version 2}]])
        (xt/execute-tx node [[:put-docs :docs {:xt/id 1 :version 3}]])
        (tu/flush-block! node)
        (c/compact-all! node #xt/duration "PT1S")

        (t/is (= #{"l01-r20210101-b00" "l01-r20220101-b00" "l01-rc-b00"}
                 (->> (cat/trie-state tc #xt/table docs)
                      (cat/current-tries)
                      (into #{} (map :trie-key)))))))))

(t/deftest dont-lose-erases-during-compaction
  (let [sql "SELECT _id, _valid_from, _valid_to FROM foo FOR ALL VALID_TIME FOR ALL SYSTEM_TIME"]
    (xt/execute-tx tu/*node* [[:put-docs :foo {:xt/id 1 :xt/valid-to #inst "2050"} {:xt/id 2 :xt/valid-to #inst "2050"}]])
    (tu/flush-block! tu/*node*)
    (c/compact-all! tu/*node* #xt/duration "PT1S")

    (t/is (= [{:xt/id 2, :xt/valid-from #xt/zdt "2020-01-01Z[UTC]", :xt/valid-to #xt/zdt "2050-01-01Z[UTC]"}
              {:xt/id 1, :xt/valid-from #xt/zdt "2020-01-01Z[UTC]", :xt/valid-to #xt/zdt "2050-01-01Z[UTC]"}]
             (xt/q tu/*node* sql)))

    (xt/execute-tx tu/*node* [[:erase-docs :foo 1 2]])

    (t/is (= [] (xt/q tu/*node* sql)))

    (tu/flush-block! tu/*node*)
    (c/compact-all! tu/*node* #xt/duration "PT1S")

    (t/is (= [] (xt/q tu/*node* sql)))))

(t/deftest null-duv-issue-4231
  (xt/execute-tx tu/*node* [[:put-docs :docs {:xt/id 1 :l [{:foo 1}]}]])
  (xt/execute-tx tu/*node* [[:put-docs :docs {:xt/id 2 :l []}]])
  (tu/flush-block! tu/*node*)
  (c/compact-all! tu/*node* nil)

  (t/is (= [{:xt/id 2, :l []} {:xt/id 1, :l [{:foo 1}]}]
           (xt/q tu/*node* ["SELECT * FROM docs"]))))

(t/deftest test-same-entity-same-transaction-4303
  (let [q "SELECT *, _valid_from, _valid_to, _system_from, _system_to FROM docs FOR ALL VALID_TIME"]
    (doseq [[test-name vf1 vt1 vf2 vt2] [["same valid-time"]
                                         ["overriden in valid-time" #inst "2023" #inst "2024" #inst "2022" #inst "2025"]
                                         ["split in valid-time" #inst "2022" #inst "2025" #inst "2023" #inst "2024"]
                                         ["overlapping valid-time" #inst "2022" #inst "2025" #inst "2023" #inst "2026"]]]
      (t/testing test-name
        (with-open [node (xtn/start-node (assoc-in tu/*node-opts* [:log]
                                                   [:in-memory {:instant-src (tu/->mock-clock (tu/->instants :year))}]))]
          (xt/execute-tx node [[:put-docs :docs
                                (cond-> {:xt/id 1, :version 1}
                                  vf1 (assoc :xt/valid-from vf1)
                                  vt1 (assoc :xt/valid-to vt1))
                                (cond-> {:xt/id 1, :version 2}
                                  vf2 (assoc :xt/valid-from vf2)
                                  vt2 (assoc :xt/valid-to vt2))]])

          (let [res-before-compaction (xt/q node q)]
            (tu/flush-block! node)
            (c/compact-all! node nil)

            (t/is (= (set res-before-compaction) (set (xt/q node q))))))))))

(t/deftest no-returning-erased-docs-4576
  (xt/execute-tx tu/*node* [[:put-docs :xt_docs {:xt/id :doc1 :col "value1"}]
                            [:put-docs :xt_docs {:xt/id :doc2 :col "value2"}]])
  (tu/finish-block! tu/*node*)
  (xt/execute-tx tu/*node* [[:erase-docs :xt_docs :doc1]])
  (t/is (= [{:xt/id :doc2 :col "value2"}] (xt/q tu/*node* "SELECT * FROM xt_docs")))

  (tu/finish-block! tu/*node*)
  (xt/execute-tx tu/*node* [[:delete-docs :xt_docs :doc2]])
  (t/is (= [] (xt/q tu/*node* "SELECT * FROM xt_docs"))))

(t/deftest keyword-hashing-issue-4572
  (xt/execute-tx tu/*node* [[:put-docs :cheeses {:xt/id :cheese/pálpusztai}]])
  (tu/finish-block! tu/*node*)

  (t/is (= [{:xt/id :cheese/pálpusztai}]
           (xt/q tu/*node* ['#(from :cheeses [{:xt/id %} *]) :cheese/pálpusztai]))))

(t/deftest test-copes-with-missing-put
  (let [node-dir (util/->path "target/compactor-test/copes-with-missing-put")]
    (util/delete-dir node-dir)
    (util/with-open [node (tu/->local-node {:node-dir node-dir
                                            :instant-src (tu/->mock-clock)})]
      (xt/execute-tx node [[:put-docs :docs
                            {:xt/id "foo", :a 1}
                            {:xt/id "bar", :a 2}
                            {:xt/id "baz", :a 3}]])
      (tu/flush-block! node)
      (xt/execute-tx node [[:delete-docs :docs "foo"]])
      (tu/flush-block! node)
      (xt/execute-tx node [[:erase-docs :docs "bar"]])
      (tu/flush-block! node)
      (xt/execute-tx node [[:delete-docs :docs "baz"]
                           [:erase-docs :docs "baz"]])
      (tu/flush-block! node)

      (c/compact-all! node #xt/duration "PT2S")

      (aet/check-arrow-edn-dir (.toPath (io/as-file (io/resource "xtdb/compactor-test/copes-with-missing-put")))
                               (.resolve node-dir "objects"))

      (t/is (= [{:xt/id "foo", :a 1,
                 :xt/valid-from #xt/zdt "2020-01-01Z[UTC]",
                 :xt/valid-to #xt/zdt "2020-01-02Z[UTC]",
                 :xt/system-from #xt/zdt "2020-01-01Z[UTC]"}
                {:xt/id "foo", :a 1,
                 :xt/valid-from #xt/zdt "2020-01-02Z[UTC]",
                 :xt/system-from #xt/zdt "2020-01-01Z[UTC]",
                 :xt/system-to #xt/zdt "2020-01-02Z[UTC]"}]
               (xt/q node "SELECT *, _valid_from, _valid_to, _system_from, _system_to
                           FROM docs FOR ALL VALID_TIME FOR ALL SYSTEM_TIME
                           ORDER BY _system_from, _valid_from"))))))
