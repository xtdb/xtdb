(ns xtdb.trie-catalog-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.compactor :as c]
            [xtdb.db-catalog :as db]
            [xtdb.garbage-collector :as gc]
            [xtdb.node :as xtn]
            [xtdb.test-util :as tu]
            [xtdb.time :as time]
            [xtdb.trie :as trie]
            [xtdb.trie-catalog :as cat]
            [xtdb.util :as util])
  (:import [java.nio.file Path]
           (java.time Duration Instant)
           (java.util.concurrent ConcurrentHashMap)
           (xtdb.api.storage ObjectStore$StoredObject)
           (xtdb.log.proto TemporalMetadata TrieMetadata)
           (xtdb.trie_catalog TrieCatalog)))

(t/use-fixtures :once tu/with-allocator)

(def ^:private as-of-time (time/->instant #inst "2000"))

(defn- apply-msgs [& trie-keys]
  (-> trie-keys
      (->> (transduce (map (fn [[trie-key size]]
                             (-> (trie/parse-trie-key trie-key)
                                 (assoc :data-file-size (or size -1)))))
                      (completing #(cat/apply-trie-notification  %1 %2 {:file-size-target 20, :as-of as-of-time}))
                      {}))))

(defn- curr-tries [& trie-keys]
  (-> (apply apply-msgs trie-keys)
      (cat/current-tries)
      (->> (into #{} (map :trie-key)))))

(defn- partitions [& trie-keys]
  (->> (apply apply-msgs trie-keys)
       cat/partitions
       (map #(update % :tries (partial map :trie-key)))
       set))

(t/deftest test-stale-msg
  (letfn [(stale? [tries trie-key]
            (boolean (cat/stale-msg? tries (-> (trie/parse-trie-key trie-key)
                                               (update :part vec)))))]

    (t/is (false? (stale? nil "l00-rc-b00")))

    (let [l0s (apply-msgs ["l00-rc-b00" 10] ["l00-rc-b01" 10] ["l00-rc-b02" 10])]
      (t/is (true? (stale? l0s "l00-rc-b00")))
      (t/is (true? (stale? l0s "l00-rc-b02")))
      (t/is (false? (stale? l0s "l00-rc-b03"))))

    (let [l1 (apply-msgs ["l01-r20200101-b01" 10])]
      (t/is (false? (stale? l1 "l01-r20200102-b00")))
      (t/is (true? (stale? l1 "l01-r20200101-b01")))
      (t/is (false? (stale? l1 "l01-r20200102-b01")))
      (t/is (false? (stale? l1 "l01-r20190101-b02")))
      (t/is (false? (stale? l1 "l01-rc-b01")))
      (t/is (false? (stale? l1 "l01-rc-b02"))))))

(defn apply-filter-msgs [& trie-keys]
  (map (fn [[trie-key recency temporal-metadata]]
         (let [^TemporalMetadata tm (apply tu/->temporal-metadata temporal-metadata)]
           (cat/map->CatalogEntry {:trie-key trie-key
                                   :recency (some-> recency time/micros->instant)
                                   :trie-metadata (-> (TrieMetadata/newBuilder)
                                                      (doto (.setTemporalMetadata tm))
                                                      (.build))})))
       trie-keys))

(defn- filter-tries [trie-keys query-bounds]
  (-> (apply apply-filter-msgs trie-keys)
      (cat/filter-tries query-bounds)
      (->> (into #{} (map :trie-key)))))

(t/deftest earilier-recency-files-can-effect-splitting-in-later-buckets-4097
  (let [query-bounds (tu/->temporal-bounds 20220101 20220102)]
    (t/is (= #{"l0-recency-2019-block-00" "l0-current-block-00"}
             (filter-tries [["l0-recency-2019-block-00" nil [20190101 20210101]]
                            ["l0-current-block-00" nil [20200101 Long/MAX_VALUE 20190101]]]
                           query-bounds))
          "earlier pages that contain data with later system time need to be taken")))

(t/deftest test-filter-tries
  (let [current-time 20200101]
    (t/testing "recency filtering (temporal metadata always overlaps the query)"
      (let [query-bounds (tu/->temporal-bounds current-time (inc current-time))]
        (t/is (empty? (filter-tries [] query-bounds)))

        (t/is (= #{"l0-current-block-02" "l0-current-block-01" "l0-current-block-00"}
                 (filter-tries [["l0-current-block-00" nil [20200101 Long/MAX_VALUE]]
                                ["l0-current-block-01" nil [20200101 Long/MAX_VALUE]]
                                ["l0-current-block-02" nil [20200101 Long/MAX_VALUE]]]
                               query-bounds)))

        (t/is (= #{"l01-current-block-00" "l01-current-block-01"}
                 (filter-tries [["l01-current-block-00"      nil      [20200101 Long/MAX_VALUE]]
                                ["l01-recency-2019-block-01" 20190101 [20180101 20190101]]
                                ["l01-current-block-01"      nil      [20200101 Long/MAX_VALUE]]
                                ["l01-recency-2020-block-01" 20200101 [20190101 20200101]]]
                               query-bounds))
              "older recency files get filtered (even at boundary)")

        (t/is (= #{"l01-current-block-00" "l01-current-block-01" "l01-recency-2022-block-01"}
                 (filter-tries [["l01-current-block-00"      nil      [20200101 Long/MAX_VALUE]]
                                ["l01-recency-2022-block-01" 20220101 [20210101 20220101]]
                                ["l01-current-block-01"      nil      [20200101 Long/MAX_VALUE]]]
                               query-bounds))
              "newer recency files get taken"))

      (let [all-st-query (tu/->temporal-bounds current-time (inc current-time) Long/MIN_VALUE Long/MAX_VALUE)
            all-vt-query (tu/->temporal-bounds Long/MIN_VALUE Long/MAX_VALUE current-time (inc current-time))
            st-range-query (tu/->temporal-bounds current-time (inc current-time) current-time Long/MAX_VALUE)
            vt-range-query (tu/->temporal-bounds current-time Long/MAX_VALUE current-time (inc current-time))]

        (t/is (= #{"l01-current-block-00" "l01-recency-2019-block-01" "l01-current-block-01" "l01-recency-2021-block-01"}
                 (filter-tries [["l01-current-block-00"      nil      [20200101 Long/MAX_VALUE]]
                                ["l01-recency-2019-block-01" 20190101 [20180101 Long/MAX_VALUE]]
                                ["l01-current-block-01"      nil      [20200101 Long/MAX_VALUE]]
                                ["l01-recency-2021-block-01" 20210101 [20200101 Long/MAX_VALUE]]]
                               all-st-query)
                 (filter-tries [["l01-current-block-00"      nil      [20200101 Long/MAX_VALUE]]
                                ["l01-recency-2019-block-01" 20190101 [20200101 Long/MAX_VALUE]]
                                ["l01-current-block-01"      nil      [20200101 Long/MAX_VALUE]]
                                ["l01-recency-2021-block-01" 20210101 [20200101 Long/MAX_VALUE]]]
                               all-vt-query))
              "all system-time or valid-time means bringing in older recency pages")

        (t/is (= #{"l01-current-block-00" "l01-current-block-01" "l01-recency-2021-block-01"}
                 (filter-tries [["l01-current-block-00"      nil      [20200101 Long/MAX_VALUE]]
                                ["l01-recency-2019-block-01" 20190101 [20180101 20190101]]
                                ["l01-current-block-01"      nil      [20200101 Long/MAX_VALUE]]
                                ["l01-recency-2021-block-01" 20210101 [20200101 20210101]]]
                               st-range-query)
                 (filter-tries [["l01-current-block-00"      nil      [20200101 Long/MAX_VALUE]]
                                ["l01-recency-2019-block-01" 20190101 [20180101 20190101]]
                                ["l01-current-block-01"      nil      [20200101 Long/MAX_VALUE]]
                                ["l01-recency-2021-block-01" 20210101 [20200101 20210101]]]
                               vt-range-query))
              "system-time range or valid-time range can filter certain pages")))

    (t/testing "filtering via temporal metadata"
      (let [query-bounds (tu/->temporal-bounds current-time Long/MAX_VALUE)]

        (t/is (= #{"l0-current-block-00" "l0-current-block-01"}
                 (filter-tries [["l0-recency-2021-block-00" 20210101 [20180101 20190101]]
                                ["l0-current-block-00"      nil      [20200101 Long/MAX_VALUE]]
                                ["l0-recency-2021-block-01" 20210101 [20190101 20200101]]
                                ["l0-current-block-01"      nil [20200101 Long/MAX_VALUE]]]
                               query-bounds))
              "recency doesn't filter, temporal metadata does filter"))

      (let [query-bounds (tu/->temporal-bounds current-time 20210101)]

        (t/is (= #{"l0-current-block-01" "l0-current-block-02"}
                 (filter-tries [["l0-current-block-00" nil [20100101 20190101]]
                                ["l0-current-block-01" nil [20190101 20250101]]
                                ["l0-current-block-02" nil [20220101 Long/MAX_VALUE]]
                                ["l0-current-block-03" nil [20250101 Long/MAX_VALUE]]]
                               query-bounds))
              "recency doesn't filter, temporal metadata does filter, files that can bound items in the query files set need to get taken")

        (t/is (= #{"l0-current-block-01"}
                 (filter-tries [["l0-current-block-00" nil [20100101 20190101 20100101]]
                                ["l0-current-block-01" nil [20190101 20250101 20200101]]
                                ["l0-current-block-02" nil [20220101 Long/MAX_VALUE 20190101]]]
                               query-bounds))
              "recency doesn't filter, temporal metadata does filter, if valid-time bounding files come earlier in system time they don't need to get taken")))))

(t/deftest test-partitions
  (t/is (= #{{:level 1,
              :part [],
              :recency #xt/date "2020-01-01",
              :tries ["l01-r20200101-b01"]
              :max-block-idx 1}
             {:level 1,
              :part [],
              :recency #xt/date "2020-01-02",
              :tries ["l01-r20200102-b01"]
              :max-block-idx 1}
             {:level 0,
              :part [],
              :recency nil,
              :tries ["l00-rc-b02" "l00-rc-b01" "l00-rc-b00"]
              :max-block-idx 2}}
           (partitions ["l00-rc-b00"] ["l00-rc-b01"] ["l00-rc-b02"]
                       ["l01-r20200101-b01"] ["l01-r20200102-b01"]))))

(t/deftest test-l0-l1-tries
  (t/is (= #{} (curr-tries)))

  (t/is (= #{"l00-rc-b00" "l00-rc-b01" "l00-rc-b02"}
           (curr-tries ["l00-rc-b00" 10] ["l00-rc-b01" 10] ["l00-rc-b02" 10])))

  (t/is (= #{"l00-rc-b00" "l00-rc-b01" "l00-rc-b02"}
           (curr-tries ["l00-rc-b00" 10] ["l00-rc-b01" 10] ["l00-rc-b02" 10]
                       ["l01-r20200101-b01" 5] ["l01-r20200102-b01" 5]))
        "historical tries nascent until we see the current")

  (t/is (= #{"l01-r20200101-b01" "l01-r20200102-b01" "l01-rc-b01" "l00-rc-b02"}
           (curr-tries ["l00-rc-b00" 10] ["l00-rc-b01" 10] ["l00-rc-b02" 10]
                       ["l01-r20200101-b01" 5] ["l01-r20200102-b01" 5] ["l01-rc-b01" 15]))
        "seen current, now historical tries are live too")

  (t/is (= #{"l01-r20200101-b01" "l01-rc-b02" "l01-rc-b03"}
           (curr-tries ["l00-rc-b00" 10] ["l00-rc-b01" 10] ["l00-rc-b02" 10] ["l00-rc-b02" 5]
                       ["l01-rc-b00" 10] ["l01-r20200101-b01" 15] ["l01-rc-b01" 5] ["l01-rc-b02" 20] ["l01-rc-b03" 5]))
        "L1C files can oscillate in size until they're full")

  (t/is (= #{"l01-rc-b01" "l00-rc-b02"}
           (curr-tries ["l00-rc-b00" 10] ["l00-rc-b01" 10] ["l00-rc-b02" 10] ["l01-rc-b01" 20]))
        "L1 file supersedes two L0 files")

  (t/is (= #{"l01-rc-b01" "l01-rc-b03" "l00-rc-b04"}
           (curr-tries ["l00-rc-b00" 10] ["l00-rc-b01" 10] ["l00-rc-b02" 10] ["l00-rc-b03" 10] ["l00-rc-b04" 10]
                       ["l01-rc-b00" 10] ["l01-rc-b01" 20] ["l01-rc-b02" 10] ["l01-rc-b03" 20]))
        "Superseded L1 files should not get returned"))

(t/deftest test-l0-addition-idempotent-4545
  ;; L0 TriesAdded message arrives after the L1 compaction
  (t/is (= #{"l01-rc-b00"}
           (curr-tries ["l00-rc-b00" 10] ["l01-rc-b00" 10] ["l00-rc-b00" 00]))))

(t/deftest keep-max-block-idx-around-4946
  (let [cat (apply-msgs ["l00-rc-b00" 10] ["l00-rc-b01" 10] ["l00-rc-b02" 10] ["l00-rc-b03" 10]
                        ["l01-rc-b00" 20] ["l01-rc-b01" 20] ["l01-rc-b02" 20] ["l01-rc-b03" 20]
                        ["l02-rc-p0-b03" 20] ["l02-rc-p1-b03" 20] ["l02-rc-p2-b03" 20] ["l02-rc-p3-b03" 20])]

    (t/is (true? (cat/stale-msg? cat (trie/parse-trie-key "l01-rc-b00"))))

    (let [new-cat (cat/remove-garbage cat (map :trie-key (cat/garbage-tries cat as-of-time)))]

      (t/is (true? (cat/stale-msg? new-cat (trie/parse-trie-key "l01-rc-b00")))))))

(t/deftest test-selects-l2-tries
  (t/is (= #{"l01-rc-b00"}
           (curr-tries ["l01-rc-b00" 2]
                       ["l02-rc-p0-b00"] ["l02-rc-p3-b00"]))
        "L2 file doesn't supersede because not all parts complete")

  (t/is (= #{"l02-rc-p0-b00" "l02-rc-p1-b00" "l02-rc-p2-b00" "l02-rc-p3-b00"}
           (curr-tries ["l01-rc-b00" 2]
                       ["l02-rc-p0-b00"] ["l02-rc-p1-b00"] ["l02-rc-p2-b00"] ["l02-rc-p3-b00"]))
        "now the L2 file is complete")

  (t/is (= #{"l02-rc-p0-b01" "l02-rc-p1-b01" "l02-rc-p2-b01" "l02-rc-p3-b01" "l00-rc-b02"}
           (curr-tries ["l00-rc-b00" 1] ["l00-rc-b01" 1] ["l00-rc-b02" 1]
                       ["l01-rc-b00" 1] ["l01-rc-b01" 2]
                       ["l02-rc-p0-b01"] ["l02-rc-p1-b01"] ["l02-rc-p2-b01"] ["l02-rc-p3-b01"]))
        "L2 file supersedes L1, L1 supersedes L0, left with a single L0 file"))

(t/deftest test-selects-l2h-tries
  (t/is (= #{"l02-r20200102-b00" "l02-r20200101-b01" "l01-r20200102-b01" "l01-rc-b01"}
           (curr-tries ["l01-r20200101-b01" 5] ["l01-r20200102-b01" 5] ["l01-rc-b01" 15] ["l01-r20200102-b00" 5]
                       ["l02-r20200101-b01" 5] ["l02-r20200102-b00" 5]))
        "L2H supersedes L1H with the same recency")

  (t/is (= #{"l02-r20200101-b02" "l02-r20200101-b03"
             "l02-r20200102-b01"
             "l02-r20200103-b00" "l02-r20200103-b01"}
           (curr-tries ["l02-r20200101-b00" 5] ["l02-r20200101-b01" 10] ["l02-r20200101-b02" 20] ["l02-r20200101-b03" 5]
                       ["l02-r20200102-b00" 10] ["l02-r20200102-b01" 15]
                       ["l02-r20200103-b00" 20] ["l02-r20200103-b01" 15]))
        "L2H is levelled within the recency partition"))

(t/deftest test-l3+
  (t/is (= #{"l03-rc-p00-b01" "l03-rc-p01-b01" "l03-rc-p02-b01" "l03-rc-p03-b01"
             ;; L2 path 0 covered
             "l02-rc-p1-b01" "l02-rc-p2-b01" "l02-rc-p3-b01"}

           (curr-tries ["l02-rc-p0-b01"] ["l02-rc-p1-b01"] ["l02-rc-p2-b01"] ["l02-rc-p3-b01"]
                       ["l03-rc-p00-b01"] ["l03-rc-p01-b01"] ["l03-rc-p02-b01"] ["l03-rc-p03-b01"]

                       ;; L2 path 1 not covered yet, missing [1 1]
                       ["l03-rc-p10-b01"] ["l03-rc-p12-b01"] ["l03-rc-p13-b01"]))

        "L3 covered idx 0 but not 1")

  (t/is (= #{"l03-r20200101-p0-b01" "l03-r20200101-p1-b01" "l03-r20200101-p2-b01" "l03-r20200101-p3-b01"

             ;; L2 20200101 covered
             "l02-r20200102-b00"}

           (curr-tries ["l02-r20200101-b01"] ["l02-r20200102-b00"]
                       ["l03-r20200101-p0-b01"] ["l03-r20200101-p1-b01"] ["l03-r20200101-p2-b01"] ["l03-r20200101-p3-b01"]

                       ;; L2 20200102 not covered yet, missing [1]
                       ["l03-r20200102-p0-b01"] ["l03-r20200102-p2-b01"] ["l03-r20200102-p3-b01"]))

        "L3 covered idx 0 but not 1")

  (t/is (= #{"l04-rc-p010-b01" "l04-rc-p011-b01" "l04-rc-p012-b01" "l04-rc-p013-b01"
             "l03-rc-p00-b01" "l03-rc-p02-b01" "l03-rc-p03-b01"} ; L3 path [0 1] covered

           (curr-tries ["l03-rc-p00-b01"] ["l03-rc-p01-b01"] ["l03-rc-p02-b01"] ["l03-rc-p03-b01"]
                       ["l03-rc-p10-b01"] ["l03-rc-p12-b01"] ["l03-rc-p13-b01"] ; L2 path 1 not covered yet, missing [1 1]
                       ["l04-rc-p010-b01"] ["l04-rc-p011-b01"] ["l04-rc-p012-b01"] ["l04-rc-p013-b01"]))
        "L4 covers L3 path [0 1]")

  (t/is (= #{"l04-r20200101-p00-b01" "l04-r20200101-p01-b01" "l04-r20200101-p02-b01" "l04-r20200101-p03-b01"
             ;; L3 path 0 covered
             "l03-r20200101-p1-b01" "l03-r20200101-p2-b01" "l03-r20200101-p3-b01"}

           (curr-tries ["l03-r20200101-p0-b01"] ["l03-r20200101-p1-b01"] ["l03-r20200101-p2-b01"] ["l03-r20200101-p3-b01"]
                       ["l04-r20200101-p00-b01"] ["l04-r20200101-p01-b01"] ["l04-r20200101-p02-b01"] ["l04-r20200101-p03-b01"]

                       ;; L2 path 1 not covered yet, missing [1 1]
                       ["l04-r20200101-p10-b01"] ["l04-r20200101-p12-b01"] ["l04-r20200101-p13-b01"]))

        "L4H covered idx 0 but not 1"))

(t/deftest reconstructs-state-on-startup
  (let [node-dir (util/->path "target/trie-catalog-test/reconstructs-state")]
    (util/delete-dir node-dir)

    (with-open [node (tu/->local-node {:node-dir node-dir, :compactor-threads 0})]
      (let [cat (.getTrieCatalog (db/primary-db node))]
        (xt/execute-tx node [[:put-docs :foo {:xt/id 1}]])
        (tu/finish-block! node)

        (xt/execute-tx node [[:put-docs :foo {:xt/id 2}]])
        (tu/finish-block! node)

        (t/is (= #{#xt/table foo, #xt/table xt/txs} (.getTables cat)))
        (t/is (= #{"l00-rc-b00" "l00-rc-b01"}
                 (->> (cat/current-tries (cat/trie-state cat #xt/table foo))
                      (into #{} (map :trie-key)))))))

    (with-open [node (tu/->local-node {:node-dir node-dir, :compactor-threads 0})]
      (let [cat (.getTrieCatalog (db/primary-db node))]
        (t/is (= #{#xt/table foo, #xt/table xt/txs} (.getTables cat)))
        (t/is (= #{"l00-rc-b01" "l00-rc-b00"}
                 (->> (cat/current-tries (cat/trie-state cat #xt/table foo))
                      (into #{} (map :trie-key)))))))

    (t/testing "artifically adding tries"

      (with-open [node (tu/->local-node {:node-dir node-dir, :compactor-threads 0})]
        (let [cat (.getTrieCatalog (db/primary-db node))]
          (.addTries cat #xt/table foo
                     (->> [["l00-rc-b00" 1] ["l00-rc-b01" 1] ["l00-rc-b02" 1] ["l00-rc-b03" 1]
                           ["l01-rc-b00" 2] ["l01-rc-b01" 2] ["l01-rc-b02" 2]
                           ["l02-rc-p0-b01" 4] ["l02-rc-p1-b01" 4] ["l02-rc-p2-b01" 4] ["l02-rc-p3-b01" 4]]
                          (map #(apply trie/->trie-details #xt/table foo %)))
                     (Instant/now))
          (tu/finish-block! node))))

    (with-open [node (tu/->local-node {:node-dir node-dir, :compactor-threads 0})]
      (let [cat (.getTrieCatalog (db/primary-db node))]
        (t/is (= #{"l00-rc-b03"
                   "l01-rc-b02"
                   "l02-rc-p0-b01"
                   "l02-rc-p1-b01"
                   "l02-rc-p2-b01"
                   "l02-rc-p3-b01"}
                 (->> (cat/current-tries (cat/trie-state cat #xt/table foo))
                      (into (sorted-set) (map :trie-key)))))))))

(defn trie-catalog-init [table->table-block]
  (TrieCatalog. nil nil
                (cat/load-tries table->table-block cat/*file-size-target*)
                cat/*file-size-target*))

(t/deftest test-trie-catalog-init-old-and-new-block-files-mixed-4664
  (let [->trie-details (partial trie/->trie-details #xt/table foo)
        old-table-blocks {:partitions
                          [{:level 0 :recency nil :part []
                            :tries [(->trie-details {:trie-key "l00-rc-b00" :data-file-size 10})
                                    (->trie-details {:trie-key "l00-rc-b01" :data-file-size 10})]}
                           {:level 1 :recency nil :part []
                            :tries [(->trie-details {:trie-key "l01-rc-b00" :data-file-size 10})]}]}

        new-table-blocks {:partitions
                          [{:level 0 :recency nil :part []
                            :tries [(->trie-details {:trie-key "l00-rc-b00" :data-file-size 10 :state
                                                     :garbage :garbage-as-of #xt/instant "2000-01-01T00:00:00Z"})
                                    (->trie-details {:trie-key "l00-rc-b01" :data-file-size 10 :state :live})]}
                           {:level 1 :recency nil :part []
                            :tries [(->trie-details {:trie-key "l01-rc-b00" :data-file-size 10 :state :live})]}]}

        cat (trie-catalog-init {#xt/table bar new-table-blocks
                                #xt/table foo old-table-blocks})]
    (t/is (= #{"l00-rc-b01" "l01-rc-b00"}
             (->> (cat/current-tries (cat/trie-state cat #xt/table foo))
                  (into (sorted-set) (map :trie-key)))
             (->> (cat/current-tries (cat/trie-state cat #xt/table bar))
                  (into (sorted-set) (map :trie-key)))))))

(t/deftest test-trie-catalog-init
  (let [->trie-details (partial trie/->trie-details #xt/table foo)]
    ;; old, no trie-state
    (let [old-table-blocks {:partitions
                            [{:level 0 :recency nil :part []
                              :tries [(->trie-details {:trie-key "l00-rc-b00" :data-file-size 10})
                                      (->trie-details {:trie-key "l00-rc-b01" :data-file-size 10})]}
                             {:level 1 :recency nil :part []
                              :tries [(->trie-details {:trie-key "l01-rc-b00" :data-file-size 10})]}
                             {:level 1 :recency #xt/date "2020-01-01" :part []
                              :tries [(->trie-details {:trie-key "l01-r20200101-b00" :data-file-size 10})]}]}

          cat (trie-catalog-init {#xt/table foo old-table-blocks})]

      (t/is (= #{"l00-rc-b01" "l01-rc-b00" "l01-r20200101-b00"} (->> (cat/current-tries (cat/trie-state cat #xt/table foo))
                                                                     (into (sorted-set) (map :trie-key)))))

      (t/is (= #{} (->> (cat/garbage-tries (cat/trie-state cat #xt/table foo)
                                           #xt/instant "2025-01-01T00:00:00Z")
                        (into (sorted-set) (map :trie-key))))))

    ;; new, with trie-state, without max-block-idx
    (let [new-table-blocks {:partitions
                            [{:level 0 :recency nil :part []
                              :tries [(->trie-details {:trie-key "l00-rc-b00" :data-file-size 10 :state
                                                       :garbage :garbage-as-of #xt/instant "2000-01-01T00:00:00Z"})
                                      (->trie-details {:trie-key "l00-rc-b01" :data-file-size 10 :state :live})]}
                             {:level 1 :recency nil :part []
                              :tries [(->trie-details {:trie-key "l01-rc-b00" :data-file-size 10 :state :live})]}
                             {:level 1 :recency #xt/date "2020-01-01" :part []
                              :tries [(->trie-details {:trie-key "l01-r20200101-b00" :data-file-size 10 :state :live})]}]}
          cat (trie-catalog-init {#xt/table foo new-table-blocks})]

      (t/is (= #{"l00-rc-b01" "l01-rc-b00" "l01-r20200101-b00"}
               (->> (cat/current-tries (cat/trie-state cat #xt/table foo))
                    (into (sorted-set) (map :trie-key)))))

      (t/is (= #{} (->> (cat/garbage-tries (cat/trie-state cat #xt/table foo)
                                           #xt/instant "2025-01-01T00:00:00Z")
                        (into (sorted-set) (map :trie-key))))))

    ;; new, with trie-state and max-block-idx
    (let [new-table-blocks {:partitions
                            [{:level 0 :recency nil :part []
                              :max-block-idx 1
                              :tries [(->trie-details {:trie-key "l00-rc-b00" :data-file-size 10 :state
                                                       :garbage :garbage-as-of #xt/instant "2000-01-01T00:00:00Z"})
                                      (->trie-details {:trie-key "l00-rc-b01" :data-file-size 10 :state :live})]}
                             {:level 1 :recency nil :part []
                              :max-block-idx 0
                              :tries [(->trie-details {:trie-key "l01-rc-b00" :data-file-size 10 :state :live})]}
                             {:level 1 :recency #xt/date "2020-01-01" :part []
                              :max-block-idx 0
                              :tries [(->trie-details {:trie-key "l01-r20200101-b00" :data-file-size 10 :state :live})]}]}
          cat (trie-catalog-init {#xt/table foo new-table-blocks})]

      (t/is (= #{"l00-rc-b01" "l01-rc-b00" "l01-r20200101-b00"}
               (->> (cat/current-tries (cat/trie-state cat #xt/table foo))
                    (into (sorted-set) (map :trie-key)))))

      (t/is (= #{} (->> (cat/garbage-tries (cat/trie-state cat #xt/table foo)
                                           #xt/instant "2025-01-01T00:00:00Z")
                        (into (sorted-set) (map :trie-key))))))))

(t/deftest trie-catalog-init-with-first-empty-partition-5017
  (t/testing "First empty partition"
    (let [->trie-details (partial trie/->trie-details #xt/table foo)
          table-blocks {:partitions
                        ;; Assumption being here there is a level above that covers L1, but it's not relevant for this test
                        ;; It's important that this partition comes first
                        [{:level 1 :recency nil :part []
                          :max-block-idx 0
                          :tries []}
                         {:level 0 :recency nil :part []
                          :max-block-idx 1
                          :tries [(->trie-details {:trie-key "l00-rc-b00" :data-file-size 10 :state :live})
                                  (->trie-details {:trie-key "l00-rc-b01" :data-file-size 10 :state :live})]}]}
          cat (trie-catalog-init {#xt/table foo table-blocks})]

      (t/is (= #{"l00-rc-b00" "l00-rc-b01"}
               (->> (cat/current-tries (cat/trie-state cat #xt/table foo))
                    (into (sorted-set) (map :trie-key)))))

      (t/is (= #{} (->> (cat/garbage-tries (cat/trie-state cat #xt/table foo)
                                           #xt/instant "2025-01-01T00:00:00Z")
                        (into (sorted-set) (map :trie-key)))))))

  ;; This shouldn't happen in practice, as there is always at least one trie if there is a block
  (t/testing "No trie-details"
    (let [no-table-blocks {:partitions
                           [{:level 1 :recency nil :part []
                             :max-block-idx 0
                             :tries []}
                            {:level 0 :recency nil :part []
                             :max-block-idx 1
                             :tries []}]}
          cat (trie-catalog-init {#xt/table foo no-table-blocks})]

      (t/is (= #{}
               (->> (cat/current-tries (cat/trie-state cat #xt/table foo))
                    (into (sorted-set) (map :trie-key))))))))

(t/deftest handles-l1h-l1c-ordering-4301
  ;; L1H and L1C are in different partitions, so (strictly speaking) we should handle these out of order
  ;; (in practice, we always submit L1C after L1H - but this keeps the invariant definition simpler to understand)
  (t/is (= #{"l01-r20200101-b01" "l01-r20200102-b01" "l01-rc-b01"}
           (curr-tries ["l01-r20200101-b01" 5] ["l01-rc-b01" 15] ["l01-r20200102-b01" 5]))))

(t/deftest stack-overflow-exception-creating-tries-4377
  (t/is (= #{"l01-rc-b3270f"}
           (apply curr-tries
                  (concat (for [n (range 10000)]
                            [(str "l00-rc-b" (util/->lex-hex-string n)) 1])
                          (for [n (range 10000)]
                            [(str "l01-rc-b" (util/->lex-hex-string n)) 1]))))))

(t/deftest l3h-missing-l2h-files-to-supersede
  ;; it was missing these because b08 was marked garbage, so it didn't continue looking for live ones
  ;; so b06, b05 and b02 were left live
  (t/is (= #{"l03-r20250101-p0-b09" "l03-r20250101-p1-b09" "l03-r20250101-p2-b09" "l03-r20250101-p3-b09"}
           (curr-tries ["l02-r20250101-b01" 10] ["l02-r20250101-b02" 25]
                       ["l02-r20250101-b03" 10] ["l02-r20250101-b04" 15] ["l02-r20250101-b05" 22]
                       ["l02-r20250101-b06" 30]
                       ["l02-r20250101-b07" 10] ["l02-r20250101-b08" 18] ["l02-r20250101-b09" 24]
                       ["l03-r20250101-p0-b09"]
                       ["l03-r20250101-p1-b09"]
                       ["l03-r20250101-p2-b09"]
                       ["l03-r20250101-p3-b09"]))))

(t/deftest test-dry-trie-catalog-gc
  (let [cat (TrieCatalog. nil nil (ConcurrentHashMap.) 20)] ;file-size-target
    (letfn [(add-tries [tries inst]
              (.addTries cat #xt/table foo
                         (map #(apply trie/->trie-details #xt/table foo %) tries)
                         inst))
            (all-tries []
              (->> (cat/all-tries (cat/trie-state cat #xt/table foo))
                   (sort-by (juxt :trie-key :state))
                   (map (juxt :trie-key :state :garbage-as-of))))

            (garbage-tries [as-of]
              (->> (cat/garbage-tries (cat/trie-state cat #xt/table foo) as-of)
                   (sort-by (juxt :trie-key :state))
                   (map (juxt :trie-key :state :garbage-as-of))))
            (delete-tries [garbage-trie-keys]
              (.deleteTries cat #xt/table foo garbage-trie-keys))]

      (add-tries [["l00-rc-b00" 1] ["l01-rc-b00" 1]]
                 #xt/instant "2000-01-01T00:00:00Z")
      (add-tries [["l00-rc-b01" 1] ["l01-rc-b01" 1]]
                 #xt/instant "2001-01-01T00:00:00Z")

      (t/is (= [["l00-rc-b00" :garbage #xt/instant "2000-01-01T00:00:00Z"]
                ["l00-rc-b01" :garbage #xt/instant "2001-01-01T00:00:00Z"]
                ["l01-rc-b00" :garbage #xt/instant "2001-01-01T00:00:00Z"]
                ["l01-rc-b01" :live nil]]
               (all-tries)))

      (add-tries [["l00-rc-b02" 1] ["l00-rc-b03" 1]
                  ["l01-rc-b02" 2]
                  ["l02-rc-p0-b01" 4] ["l02-rc-p1-b01" 4] ["l02-rc-p2-b01" 4] ["l02-rc-p3-b01" 4]]
                 #xt/instant "2002-01-01T00:00:00Z")

      (t/is (= [["l00-rc-b00" :garbage #xt/instant "2000-01-01T00:00:00Z"]
                ["l00-rc-b01" :garbage #xt/instant "2001-01-01T00:00:00Z"]
                ["l00-rc-b02" :garbage #xt/instant "2002-01-01T00:00:00Z"]
                ["l00-rc-b03" :live nil]
                ["l01-rc-b00" :garbage #xt/instant "2001-01-01T00:00:00Z"]
                ["l01-rc-b01" :garbage #xt/instant "2002-01-01T00:00:00Z"]
                ["l01-rc-b02" :live nil]
                ["l02-rc-p0-b01" :live nil]
                ["l02-rc-p1-b01" :live nil]
                ["l02-rc-p2-b01" :live nil]
                ["l02-rc-p3-b01" :live nil]]
               (all-tries)))

      (t/is (= [["l01-rc-b00" :garbage #xt/instant "2001-01-01T00:00:00Z"]
                ["l01-rc-b01" :garbage #xt/instant "2002-01-01T00:00:00Z"]]
               (garbage-tries #xt/instant "2003-01-01T00:00:00Z")))

      (delete-tries #{"l01-rc-b00" "l01-rc-b01"})

      (t/is (= [["l00-rc-b00" :garbage #xt/instant "2000-01-01T00:00:00Z"]
                ["l00-rc-b01" :garbage #xt/instant "2001-01-01T00:00:00Z"]
                ["l00-rc-b02" :garbage #xt/instant "2002-01-01T00:00:00Z"]
                ["l00-rc-b03" :live nil]
                ["l01-rc-b02" :live nil]
                ["l02-rc-p0-b01" :live nil]
                ["l02-rc-p1-b01" :live nil]
                ["l02-rc-p2-b01" :live nil]
                ["l02-rc-p3-b01" :live nil]]
               (all-tries))))))

(t/deftest test-default-garbage-collection
  (binding [c/*ignore-signal-block?* true]
    (let [node-dir (util/->path "target/trie-catalog-test/test-default-garbage-collection")
          clock (tu/->mock-clock (tu/->instants :hour))
          opts {:node-dir node-dir, :compactor-threads 1 :instant-src clock
                :gc? false :blocks-to-keep 2 :garbage-lifetime (Duration/ofHours 0)
                :instant-source-for-non-tx-msgs? true}]
      (util/delete-dir node-dir)

      (with-open [node (tu/->local-node opts)]
        (let [gc (gc/garbage-collector node)
              db (db/primary-db node)
              bp (.getBufferPool db)
              cat (.getTrieCatalog db)]
          (doseq [i (range 4)]
            (xt/execute-tx node [[:put-docs :foo {:xt/id i}]])
            (tu/finish-block! node)
            (c/compact-all! node #xt/duration "PT1S"))

          (.collectAllGarbage gc)

          ;; we keep block 01 and 02
          ;; the 01 latest-complete-tx is cutoff (no garbage lifetime), i.e. the level 1 block 01 remains as it is compacted later
          (t/is (= ["l00-rc-b00"
                    "l00-rc-b01"
                    "l00-rc-b02"
                    "l00-rc-b03"
                    "l01-rc-b01"
                    "l01-rc-b02"
                    "l01-rc-b03"]
                   (->> (cat/all-tries (cat/trie-state cat #xt/table foo))
                        (map :trie-key))))

          ;; all l0 files are present
          (t/is (= ["tables/public$foo/blocks/b02.binpb"
                    "tables/public$foo/blocks/b03.binpb"
                    "tables/public$foo/data/l00-rc-b00.arrow"
                    "tables/public$foo/data/l00-rc-b01.arrow"
                    "tables/public$foo/data/l00-rc-b02.arrow"
                    "tables/public$foo/data/l00-rc-b03.arrow"
                    "tables/public$foo/data/l01-rc-b01.arrow"
                    "tables/public$foo/data/l01-rc-b02.arrow"
                    "tables/public$foo/data/l01-rc-b03.arrow"
                    "tables/public$foo/meta/l00-rc-b00.arrow"
                    "tables/public$foo/meta/l00-rc-b01.arrow"
                    "tables/public$foo/meta/l00-rc-b02.arrow"
                    "tables/public$foo/meta/l00-rc-b03.arrow"
                    "tables/public$foo/meta/l01-rc-b01.arrow"
                    "tables/public$foo/meta/l01-rc-b02.arrow"
                    "tables/public$foo/meta/l01-rc-b03.arrow"]

                   (->>
                    (.listAllObjects bp (util/->path "tables/public$foo"))
                    (map #(str (.getKey ^ObjectStore$StoredObject %)))))))))))

(t/deftest test-compactor-reset
  (let [table-trie-cat (apply-msgs ["l00-rc-b00" 10] ["l00-rc-b01" 10] ["l00-rc-b02" 10] ["l00-rc-b03" 10] ["l00-rc-b04" 10]
                                   ["l01-rc-b00" 10] ["l01-rc-b01" 20] ["l01-rc-b02" 10] ["l01-rc-b03" 20])]
    (t/is (= #{"l01-rc-b01" "l01-rc-b03" "l00-rc-b04"}
             (->> (cat/current-tries table-trie-cat)
                  (into #{} (map :trie-key)))))

    (t/is (= #{"l01-rc-b00" "l01-rc-b01" "l01-rc-b02" "l01-rc-b03"}
             (set (cat/compacted-trie-keys table-trie-cat))))

    (t/is (= #{"l00-rc-b00" "l00-rc-b01" "l00-rc-b02" "l00-rc-b03" "l00-rc-b04"}
             (->> (cat/reset->l0 table-trie-cat)
                  (cat/current-tries)
                  (into #{} (map :trie-key)))))))

(t/deftest max-block-index-always-set-test-5139
  (let [{:keys [tries] :as _table-trie-cat} (apply-msgs ["l02-r20200203-b00" 10])]
    (t/is (true? (every? (comp int? :max-block-idx) (vals tries))))))

(t/deftest test-protobuf-additions
  ;; There have been (so far) two changes in the protobuffers that are relevant for the trie-catalog since 2.0.0
  ;; GCing - and hence trie states in the TrieDetails message
  ;; Addition of the Partition message - TrieDetails got moved from TableBlock into Partition
  (binding [cat/*file-size-target* 1024]
    (letfn [(resource->path [p] (util/->path (io/as-file (io/resource p))))]
      (doseq [^Path node-root (map resource->path ["xtdb/trie-catalog-test/node-2.0.0"
                                                   "xtdb/trie-catalog-test/node-fcb7714ea"])]
        (let [tmp-dir (util/tmp-dir "test-protobuf-addtions")]
          (util/copy-dir node-root tmp-dir)
          (with-open [node (xtn/start-node {:log [:local {:path (.resolve tmp-dir "log")}]
                                            :storage [:local {:path (.resolve tmp-dir "objects")}]})]
            (t/is (= [{:cnt 8000}]
                     (xt/q node "SELECT COUNT(*) AS CNT FROM docs FOR VALID_TIME ALL")))))))))

(t/deftest partitions->max-block-idx-test
  (t/is (= {[0 nil []] {:max-block-idx 0}, [1 nil []] {:max-block-idx 0}}
           (cat/partitions->max-block-idx-map
            [{:level 0 :recency nil :part []
              :tries []}
             {:level 1 :recency nil :part []
              :tries []}]))
        "partitions with no tries yield max-block-idx 0")

  (t/is (= {[0 nil []] {:max-block-idx 2}, [1 nil []] {:max-block-idx 0}}
           (cat/partitions->max-block-idx-map
            [{:level 0 :recency nil :part []
              :tries [{:trie-key "l00-rc-b00" :block-idx 0}
                      {:trie-key "l00-rc-b01" :block-idx 2}
                      {:trie-key "l00-rc-b02" :block-idx 1}]}
             {:level 1 :recency nil :part []
              :tries [{:trie-key "l01-rc-b00" :block-idx 0}]}])))

  (t/is (= {[0 nil []] {:max-block-idx 15},
            [1 nil []] {:max-block-idx 15},
            [2 nil [0]] {:max-block-idx 7},
            [3 nil [0 2]] {:max-block-idx 7}}
           (cat/partitions->max-block-idx-map
            [{:level 0 :recency nil :part []
              :tries [{:trie-key "l00-rc-b0f" :block-idx 15}]}
             {:level 1 :recency nil :part []
              :tries [{:trie-key "l01-rc-b0f" :block-idx 15}]}
             {:level 3 :recency nil :part [0 2]
              :tries [{:trie-key "l03-rc-p02-b07" :block-idx 7}]}])))

  (t/is (= {[0 nil []] {:max-block-idx 47},
            [1 #xt/date "2020-01-01" []] {:max-block-idx 31},
            [2 #xt/date "2020-01-01" []] {:max-block-idx 31},
            [3 #xt/date "2020-01-01" [0]] {:max-block-idx 7}}
           (cat/partitions->max-block-idx-map
            [{:level 0 :recency nil :part []
              :tries [{:trie-key "l00-rc-b2f" :block-idx 47}]}
             {:level 1 :recency #xt/date "2020-01-01" :part []
              :tries [{:trie-key "l01-r20200101-b0f" :block-idx 15}]}
             {:level 2 :recency #xt/date "2020-01-01" :part []
              :tries [{:trie-key "l02-r20200101-b11f" :block-idx 31}]}
             {:level 3 :recency #xt/date "2020-01-01" :part [0]
              :tries [{:trie-key "l03-r20200101-p0-b07" :block-idx 7}]}]))))

(t/deftest test-delete-garbage-l1-l2-with-live-l3
  (let [cat (TrieCatalog. nil nil (ConcurrentHashMap.) 20)
        table #xt/table foo
        add-tries (fn [tries inst]
                    (.addTries cat table
                               (map #(apply trie/->trie-details table %) tries)
                               inst))]

    ;; Setup: Create L0s, L1s, L2s, then L3s that supersede L2 partition 0
    (add-tries [["l00-rc-b00" 1] ["l00-rc-b01" 1]] #xt/instant "2000-01-01T00:00:00Z")
    (add-tries [["l01-rc-b00" 2] ["l01-rc-b01" 2]] #xt/instant "2001-01-01T00:00:00Z")
    (add-tries [["l02-rc-p0-b01" 4] ["l02-rc-p1-b01" 4]
                ["l02-rc-p2-b01" 4] ["l02-rc-p3-b01" 4]]
               #xt/instant "2002-01-01T00:00:00Z")
    (add-tries [["l03-rc-p00-b01" 8] ["l03-rc-p01-b01" 8]
                ["l03-rc-p02-b01" 8] ["l03-rc-p03-b01" 8]]
               #xt/instant "2003-01-01T00:00:00Z")

    (let [state (cat/trie-state cat table)
          trie-keys #(into (sorted-set) (map :trie-key) %)]

      ;; L3s supersede L2-p0, L2 supersedes L1, L1 supersedes L0
      (t/is (= #{"l03-rc-p00-b01" "l03-rc-p01-b01" "l03-rc-p02-b01" "l03-rc-p03-b01"
                 "l02-rc-p1-b01" "l02-rc-p2-b01" "l02-rc-p3-b01"}
               (trie-keys (cat/current-tries state))))

      ;; Delete garbage tries
      (let [garbage (trie-keys (cat/garbage-tries state #xt/instant "2004-01-01T00:00:00Z"))]
        (t/is (= #{"l01-rc-b00" "l01-rc-b01" "l02-rc-p0-b01"} garbage))
        (.deleteTries cat table garbage))

      ;; Verify remaining tries
      (t/is (= #{"l00-rc-b00" "l00-rc-b01"
                 "l02-rc-p1-b01" "l02-rc-p2-b01" "l02-rc-p3-b01"
                 "l03-rc-p00-b01" "l03-rc-p01-b01" "l03-rc-p02-b01" "l03-rc-p03-b01"}
               (trie-keys (cat/all-tries (cat/trie-state cat table))))
            "garbage tries are removed"))))