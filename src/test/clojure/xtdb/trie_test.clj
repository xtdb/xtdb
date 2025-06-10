(ns xtdb.trie-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t :refer [deftest]]
            [xtdb.test-json :as tj]
            [xtdb.test-util :as tu]
            [xtdb.time :as time]
            [xtdb.trie :as trie]
            [xtdb.util :as util])
  (:import (org.apache.arrow.memory RootAllocator)
           xtdb.arrow.Relation
           xtdb.operator.scan.MergePlanPage
           (xtdb.trie ArrowHashTrie ArrowHashTrie$Leaf HashTrieKt MergePlanNode MergePlanTask)
           (xtdb.util TemporalBounds TemporalDimension)))

(t/use-fixtures :each tu/with-allocator)

(deftest parses-trie-paths
  (letfn [(parse [trie-key]
            (-> (trie/parse-trie-key trie-key)
                (update :part #(some-> % vec))
                (mapv [:level :recency :part :block-idx])))]
    (t/is (= "l00-rc-b04" (trie/->l0-trie-key 4)))
    (t/is (= [0 nil [] 4] (parse "l00-rc-b04")))

    (t/is (= "l06-rc-p0013-b178" (trie/->trie-key 6 nil (byte-array [0 0 1 3]) 120)))
    (t/is (= [6 nil [0 0 1 3] 120] (parse "l06-rc-p0013-b178")))

    (t/is (= "l02-r20240101-b0c"
             (trie/->trie-key 2 #xt/date "2024-01-01" nil 12)
             (trie/->trie-key 2 #xt/date "2024-01-01" (byte-array []) 12)))
    (t/is (= [2 #xt/date "2024-01-01" [] 12] (parse "l02-r20240101-b0c")))

    (t/is (neg? (compare (trie/->trie-key 1 #xt/date "2024-01-01" nil 12)
                         (trie/->trie-key 1 nil nil 12)))
          "current comes lexically after historical")))

(defn- ->arrow-hash-trie [^Relation meta-rel]
  (ArrowHashTrie. (.get meta-rel "nodes")))

(defn- merge-plan-nodes->path+pages [mp-nodes]
  (->> mp-nodes
       (map (fn [^MergePlanTask merge-plan-node]
              (let [path (.getPath merge-plan-node)
                    mp-nodes (.getMpNodes merge-plan-node)]
                {:path (vec path)
                 :pages (mapv (fn [^MergePlanNode merge-plan-node]
                                (let [segment (.getSegment merge-plan-node)
                                      ^ArrowHashTrie$Leaf node (.getNode merge-plan-node)]
                                  {:seg (:seg segment), :page-idx (.getDataPageIndex node)}))
                              mp-nodes)})))
       (sort-by :path)))

(deftest test-merge-plan-with-nil-nodes-2700
  (with-open [al (RootAllocator.)
              t1-rel (tu/open-arrow-hash-trie-rel al [[nil 0 nil 1] 2 nil 3])
              log-rel (tu/open-arrow-hash-trie-rel al 0)
              log2-rel (tu/open-arrow-hash-trie-rel al [nil nil 0 1])]

    (t/is (= [{:path [1], :pages [{:seg :t1, :page-idx 2} {:seg :log, :page-idx 0}]}
              {:path [2], :pages [{:seg :log, :page-idx 0} {:seg :log2, :page-idx 0}]}
              {:path [3], :pages [{:page-idx 3, :seg :t1} {:page-idx 0, :seg :log} {:page-idx 1, :seg :log2}]}
              {:path [0 0], :pages [{:seg :log, :page-idx 0}]}
              {:path [0 1], :pages [{:seg :t1, :page-idx 0} {:seg :log, :page-idx 0}]}
              {:path [0 2], :pages [{:seg :log, :page-idx 0}]}
              {:path [0 3], :pages [{:seg :t1, :page-idx 1} {:seg :log, :page-idx 0}]}]
             (->> (HashTrieKt/toMergePlan [(-> (trie/->Segment (->arrow-hash-trie t1-rel))
                                               (assoc :seg :t1))
                                           (-> (trie/->Segment (->arrow-hash-trie log-rel))
                                               (assoc :seg :log))
                                           (-> (trie/->Segment (->arrow-hash-trie log2-rel))
                                               (assoc :seg :log2))]
                                          nil)
                  (merge-plan-nodes->path+pages)))))

  (with-open [al (RootAllocator.)
              t1-rel (tu/open-arrow-hash-trie-rel al [[nil 0 nil 1] 2 nil 3])
              t2-rel (tu/open-arrow-hash-trie-rel al [[0 1 nil [nil 2 nil 3]] nil nil [nil 4 nil 5]])]

    (t/is (= [{:path [1], :pages [{:seg :t1, :page-idx 2}]}
              {:path [0 0], :pages [{:seg :t2, :page-idx 0}]}
              {:path [0 1],
               :pages [{:seg :t1, :page-idx 0} {:seg :t2, :page-idx 1}]}
              {:path [3 0], :pages [{:seg :t1, :page-idx 3}]}
              {:path [3 1],
               :pages [{:seg :t1, :page-idx 3} {:seg :t2, :page-idx 4}]}
              {:path [3 2], :pages [{:seg :t1, :page-idx 3}]}
              {:path [3 3],
               :pages [{:seg :t1, :page-idx 3} {:seg :t2, :page-idx 5}]}
              {:path [0 3 0], :pages [{:seg :t1, :page-idx 1}]}
              {:path [0 3 1],
               :pages [{:seg :t1, :page-idx 1} {:seg :t2, :page-idx 2}]}
              {:path [0 3 2], :pages [{:seg :t1, :page-idx 1}]}
              {:path [0 3 3],
               :pages [{:seg :t1, :page-idx 1} {:seg :t2, :page-idx 3}]}]

             (->> (HashTrieKt/toMergePlan [(-> (trie/->Segment (->arrow-hash-trie t1-rel))
                                               (assoc :seg :t1))
                                           (-> (trie/->Segment (->arrow-hash-trie t2-rel))
                                               (assoc :seg :t2))]
                                          nil)
                  (merge-plan-nodes->path+pages))))))

(defrecord MockMergePlanPage [page metadata-matches? temporal-metadata]
  MergePlanPage
  (loadPage [_ _root-cache] (throw (UnsupportedOperationException.)))
  (testMetadata [_] metadata-matches?)
  (getTemporalMetadata [_] temporal-metadata))

(defn ->mock-merge-plan-page
  ([page] (->mock-merge-plan-page page true))
  ([page metadata-matches?] (->MockMergePlanPage page metadata-matches? util/unconstraint-temporal-metadata))
  ([page min-valid-from max-valid-to] (->mock-merge-plan-page page true min-valid-from max-valid-to))
  ([page metadata-matches? min-valid-from max-valid-to]
   (->MockMergePlanPage page metadata-matches? (tu/->temporal-metadata min-valid-from max-valid-to)))
  ([page metadata-matches? min-vf max-vt min-sf]
   (->mock-merge-plan-page page metadata-matches? min-vf max-vt min-sf Long/MAX_VALUE))
  ([page metadata-matches? min-vf max-vt min-sf max-sf]
   (->MockMergePlanPage page metadata-matches? (tu/->temporal-metadata min-vf max-vt min-sf max-sf))))

(def ^:private inst->micros (comp time/instant->micros time/->instant))

(deftest test-to-merge-task
  (let [[sf1 sf2 sf3] [20200 20210 20220]
        [vt0 vt1 vt2 vt3] [20200 20210 20220 20230]
        ->pages #(into #{} (map :page) %)]

    ;; In this test we have 2 pages where one always matches and another one always doesn't match.
    ;; For the purpose of this test the metadata matching dimension is fixed. The only variation
    ;; in the tests is if the first or second page is the one that matches metadata.

    ;; The other dimensions of the test (which vary) are query `system-time` and `valid-time` as well
    ;; as `valid-time` bounds of the pages and `system-from` bounds for the pages. As we are doing
    ;; temporal resolution on query, all events have `system-to` inf. This means for pages we are only interested
    ;; in `system-from` variations.

    ;; Every test was tried to be written with the tightest/loosest bounds, but without defining new sf1, vf1, etc...
    (t/testing "Smaller recency pages"

      (t/testing "No constraints on query bounds"

        (t/is (= #{1} (->> (trie/filter-meta-objects [(->mock-merge-plan-page 0 false vt0 vt3 sf1 (dec sf2))
                                                      (->mock-merge-plan-page 1 true vt1 vt2 sf2 sf2)]
                                                     (tu/->temporal-bounds vt0 Long/MAX_VALUE sf1 Long/MAX_VALUE))
                           ->pages))
              "Take page 1. Page 0 system from strictly before page 1.")
        (t/is (= #{0 1} (->> (trie/filter-meta-objects [(->mock-merge-plan-page 0 false vt0 (inc vt1) sf1 sf2)
                                                        (->mock-merge-plan-page 1 true vt1 vt2 sf2 sf2)]
                                                       (tu/->temporal-bounds vt0 Long/MAX_VALUE sf1 Long/MAX_VALUE))
                             ->pages))
              "Take page 1. Page 0 system from overlaps.")

        (t/is (= #{1} (->> (trie/filter-meta-objects [(->mock-merge-plan-page 0 false vt0 vt1 sf1 (inc sf2))
                                                      (->mock-merge-plan-page 1 true vt1 vt2 sf2 sf2)]
                                                     (tu/->temporal-bounds vt0 Long/MAX_VALUE sf1 Long/MAX_VALUE))
                           ->pages))
              "Take page 1. Page 0 valid-time strictly before page 1.")

        (t/is (= #{0 1} (->> (trie/filter-meta-objects [(->mock-merge-plan-page 0 false vt0 (inc vt1) sf1 (inc sf2))
                                                        (->mock-merge-plan-page 1 true vt1 vt2 sf2 sf2)]
                                                       (tu/->temporal-bounds vt0 Long/MAX_VALUE sf1 Long/MAX_VALUE))
                             ->pages))
              "Take page 1. Page 0 valid-time overlpas."))


      (t/testing "Tighter constraints on query bounds"
        ;; TODO could we filter page 0 in the two assertions below?
        (t/is (= #{0 1} (->> (trie/filter-meta-objects [(->mock-merge-plan-page 0 false vt0 vt2 sf1 sf2)
                                                        (->mock-merge-plan-page 1 true vt1 vt2 sf2 sf2)]
                                                       (tu/->temporal-bounds vt0 Long/MAX_VALUE (inc sf2) Long/MAX_VALUE))
                             ->pages))
              "Take page 1. Page 0 system from overlaps. Query system-time bounds don't touch page 0")

        (t/is (= #{0 1} (->> (trie/filter-meta-objects [(->mock-merge-plan-page 0 false vt0 vt1 sf1 (inc sf2))
                                                        (->mock-merge-plan-page 1 true (dec vt1) vt2 sf2 sf2)]
                                                       (tu/->temporal-bounds (inc vt1) Long/MAX_VALUE sf1 Long/MAX_VALUE))
                             ->pages))
              "Take page 1. Page 0 valid-time overlpas. Query valid-time bounds don't touch page 0")))

    (t/testing "Larger recency pages"

      (t/testing "No constraints on query bounds"

        (t/is (= #{1} (->> (trie/filter-meta-objects [(->mock-merge-plan-page 1 true vt1 vt2 sf2 sf2)
                                                      (->mock-merge-plan-page 2 false vt2 vt3 sf3 sf3)]
                                                     (tu/->temporal-bounds vt0 Long/MAX_VALUE sf1 Long/MAX_VALUE))
                           ->pages))
              "Take page 1. Page 2 doesn't overlap in valid-time")

        (t/is (= #{1 2} (->> (trie/filter-meta-objects [(->mock-merge-plan-page 1 true vt1 (inc vt2) sf2 sf2)
                                                        (->mock-merge-plan-page 2 false vt2 vt3 sf3 sf3)]
                                                       (tu/->temporal-bounds vt0 Long/MAX_VALUE sf1 Long/MAX_VALUE))
                             ->pages))
              "Take page 1. Page 2 overlaps in valid-time")

        (t/is (= #{1 2} (->> (trie/filter-meta-objects [(->mock-merge-plan-page 1 true vt1 (inc vt2) sf2 sf2)
                                                        (->mock-merge-plan-page 2 false vt2 (inc vt3) sf3 sf3)]
                                                       (tu/->temporal-bounds vt0 Long/MAX_VALUE sf1 Long/MAX_VALUE))
                             ->pages))
              "Take page 1. Page 2 can bound page 1 and we can't filter newer pages (no constraints on query system-time)."))


      (t/testing "Constraints on query bounds"
        ;; TODO can 2 be filtered here?
        (t/is (= #{1 2} (->> (trie/filter-meta-objects [(->mock-merge-plan-page 1 true vt1 (inc vt2) sf2 sf2)
                                                        (->mock-merge-plan-page 2 false vt2 vt3 sf3 sf3)]
                                                       (tu/->temporal-bounds vt1 (dec vt2) sf1 Long/MAX_VALUE))
                             ->pages))
              "Take page 1. Page 2 overlaps in valid-time")


        (t/is (= #{1} (->> (trie/filter-meta-objects [(->mock-merge-plan-page 1 true vt1 (inc vt2) sf2 sf3)
                                                      (->mock-merge-plan-page 2 false vt2 (inc vt3) sf3 sf3)]
                                                     (tu/->temporal-bounds vt0 Long/MAX_VALUE sf1 sf3))
                           ->pages))
              "Take page 1. Page 2 can bound page 1 and we can filter newer pages because of query system-time constraints.")))


    (t/is (= #{0 1 2} (->> (trie/filter-meta-objects [(->mock-merge-plan-page 0 true vt0 Long/MAX_VALUE sf1 sf1)
                                                      (->mock-merge-plan-page 1 false vt1 vt2 sf2 sf2)
                                                      (->mock-merge-plan-page 2 false vt2 vt3 sf3 sf3)])
                           ->pages))
          "All pages pages need to be taken if later pages bound valid-time")))

(deftest test-to-merge-task-temporal-filters
  (let [[micros-2018 micros-2019 micros-2020 micros-2021 micros-2022 micros-2023]
        (mapv inst->micros [#inst "2018-01-01" #inst "2019-01-01" #inst "2020-01-01"
                            #inst "2021-01-01" #inst "2022-01-01" #inst "2023-01-01"])]

    (letfn [(query-bounds+temporal-page-metadata->pages [temporal-page-metadata query-bounds]
              (->> (trie/filter-meta-objects
                    (for [[page min-vf max-vt] temporal-page-metadata]
                      (->mock-merge-plan-page page min-vf max-vt))
                    query-bounds)
                   (mapv :page)))]

      (t/testing "non intersecting pages and no retrospective updates"
        (let [query-bounds->pages (partial query-bounds+temporal-page-metadata->pages
                                           [[0 2018 micros-2019]
                                            [1 micros-2019 micros-2020]
                                            [2 micros-2020 micros-2021]
                                            [3 micros-2021 micros-2022]
                                            [4 micros-2022 Long/MAX_VALUE]])]

          (t/is (= [2] (query-bounds->pages (tu/->temporal-bounds micros-2020 micros-2021 micros-2020 (inc micros-2020))))
                "only 2020")

          (t/is (= [1 2] (query-bounds->pages (tu/->temporal-bounds (dec micros-2020) micros-2021 micros-2020 (inc micros-2020))))
                "end of 2019 + 2020")

          (t/is (= [2] (query-bounds->pages (tu/->temporal-bounds micros-2020 (inc micros-2021) micros-2020 (inc micros-2020))))
                "2020 + start of 2021 + system time bounds")

          (t/is (= [1 2 3] (query-bounds->pages (TemporalBounds. (TemporalDimension. (dec micros-2020) (inc micros-2021)) (TemporalDimension.))))
                "end of 2019 + 2020 + start of 2021 + no system time bounds")

          (t/is (= [0 1 2 3 4] (query-bounds->pages (TemporalBounds.)))
                "no bounds")))

      (t/testing "with retrospective updates for the \"current\" (2020) page"
        (let [query-bounds->pages (partial query-bounds+temporal-page-metadata->pages
                                           [[0 micros-2018 micros-2019]
                                            [1 micros-2019 micros-2020]
                                            [2 micros-2020 micros-2021]
                                            [3 micros-2021 micros-2022]
                                            [4 micros-2020 Long/MAX_VALUE]])]
          (t/is (= [2 4] (query-bounds->pages (tu/->temporal-bounds micros-2020 micros-2021 micros-2020 (inc micros-2020))))
                "only 2020")

          ;; end of 2019 + 2020
          (t/is (= [1 2 4] (query-bounds->pages (tu/->temporal-bounds (dec micros-2020) micros-2021 micros-2020 (inc micros-2020))))
                "end of 2019 + 2020")

          (t/is (= [1 2 3 4] (query-bounds->pages (TemporalBounds. (TemporalDimension. (dec micros-2020) (inc micros-2021)) (TemporalDimension.))))
                "end of 2019 + 2020 + start of 2021 + no system time bounds")

          (t/is (= [2 4] (query-bounds->pages (tu/->temporal-bounds micros-2020 (inc micros-2021) micros-2020 (inc micros-2020))))
                "2020 + start of 2021 + system time bounds")

          (t/is (= [0 1 2 3 4] (query-bounds->pages (TemporalBounds.)))
                "no bounds")))

      (t/testing "page 2 is unbounded"
        (let [query-bounds->pages (partial query-bounds+temporal-page-metadata->pages
                                           [[0 micros-2018 micros-2019]
                                            [1 micros-2019 micros-2020]
                                            [2 micros-2020 Long/MAX_VALUE]
                                            [3 micros-2021 micros-2022]
                                            [4 micros-2022 Long/MAX_VALUE]])]

          (t/is (= [2 3 4] (query-bounds->pages (tu/->temporal-bounds micros-2020 micros-2021 micros-2023 (inc micros-2023))))
                "2020, but page 3 and 4 can bound page 2")))

      (t/testing "page two can be bounded by some, but not all pages in system time future"
        (let [query-bounds->pages (partial query-bounds+temporal-page-metadata->pages
                                           [[0 micros-2018 micros-2019]
                                            [1 micros-2019 micros-2020]
                                            [2 micros-2020 micros-2022]
                                            [3 micros-2021 micros-2022]
                                            [4 micros-2022 Long/MAX_VALUE]])]

          (t/is (= [2 3] (query-bounds->pages (tu/->temporal-bounds micros-2020 micros-2021 micros-2023 (inc micros-2023))))
                "2020, but only 3 can bound page 2 in 2020"))))))

(deftest test-data-file-writing
  (let [page-idx->documents
        {0 [[:put {:xt/id #uuid "00000000-0000-0000-0000-000000000000"
                   :foo "bar"}]
            [:put {:xt/id #uuid "00100000-0000-0000-0000-000000000000"
                   :foo "bar"}]]
         1 [[:put {:xt/id #uuid "01000000-0000-0000-0000-000000000000"
                   :foo "bar"}]]}]
    (util/with-tmp-dirs #{tmp-dir}
      (let [data-file-path (.resolve tmp-dir "data-file.arrow")]

        (tu/write-arrow-data-file tu/*allocator* page-idx->documents data-file-path)

        (tj/check-json (.toPath (io/as-file (io/resource "xtdb/trie-test/data-file-writing")))
                       tmp-dir)))))
