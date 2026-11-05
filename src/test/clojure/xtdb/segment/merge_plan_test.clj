(ns xtdb.segment.merge-plan-test
  (:require [clojure.test :as t :refer [deftest]]
            [xtdb.test-util :as tu]
            [xtdb.time :as time]
            [xtdb.trie :as trie]
            [xtdb.util :as util])
  (:import (xtdb.segment Segment$PageMeta)
           (xtdb.util TemporalBounds TemporalDimension)))

(t/use-fixtures :each tu/with-allocator)

(defrecord MockPage [page metadata-matches? temporal-metadata recency]
  Segment$PageMeta
  (testMetadata [_] metadata-matches?)
  (getRecency [_] recency)
  (getTemporalMetadata [_] temporal-metadata))

(defn ->mock-page
  ([page] (->mock-page page true))
  ([page metadata-matches?] (->MockPage page metadata-matches? util/unconstraint-temporal-metadata Long/MAX_VALUE))

  ([page min-valid-from max-valid-to] (->mock-page page true min-valid-from max-valid-to))
  ([page metadata-matches? min-valid-from max-valid-to]
   (->MockPage page metadata-matches? (tu/->temporal-metadata min-valid-from max-valid-to) Long/MAX_VALUE))

  ([page metadata-matches? min-vf max-vt min-sf]
   (->mock-page page metadata-matches? min-vf max-vt min-sf Long/MAX_VALUE))
  ([page metadata-matches? min-vf max-vt min-sf max-sf]
   (->MockPage page metadata-matches? (tu/->temporal-metadata min-vf max-vt min-sf max-sf) Long/MAX_VALUE)))

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

        (t/is (= #{1} (->> (trie/filter-pages [(->mock-page 0 false vt0 vt3 sf1 (dec sf2))
                                               (->mock-page 1 true vt1 vt2 sf2 sf2)]
                                              {:query-bounds (tu/->temporal-bounds vt0 Long/MAX_VALUE sf1 Long/MAX_VALUE)
                                               :projects-temporal-cols? true})
                           ->pages))
              "Take page 1. Page 0 system from strictly before page 1.")
        (t/is (= #{0 1} (->> (trie/filter-pages [(->mock-page 0 false vt0 (inc vt1) sf1 sf2)
                                                 (->mock-page 1 true vt1 vt2 sf2 sf2)]
                                                {:query-bounds (tu/->temporal-bounds vt0 Long/MAX_VALUE sf1 Long/MAX_VALUE)
                                                 :projects-temporal-cols? true})
                             ->pages))
              "Take page 1. Page 0 system from overlaps.")

        (t/is (= #{1} (->> (trie/filter-pages [(->mock-page 0 false vt0 vt1 sf1 (inc sf2))
                                               (->mock-page 1 true vt1 vt2 sf2 sf2)]
                                              {:query-bounds (tu/->temporal-bounds vt0 Long/MAX_VALUE sf1 Long/MAX_VALUE)
                                               :projects-temporal-cols? true})
                           ->pages))
              "Take page 1. Page 0 valid-time strictly before page 1.")

        (t/is (= #{0 1} (->> (trie/filter-pages [(->mock-page 0 false vt0 (inc vt1) sf1 (inc sf2))
                                                 (->mock-page 1 true vt1 vt2 sf2 sf2)]
                                                {:query-bounds (tu/->temporal-bounds vt0 Long/MAX_VALUE sf1 Long/MAX_VALUE)
                                                 :projects-temporal-cols? true})
                             ->pages))
              "Take page 1. Page 0 valid-time overlpas."))

      (t/testing "Tighter constraints on query bounds"
        ;; TODO could we filter page 0 in the two assertions below?
        (t/is (= #{0 1} (->> (trie/filter-pages [(->mock-page 0 false vt0 vt2 sf1 sf2)
                                                 (->mock-page 1 true vt1 vt2 sf2 sf2)]
                                                {:query-bounds (tu/->temporal-bounds vt0 Long/MAX_VALUE (inc sf2) Long/MAX_VALUE)
                                                 :projects-temporal-cols? true})
                             ->pages))
              "Take page 1. Page 0 system from overlaps. Query system-time bounds don't touch page 0")

        (t/is (= #{0 1} (->> (trie/filter-pages [(->mock-page 0 false vt0 vt1 sf1 (inc sf2))
                                                 (->mock-page 1 true (dec vt1) vt2 sf2 sf2)]
                                                {:query-bounds (tu/->temporal-bounds (inc vt1) Long/MAX_VALUE sf1 Long/MAX_VALUE)
                                                 :projects-temporal-cols? true})
                             ->pages))
              "Take page 1. Page 0 valid-time overlpas. Query valid-time bounds don't touch page 0")))

    (t/testing "Larger recency pages"

      (t/testing "No constraints on query bounds"

        (t/is (= #{1} (->> (trie/filter-pages [(->mock-page 1 true vt1 vt2 sf2 sf2)
                                               (->mock-page 2 false vt2 vt3 sf3 sf3)]
                                              {:query-bounds (tu/->temporal-bounds vt0 Long/MAX_VALUE sf1 Long/MAX_VALUE)
                                               :projects-temporal-cols? true})
                           ->pages))
              "Take page 1. Page 2 doesn't overlap in valid-time")

        (t/is (= #{1 2} (->> (trie/filter-pages [(->mock-page 1 true vt1 (inc vt2) sf2 sf2)
                                                 (->mock-page 2 false vt2 vt3 sf3 sf3)]
                                                {:query-bounds (tu/->temporal-bounds vt0 Long/MAX_VALUE sf1 Long/MAX_VALUE)
                                                 :projects-temporal-cols? true})
                             ->pages))
              "Take page 1. Page 2 overlaps in valid-time")

        (t/is (= #{1 2} (->> (trie/filter-pages [(->mock-page 1 true vt1 (inc vt2) sf2 sf2)
                                                 (->mock-page 2 false vt2 (inc vt3) sf3 sf3)]
                                                {:query-bounds (tu/->temporal-bounds vt0 Long/MAX_VALUE sf1 Long/MAX_VALUE)
                                                 :projects-temporal-cols? true})
                             ->pages))
              "Take page 1. Page 2 can bound page 1 and we can't filter newer pages (no constraints on query system-time)."))

      (t/testing "Constraints on query bounds"
        (t/is (= #{1 2} (->> (trie/filter-pages [(->mock-page 1 true vt1 (inc vt2) sf2 sf2)
                                                 (->mock-page 2 false vt2 vt3 sf3 sf3)]
                                                {:query-bounds (tu/->temporal-bounds vt1 (dec vt2) sf1 Long/MAX_VALUE)
                                                 :projects-temporal-cols? true})
                             ->pages))
              "Take page 1 and page 2. Page 2 is constrain-only — kept because the query projects temporal cols.")

        (t/is (= #{1} (->> (trie/filter-pages [(->mock-page 1 true vt1 (inc vt2) sf2 sf2)
                                               (->mock-page 2 false vt2 vt3 sf3 sf3)]
                                              {:query-bounds (tu/->temporal-bounds vt1 (dec vt2) sf1 Long/MAX_VALUE)
                                               :projects-temporal-cols? false})
                           ->pages))
              "Take page 1 only. Page 2 is constrain-only — dropped because the query projects no temporal col.")

        (t/is (= #{1} (->> (trie/filter-pages [(->mock-page 1 true vt1 (inc vt2) sf2 sf3)
                                               (->mock-page 2 false vt2 (inc vt3) sf3 sf3)]
                                              {:query-bounds (tu/->temporal-bounds vt0 Long/MAX_VALUE sf1 sf3)
                                               :projects-temporal-cols? true})
                           ->pages))
              "Take page 1. Page 2 can bound page 1 and we can filter newer pages because of query system-time constraints.")))

    (t/is (= #{0 1 2} (->> (trie/filter-pages [(->mock-page 0 true vt0 Long/MAX_VALUE sf1 sf1)
                                               (->mock-page 1 false vt1 vt2 sf2 sf2)
                                               (->mock-page 2 false vt2 vt3 sf3 sf3)]
                                              {:query-bounds (TemporalBounds.)
                                               :projects-temporal-cols? true})
                           ->pages))
          "All pages pages need to be taken if later pages bound valid-time")))

(deftest test-to-merge-task-temporal-filters
  (let [[micros-2018 micros-2019 micros-2020 micros-2021 micros-2022 micros-2023]
        (mapv inst->micros [#inst "2018-01-01" #inst "2019-01-01" #inst "2020-01-01"
                            #inst "2021-01-01" #inst "2022-01-01" #inst "2023-01-01"])]

    (letfn [(query-bounds+temporal-page-metadata->pages [temporal-page-metadata query-bounds]
              ;; this deftest covers temporal filtering, not the constrain-drop optimisation;
              ;; the helper pins `:projects-temporal-cols? true` so the assertions below
              ;; reflect the unoptimised behaviour.
              (->> (trie/filter-pages
                    (for [[page min-vf max-vt] temporal-page-metadata]
                      (->mock-page page min-vf max-vt))
                    {:query-bounds query-bounds, :projects-temporal-cols? true})
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

(t/deftest test-filters-by-recency
  (t/is (= [1]
           (->> (trie/filter-pages [(->MockPage 1 true (tu/->temporal-metadata 20251205 Long/MAX_VALUE) Long/MAX_VALUE)
                                    (->MockPage 2 false (tu/->temporal-metadata 20250706 Long/MAX_VALUE 20250707 20251208) 20250707)]
                                   {:query-bounds (tu/->temporal-bounds 20251212 Long/MAX_VALUE)
                                    :projects-temporal-cols? true})

                (mapv :page)))))

(t/deftest test-recency-excludes-from-emit
  ;; A page with vf=2020, validTo=∞, and recency=2025. Models a historical trie:
  ;; raw vt extends to ∞ (no explicit valid_to), but the polygon ended at recency=2025.
  ;; Querying past the recency, the page can't contribute live rows — `temporal-intersects?`
  ;; rules it out via the recency clause, regardless of the raw vt overlap.
  (let [page (->MockPage 0 true (tu/->temporal-metadata 20200101 Long/MAX_VALUE) 20250101)
        query-bounds (tu/->temporal-bounds 20260101 (inc 20260101) 20260101 (inc 20260101))]

    (t/is (= []
             (->> (trie/filter-pages [page] {:query-bounds query-bounds, :projects-temporal-cols? true})
                  (mapv :page)))
          "page recency 20250101 fails `min(query.lower) < page.recency` (20260101 ≮ 20250101)")))

(t/deftest test-projects-temporal-cols-flag
  ;; The four corners of the optimisation:
  ;;
  ;; - emit page (vt-intersects-query, content matches): kept under both flag values.
  ;; - supersede page (vt-intersects-query, content fails): kept under both flag values
  ;;   (still affects multiplicity).
  ;; - constrain-only page (vt-disjoint from query, vt-overlaps the emit page): kept when
  ;;   flag is true; dropped when flag is false (its only contribution is to a temporal
  ;;   column the consumer doesn't write).
  (let [pages [(->mock-page 0 true 0 30 0 0)         ; emit
               (->mock-page 1 false 0 30 0 0)        ; supersede — vt-intersects, content fails
               (->mock-page 2 true 15 25 0 0)]       ; constrain-only — vt 15→25, disjoint from query 0→10, overlaps emit page 0→30
        query-bounds (tu/->temporal-bounds 0 10)]

    (t/is (= #{0 1 2}
             (->> (trie/filter-pages pages {:query-bounds query-bounds, :projects-temporal-cols? true})
                  (into #{} (map :page))))
          "with :projects-temporal-cols? true: emit, supersede, and constrain-only all kept")

    (t/is (= #{0 1}
             (->> (trie/filter-pages pages {:query-bounds query-bounds, :projects-temporal-cols? false})
                  (into #{} (map :page))))
          "with :projects-temporal-cols? false: emit and supersede kept; constrain-only dropped")

    (t/is (= #{0 1}
             (->> (trie/filter-pages pages {:query-bounds query-bounds
                                            :projects-temporal-cols? true
                                            :clamp-valid-time? true})
                  (into #{} (map :page))))
          "with :clamp-valid-time? true: constrain-only dropped even though temporal cols are projected — its only contribution would be clamped away")))

(t/deftest candidates-are-kept-only-for-an-emit-page-they-can-affect
  (let [kept (->> (trie/filter-pages [(->mock-page 0 true 0 10 0 0)
                                      (->mock-page 1 true 20 30 10 10)
                                      (->mock-page 2 false 10 20 5 5)
                                      (->mock-page 3 false 25 40 15 15)
                                      (->mock-page 4 false 22 24 5 5)
                                      (->mock-page 5 false Long/MIN_VALUE Long/MAX_VALUE 25 25)]
                                     {:query-bounds (tu/->temporal-bounds 0 30 0 20)
                                      :projects-temporal-cols? true})
                  (into #{} (map :page)))]

    (t/is (not (contains? kept 2))
          "page 2 lies within the emit pages' envelope, but only touches each emit page")

    (t/is (contains? kept 3)
          "page 3 overlaps emit page 1 and is newer")

    (t/is (not (contains? kept 4))
          "page 4 overlaps emit page 1 but is older than it")

    (t/is (contains? kept 5)
          "page 5 may hold an erase, after the query's system-time window")))
