(ns xtdb.trie-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t :refer [deftest]]
            [xtdb.operator.scan :as scan]
            [xtdb.test-json :as tj]
            [xtdb.test-util :as tu]
            [xtdb.time :as time]
            [xtdb.trie :as trie :refer [MergePlanPage]]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.writer :as vw])
  (:import io.micrometer.core.instrument.simple.SimpleMeterRegistry
           (java.nio.file Paths)
           java.nio.file.Path
           (java.util.function IntPredicate Predicate)
           (org.apache.arrow.memory BufferAllocator RootAllocator)
           (org.apache.arrow.vector.types.pojo Field)
           (xtdb ICursor)
           xtdb.api.storage.Storage
           xtdb.arrow.Relation
           xtdb.buffer_pool.LocalBufferPool
           (xtdb.metadata ITableMetadata)
           (xtdb.trie ArrowHashTrie ArrowHashTrie$Leaf HashTrieKt MergePlanNode MergePlanTask)
           (xtdb.util TemporalBounds TemporalDimension)
           (xtdb.vector RelationWriter)))

(t/use-fixtures :each tu/with-allocator)

(deftest parses-trie-paths
  (letfn [(parse [trie-key]
            (-> (trie/parse-trie-file-path (util/->path (str trie-key ".arrow")))
                (update :part #(some-> % vec))
                (mapv [:level :part :first-row :next-row :rows])))]
    (t/is (= [0 nil 22 46 32] (parse (trie/->log-l0-l1-trie-key 0 22 46 32))))
    (t/is (= [2 [0 0 1 3] nil 120 nil] (parse (trie/->log-l2+-trie-key 2 (byte-array [0 0 1 3]) 120))))))

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
              t1-rel (tu/open-arrow-hash-trie-rel al [{Long/MAX_VALUE [nil 0 nil 1]} 2 nil
                                                      {(time/instant->micros (time/->instant #inst "2023-01-01")) 3
                                                       Long/MAX_VALUE 4}])
              log-rel (tu/open-arrow-hash-trie-rel al 0)
              log2-rel (tu/open-arrow-hash-trie-rel al [nil nil 0 1])]

    (t/is (= [{:path [1], :pages [{:seg :t1, :page-idx 2} {:seg :log, :page-idx 0}]}
              {:path [2], :pages [{:seg :log, :page-idx 0} {:seg :log2, :page-idx 0}]}
              {:path [3], :pages [{:page-idx 3, :seg :t1} {:page-idx 4, :seg :t1} {:page-idx 0, :seg :log} {:page-idx 1, :seg :log2}]}
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
                                          nil
                                          (TemporalBounds.))
                  (merge-plan-nodes->path+pages))))))

(deftest test-merge-plan-recency-filtering
  (with-open [al (RootAllocator.)
              t1-rel (tu/open-arrow-hash-trie-rel al [{Long/MAX_VALUE [nil 0 nil 1]}
                                                      {Long/MAX_VALUE 2}
                                                      nil
                                                      {(time/instant->micros (time/->instant #inst "2020-01-01")) 3
                                                       Long/MAX_VALUE 4}])
              t2-rel (tu/open-arrow-hash-trie-rel al [{(time/instant->micros (time/->instant #inst "2019-01-01")) 0
                                                       (time/instant->micros (time/->instant #inst "2020-01-01")) 1
                                                       Long/MAX_VALUE [nil 2 nil 3]}
                                                      nil
                                                      nil
                                                      {(time/instant->micros (time/->instant #inst "2020-01-01")) 4
                                                       Long/MAX_VALUE 5}])]
    (t/testing "pages 3 of t1 and 0,1 and 4 of t2 should not make it to the output"
      ;; setting up bounds for a current-time 2020-01-01
      (let [current-time (time/instant->micros (time/->instant #inst "2020-01-01"))
            temporal-bounds (TemporalBounds. (TemporalDimension. current-time (inc current-time))
                                             (TemporalDimension. current-time (inc current-time)))]


        (t/is (= [{:path [1], :pages [{:seg :t1, :page-idx 2}]}
                  {:path [3], :pages [{:seg :t1, :page-idx 4} {:seg :t2, :page-idx 5}]}
                  {:path [0 1],
                   :pages [{:seg :t1, :page-idx 0} {:seg :t2, :page-idx 2}]}
                  {:path [0 3],
                   :pages [{:seg :t1, :page-idx 1} {:seg :t2, :page-idx 3}]}]
                 (->> (HashTrieKt/toMergePlan [(-> (trie/->Segment (->arrow-hash-trie t1-rel))
                                                   (assoc :seg :t1))
                                               (-> (trie/->Segment (->arrow-hash-trie t2-rel))
                                                   (assoc :seg :t2))]
                                              nil
                                              temporal-bounds)
                      (merge-plan-nodes->path+pages))))))

    (t/testing "going one chronon below should bring in pages 3 of t1 and pages 1 and 4 of t2"
      (let [current-time (dec (time/instant->micros (time/->instant #inst "2020-01-01")))
            temporal-bounds (TemporalBounds. (TemporalDimension. current-time (inc current-time))
                                             (TemporalDimension. current-time (inc current-time)))]

        (t/is (=
               [{:path [1], :pages [{:seg :t1, :page-idx 2}]}
                {:path [3],
                 :pages [{:seg :t1, :page-idx 3} {:seg :t1, :page-idx 4} {:seg :t2, :page-idx 4} {:seg :t2, :page-idx 5}]}
                {:path [0 0],
                 :pages [{:seg :t2, :page-idx 1}]}
                {:path [0 1],
                 :pages [{:seg :t1, :page-idx 0} {:seg :t2, :page-idx 1} {:seg :t2, :page-idx 2}]}
                {:path [0 2],
                 :pages [{:seg :t2, :page-idx 1}]}
                {:path [0 3],
                 :pages [{:seg :t1, :page-idx 1} {:seg :t2, :page-idx 1} {:seg :t2, :page-idx 3}]}]
               (->> (HashTrieKt/toMergePlan [(-> (trie/->Segment (->arrow-hash-trie t1-rel))
                                                 (assoc :seg :t1))
                                             (-> (trie/->Segment (->arrow-hash-trie t2-rel))
                                                 (assoc :seg :t2))]
                                            nil
                                            temporal-bounds)
                    (merge-plan-nodes->path+pages))))))

    (t/testing "no bounds on system-time should bring in all pages"
      (let [current-time (time/instant->micros (time/->instant #inst "2020-01-01"))
            temporal-bounds (TemporalBounds. (TemporalDimension. current-time (inc current-time)) (TemporalDimension.))]

        (t/is (= [{:seg :t1, :page-idx 0}
                  {:seg :t1, :page-idx 1}
                  {:seg :t1, :page-idx 2}
                  {:seg :t1, :page-idx 3}
                  {:seg :t1, :page-idx 4}
                  {:seg :t2, :page-idx 0}
                  {:seg :t2, :page-idx 1}
                  {:seg :t2, :page-idx 2}
                  {:seg :t2, :page-idx 3}
                  {:seg :t2, :page-idx 4}
                  {:seg :t2, :page-idx 5}]
                 (->> (HashTrieKt/toMergePlan [(-> (trie/->Segment (->arrow-hash-trie t1-rel))
                                                   (assoc :seg :t1))
                                               (-> (trie/->Segment (->arrow-hash-trie t2-rel))
                                                   (assoc :seg :t2))]
                                              nil
                                              temporal-bounds)
                      (merge-plan-nodes->path+pages)
                      (mapcat :pages)
                      (sort-by (juxt :seg :page-idx))
                      distinct)))))))


(defrecord MockMergePlanPage [page metadata-matches? temporal-bounds]
  MergePlanPage
  (load-page [_ _ _] (throw (UnsupportedOperationException.)))
  (test-metadata [_] metadata-matches?)
  (temporal-bounds [_] temporal-bounds))

(defn ->mock-merge-plan-page
  ([page] (->mock-merge-plan-page page true))
  ([page metadata-matches?] (->MockMergePlanPage page metadata-matches? (TemporalBounds.)))
  ([page min-valid-from max-valid-to] (->mock-merge-plan-page page true min-valid-from max-valid-to))
  ([page metadata-matches? min-valid-from max-valid-to]
   (->MockMergePlanPage page metadata-matches? (tu/->min-max-page-bounds min-valid-from max-valid-to))))

(deftest test-to-merge-task
  (t/is (= [1 2] (->> (trie/->merge-task [(->mock-merge-plan-page 0 false)
                                          (->mock-merge-plan-page 1 true)
                                          (->mock-merge-plan-page 2 false)])
                      (map :page)))
        "earlier pages don't need to be taken if metadata doesn't match")

  (t/is (= [0 1 2] (->> (trie/->merge-task [(->mock-merge-plan-page 0 true)
                                            (->mock-merge-plan-page 1 false)
                                            (->mock-merge-plan-page 2 false)])
                        (map :page)))
        "all pages pages need to be taken if the earliest matches"))


(def ^:private inst->micros (comp time/instant->micros time/->instant))

(deftest test-to-merge-task-temporal-filters
  (let [[micros-2018 micros-2019 micros-2020 micros-2021 micros-2022 micros-2023]
        (mapv inst->micros [#inst "2018-01-01" #inst "2019-01-01" #inst "2020-01-01"
                            #inst "2021-01-01" #inst "2022-01-01" #inst "2023-01-01"])]

    (letfn [(query-bounds+temporal-page-metadata->pages [temporal-page-metadata query-bounds]
              (->> (trie/->merge-task
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

          (t/is (= [2] (query-bounds->pages (tu/->min-max-page-bounds micros-2020 micros-2021 micros-2020 (inc micros-2020))))
                "only 2020")

          (t/is (= [1 2] (query-bounds->pages (tu/->min-max-page-bounds (dec micros-2020) micros-2021 micros-2020 (inc micros-2020))))
                "end of 2019 + 2020")

          (t/is (= [2] (query-bounds->pages (tu/->min-max-page-bounds micros-2020 (inc micros-2021) micros-2020 (inc micros-2020))))
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
          (t/is (= [2 4] (query-bounds->pages (tu/->min-max-page-bounds micros-2020 micros-2021 micros-2020 (inc micros-2020))))
                "only 2020")

          ;; end of 2019 + 2020
          (t/is (= [1 2 4] (query-bounds->pages (tu/->min-max-page-bounds (dec micros-2020) micros-2021 micros-2020 (inc micros-2020))))
                "end of 2019 + 2020")

          (t/is (= [1 2 3 4] (query-bounds->pages (TemporalBounds. (TemporalDimension. (dec micros-2020) (inc micros-2021)) (TemporalDimension.))))
                "end of 2019 + 2020 + start of 2021 + no system time bounds")

          (t/is (= [2 4] (query-bounds->pages (tu/->min-max-page-bounds micros-2020 (inc micros-2021) micros-2020 (inc micros-2020))))
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

          (t/is (= [2 3 4] (query-bounds->pages (tu/->min-max-page-bounds micros-2020 micros-2021 micros-2023 (inc micros-2023))))
                "2020, but page 3 and 4 can bound page 2")))

      (t/testing "page two can be bounded by some, but not all pages in system time future"
        (let [query-bounds->pages (partial query-bounds+temporal-page-metadata->pages
                                           [[0 micros-2018 micros-2019]
                                            [1 micros-2019 micros-2020]
                                            [2 micros-2020 micros-2022]
                                            [3 micros-2021 micros-2022]
                                            [4 micros-2022 Long/MAX_VALUE]])]

          (t/is (= [2 3] (query-bounds->pages (tu/->min-max-page-bounds micros-2020 micros-2021 micros-2023 (inc micros-2023))))
                "2020, but only 3 can bound page 2 in 2020"))))))

(defn ->constantly-int-pred [v]
  (reify IntPredicate (test [_ _] v)))

(defn ->constantly-pred [v]
  (reify Predicate (test [_ _] v)))

(defn ->merge-tasks [segments query-bounds]
  (->> (HashTrieKt/toMergePlan segments (->constantly-pred true) query-bounds)
       (into [] (keep (fn [^MergePlanTask mpt]
                        (when-let [leaves (trie/->merge-task
                                           (for [^MergePlanNode mpn (.getMpNodes mpt)
                                                 :let [{:keys [seg page-bounds-fn]} (.getSegment mpn)
                                                       node (.getNode mpn)]]
                                             (->MockMergePlanPage {:seg seg :page-idx (.getDataPageIndex ^ArrowHashTrie$Leaf node)}
                                                                  true
                                                                  (page-bounds-fn (.getDataPageIndex ^ArrowHashTrie$Leaf node))))
                                           query-bounds)]
                          {:path (vec (.getPath mpt))
                           :pages (mapv :page leaves)}))))
       (sort-by :path)))

(deftest test-to-merge-tasks-with-temporal-filters
  (let [[micros-2018 micros-2019 micros-2020 micros-2021 micros-2022 micros-2023]
        (mapv inst->micros [#inst "2018-01-01" #inst "2019-01-01" #inst "2020-01-01"
                            #inst "2021-01-01" #inst "2022-01-01" #inst "2023-01-01"])
        current Long/MAX_VALUE

        t1 [{micros-2021 [nil 0 nil 1]}
            {micros-2021 2}
            nil
            {micros-2020 3
             micros-2021 4
             current 5}]
        t2 [{micros-2019 0
             micros-2020 1
             micros-2021 [nil 2 nil 3]
             micros-2022 [nil 4 nil 5]
             current 6}
            nil
            nil
            {micros-2020 7
             micros-2021 8}]]

    (with-open [al (RootAllocator.)
                t1-root (tu/open-arrow-hash-trie-rel al t1)
                t2-root (tu/open-arrow-hash-trie-rel al t2)]

      (let [t1-page-bounds {#{3}       [micros-2019 micros-2020]
                            #{0 1 2 4} [micros-2020 micros-2021]
                            #{5}       [micros-2022 micros-2023]}
            t2-page-bounds {#{0}     [micros-2018 micros-2019]
                            #{1 7}   [micros-2019 micros-2020]
                            #{2 3 8} [micros-2020 micros-2021]
                            #{4 5}   [micros-2021 micros-2022]
                            #{6}     [micros-2022 micros-2023]}
            _ (tu/verify-hash-tries+page-bounds t1 t1-page-bounds)
            _ (tu/verify-hash-tries+page-bounds t2 t2-page-bounds)

            ;; vt [2020 2021) st at 2020
            query-bounds (tu/->min-max-page-bounds micros-2020 micros-2021 micros-2020 (inc micros-2020))
            ;; vt [2020 2021) st unbounded
            query-bounds-no-sys-time-filter (TemporalBounds. (TemporalDimension. micros-2020 micros-2021) (TemporalDimension.))]

        (t/testing "recency and valid-time range filtering"
          (t/is (= [{:path [1], :pages [{:seg :t1, :page-idx 2}]}
                    {:path [3], :pages [{:seg :t1, :page-idx 4} {:seg :t2, :page-idx 8}]}
                    {:path [0 1], :pages [{:seg :t1, :page-idx 0} {:seg :t2, :page-idx 2}]}
                    {:path [0 3], :pages [{:seg :t1, :page-idx 1} {:seg :t2, :page-idx 3}]}]
                   (->merge-tasks [(-> (trie/->Segment (->arrow-hash-trie t1-root))
                                       (assoc :seg :t1
                                              :page-bounds-fn (tu/->page-bounds-fn t1-page-bounds)))
                                   (-> (trie/->Segment (->arrow-hash-trie t2-root))
                                       (assoc :seg :t2
                                              :page-bounds-fn (tu/->page-bounds-fn t2-page-bounds)))]
                                  query-bounds))
                "recency filtering")

          (t/is (= [{:path [1], :pages [{:seg :t1, :page-idx 2}]}
                    {:path [3], :pages [{:seg :t1, :page-idx 4} {:seg :t2, :page-idx 8}]}
                    {:path [0 1], :pages [{:seg :t1, :page-idx 0} {:seg :t2, :page-idx 2}]}
                    {:path [0 3], :pages [{:seg :t1, :page-idx 1} {:seg :t2, :page-idx 3}]}]
                   (->merge-tasks [(-> (trie/->Segment (->arrow-hash-trie t1-root))
                                       (assoc :seg :t1
                                              :page-bounds-fn (tu/->page-bounds-fn t1-page-bounds)))
                                   (-> (trie/->Segment (->arrow-hash-trie t2-root))
                                       (assoc :seg :t2
                                              :page-bounds-fn (tu/->page-bounds-fn t2-page-bounds)))]
                                  query-bounds-no-sys-time-filter))
                "valid-time range filtering")


          (t/testing "the current pages have retrospective updates"
            (let [t1-page-bounds (assoc t1-page-bounds #{5} [micros-2019 micros-2023])
                  t2-page-bounds (assoc t2-page-bounds #{6} [micros-2019 micros-2023])]
              (tu/verify-hash-tries+page-bounds t1 t1-page-bounds)
              (tu/verify-hash-tries+page-bounds t2 t2-page-bounds)

              (t/is (= [{:path [1], :pages [{:seg :t1, :page-idx 2}]}
                        {:path [3], :pages [{:seg :t1, :page-idx 4} {:seg :t1, :page-idx 5} {:seg :t2, :page-idx 8}]}
                        {:path [0 0], :pages [{:seg :t2, :page-idx 6}]}
                        {:path [0 1], :pages [{:seg :t1, :page-idx 0} {:seg :t2, :page-idx 2} {:seg :t2, :page-idx 6}]}
                        {:path [0 2], :pages [{:seg :t2, :page-idx 6}]}
                        {:path [0 3], :pages [{:seg :t1, :page-idx 1} {:seg :t2, :page-idx 3} {:seg :t2, :page-idx 6}]}]

                       (->merge-tasks [(-> (trie/->Segment (->arrow-hash-trie t1-root))
                                           (assoc :seg :t1
                                                  :page-bounds-fn (tu/->page-bounds-fn t1-page-bounds)))
                                       (-> (trie/->Segment (->arrow-hash-trie t2-root))
                                           (assoc :seg :t2
                                                  :page-bounds-fn (tu/->page-bounds-fn t2-page-bounds)))]
                                      query-bounds)))))

          (t/testing (str "some pages have retrospective updates. The newer ones (6 for t2) should be included because of potential"
                          "valid-to bounding effects")

            (let [t1-page-bounds (assoc t1-page-bounds #{5} [micros-2019 micros-2023])
                  t2-page-bounds (assoc t2-page-bounds #{4 5} [micros-2019 micros-2023])
                  ;; recency needs to be updated for t2
                  t2 (-> t2
                         (update 0 dissoc micros-2022)
                         (update 0 assoc micros-2023 [nil 4 nil 5]))]
              (with-open [t2-root (tu/open-arrow-hash-trie-rel al t2)]
                (tu/verify-hash-tries+page-bounds t1 t1-page-bounds)
                (tu/verify-hash-tries+page-bounds t2 t2-page-bounds)
                (t/is (= [{:path [1], :pages [{:seg :t1, :page-idx 2}]}
                          {:path [3], :pages [{:seg :t1, :page-idx 4} {:seg :t1, :page-idx 5} {:seg :t2, :page-idx 8}]}
                          {:path [0 1],
                           :pages [{:seg :t1, :page-idx 0} {:seg :t2, :page-idx 2} {:seg :t2, :page-idx 4} {:seg :t2, :page-idx 6}]}
                          {:path [0 3],
                           :pages [{:seg :t1, :page-idx 1} {:seg :t2, :page-idx 3} {:seg :t2, :page-idx 5} {:seg :t2, :page-idx 6}]}]

                         (->merge-tasks [(-> (trie/->Segment (->arrow-hash-trie t1-root))
                                             (assoc :seg :t1
                                                    :page-bounds-fn (tu/->page-bounds-fn t1-page-bounds)))
                                         (-> (trie/->Segment (->arrow-hash-trie t2-root))
                                             (assoc :seg :t2
                                                    :page-bounds-fn (tu/->page-bounds-fn t2-page-bounds)))]
                                        query-bounds-no-sys-time-filter)))))))))))

(defn ->trie-file-name
  " L0/L1 keys are submitted as [level first-row next-row rows]; L2+ as [level part-vec next-row]"
  [[level & args]]

  (case (long level)
    (0 1) (let [[first-row next-row rows] args]
            (util/->path (str (trie/->log-l0-l1-trie-key level first-row next-row (or rows 0)) ".arrow")))

    (let [[part next-row] args]
      (util/->path (str (trie/->log-l2+-trie-key level (byte-array part) next-row) ".arrow")))))

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

(defn- str->path [s]
  (Paths/get s, (make-array String 0)))


(defn ->mock-arrow-merge-plan-page [data-file page-idx]
  (scan/->ArrowMergePlanPage (str->path data-file)
                             (->constantly-int-pred true)
                             page-idx
                             (reify ITableMetadata
                               (temporalBounds [_ _] (TemporalBounds.)))))

(defn dir->buffer-pool
  "Creates a local storage buffer pool from the given directory."
  ^xtdb.BufferPool [^BufferAllocator allocator, ^Path dir]
  (let [bp-path (util/tmp-dir "tmp-buffer-pool")
        storage-root (.resolve bp-path Storage/storageRoot)]
    (util/copy-dir dir storage-root)
    (LocalBufferPool. allocator (Storage/localStorage bp-path) (SimpleMeterRegistry.))))

(deftest test-trie-cursor-with-multiple-recency-nodes-from-same-file-3298
  (let [eid #uuid "00000000-0000-0000-0000-000000000000"]
    (util/with-tmp-dirs #{tmp-dir}
      (with-open [al (RootAllocator.)
                  iid-arrow-buf (util/->arrow-buf-view al (util/->iid eid))
                  t1-rel (tu/open-arrow-hash-trie-rel al [{(time/instant->micros (time/->instant #inst "2023-01-01"))
                                                           [0 nil nil 1]
                                                           Long/MAX_VALUE [2 nil nil 3]}
                                                          nil nil 4])
                  t2-rel (tu/open-arrow-hash-trie-rel al [0 nil nil nil])]
        (let [id #uuid "00000000-0000-0000-0000-000000000000"
              t1-data-file "t1-data-file.arrow"
              t2-data-file "t2-data-file.arrow"]


          (tu/write-arrow-data-file al
                                    {0 [[:put {:xt/id id :foo "bar2" :xt/system-from 2}]
                                        [:put {:xt/id id :foo "bar1" :xt/system-from 1}]]
                                     1 []
                                     2 [[:put {:xt/id id :foo "bar0" :xt/system-from 0}]]
                                     3 []
                                     4 []}
                                    (.resolve tmp-dir t1-data-file))

          (tu/write-arrow-data-file al
                                    {0 [[:put {:xt/id id :foo "bar3" :xt/system-from 3}]]}
                                    (.resolve tmp-dir t2-data-file))

          (with-open [buffer-pool (dir->buffer-pool al tmp-dir)]

            (let [arrow-hash-trie1 (->arrow-hash-trie t1-rel)
                  arrow-hash-trie2 (->arrow-hash-trie t2-rel)
                  leaves [(->mock-arrow-merge-plan-page t1-data-file 0)
                          (->mock-arrow-merge-plan-page t1-data-file 2)
                          (->mock-arrow-merge-plan-page t2-data-file 0)]
                  merge-tasks [{:leaves leaves :path (byte-array [0 0])}]]
              (util/with-close-on-catch [out-rel (RelationWriter. al (for [^Field field
                                                                           [(types/->field "_id" (types/->arrow-type :uuid) false)
                                                                            (types/->field "foo" (types/->arrow-type :utf8) false)]]
                                                                       (vw/->writer (.createVector field al))))]

                (let [^ICursor trie-cursor (scan/->TrieCursor al (.iterator ^Iterable merge-tasks) out-rel
                                                              ["_id" "foo"] {}
                                                              (TemporalBounds.)
                                                              {} nil
                                                              (scan/->vsr-cache buffer-pool al)
                                                              buffer-pool)]

                  (t/is (= [[{:xt/id #uuid "00000000-0000-0000-0000-000000000000", :foo "bar3"}
                             {:xt/id #uuid "00000000-0000-0000-0000-000000000000", :foo "bar2"}
                             {:xt/id #uuid "00000000-0000-0000-0000-000000000000", :foo "bar1"}
                             {:xt/id #uuid "00000000-0000-0000-0000-000000000000", :foo "bar0"}]]
                           (tu/<-cursor trie-cursor)))
                  (.close trie-cursor))

                (t/is (= [{:path [0 0],
                           :pages [{:seg :t1, :page-idx 0}
                                   {:seg :t1, :page-idx 2}
                                   {:seg :t2, :page-idx 0}]}]
                         (->> (HashTrieKt/toMergePlan [(-> (trie/->Segment arrow-hash-trie1) (assoc :seg :t1))
                                                       (-> (trie/->Segment arrow-hash-trie2) (assoc :seg :t2))]
                                                      (scan/->path-pred iid-arrow-buf)
                                                      (TemporalBounds.))
                              (merge-plan-nodes->path+pages))))))))))))
