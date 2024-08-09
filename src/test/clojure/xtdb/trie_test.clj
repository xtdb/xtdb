(ns xtdb.trie-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t :refer [deftest]]
            [xtdb.buffer-pool :as bp]
            [xtdb.operator.scan :as scan :refer [MergePlanPage]]
            [xtdb.test-json :as tj]
            [xtdb.test-util :as tu]
            [xtdb.time :as time]
            [xtdb.trie :as trie]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.writer :as vw])
  (:import (java.nio.file Paths)
           (java.util.function IntPredicate Predicate)
           (org.apache.arrow.memory RootAllocator)
           (org.apache.arrow.vector.types.pojo Field)
           (xtdb ICursor)
           xtdb.arrow.Relation
           (xtdb.metadata ITableMetadata)
           (xtdb.trie ArrowHashTrie ArrowHashTrie$Leaf HashTrieKt MergePlanNode MergePlanTask)
           (xtdb.util TemporalBounds TemporalColumn)
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
              {:path [0 3], :pages [{:seg :t1, :page-idx 1} {:seg :log, :page-idx 0} ]}]
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
      (let [temporal-bounds (TemporalBounds.)
            current-time (time/instant->micros (time/->instant #inst "2020-01-01"))]
        (.lte (.getSystemFrom temporal-bounds) current-time)
        (.gt (.getSystemTo temporal-bounds) current-time)
        (.lte (.getValidFrom temporal-bounds) current-time)
        (.gt (.getValidTo temporal-bounds) current-time)


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
      (let [temporal-bounds (TemporalBounds.)
            current-time (- (time/instant->micros (time/->instant #inst "2020-01-01")) 1) ]
        (.lte (.getSystemFrom temporal-bounds) current-time)
        (.gt (.getSystemTo temporal-bounds) current-time)
        (.lte (.getValidFrom temporal-bounds) current-time)
        (.gt (.getValidTo temporal-bounds) current-time)


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
      (let [temporal-bounds (TemporalBounds.)
            current-time (- (time/instant->micros (time/->instant #inst "2020-01-01")) 1) ]
        (.lte (.getValidFrom temporal-bounds) current-time)
        (.gt (.getValidTo temporal-bounds) current-time)

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
  ([page min-valid-from max-valid-to] (->mock-merge-plan-page page true min-valid-from max-valid-to ))
  ([page metadata-matches? min-valid-from max-valid-to]
   (->MockMergePlanPage page metadata-matches? (tu/->min-max-page-bounds min-valid-from max-valid-to))))

(deftest test-to-merge-task
  (t/is (= [1 2] (->> (scan/->merge-task [(->mock-merge-plan-page 0 false)
                                          (->mock-merge-plan-page 1 true)
                                          (->mock-merge-plan-page 2 false)])
                      (map :page))))

  (t/is (= [0 1 2] (->> (scan/->merge-task [(->mock-merge-plan-page 0 true)
                                            (->mock-merge-plan-page 1 false)
                                            (->mock-merge-plan-page 2 false)])
                        (map :page)))))


(def ^:private inst->micros (comp time/instant->micros time/->instant))

(deftest test-to-merge-task-temporal-filters
  (let [[micros-2018 micros-2019 micros-2020 micros-2021 micros-2022 micros-2023]
        (mapv inst->micros [#inst "2018-01-01" #inst "2019-01-01" #inst "2020-01-01"
                            #inst "2021-01-01" #inst "2022-01-01" #inst "2023-01-01"])
        at-time micros-2020
        to-time micros-2021]

    (letfn [(query-bounds->pages [query-bounds]
              (->> (scan/->merge-task
                    (for [[page min-vf max-vt] [[0 micros-2018 micros-2019]
                                                [1 micros-2019 micros-2020]
                                                [2 micros-2020 micros-2021]
                                                [3 micros-2021 micros-2022]
                                                [4 micros-2022 Long/MAX_VALUE]]]
                      (->mock-merge-plan-page page min-vf max-vt))
                    query-bounds)
                   (mapv :page)))]


      (let [query-bounds (TemporalBounds.)]
        ;; stricly inside 2020
        (.lte (.getSystemFrom query-bounds) (inc at-time))
        (.gt (.getSystemTo query-bounds) (inc at-time))
        (.lte (.getValidFrom query-bounds) (dec to-time))
        (.gte (.getValidTo query-bounds) (inc at-time))

        (t/is (= [2] (query-bounds->pages query-bounds))))


      (let [query-bounds (TemporalBounds.)]
        ;; 2020
        (.lte (.getSystemFrom query-bounds) at-time)
        (.gt (.getSystemTo query-bounds) at-time)
        (.lte (.getValidFrom query-bounds) to-time)
        (.gte (.getValidTo query-bounds) at-time)

        (t/is (= [1 2] (query-bounds->pages query-bounds))))

      (let [query-bounds (TemporalBounds.)]
        ;; no system-bounds
        (.lte (.getValidFrom query-bounds) to-time)
        (.gte (.getValidTo query-bounds) at-time)

        (t/is (= [1 2 3] (query-bounds->pages query-bounds))))

      (let [query-bounds (TemporalBounds.)]
        ;; no bounds

        (t/is (= [0 1 2 3 4] (query-bounds->pages query-bounds)))))

    ;; with retrospective updates for the "current" page
    (letfn [(query-bounds->pages [query-bounds]
              (->> (scan/->merge-task
                    (for [[page min-vf max-vt] [[0 micros-2018 micros-2019]
                                                [1 micros-2019 micros-2020]
                                                [2 micros-2020 micros-2021]
                                                [3 micros-2021 micros-2022]
                                                [4 micros-2020 Long/MAX_VALUE]]]
                      (->mock-merge-plan-page page min-vf max-vt))
                    query-bounds)
                   (mapv :page)))]


      (let [query-bounds (TemporalBounds.)]
        ;; stricly inside 2020
        (.lte (.getSystemFrom query-bounds) (inc at-time))
        (.gt (.getSystemTo query-bounds) (inc at-time))
        (.lte (.getValidFrom query-bounds) (dec to-time))
        (.gte (.getValidTo query-bounds) (inc at-time))

        (t/is (= [2 4] (query-bounds->pages query-bounds))))

      (let [query-bounds (TemporalBounds.)]
        ;; 2020
        (.lte (.getSystemFrom query-bounds) at-time)
        (.gt (.getSystemTo query-bounds) at-time)
        (.lte (.getValidFrom query-bounds) to-time)
        (.gte (.getValidTo query-bounds) at-time)

        (t/is (= [1 2 4] (query-bounds->pages query-bounds))))

      (let [query-bounds (TemporalBounds.)]
        ;; no system-bounds
        (.lte (.getValidFrom query-bounds) to-time)
        (.gte (.getValidTo query-bounds) at-time)

        (t/is (= [1 2 3 4] (query-bounds->pages query-bounds))))

      (let [query-bounds (TemporalBounds.)]
        ;; no bounds

        (t/is (= [0 1 2 3 4] (query-bounds->pages query-bounds)))))))

(defn ->constantly-int-pred [v]
  (reify IntPredicate (test [_ _] v)))

(defn ->constantly-pred [v]
  (reify Predicate (test [_ _] v)))

(defn ->merge-tasks [segments query-bounds]
  (->> (HashTrieKt/toMergePlan segments (->constantly-pred true) query-bounds)
       (into [] (keep (fn [^MergePlanTask mpt]
                        (when-let [leaves (scan/->merge-task
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
  (let [
        [micros-2018 micros-2019 micros-2020 micros-2021 micros-2022 micros-2023]
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

            query-bounds (TemporalBounds.)
            query-bounds-no-sys-time-filter (TemporalBounds.)
            at-time (inst->micros #inst "2020-01-02")
            to-time (dec (inst->micros #inst "2021-01-01"))]

        (.lte (.getSystemFrom query-bounds) at-time)
        (.gt (.getSystemTo query-bounds) at-time)
        (.lte (.getValidFrom query-bounds) to-time)
        (.gt (.getValidTo query-bounds) at-time)

        ;; no sys time filtering, everything should make it through recency filtering
        (.lte (.getValidFrom query-bounds-no-sys-time-filter) to-time)
        (.gt (.getValidTo query-bounds-no-sys-time-filter) at-time)

        (t/testing "testing a pages-bound-fn that filters certain pages"

          ;; recency filtering
          (t/is (= [{:path [1], :pages [{:seg :t1, :page-idx 2}]}
                    {:path [3], :pages [{:seg :t1, :page-idx 4} {:seg :t2, :page-idx 8}]}
                    {:path [0 1], :pages [{:seg :t1, :page-idx 0} {:seg :t2, :page-idx 2}]}
                    {:path [0 3], :pages [{:seg :t1, :page-idx 1} {:seg :t2, :page-idx 3} ]}]
                   (->merge-tasks [(-> (trie/->Segment (->arrow-hash-trie t1-root))
                                       (assoc :seg :t1
                                              :page-bounds-fn (tu/->page-bounds-fn t1-page-bounds)))
                                   (-> (trie/->Segment (->arrow-hash-trie t2-root))
                                       (assoc :seg :t2
                                              :page-bounds-fn (tu/->page-bounds-fn t2-page-bounds)))]
                                  query-bounds)))

          ;; valid time range filtering
          (t/is (= [{:path [1], :pages [{:seg :t1, :page-idx 2}]}
                    {:path [3], :pages [{:seg :t1, :page-idx 4} {:seg :t2, :page-idx 8}]}
                    {:path [0 1], :pages [{:seg :t1, :page-idx 0} {:seg :t2, :page-idx 2}]}
                    {:path [0 3], :pages [{:seg :t1, :page-idx 1} {:seg :t2, :page-idx 3} ]}]
                   (->merge-tasks [(-> (trie/->Segment (->arrow-hash-trie t1-root))
                                       (assoc :seg :t1
                                              :page-idx-pred (->constantly-int-pred true)
                                              :page-bounds-fn (tu/->page-bounds-fn t1-page-bounds)))
                                   (-> (trie/->Segment (->arrow-hash-trie t2-root))
                                       (assoc :seg :t2
                                              :page-idx-pred (->constantly-int-pred true)
                                              :page-bounds-fn (tu/->page-bounds-fn t2-page-bounds)))]
                                  query-bounds-no-sys-time-filter)))


          ;; we make the current pages have retrospective updates
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
                                    query-bounds)))

            ;; pages 4 and 5 of t2 are also included as they can potentially bound intervals from above
            (t/is (= [{:path [1], :pages [{:seg :t1, :page-idx 2}]}
                      {:path [3], :pages [{:seg :t1, :page-idx 4} {:seg :t1, :page-idx 5} {:seg :t2, :page-idx 8}]}
                      {:path [0 0], :pages [{:seg :t2, :page-idx 6}]}
                      {:path [0 1],
                       :pages [{:seg :t1, :page-idx 0} {:seg :t2, :page-idx 2} {:seg :t2, :page-idx 6} {:seg :t2, :page-idx 4}]}
                      {:path [0 2], :pages [{:seg :t2, :page-idx 6}]}
                      {:path [0 3],
                       :pages [{:seg :t1, :page-idx 1} {:seg :t2, :page-idx 3} {:seg :t2, :page-idx 6} {:seg :t2, :page-idx 5}]}]

                     (->merge-tasks [(-> (trie/->Segment (->arrow-hash-trie t1-root))
                                         (assoc :seg :t1
                                                :page-bounds-fn (tu/->page-bounds-fn t1-page-bounds)))
                                     (-> (trie/->Segment (->arrow-hash-trie t2-root))
                                         (assoc :seg :t2
                                                :page-bounds-fn (tu/->page-bounds-fn t2-page-bounds)))]
                                    query-bounds-no-sys-time-filter)))))))))


(defn ->trie-file-name
  " L0/L1 keys are submitted as [level first-row next-row rows]; L2+ as [level part-vec next-row]"
  [[level & args]]

  (case (long level)
    (0 1) (let [[first-row next-row rows] args]
            (util/->path (str (trie/->log-l0-l1-trie-key level first-row next-row (or rows 0)) ".arrow")))

    (let [[part next-row] args]
      (util/->path (str (trie/->log-l2+-trie-key level (byte-array part) next-row) ".arrow")))))

(t/deftest test-selects-current-tries
  (letfn [(f [trie-keys]
            (->> (trie/current-trie-files (map ->trie-file-name trie-keys))
                 (mapv (comp (juxt :level (comp #(some-> % vec) :part) :next-row)
                             trie/parse-trie-file-path))))]

    (t/is (= [] (f [])))

    (t/testing "L0/L1 only"
      (t/is (= [[0 nil 1] [0 nil 2] [0 nil 3]]
               (f [[0 0 1 1] [0 1 2 1] [0 2 3 1]])))

      (t/is (= [[1 nil 2] [0 nil 3]]
               (f [[1 0 2 2] [0 0 1 1] [0 1 2 1] [0 2 3 1]]))
            "L1 file supersedes two L0 files")

      (t/is (= [[1 nil 3] [1 nil 4] [0 nil 5]]
               (f [[0 0 1 1] [0 1 2 1] [0 2 3 1] [0 3 4 1] [0 4 5 1]
                   [1 0 1 1] [1 0 2 2] [1 0 3 3] [1 3 4 1]]))
            "Superseded L1 files should not get returned"))

    (t/testing "L2"
      (t/is (= [[1 nil 2]]
               (f [[2 [0] 2] [2 [3] 2] [1 0 2 2]]))
            "L2 file doesn't supersede because not all parts complete")

      (t/is (= [[2 [0] 2] [2 [1] 2] [2 [2] 2] [2 [3] 2]]
               (f [[2 [0] 2] [2 [1] 2] [2 [2] 2] [2 [3] 2]
                   [1 0 2 2]]))
            "now the L2 file is complete")

      (t/is (= [[2 [0] 2] [2 [1] 2] [2 [2] 2] [2 [3] 2] [0 nil 3]]
               (f [[2 [0] 2] [2 [1] 2] [2 [2] 2] [2 [3] 2]
                   [1 0 2 2]
                   [0 0 1 1] [0 1 2 1] [0 2 3 1]]))
            "L2 file supersedes L1, L1 supersedes L0, left with a single L0 file"))

    (t/testing "L3+"
      (t/is (= [[3 [0 0] 2] [3 [0 1] 2] [3 [0 2] 2] [3 [0 3] 2]
                [2 [1] 2] [2 [2] 2] [2 [3] 2] ; L2 path 0 covered
                [0 nil 3]]
               (f [[3 [0 0] 2] [3 [0 1] 2] [3 [0 2] 2] [3 [0 3] 2]
                   [3 [1 0] 2] [3 [1 2] 2] [3 [1 3] 2] ; L2 path 1 not covered yet, missing [1 1]
                   [2 [0] 2] [2 [1] 2] [2 [2] 2] [2 [3] 2]
                   [1 0 2 2]
                   [0 0 1 1] [0 1 2 1] [0 2 3 1]]))
            "L3 covered idx 0 but not 1")

      (t/is (= [[4 [0 1 0] 2] [4 [0 1 1] 2] [4 [0 1 2] 2] [4 [0 1 3] 2]
                [3 [0 0] 2] [3 [0 2] 2] [3 [0 3] 2] ; L3 path [0 1] covered
                [2 [1] 2] [2 [2] 2] [2 [3] 2] ; L2 path 0 covered
                [0 nil 3]]

               (f [[4 [0 1 0] 2] [4 [0 1 1] 2] [4 [0 1 2] 2] [4 [0 1 3] 2]
                   [3 [0 0] 2] [3 [0 1] 2] [3 [0 2] 2] [3 [0 3] 2]
                   [3 [1 0] 2] [3 [1 2] 2] [3 [1 3] 2] ; L2 path 1 not covered yet, missing [1 1]
                   [2 [0] 2] [2 [1] 2] [2 [2] 2] [2 [3] 2]
                   [1 0 2 2]
                   [0 0 1 1] [0 1 2 1] [0 2 3 1]]))
            "L4 covers L3 path [0 1]")

      (t/is (= [[4 [0 1 0] 2] [4 [0 1 1] 2] [4 [0 1 2] 2] [4 [0 1 3] 2]
                [3 [0 0] 2] [3 [0 2] 2] [3 [0 3] 2] ; L3 path [0 1] covered
                [2 [1] 2] [2 [2] 2] [2 [3] 2] ; L2 path 0 covered even though [0 1] GC'd
                [0 nil 3]]

               (f [[4 [0 1 0] 2] [4 [0 1 1] 2] [4 [0 1 2] 2] [4 [0 1 3] 2]
                   [3 [0 0] 2] [3 [0 2] 2] [3 [0 3] 2]
                   [3 [1 0] 2] [3 [1 2] 2] [3 [1 3] 2] ; L2 path 1 not covered yet, missing [1 1]
                   [2 [0] 2] [2 [1] 2] [2 [2] 2] [2 [3] 2]
                   [1 0 2 2]
                   [0 0 1 1] [0 1 2 1] [0 2 3 1]]))
            "L4 covers L3 path [0 1], L3 [0 1] GC'd, still correctly covered"))

    (t/testing "empty levels"
      (t/is (= [[2 [0] 2] [2 [1] 2] [2 [2] 2] [2 [3] 2] [0 nil 3]]
               (f [[2 [0] 2] [2 [1] 2] [2 [2] 2] [2 [3] 2]
                   [0 0 1 1] [0 1 2 1] [0 2 3 1]]))
            "L1 empty")

      (t/is (= [[2 [0] 2] [2 [1] 2] [2 [2] 2] [2 [3] 2]]
               (f [[2 [0] 2] [2 [1] 2] [2 [2] 2] [2 [3] 2]]))
            "L1 and L0 empty")

      (t/is (= [[3 [0 0] 2] [3 [0 1] 2] [3 [0 2] 2] [3 [0 3] 2]
                [3 [1 0] 2] [3 [1 1] 2] [3 [1 2] 2] [3 [1 3] 2]
                [3 [2 0] 2] [3 [2 1] 2] [3 [2 2] 2] [3 [2 3] 2]
                [3 [3 0] 2] [3 [3 1] 2] [3 [3 2] 2] [3 [3 3] 2]
                [0 nil 3]]
               (f [[3 [0 0] 2] [3 [0 1] 2] [3 [0 2] 2] [3 [0 3] 2]
                   [3 [1 0] 2] [3 [1 1] 2] [3 [1 2] 2] [3 [1 3] 2]
                   [3 [2 0] 2] [3 [2 1] 2] [3 [2 2] 2] [3 [2 3] 2]
                   [3 [3 0] 2] [3 [3 1] 2] [3 [3 2] 2] [3 [3 3] 2]
                   [1 0 2 2]
                   [0 0 1 1] [0 1 2 1] [0 2 3 1]]))
            "L2 missing, still covers L1"))))

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

(deftest test-trie-cursor-with-multiple-recency-nodes-from-same-file-3298
  (let [eid #uuid "00000000-0000-0000-0000-000000000000"]
    (util/with-tmp-dirs #{tmp-dir}
      (with-open [al (RootAllocator.)
                  iid-arrow-buf (util/->arrow-buf-view al (trie/->iid eid))
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

          (with-open [buffer-pool (bp/dir->buffer-pool al tmp-dir)]

            (let [arrow-hash-trie1 (->arrow-hash-trie t1-rel)
                  arrow-hash-trie2 (->arrow-hash-trie t2-rel)
                  leaves [(->mock-arrow-merge-plan-page t1-data-file 0)
                          (->mock-arrow-merge-plan-page t1-data-file 2)
                          (->mock-arrow-merge-plan-page t2-data-file 0)]
                  merge-tasks [{:leaves leaves :path (byte-array [0 0])} ]]
              (util/with-close-on-catch [out-rel (RelationWriter. al (for [^Field field
                                                                           [(types/->field "xt$id" (types/->arrow-type :uuid) false)
                                                                            (types/->field "foo" (types/->arrow-type :utf8) false)]]
                                                                       (vw/->writer (.createVector field al))))]

                (let [^ICursor trie-cursor (scan/->TrieCursor al (.iterator ^Iterable merge-tasks) out-rel
                                                              ["xt$id" "foo"] {}
                                                              (TemporalBounds.)
                                                              nil
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
