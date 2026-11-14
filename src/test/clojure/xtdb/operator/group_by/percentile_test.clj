(ns xtdb.operator.group-by.percentile-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.operator.group-by :as group-by]
            [xtdb.operator.order-by :as order-by]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import (java.nio.file Files)
           (java.nio.file.attribute FileAttribute)
           (java.time LocalDate)
           (java.util Random)
           (xtdb.api ICursor)
           (xtdb.arrow RelationReader VectorMask)
           (xtdb.arrow.agg AggregateSpec)
           (xtdb.operator.order_by IExternalSorter)))

(t/use-fixtures :each tu/with-allocator tu/with-node)

(t/deftest test-percentile-cont-basic
  (t/is (= [{:median 20.0}]
           (tu/query-ra '[:group-by {:columns [{median (percentile_cont 0.5 [v {:direction :asc}])}]}
                          [:table {:rows [{:v 10} {:v 20} {:v 30}]}]]))
        "median of 3 values"))

(t/deftest test-percentile-cont-interpolation
  (t/is (= [{:p 2.5}]
           (tu/query-ra '[:group-by {:columns [{p (percentile_cont 0.5 [v {:direction :asc}])}]}
                          [:table {:rows [{:v 1} {:v 2} {:v 3} {:v 4}]}]]))
        "interpolates between 2 and 3")

  (t/is (= [{:p 1.0}]
           (tu/query-ra '[:group-by {:columns [{p (percentile_cont 0.0 [v {:direction :asc}])}]}
                          [:table {:rows [{:v 1} {:v 2} {:v 3}]}]]))
        "fraction=0 returns first")

  (t/is (= [{:p 3.0}]
           (tu/query-ra '[:group-by {:columns [{p (percentile_cont 1.0 [v {:direction :asc}])}]}
                          [:table {:rows [{:v 1} {:v 2} {:v 3}]}]]))
        "fraction=1 returns last"))

(t/deftest test-percentile-cont-single-value
  (t/is (= [{:p 42.0}]
           (tu/query-ra '[:group-by {:columns [{p (percentile_cont 0.5 [v {:direction :asc}])}]}
                          [:table {:rows [{:v 42}]}]]))
        "single value returns that value"))

(t/deftest test-percentile-cont-with-nulls
  (t/is (= [{:p 20.0}]
           (tu/query-ra '[:group-by {:columns [{p (percentile_cont 0.5 [v {:direction :asc}])}]}
                          [:table {:rows [{:v 10} {:v nil} {:v 30}]}]]))
        "nulls are excluded from percentile computation"))

(t/deftest test-percentile-cont-empty
  (t/is (= [{}]
           (tu/query-ra '[:group-by {:columns [{p (percentile_cont 0.5 [v {:direction :asc}])}]}
                          [::tu/pages {v #xt/type [:? :i64]} []]]))
        "empty input returns null (zero-row)"))

(t/deftest test-percentile-cont-desc
  (t/is (= [{:p 30.0}]
           (tu/query-ra '[:group-by {:columns [{p (percentile_cont 0.0 [v {:direction :desc}])}]}
                          [:table {:rows [{:v 10} {:v 20} {:v 30}]}]]))
        "desc ordering: fraction=0 returns the largest (first in desc order)"))

(t/deftest test-percentile-cont-grouped
  (t/is (= #{{:g 1, :p 15.0}
             {:g 2, :p 35.0}}
           (set (tu/query-ra '[:group-by {:columns [g {p (percentile_cont 0.5 [v {:direction :asc}])}]}
                               [:table {:rows [{:g 1, :v 10}
                                               {:g 1, :v 20}
                                               {:g 2, :v 30}
                                               {:g 2, :v 40}]}]])))
        "different medians per group"))

(t/deftest test-percentile-disc-basic
  (t/is (= [{:p 20}]
           (tu/query-ra '[:group-by {:columns [{p (percentile_disc 0.5 [v {:direction :asc}])}]}
                          [:table {:rows [{:v 10} {:v 20} {:v 30}]}]]))
        "disc picks exact value, no interpolation")

  (t/is (= [{:p 10}]
           (tu/query-ra '[:group-by {:columns [{p (percentile_disc 0.0 [v {:direction :asc}])}]}
                          [:table {:rows [{:v 10} {:v 20} {:v 30}]}]]))
        "fraction=0 returns first")

  (t/is (= [{:p 30}]
           (tu/query-ra '[:group-by {:columns [{p (percentile_disc 1.0 [v {:direction :asc}])}]}
                          [:table {:rows [{:v 10} {:v 20} {:v 30}]}]]))
        "fraction=1 returns last"))

(t/deftest test-percentile-disc-desc
  (t/is (= [{:p 30}]
           (tu/query-ra '[:group-by {:columns [{p (percentile_disc 0.0 [v {:direction :desc}])}]}
                          [:table {:rows [{:v 10} {:v 20} {:v 30}]}]]))
        "desc: fraction=0 returns largest"))

(t/deftest test-percentile-disc-grouped
  (t/is (= #{{:g 1, :p 10}
             {:g 2, :p 30}}
           (set (tu/query-ra '[:group-by {:columns [g {p (percentile_disc 0.5 [v {:direction :asc}])}]}
                               [:table {:rows [{:g 1, :v 10}
                                               {:g 1, :v 20}
                                               {:g 2, :v 30}
                                               {:g 2, :v 40}]}]])))
        "disc with groups"))

(t/deftest test-percentile-disc-strings
  (t/is (= [{:p "banana"}]
           (tu/query-ra '[:group-by {:columns [{p (percentile_disc 0.5 [v {:direction :asc}])}]}
                          [:table {:rows [{:v "apple"} {:v "banana"} {:v "cherry"}]}]]))
        "disc works with non-numeric types"))

(t/deftest test-percentile-cont-float-input
  (t/is (= [{:p 2.0}]
           (tu/query-ra '[:group-by {:columns [{p (percentile_cont 0.5 [v {:direction :asc}])}]}
                          [:table {:rows [{:v 1.0} {:v 3.0}]}]]))
        "float inputs work without promotion"))

(t/deftest test-percentile-via-sql
  (xt/submit-tx tu/*node* [[:sql "INSERT INTO scores RECORDS {_id: 1, team: 'a', score: 10},
                                                              {_id: 2, team: 'a', score: 20},
                                                              {_id: 3, team: 'a', score: 30},
                                                              {_id: 4, team: 'b', score: 100},
                                                              {_id: 5, team: 'b', score: 200}"]])

  (t/is (= [{:median 20.0}]
           (xt/q tu/*node* "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY score) AS median FROM scores WHERE team = 'a'"))
        "SQL PERCENTILE_CONT")

  (t/is (= [{:median 20}]
           (xt/q tu/*node* "SELECT PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY score) AS median FROM scores WHERE team = 'a'"))
        "SQL PERCENTILE_DISC")

  (t/is (= #{{:team "a", :median 20.0}
             {:team "b", :median 150.0}}
           (set (xt/q tu/*node* "SELECT team, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY score) AS median FROM scores GROUP BY team")))
        "SQL PERCENTILE_CONT with GROUP BY"))

(t/deftest test-percentile-cont-all-nulls
  (t/is (= [{}]
           (tu/query-ra '[:group-by {:columns [{p (percentile_cont 0.5 [v {:direction :asc}])}]}
                          [:table {:rows [{:v nil} {:v nil}]}]]))
        "all null values returns null"))

(t/deftest test-percentile-across-batches
  (t/is (= [{:p 25.0}]
           (tu/query-ra '[:group-by {:columns [{p (percentile_cont 0.5 [v {:direction :asc}])}]}
                          [::tu/pages {v #xt/type :i64}
                           [[{:v 10} {:v 20}]
                            [{:v 30} {:v 40}]]]]))
        "works across multiple batches"))

(defn- reference-percentile [mode fraction direction vs]
  (let [vs (vec (cond-> (sort (remove nil? vs))
                  (= :desc direction) reverse))
        n (count vs)]
    (when (pos? n)
      (case mode
        :cont (let [rank (* fraction (dec n))
                    lo (long (Math/floor rank))
                    hi (long (Math/ceil rank))
                    v-lo (double (nth vs lo))
                    v-hi (double (nth vs hi))]
                (if (== lo hi)
                  v-hi
                  (+ v-lo (* (- rank lo) (- v-hi v-lo)))))
        :disc (nth vs (cond
                        (<= fraction 0.0) 0
                        (>= fraction 1.0) (dec n)
                        :else (max 0 (dec (long (Math/ceil (* fraction n)))))))))))

(def ^:private spill-aggs
  '[{c-asc (percentile_cont 0.3 [v {:direction :asc}])}
    {c-desc (percentile_cont 0.3 [v {:direction :desc}])}
    {c-last (percentile_cont 1.0 [v {:direction :asc}])}
    {d-asc (percentile_disc 0.3 [v {:direction :asc}])}
    {d-desc (percentile_disc 0.3 [v {:direction :desc}])}
    {d-first (percentile_disc 0.0 [v {:direction :asc}])}
    {c-filtered (percentile_cont 0.5 [v {:direction :asc}] {:filter (> w 0)})}
    {d-filtered-out (percentile_disc 0.5 [v {:direction :asc}] {:filter (> w 1000)})}
    {c-param (percentile_cont ?f [v {:direction :asc}])}])

(defn- reference-row [rows]
  (let [vs (map :v rows)]
    (->> {:c-asc (reference-percentile :cont 0.3 :asc vs)
          :c-desc (reference-percentile :cont 0.3 :desc vs)
          :c-last (reference-percentile :cont 1.0 :asc vs)
          :d-asc (reference-percentile :disc 0.3 :asc vs)
          :d-desc (reference-percentile :disc 0.3 :desc vs)
          :d-first (reference-percentile :disc 0.0 :asc vs)
          :c-filtered (reference-percentile :cont 0.5 :asc (map :v (filter (comp pos? :w) rows)))
          :d-filtered-out nil
          :c-param (reference-percentile :cont 0.25 :asc vs)}
         (into {} (remove (comp nil? val))))))

(defn- with-spill-dir* [f]
  (let [parent (Files/createTempDirectory "test-spill-" (make-array FileAttribute 0))
        !spills (atom 0)]
    (try
      (with-redefs [util/tmp-dir (fn [prefix]
                                   (swap! !spills inc)
                                   (Files/createTempDirectory parent prefix (make-array FileAttribute 0)))]
        (f (.toFile parent) !spills))
      (finally
        (util/delete-dir parent)))))

(t/deftest test-percentile-spill-matches-in-memory
  (let [rnd (Random. 6401)
        rows (vec (concat (for [i (range 1000)]
                            {:g (mod i 5)
                             :v (when (pos? (.nextInt rnd 10)) (.nextInt rnd 1000))
                             :w (- (.nextInt rnd 10) 5)})
                          (for [_ (range 3)]
                            {:g 5, :v nil, :w 1})))
        batches (mapv vec (partition-all 13 (shuffle rows)))
        query (fn [group-cols]
                (->> (tu/query-ra [:group-by {:columns (into group-cols spill-aggs)}
                                   [::tu/pages batches]]
                                  {:args {:f 0.25}})
                     (sort-by :g)))
        expected-grouped (->> (group-by :g rows)
                              (map (fn [[g rows]] (assoc (reference-row rows) :g g)))
                              (sort-by :g))
        expected-ungrouped [(reference-row rows)]]

    (with-spill-dir*
      (fn [parent !spills]
        (t/is (= expected-grouped (query '[g])) "grouped, in memory")
        (t/is (= expected-ungrouped (query [])) "ungrouped, in memory")
        (t/is (zero? @!spills) "in-memory runs don't spill")

        (binding [order-by/*block-size* 10]
          (t/is (= expected-grouped (query '[g])) "grouped, spilled")
          (t/is (= expected-ungrouped (query [])) "ungrouped, spilled"))

        (t/is (pos? @!spills) "spilled runs spill")
        (t/is (empty? (.list parent)) "spill dirs cleaned up after a full drain")))))

(t/deftest test-percentile-spill-non-integer-values
  (let [rnd (Random. 6402)
        rows (vec (for [i (range 500)]
                    {:g (mod i 3)
                     :s (when (pos? (.nextInt rnd 10)) (str "s" (.nextInt rnd 1000)))
                     :d (when (pos? (.nextInt rnd 10)) (.nextDouble rnd))
                     :t (when (pos? (.nextInt rnd 10)) (LocalDate/ofEpochDay (.nextInt rnd 20000)))}))
        batches (mapv vec (partition-all 7 rows))
        aggs '[{s-asc (percentile_disc 0.3 [s {:direction :asc}])}
               {s-desc (percentile_disc 0.3 [s {:direction :desc}])}
               {d-asc (percentile_cont 0.3 [d {:direction :asc}])}
               {d-desc (percentile_cont 0.7 [d {:direction :desc}])}
               {t-asc (percentile_disc 0.5 [t {:direction :asc}])}]
        query (fn [group-cols]
                (->> (tu/query-ra [:group-by {:columns (into group-cols aggs)}
                                   [::tu/pages batches]])
                     (sort-by :g)))]

    (with-spill-dir*
      (fn [parent !spills]
        (let [in-memory-grouped (query '[g])
              in-memory-ungrouped (query [])]
          (t/is (zero? @!spills) "in-memory runs don't spill")
          (t/is (every? #(= #{:g :s-asc :s-desc :d-asc :d-desc :t-asc} (set (keys %))) in-memory-grouped)
                "every group has a result for every aggregate")

          (binding [order-by/*block-size* 10]
            (t/is (= in-memory-grouped (query '[g])) "grouped, spilled")
            (t/is (= in-memory-ungrouped (query [])) "ungrouped, spilled"))

          (t/is (pos? @!spills) "spilled runs spill")
          (t/is (empty? (.list parent)) "spill dirs cleaned up after a full drain"))))))

(defn- recording-sorted-page-sizes [!page-sizes ->sorter]
  (fn [allocator vec-types order-specs ^long row-limit]
    (let [^IExternalSorter sorter (->sorter allocator vec-types order-specs row-limit)]
      (reify IExternalSorter
        (append [_ rel] (.append sorter rel))
        (finish [_]
          (let [cursor (.finish sorter)]
            (reify ICursor
              (getCursorType [_] (.getCursorType cursor))
              (getChildCursors [_] (.getChildCursors cursor))
              (tryAdvance [_ c]
                (.tryAdvance cursor (fn [^RelationReader page]
                                      (swap! !page-sizes conj (.getRowCount page))
                                      (.accept c page))))
              (close [_] (.close cursor)))))
        (close [_] (.close sorter))))))

(t/deftest test-percentile-cont-interpolates-across-a-sorted-page-boundary
  (let [rows (vec (concat (for [v (range 3)] {:g 0, :v v})
                          (for [v (range 16)] {:g 1, :v (* 10 v)})))
        !page-sizes (atom [])]
    (binding [order-by/*block-size* 10]
      (with-redefs [order-by/->external-sorter (recording-sorted-page-sizes !page-sizes order-by/->external-sorter)]
        (t/is (= [{:g 0, :p 1.0} {:g 1, :p 75.0}]
                 (->> (tu/query-ra [:group-by '{:columns [g {p (percentile_cont 0.5 [v {:direction :asc}])}]}
                                    [::tu/pages (mapv vec (partition-all 2 rows))]])
                      (sort-by :g))))))

    (let [first-page-end (first @!page-sizes)
          group-1-start 3
          lo (+ group-1-start 7)
          hi (+ group-1-start 8)]
      (t/is (and (< 1 (count @!page-sizes)) (<= first-page-end hi) (< lo first-page-end))
            (str "group 1's lo and hi ranks straddle the first sorted page boundary, pages: " @!page-sizes)))))

(t/deftest test-percentile-spill-dir-cleaned-up-after-an-error-mid-aggregation
  (with-spill-dir*
    (fn [parent !spills]
      (binding [order-by/*block-size* 10]
        (t/is (thrown-with-msg? Exception #"division by zero"
                                (tu/query-ra [:group-by '{:columns [{p (percentile_cont 0.5 [v {:direction :asc}])}]}
                                              [:project '{:projections [{v (/ 100 x)}]}
                                               [::tu/pages (conj (vec (for [x (range 1 31)] [{:x x}]))
                                                                 [{:x 0}])]]]))))

      (t/is (pos? @!spills) "spilled before the error")
      (t/is (empty? (.list parent)) "spill dir cleaned up after the error"))))

(t/deftest test-percentile-spill-zero-row
  (binding [order-by/*block-size* 10]
    (t/is (= [{}]
             (tu/query-ra [:group-by '{:columns [{p (percentile_cont 0.5 [v {:direction :asc}] {:filter (> v 1000)})}]}
                           [::tu/pages [(vec (for [v (range 25)] {:v v}))]]]))
          "every row filtered out returns null")))

(t/deftest test-percentile-spill-closed-before-finish-deletes-its-spill
  (with-spill-dir*
    (fn [parent _]
      (binding [order-by/*block-size* 10]
        (with-open [rel (tu/open-rel {"v" (vec (range 25))})
                    group-mapping (tu/open-vec "group-mapping" #xt/type :i32 (vec (repeat 25 (int 0))))]
          (with-open [^AggregateSpec spec (.build (group-by/->aggregate-factory {:f :percentile_cont
                                                                                 :from-name 'v
                                                                                 :from-type #xt/type :i64
                                                                                 :fraction [:literal 0.5]
                                                                                 :to-name 'p
                                                                                 :zero-row? true
                                                                                 :sort-opts {:direction :asc}})
                                                  tu/*allocator* nil)]
            (.aggregate spec rel group-mapping VectorMask/ALL)
            (t/is (seq (.list parent)) "spilled a run"))

          (t/is (empty? (.list parent)) "spill dir cleaned up on close"))))))

(t/deftest test-percentile-fraction-out-of-range
  (doseq [f '[percentile_cont percentile_disc]
          fraction [1.5 -0.5]
          [input desc] [['[:table {:rows [{:v 1} {:v 2} {:v 3}]}] "three rows"]
                        ['[:table {:rows [{:v 1}]}] "one row"]
                        ['[::tu/pages {v #xt/type [:? :i64]} []] "empty input"]]]
    (t/is (thrown-with-msg? Exception #"percentile fraction must be between 0 and 1"
                            (tu/query-ra [:group-by {:columns [{'p (list f fraction '[v {:direction :asc}])}]}
                                          input]))
          (str f " " fraction ", literal, " desc))

    (t/is (thrown-with-msg? Exception #"percentile fraction must be between 0 and 1"
                            (tu/query-ra [:group-by {:columns [{'p (list f '?f '[v {:direction :asc}])}]}
                                          input]
                                         {:args {:f fraction}}))
          (str f " " fraction ", param, " desc))))
