(ns xtdb.temporal-join-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.test-util :as tu])
  (:import [java.time Instant ZonedDateTime]))

(t/use-fixtures :each tu/with-mock-clock tu/with-node)

(defn- year ^Instant [y]
  (Instant/parse (format "%d-01-01T00:00:00Z" y)))

(def ^:private entity-count 20)
(def ^:private years (range 2000 2025))

(defn- seed-history!
  [table]
  (doseq [decade [2000 2010 2020]]
    (xt/execute-tx tu/*node* (for [y years
                                   :when (<= decade y (+ decade 9))
                                   id (range 1 (inc entity-count))]
                               [:put-docs {:into table, :valid-from (year y)} {:xt/id id, :k id, :v y}]))
    (tu/flush-block! tu/*node*)))

(defn- versions []
  (for [id (range 1 (inc entity-count)), y years]
    {:k id, :v y, :vf (year y), :vt (when (< y (last years)) (year (inc y)))}))

(defn- put-a! [rows]
  (xt/execute-tx tu/*node* (for [{:keys [id k vf vt t]} rows]
                             [:put-docs (cond-> {:into :a, :valid-from vf}
                                          vt (assoc :valid-to vt))
                              (cond-> {:xt/id id, :k k}
                                t (assoc :t t))]))
  (tu/flush-block! tu/*node*))

(defn- before? [^Instant a, ^Instant b]
  (cond (nil? b) true
        (nil? a) false
        :else (.isBefore a b)))

(defn- overlaps? [{f1 :vf, t1 :vt} {f2 :vf, t2 :vt}]
  (and (before? f1 t2) (before? f2 t1)))

(defn- contains-period? [{f1 :vf, t1 :vt} {f2 :vf, t2 :vt}]
  (and (not (before? f2 f1))
       (or (nil? t1) (and (some? t2) (not (before? t1 t2))))))

(defn- expected [a-rows pred]
  (set (for [a a-rows, b (versions)
             :when (and (= (:k a) (:k b)) (pred b a))]
         {:a-id (:id a), :v (:v b)})))

(defn- join-sql [key-pred period-pred]
  (format "SELECT a._id AS a_id, b.v FROM a FOR ALL VALID_TIME JOIN b FOR ALL VALID_TIME ON %s AND %s" key-pred period-pred))

(defn- q [sql] (set (xt/q tu/*node* sql)))

(defn- scan-row [sql table]
  (->> (xt/q tu/*node* (str "EXPLAIN ANALYZE " sql))
       (filter #(and (= :scan (:op %)) (= (str "public." table) (:scan-source (:attributes %)))))
       first))

(t/deftest a-narrow-build-period-reads-one-version-per-entity
  (seed-history! :b)
  (put-a! (for [i (range 1 4)] {:id i, :k i, :vf #inst "2021-03-01", :vt #inst "2021-04-01"}))

  (doseq [period-pred ["b._valid_time CONTAINS a._valid_time" "b._valid_time OVERLAPS a._valid_time"]]
    (let [expected #{{:a-id 1, :v 2021} {:a-id 2, :v 2021} {:a-id 3, :v 2021}}
          by-iid (join-sql "b._id = a.k" period-pred)
          by-col (join-sql "b.k = a.k" period-pred)]
      (t/is (= expected (q by-iid)) period-pred)
      (t/is (= expected (q by-col)) period-pred)

      (let [{:keys [row-count pushdowns]} (scan-row by-iid "b")]
        (t/is (= 3 row-count) (str "one version per probed entity: " period-pred))
        (t/is (some :valid-time pushdowns) (str "the bound reaches the scan: " period-pred)))

      (t/is (= entity-count (:row-count (scan-row by-col "b")))
            (str "one version per entity without an IID pushdown: " period-pred)))))

(t/deftest mixed-build-periods-match-the-unpushed-join
  (seed-history! :b)
  (let [a-rows [{:id 1, :k 1, :vf #inst "2021-03-01", :vt #inst "2021-04-01"}
                {:id 4, :k 4, :vf #inst "2005-06-01", :vt #inst "2012-06-01"}
                {:id 5, :k 5, :vf #inst "2022-07-01"}
                {:id 6, :k 6, :vf #inst "2022-07-01", :vt #inst "2022-07-02"}
                {:id 7, :k 7, :vf #inst "1990-01-01", :vt #inst "1991-01-01"}]
        a-rows (map #(update % :vf (fn [^java.util.Date d] (.toInstant d))) a-rows)
        a-rows (map #(cond-> % (:vt %) (update :vt (fn [^java.util.Date d] (.toInstant d)))) a-rows)]
    (put-a! a-rows)

    (t/is (= (expected a-rows overlaps?)
             (q (join-sql "b._id = a.k" "b._valid_time OVERLAPS a._valid_time"))))

    (t/is (= (expected a-rows contains-period?)
             (q (join-sql "b._id = a.k" "b._valid_time CONTAINS a._valid_time"))))

    (t/is (= (expected a-rows (fn [b a]
                                (and (some? (:vt a)) (before? (:vf b) (:vt a))
                                     (some? (:vt b)) (not (before? (:vt b) (:vf a))))))
             (q (join-sql "b._id = a.k" "b._valid_from < a._valid_to AND b._valid_to >= a._valid_from"))))))

(t/deftest a-null-build-value-cannot-match-and-is-skipped
  (seed-history! :b)
  (put-a! [{:id 1, :k 1, :vf (year 2000), :t (Instant/parse "2021-03-15T00:00:00Z")}
           {:id 2, :k 2, :vf (year 2000)}])

  (let [sql (join-sql "b._id = a.k" "b._valid_from <= a.t AND b._valid_to > a.t")]
    (t/is (= #{{:a-id 1, :v 2021}} (q sql)))
    (t/is (= 2 (:row-count (scan-row sql "b"))) "the 2021 version of each probed entity")))

(t/deftest an-empty-build-side-joins-to-nothing
  (seed-history! :b)
  (put-a! [{:id 1, :k 1, :vf (year 1990), :vt (year 1991)}])

  (t/is (empty? (q "SELECT a._id AS a_id, b.v FROM a JOIN b FOR ALL VALID_TIME ON b._id = a.k AND b._valid_time OVERLAPS a._valid_time"))))

(t/deftest a-one-chronon-build-period-resolves-as-a-point
  (seed-history! :b)
  (put-a! [{:id 1, :k 1, :vf (Instant/parse "2021-03-15T00:00:00Z"), :vt (Instant/parse "2021-03-15T00:00:00.000001Z")}])

  (let [sql (join-sql "b._id = a.k" "b._valid_time OVERLAPS a._valid_time")]
    (t/is (= #{{:a-id 1, :v 2021}} (q sql)))
    (t/is (= 1 (:row-count (scan-row sql "b"))))))

(t/deftest a-probe-read-at-a-point-keeps-that-point-under-the-bound
  (seed-history! :b)
  (xt/execute-tx tu/*node* [[:put-docs {:into :b, :valid-from (year 2000)} {:xt/id 21, :k 21, :v 2000}]])
  (tu/flush-block! tu/*node*)
  (put-a! (for [k [1 21]] {:id k, :k k, :vf #inst "2021-03-01", :vt #inst "2021-04-01"}))

  (let [sql "SELECT a._id AS a_id, b.v
             FROM a FOR ALL VALID_TIME
             JOIN b FOR VALID_TIME AS OF DATE '2025-06-01' ON b._id = a.k AND b._valid_time OVERLAPS a._valid_time"]
    (t/is (= #{{:a-id 21, :v 2000}} (q sql))
          "entity 1's 2025 version lies outside the periods; entity 21's only version covers them")
    (t/is (= 1 (:row-count (scan-row sql "b"))))))

(t/deftest a-computed-column-named-valid-from-takes-no-bound
  (seed-history! :b)
  (put-a! [{:id 1, :k 1, :vf (year 2000), :t (Instant/parse "2021-03-15T00:00:00Z")}])

  (t/is (= (set (for [y (range 2000 2023)] {:a-id 1, :v y}))
           (q "SELECT a._id AS a_id, b2.v
               FROM a FOR ALL VALID_TIME
               JOIN (SELECT _id, v, _valid_from - INTERVAL 'P1Y' AS _valid_from FROM b FOR ALL VALID_TIME) AS b2
                 ON b2._id = a.k AND b2._valid_from <= a.t"))))

(t/deftest the-bound-does-not-cross-a-limit
  (seed-history! :b)
  (put-a! [{:id 1, :k 1, :vf (year 2000), :t (Instant/parse "2021-03-15T00:00:00Z")}])

  (t/is (empty? (q "SELECT a._id AS a_id, b1.v
                    FROM a FOR ALL VALID_TIME
                    JOIN (SELECT _id, v, _valid_from FROM b FOR ALL VALID_TIME ORDER BY _valid_from DESC, _id LIMIT 1) AS b1
                      ON b1._id = a.k AND b1._valid_from <= a.t"))
        "the latest version overall is 2024, which the condition rejects"))

(t/deftest a-clamped-probe-scan-keeps-its-own-bounds
  (seed-history! :b)
  (put-a! [{:id 1, :k 1, :vf #inst "2024-06-01", :vt #inst "2024-07-01"}])

  (t/is (= #{{:a-id 1, :v 2024, :valid-to (year 2030)}}
           (->> (xt/q tu/*node* "SELECT a._id AS a_id, b.v, b._valid_to AS valid_to
                                 FROM a FOR ALL VALID_TIME
                                 JOIN b FOR VALID_TIME ONLY FROM DATE '2000-01-01' TO DATE '2030-01-01'
                                   ON b._id = a.k AND b._valid_time CONTAINS a._valid_time")
                (map #(update % :valid-to (fn [^ZonedDateTime zdt] (.toInstant zdt))))
                set))))

(t/deftest a-three-table-overlaps-bounds-the-last-joined-scan
  (seed-history! :b)
  (seed-history! :c)
  (let [a-rows [{:id 1, :k 1, :vf (year 2021), :vt (Instant/parse "2021-04-01T00:00:00Z")}]
        sql "SELECT a._id AS a_id, b.v AS bv, c.v AS cv
             FROM a FOR ALL VALID_TIME, b FOR ALL VALID_TIME, c FOR ALL VALID_TIME
             WHERE b._id = a.k AND c._id = b.k AND OVERLAPS(a._valid_time, b._valid_time, c._valid_time)"]
    (put-a! a-rows)

    (t/is (= (set (for [a a-rows, b (versions), c (versions)
                        :when (and (= (:k a) (:k b) (:k c)) (overlaps? a b) (overlaps? a c) (overlaps? b c))]
                    {:a-id (:id a), :bv (:v b), :cv (:v c)}))
             (q sql)))

    (t/is (some #(some :valid-time (:pushdowns (scan-row sql %))) ["b" "c"])
          "one of the two history scans takes the bound")))
