(ns xtdb.tpch-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [xtdb.core.sql :as core.sql]
            [xtdb.datalog :as d]
            [xtdb.datasets.tpch :as tpch]
            [xtdb.datasets.tpch.datalog :as tpch-datalog]
            [xtdb.datasets.tpch.ra :as tpch-ra]
            [xtdb.sql :as sql]
            xtdb.sql-test
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import (java.nio.file Path)))

(def ^:dynamic *node* nil)

;; (slurp (io/resource (format "io/airlift/tpch/queries/q%d.sql" 1)))

(defn with-tpch-data [{:keys [method ^Path node-dir scale-factor]} f]
  (if *node*
    (f)

    (do
      (util/delete-dir node-dir)
      (with-open [node (tu/->local-node {:node-dir node-dir})]
        (let [last-tx (case method
                        :docs (tpch/submit-docs! node scale-factor)
                        :dml (tpch/submit-dml! node scale-factor))]
          (tu/then-await-tx* last-tx node)
          (tu/finish-chunk! node)

          (binding [*node* node]
            (f)))))))

(defn is-equal?
  [expected actual]
  (t/is (= (count expected) (count actual)) (pr-str [expected actual]))
  (if (or (empty? expected) (empty? actual))
    (t/is (= expected actual))
    (->> (for [[expected-row actual-row] (map vector expected actual)]
           (let [row-cols (keys expected-row)]
             (boolean
               (and
                 (t/is (= (set row-cols) (set (keys actual-row))))
                 (->> row-cols
                      (mapv
                        (fn [col]
                          (let [x (col expected-row)
                                y (col actual-row)
                                msg (pr-str [col expected-row actual-row])]
                            (if (and (number? x) (number? y))
                              (let [epsilon 0.001
                                    diff (Math/abs (- (double x) (double y)))]
                                (t/is (<= diff epsilon) msg))
                              (t/is (= x y) msg)))))
                     (every? true?))))))
         (every? true?))))

(def ^:private ^:dynamic *qs*
  (set (range 1 23)))

(def results-sf-001
  (-> (io/resource "xtdb/tpch/results-sf-001.edn") slurp read-string))

(def results-sf-01
  (-> (io/resource "xtdb/tpch/results-sf-01.edn") slurp read-string))

(defn test-ra-query [n res]
  (when (contains? *qs* (inc n))
    (let [q @(nth tpch-ra/queries n)
          {::tpch-ra/keys [params table-args]} (meta q)]
      (tu/with-allocator
        (fn []
          (t/is (= res (tu/query-ra q {:node *node*, :params params, :table-args table-args}))
                (format "Q%02d" (inc n))))))))

(t/deftest test-001-ra
  (with-tpch-data {:method :docs, :scale-factor 0.001
                   :node-dir (util/->path "target/tpch-queries-ra-sf-001")}
    (fn []
      (dorun
       (map-indexed test-ra-query results-sf-001)))))

(comment
  (binding [*qs* #{11 17}]
    (t/run-test test-001-ra)))

(t/deftest ^:integration test-01-ra
  (with-tpch-data {:method :docs, :scale-factor 0.01
                   :node-dir (util/->path "target/tpch-queries-ra-sf-01")}
    (fn []
      (dorun
       (map-indexed test-ra-query results-sf-01)))))

(comment
  (binding [*qs* #{11 17}]
    (t/run-test test-01-ra)))

(def ^:private ^:dynamic *datalog-qs*
  ;; replace with *qs* once these are all expected to work
  (-> (set (range 1 23))
      (disj 21) ; TODO apply decorr
      (disj 7 20) ; TODO general fail
      (disj 13) ; TODO left-join
      (disj 15) ; TODO has a view, not sure how to represent this
      (disj 19 22) ; TODO cardinality-many literals
      ))

(defn test-datalog-query [n expected-res]
  (let [q (inc n)]
    (when (contains? *datalog-qs* q)
      (let [query @(nth tpch-datalog/queries n)
            {::tpch-datalog/keys [in-args]} (meta query)]
        (t/is (is-equal? expected-res (apply d/q *node* query in-args))
              (format "Q%02d" (inc n)))))))

(t/deftest test-001-datalog
  (with-tpch-data {:method :docs, :scale-factor 0.001
                   :node-dir (util/->path "target/tpch-queries-datalog-sf-001")}
    (fn []
      (dorun
       (map-indexed test-datalog-query results-sf-001)))))

(t/deftest ^:integration test-01-datalog
  (with-tpch-data {:method :docs, :scale-factor 0.01
                   :node-dir (util/->path "target/tpch-queries-datalog-sf-01")}
    (fn []
      (dorun
       (map-indexed test-datalog-query results-sf-01)))))

(comment
  (binding [*datalog-qs* #{2}]
    (t/run-test test-001-datalog))

  #_{:clj-kondo/ignore [:unresolved-namespace]}
  (binding [*node* dev/node
            *datalog-qs* #{1}]
    (t/run-test test-01-datalog)))

(defn slurp-sql-query [query-no]
  (slurp (io/resource (str "xtdb/sql/tpch/" (format "q%02d.sql" query-no)))))

;; TODO unable to decorr Q19, select stuck under this top/union exists thing

(t/deftest test-sql-plans
  (dotimes [n 22]
    (let [n (inc n)]
      (when (contains? *qs* n)
        (t/is (=plan-file
               (format "tpch/q%02d" n)
               (core.sql/compile-query (slurp-sql-query n)))
              (format "Q%02d" n))))))

(defn test-sql-query
  ([n res] (test-sql-query {:decorrelate? true} n res))
  ([opts n res]
   (let [q (inc n)]
     (when (contains? *qs* q)
       (t/is (is-equal? res (sql/q *node* (slurp-sql-query q) opts))
             (format "Q%02d" (inc n)))))))

(t/deftest test-001-sql
  (with-tpch-data {:method :dml, :scale-factor 0.001
                   :node-dir (util/->path "target/tpch-queries-sql-sf-001")}
    (fn []
      (dorun
       (map-indexed test-sql-query results-sf-001)))))

(t/deftest ^:integration test-01-sql
  (with-tpch-data {:method :dml, :scale-factor 0.01
                   :node-dir (util/->path "target/tpch-queries-sql-sf-01")}
    (fn []
      (dorun
       (map-indexed test-sql-query results-sf-01)))))

(comment
  (binding [*qs* #{1}]
    (t/run-test test-001-sql)))
