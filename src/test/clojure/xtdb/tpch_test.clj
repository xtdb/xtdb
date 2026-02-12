(ns xtdb.tpch-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.compactor :as c]
            [xtdb.datasets.tpch :as tpch]
            [xtdb.datasets.tpch.ra :as tpch-ra]
            [xtdb.datasets.tpch.xtql :as tpch-xtql]
            [xtdb.log :as xt-log]
            [xtdb.operator.join :as join]
            [xtdb.sql :as sql]
            xtdb.sql-test
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import (java.nio.file Path)
           [java.time Duration]))

(t/use-fixtures :each tu/with-allocator)

(def ^:dynamic *node* nil)

;; (slurp (io/resource (format "io/airlift/tpch/queries/q%d.sql" 1)))

(defn with-tpch-data [{:keys [method ^Path node-dir scale-factor]} f]
  (if *node*
    (f)

    (do
      (util/delete-dir node-dir)
      (with-open [node (tu/->local-node {:node-dir node-dir})]
        (case method
          :rels (tpch/submit-rels! node scale-factor)
          :docs (tpch/submit-docs! node scale-factor)
          :dml (tpch/submit-dml! node scale-factor))

        (xt-log/sync-node node)
        (tu/flush-block! node)

        (c/compact-all! node (Duration/ofMinutes 5))

        (binding [*node* node]
          (f))))))

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
          {::tpch-ra/keys [args]} (meta q)]
      (tu/with-allocator
        (fn []
          (t/is (is-equal? res (tu/query-ra q {:node *node*, :args args, :key-fn :snake-case-keyword}))
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
  (time
    (with-tpch-data {:method :rels, :scale-factor 0.01
                     :node-dir (util/->path "target/tpch-queries-ra-sf-01")}
      (fn []
        (dorun
          (map-indexed test-ra-query results-sf-01))))))

(t/deftest ^:integration test-01-ra-dbj
  (with-tpch-data {:method :docs, :scale-factor 0.01
                   :node-dir (util/->path "target/tpch-queries-ra-sf-01")}
    (fn []
      (binding [join/*disk-join-threshold-rows* 1000]
        (dorun
         (map-indexed test-ra-query results-sf-01))))))

(comment
  (binding [*qs* #{11 17}]
    (t/run-test test-01-ra)))

(def ^:private ^:dynamic *xtql-qs*
  ;; replace with *qs* once these are all expected to work
  (-> (set (range 1 23))
      (disj 1 3 5 6 7 8 9 10 11 12 14) ; TODO nested aggs
      (disj 2 20) ; TODO #3022
      (disj 15) ; TODO `letfn`
      (disj 21) ; TODO general fail
      (disj 12 16 18 19 22) ; TODO `in?`
      ))

(defn test-xtql-query [n expected-res]
  (let [q (inc n)]
    (when (contains? *xtql-qs* q)
      (let [query+args @(nth tpch-xtql/queries n)]
        (t/is (is-equal? expected-res (xt/q *node* query+args {:key-fn :snake-case-keyword}))
              (format "Q%02d" (inc n)))))))

(t/deftest test-001-xtql
  (with-tpch-data {:method :docs, :scale-factor 0.001
                   :node-dir (util/->path "target/tpch-queries-xtql-sf-001")}
    (fn []
      (dorun
       (map-indexed test-xtql-query results-sf-001)))))

(t/deftest ^:integration test-01-xtql
  (with-tpch-data {:method :docs, :scale-factor 0.01
                   :node-dir (util/->path "target/tpch-queries-xtql-sf-01")}
    (fn []
      (dorun
       (map-indexed test-xtql-query results-sf-01)))))

(comment
  (binding [*xtql-qs* #{2}]
    (t/run-test test-001-xtql))

  #_{:clj-kondo/ignore [:unresolved-namespace]}
  (binding [*node* dev/node
            *xtql-qs* #{1}]
    (t/run-test test-01-xtql)))

(defn slurp-sql-query [query-no]
  (slurp (io/resource (str "xtdb/sql/tpch/" (format "q%02d.sql" query-no)))))

(def tpch-table-info
  {#xt/table customer #{"_id" "c_name" "c_address" "c_nationkey" "c_phone" "c_acctbal" "c_mktsegment" "c_comment"}
   #xt/table lineitem #{"_id" "l_orderkey" "l_partkey" "l_suppkey" "l_linenumber" "l_quantity" "l_extendedprice" "l_discount" "l_tax" "l_returnflag" "l_linestatus" "l_shipdate" "l_commitdate" "l_receiptdate" "l_shipinstruct" "l_shipmode" "l_comment"}
   #xt/table nation #{"_id" "n_name" "n_regionkey" "n_comment"}
   #xt/table orders #{"_id" "o_custkey" "o_orderstatus" "o_totalprice" "o_orderdate" "o_orderpriority" "o_clerk" "o_shippriority" "o_comment"}
   #xt/table part #{"_id" "p_name" "p_mfgr" "p_brand" "p_type" "p_size" "p_container" "p_retailprice" "p_comment"}
   #xt/table partsupp #{"_id" "ps_partkey" "ps_suppkey" "ps_availqty" "ps_supplycost" "ps_comment"}
   #xt/table region #{"_id" "r_name" "r_comment"}
   #xt/table supplier #{"_id" "s_name" "s_address" "s_nationkey" "s_phone" "s_acctbal" "s_comment"}})

(t/deftest test-sql-plans
  (dotimes [n 22]
    (let [n (inc n)]
      (when (contains? *qs* n)
        (t/is (=plan-file (format "tpch/q%02d" n)
                          (sql/plan (slurp-sql-query n)
                                    {:table-info tpch-table-info}))
              (format "Q%02d" n))))))

(defn test-sql-query
  ([n res] (test-sql-query {:decorrelate? true, :key-fn :snake-case-keyword} n res))
  ([opts n res]
   (let [q (inc n)]
     (when (contains? *qs* q)
       (t/is (is-equal? res (xt/q *node* (slurp-sql-query q) opts))
             (format "Q%02d" (inc n)))))))

(t/deftest test-001-sql
  (with-tpch-data {:method :dml, :scale-factor 0.001
                   :node-dir (util/->path "target/tpch-queries-sql-sf-001")}
    (fn []
      (dorun
       (map-indexed test-sql-query results-sf-001)))))

(t/deftest test-001-sql-explain
  (with-tpch-data {:method :dml, :scale-factor 0.001
                   :node-dir (util/->path "target/tpch-queries-sql-sf-001")}
    (fn []
      (dotimes [n 22]
        (let [q (inc n)]
          (when (contains? *qs* q)
            (t/is (xt/q *node* (str "EXPLAIN " (slurp-sql-query q)))
                  (format "EXPLAIN Q%02d" (inc n)))

            (t/is (xt/q *node* (str "EXPLAIN ANALYZE " (slurp-sql-query q)))
                  (format "EXPLAIN ANALYZE Q%02d" (inc n)))))))))

(t/deftest ^:integration test-01-sql
  (with-tpch-data {:method :dml, :scale-factor 0.01
                   :node-dir (util/->path "target/tpch-queries-sql-sf-01")}
    (fn []
      (dorun
       (map-indexed test-sql-query results-sf-01)))))

(comment
  (binding [*qs* #{1}]
    (t/run-test test-001-sql)))
