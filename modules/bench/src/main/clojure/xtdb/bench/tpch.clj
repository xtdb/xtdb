(ns xtdb.bench.tpch
  (:require [clojure.java.io :as io]
            [clojure.pprint :as pp]
            [clojure.string :as str]
            [clojure.test :as t]
            [clojure.tools.logging :as log]
            [xtdb.api :as xt]
            [xtdb.bench :as b]
            [xtdb.datasets.tpch :as tpch]
            [xtdb.datasets.tpch.ra :as tpch-ra]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import (java.time Duration)
           (java.util.concurrent TimeUnit TimeoutException)
           (jdk.jfr Configuration Recording)))

;; TEMPORARY profiling patch - runs the SQL TPC-H queries through the planner as well as the RA ones,
;; captures EXPLAIN / EXPLAIN ANALYZE and optional per-query JFR recordings.
;; env: TPCH_QS=1,2,3  TPCH_MODE=sql|ra|both  TPCH_OUT=dir  TPCH_JFR=true  TPCH_QUERY_TIMEOUT_MINS=20

(def ^:dynamic *qs*
  (if-let [qs (System/getenv "TPCH_QS")]
    (into #{} (map parse-long) (str/split qs #","))
    (set (range 1 23))))

(def out-dir (doto (io/file (or (System/getenv "TPCH_OUT") "/tmp/tpch-out")) (.mkdirs)))
(def mode (keyword (or (System/getenv "TPCH_MODE") "both")))
(def jfr? (= "true" (System/getenv "TPCH_JFR")))
(def query-timeout-mins (parse-long (or (System/getenv "TPCH_QUERY_TIMEOUT_MINS") "20")))

(defn- with-timeout [f]
  (let [fut (future (f))]
    (try
      (.get fut query-timeout-mins TimeUnit/MINUTES)
      (catch TimeoutException _
        (future-cancel fut)
        (throw (ex-info (str "query timed out after " query-timeout-mins " mins") {})))
      (catch java.util.concurrent.ExecutionException e
        (throw (.getCause e))))))

(defn- with-jfr [rec-name f]
  (if jfr?
    (let [rec (doto (Recording. (Configuration/getConfiguration "profile")) (.start))]
      (try
        (f)
        (finally
          (.stop rec)
          (.dump rec (.toPath (io/file out-dir (str rec-name ".jfr"))))
          (.close rec))))
    (f)))

(defn- fmt-ea [rows]
  (with-out-str
    (doseq [row rows
            :let [g (fn [k] (or (get row k) (get row (keyword (str/replace (name k) "_" "-")))))]]
      (println (format "%-44s %12s  ttfp=%-12s pages=%-6s rows=%-10s %s %s"
                       (str (g :depth) " " (name (g :op)))
                       (str (g :total_time)) (str (g :time_to_first_page)) (g :page_count) (g :row_count)
                       (or (some-> (g :attributes) pr-str) "")
                       (or (some-> (g :pushdowns) pr-str) ""))))))

(defn- timed [f]
  (let [t0 (System/nanoTime)
        res (f)]
    [res (/ (- (System/nanoTime) t0) 1e6)]))

(defn- sql-q [n] (slurp (io/resource (format "xtdb/sql/tpch/q%02d.sql" n))))

(defn query-sql [stage-name n {:keys [analyze?]}]
  (let [q (sql-q n)
        stage (keyword (format "%s-q%02d" (name stage-name) n))]
    {:t :call, :stage stage
     :f (fn [{:keys [node]}]
          (try
            (spit (io/file out-dir (format "sql-q%02d.plan.edn" n))
                  (with-out-str (pp/pprint (xt/q node (str "EXPLAIN " q) {:key-fn :snake-case-keyword}))))
            (let [[cnt ms] (with-timeout #(with-jfr (name stage) (fn [] (timed (fn [] (count (xt/q node q {:key-fn :snake-case-keyword})))))))]
              (log/infof "TPCHRESULT %s q%02d rows=%d ms=%.1f" (name stage-name) n cnt ms)
              (when analyze?
                (let [[ea ea-ms] (with-timeout #(timed (fn [] (xt/q node (str "EXPLAIN ANALYZE " q) {:key-fn :snake-case-keyword}))))]
                  (log/infof "TPCHRESULT %s-analyze q%02d ms=%.1f" (name stage-name) n ea-ms)
                  (spit (io/file out-dir (format "sql-q%02d.analyze.txt" n)) (fmt-ea ea)))))
            (catch Throwable e
              (log/errorf e "TPCHRESULT %s q%02d FAILED %s" (name stage-name) n (.getMessage e)))))}))

(defn query-ra [stage-name n {:keys [analyze?]}]
  (let [q @(nth tpch-ra/queries (dec n))
        {::tpch-ra/keys [args]} (meta q)
        stage (keyword (format "%s-q%02d" (name stage-name) n))]
    {:t :call, :stage stage
     :f (fn [{:keys [node]}]
          (try
            (let [[cnt ms] (with-timeout #(with-jfr (name stage) (fn [] (timed (fn [] (count (tu/query-ra q {:node node, :args args})))))))]
              (log/infof "TPCHRESULT %s q%02d rows=%d ms=%.1f" (name stage-name) n cnt ms)
              (when analyze?
                (let [[ea ea-ms] (with-timeout #(timed (fn [] (tu/query-ra q {:node node, :args args, :explain-analyze? true}))))]
                  (log/infof "TPCHRESULT %s-analyze q%02d ms=%.1f" (name stage-name) n ea-ms)
                  (spit (io/file out-dir (format "ra-q%02d.analyze.txt" n)) (fmt-ea ea)))))
            (catch Throwable e
              (log/errorf e "TPCHRESULT %s q%02d FAILED %s" (name stage-name) n (.getMessage e)))))}))

(defn queries-stage [stage-name query-fn opts]
  {:t :do, :stage stage-name
   :tasks (vec (for [n (range 1 23)
                     :when (contains? *qs* n)]
                 (query-fn stage-name n opts)))})

(defmethod b/cli-flags :tpch [_]
  [["-s" "--scale-factor SCALE_FACTOR" "TPC-H scale factor to use"
    :parse-fn parse-double
    :default 0.01]

   ["-h" "--help"]])

(defmethod b/->benchmark :tpch [_ {:keys [scale-factor seed no-load?],
                                   :or {scale-factor 0.01, seed 0}}]
  (log/info {:scale-factor scale-factor :seed seed :no-load? no-load? :mode mode :qs *qs* :jfr? jfr? :out-dir (str out-dir)})

  {:title "TPC-H (OLAP)"
   :benchmark-type :tpch
   :seed seed
   :parameters {:scale-factor scale-factor :seed seed :no-load? no-load?}
   :->state #(do {:!state (atom {})})
   :tasks (vec
           (concat
            [{:t :do
              :stage :ingest
              :tasks (when-not no-load?
                       [{:t :call, :stage :submit-rels
                         :f (fn [{:keys [node]}] (tpch/submit-rels! node scale-factor))}

                        {:t :call, :stage :sync,
                         :f (fn [{:keys [node]}] (b/sync-node node (Duration/ofHours 5)))}

                        {:t :call, :stage :finish-block
                         :f (fn [{:keys [node]}] (b/flush-block! node))}

                        {:t :call, :stage :compact
                         :f (fn [{:keys [node]}] (b/compact! node))}])}]

            (when (#{:sql :both} mode)
              [(queries-stage :sql-cold query-sql {:analyze? false})
               (queries-stage :sql-hot query-sql {:analyze? true})])

            (when (#{:ra :both} mode)
              [(queries-stage :ra-cold query-ra {:analyze? false})
               (queries-stage :ra-hot query-ra {:analyze? true})])))})

(t/deftest ^:bench tpch-benchmark
  (binding [*qs* #{1}]
    (-> (b/->benchmark :tpch
                       {:scale-factor 1
                        :no-load? true
                        :seed 42})
        (b/run-benchmark {:node-dir (util/->path (str (System/getProperty "user.home") "/tmp/tpch-1"))
                          :no-load? true
                          }))))
