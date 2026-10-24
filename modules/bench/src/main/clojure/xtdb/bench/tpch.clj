(ns xtdb.bench.tpch
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [xtdb.api :as xt]
            [xtdb.bench :as b]
            [xtdb.datasets.tpch :as tpch]
            [xtdb.datasets.tpch.ra :as tpch-ra]
            [xtdb.error :as err]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import (java.time Duration)))

(def ^:dynamic *qs*
  (into #{} (map parse-long) (str/split (System/getProperty "xtdb.bench.qs" "1,6") #",")))

(def hot-reps (parse-long (System/getProperty "xtdb.bench.hotReps" "50")))

(defn query-tpch [stage-name query-language scale-factor expected i]
  (let [q (nth tpch-ra/queries i)
        stage-name (keyword (str (name stage-name) "-" (:name (meta q))))
        run-query (case query-language
                    :ra (let [q @q
                              args (tpch-ra/args q scale-factor)]
                          #(tu/query-ra q {:node %, :args args, :key-fn :snake-case-keyword}))
                    :sql (let [query+args (tpch/sql-query (inc i) scale-factor)]
                           #(xt/q % query+args {:key-fn :snake-case-keyword})))]
    (cond-> {:t :call, :stage stage-name
             :f (fn [{:keys [node]}]
                  (try
                    (run-query node)
                    (catch Exception e
                      (.printStackTrace e)
                      (throw e))))}
      expected (assoc :verify (fn [rows]
                                (when-let [mismatch (tpch/result-mismatch expected rows)]
                                  (throw (err/fault ::wrong-result (format "Q%02d doesn't match its pinned result: %s" (inc i) (pr-str mismatch)) mismatch))))))))

(defn queries-stage [stage-name query-language scale-factor]
  (let [results (tpch/results scale-factor)]
    {:t :do, :stage stage-name
     :tasks (vec (for [i (range (count tpch-ra/queries))
                       :when (contains? *qs* (inc i))]
                   (query-tpch stage-name query-language scale-factor (get results i) i)))}))

(defmethod b/cli-flags :tpch [_]
  [["-s" "--scale-factor SCALE_FACTOR" "TPC-H scale factor to use"
    :parse-fn parse-double
    :default 0.01]

   [nil "--query-language LANGUAGE" "ra: the hand-written plans in xtdb.datasets.tpch.ra; sql: the SQL queries, through the planner"
    :parse-fn keyword
    :default :ra
    :validate [#{:ra :sql} "query language must be one of ra, sql"]]

   ["-h" "--help"]])

(defmethod b/->benchmark :tpch [_ {:keys [scale-factor query-language seed no-load?],
                                   :or {scale-factor 0.01, query-language :ra, seed 0}}]
  (log/info {:scale-factor scale-factor :query-language query-language :seed seed :no-load? no-load?})

  {:title (case query-language
            :ra "TPC-H (OLAP)"
            :sql "TPC-H (OLAP, SQL)")
   :benchmark-type :tpch
   :seed seed
   :parameters {:scale-factor scale-factor :query-language query-language :seed seed :no-load? no-load?}
   :->state #(do {:!state (atom {})})
   :tasks (filterv some?
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
                       :f (fn [{:keys [node]}] (b/compact! node))}])}

           ;; #6174: a restarted node plans from the snapshot taken before compaction's TriesAdded replay; one tx refreshes it
           (when (and no-load? (not (Boolean/getBoolean "xtdb.bench.staleSnapshot")))
             {:t :call, :stage :refresh-snapshot
              :f (fn [{:keys [node]}] (xt/execute-tx node [[:put-docs :bench_refresh {:xt/id 1}]]))})

           (queries-stage :cold-queries query-language scale-factor)

           {:t :do, :stage :hot-repeats
            :tasks (vec (for [rep (range hot-reps)]
                          (queries-stage (keyword (str "hot-queries-" rep)) query-language scale-factor)))}])})

(comment
  (binding [*qs* #{1}]
    (-> (b/->benchmark :tpch
                       {:scale-factor 1
                        :no-load? true
                        :seed 42})
        (b/run-benchmark {:node-dir (util/->path (str (System/getProperty "user.home") "/tmp/tpch-1"))
                          :no-load? true
                          }))))
