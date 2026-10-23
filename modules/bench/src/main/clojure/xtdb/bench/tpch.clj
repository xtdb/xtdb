(ns xtdb.bench.tpch
  (:require [clojure.tools.logging :as log]
            [clojure.walk :as walk]
            [next.jdbc :as jdbc]
            [xtdb.api :as xt]
            [xtdb.bench :as b]
            [xtdb.db-catalog :as db]
            [xtdb.datasets.tpch :as tpch]
            [xtdb.datasets.tpch.ra :as tpch-ra]
            [xtdb.error :as err]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import (java.time Duration Instant)
           (xtdb.api.storage Storage$LocalStorageFactory)))

(def ^:dynamic *qs*
  (-> (into #{} (range 1 (inc (count tpch-ra/queries))))
      (disj 21)))

(defn query-tpch [stage-name query-language db-name scale-factor expected i]
  (let [q (nth tpch-ra/queries i)
        stage-name (keyword (str (name stage-name) "-" (:name (meta q))))
        run-query (case query-language
                    :ra (let [args (tpch-ra/args @q scale-factor)
                              q (walk/postwalk #(cond-> % (and (map? %) (:db-name %)) (assoc :db-name db-name)) @q)]
                          #(tu/query-ra q {:node %, :args args, :key-fn :snake-case-keyword}))
                    :sql (let [query+args (tpch/sql-query (inc i) scale-factor)]
                           #(xt/q % query+args {:key-fn :snake-case-keyword, :database db-name})))]
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

(defn queries-stage [stage-name query-language db-name scale-factor]
  (let [results (tpch/results scale-factor)]
    {:t :do, :stage stage-name
     :tasks (vec (for [i (range (count tpch-ra/queries))
                       :when (contains? *qs* (inc i))]
                   (query-tpch stage-name query-language db-name scale-factor (get results i) i)))}))

(defn- attach-tpch-source!
  [node scale-factor threads]
  (let [^Storage$LocalStorageFactory primary-storage (.getStorage (.getConfig (db/primary-db node)))
        tpch-dir (.resolveSibling (.getPath primary-storage) "tpch")
        deadline (.plus (Instant/now) (Duration/ofHours 5))]
    (jdbc/execute! node [(format "ATTACH DATABASE tpch WITH $$
log: !Local
  path: '%s'
storage: !Local
  path: '%s'
externalSource: !Tpch
  scaleFactor: %s
  threads: %d
$$" (.resolve tpch-dir "log") (.resolve tpch-dir "objects") scale-factor threads)])

    (let [db (.databaseOrNull (db/<-node node) "tpch")]
      (while (nil? (.getExternalSourceToken (.getWatchers db)))
        (when-let [e (.getIngestionError db)] (throw e))
        (when (.isAfter (Instant/now) deadline)
          (throw (err/fault ::tpch-load-timeout "TPC-H source load didn't complete in time")))
        (Thread/sleep 10)))))

(defmethod b/cli-flags :tpch [_]
  [["-s" "--scale-factor SCALE_FACTOR" "TPC-H scale factor to use"
    :parse-fn parse-double
    :default 0.01]

   [nil "--query-language LANGUAGE" "ra: the hand-written plans in xtdb.datasets.tpch.ra; sql: the SQL queries, through the planner"
    :parse-fn keyword
    :default :ra
    :validate [#{:ra :sql} "query language must be one of ra, sql"]]

   [nil "--ingest-via INGEST" "rels: submit-rels! into the primary database; source: attach a `tpch` database with a `!Tpch` external source"
    :parse-fn keyword
    :default :rels
    :validate [#{:rels :source} "ingest-via must be one of rels, source"]]

   [nil "--threads THREADS" "generator threads, for --ingest-via source"
    :parse-fn parse-long
    :default (.availableProcessors (Runtime/getRuntime))]

   ["-h" "--help"]])

(defmethod b/->benchmark :tpch [_ {:keys [scale-factor query-language ingest-via threads seed no-load?],
                                   :or {scale-factor 0.01, query-language :ra, ingest-via :rels, seed 0
                                        threads (.availableProcessors (Runtime/getRuntime))}}]
  (log/info {:scale-factor scale-factor :query-language query-language :ingest-via ingest-via :threads threads
             :seed seed :no-load? no-load?})

  {:title (case query-language
            :ra "TPC-H (OLAP)"
            :sql "TPC-H (OLAP, SQL)")
   :benchmark-type :tpch
   :seed seed
   :parameters {:scale-factor scale-factor :query-language query-language :ingest-via ingest-via :threads threads
                :seed seed :no-load? no-load?}
   :->state #(do {:!state (atom {})})
   :tasks (let [db-name (case ingest-via :rels "xtdb", :source "tpch")]
            [{:t :do
              :stage :ingest
              :tasks (when-not no-load?
                       (concat
                        (case ingest-via
                          :rels [{:t :call, :stage :submit-rels
                                  :f (fn [{:keys [node]}] (tpch/submit-rels! node scale-factor))}

                                 {:t :call, :stage :sync,
                                  :f (fn [{:keys [node]}] (b/sync-node node (Duration/ofHours 5)))}]

                          :source [{:t :call, :stage :attach-source
                                    :f (fn [{:keys [node]}] (attach-tpch-source! node scale-factor threads))}])

                        [{:t :call, :stage :finish-block
                          :f (fn [{:keys [node]}] (b/flush-block! node))}

                         {:t :call, :stage :compact
                          :f (fn [{:keys [node]}]
                               (-> (.getCompactor (.databaseOrNull (db/<-node node) db-name))
                                   (.compactAllSync (Duration/ofMinutes 10))))}]))}

             (queries-stage :cold-queries query-language db-name scale-factor)

             (queries-stage :hot-queries query-language db-name scale-factor)])})

(comment
  (binding [*qs* #{1}]
    (-> (b/->benchmark :tpch
                       {:scale-factor 1
                        :no-load? true
                        :seed 42})
        (b/run-benchmark {:node-dir (util/->path (str (System/getProperty "user.home") "/tmp/tpch-1"))
                          :no-load? true
                          }))))
