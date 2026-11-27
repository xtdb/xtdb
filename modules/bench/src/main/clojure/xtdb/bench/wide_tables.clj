(ns xtdb.bench.wide-tables
  (:require [clojure.tools.logging :as log]
            [taoensso.tufte :as tufte :refer [p]]
            [xtdb.api :as xt]
            [xtdb.bench :as b]
            [xtdb.compactor :as c]
            [xtdb.error :as err]
            [xtdb.test-util :as tu])
  (:import (java.time Duration)
           (java.util ArrayList Collections List Random)))

(defn- ->tables [{:keys [n-big-tables n-big-rows n-small-tables n-small-rows]}]
  (concat (for [idx (range n-big-tables)]
            {:table (keyword (str "big" idx)), :n-rows n-big-rows, :big? true})
          (for [idx (range n-small-tables)]
            {:table (keyword (str "small" idx)), :n-rows n-small-rows, :big? false})))

(defn- ->doc [^Random random n-columns id]
  (into {:xt/id id}
        (for [col-idx (range n-columns)]
          (if (even? col-idx)
            [(keyword (str "s" col-idx)) (str "s" col-idx "-" id)]
            [(keyword (str "n" col-idx)) (.nextLong random)]))))

(defn- load-tables! [node ^Random random tables {:keys [n-columns]}]
  (doseq [batch-start (range 0 (transduce (map :n-rows) max 0 tables) 1000)
          {:keys [table n-rows]} tables
          :when (< batch-start n-rows)]
    (xt/submit-tx node [(into [:put-docs table]
                              (map #(->doc random n-columns %))
                              (range batch-start (min n-rows (+ batch-start 1000))))])))

(defn- point-lookup [conn table id]
  (xt/q conn [(str "SELECT * FROM " (name table) " WHERE _id = ?") id]))

(defn- string-equality [conn table id]
  (xt/q conn [(str "SELECT _id FROM " (name table) " WHERE s0 = ?") (str "s0-" id)]))

(defn- query-round [conn ^Random random tables]
  (let [tables (doto (ArrayList. ^List tables) (Collections/shuffle random))]
    (doseq [{:keys [table n-rows big?]} tables
            :let [id (long (.nextInt random (int n-rows)))]]
      (if big?
        (do
          (p :point-lookup (point-lookup conn table id))
          (p :string-equality (string-equality conn table id)))
        (do
          (point-lookup conn table id)
          (string-equality conn table id))))))

(defn- check-queries-match! [conn tables]
  (doseq [{:keys [table]} tables
          [query-id f] [[:point-lookup point-lookup] [:string-equality string-equality]]
          :let [n (count (f conn table 0))]]
    (when-not (= 1 n)
      (throw (err/fault ::unexpected-row-count
                        (format "%s on %s matched %d rows, expected 1" query-id (name table) n)
                        {:query query-id, :table table, :actual n})))))

(defn- profile [node random tables rounds]
  (with-open [conn (.build (.createConnectionBuilder ^xtdb.api.DataSource node))]
    (check-queries-match! conn tables)
    (query-round conn random tables)
    (let [[_ pstats] (tufte/profiled {} (dotimes [_ rounds] (query-round conn random tables)))]
      (println (tufte/format-pstats pstats))
      (for [[id {:keys [n min p50 p90 p95 p99 max mean]}] (:stats @pstats)]
        {:id id :n n :min min :p50 p50 :p90 p90 :p95 p95 :p99 p99 :max max :mean mean}))))

(defmethod b/cli-flags :wide-tables [_]
  [[nil "--big-tables N_BIG_TABLES" "Number of profiled tables"
    :id :n-big-tables
    :parse-fn parse-long
    :default 10]
   [nil "--big-rows N_BIG_ROWS" "Rows per profiled table"
    :id :n-big-rows
    :parse-fn parse-long
    :default 300000]
   [nil "--small-tables N_SMALL_TABLES" "Number of unprofiled tables, queried as often as the profiled ones"
    :id :n-small-tables
    :parse-fn parse-long
    :default 300]
   [nil "--small-rows N_SMALL_ROWS" "Rows per unprofiled table"
    :id :n-small-rows
    :parse-fn parse-long
    :default 1000]
   [nil "--columns N_COLUMNS" "Columns per table, alternating strings and longs"
    :id :n-columns
    :parse-fn parse-long
    :default 50]
   [nil "--rounds N_ROUNDS" "Profiled passes over every table"
    :id :n-rounds
    :parse-fn parse-long
    :default 10]
   ["-h" "--help"]])

(defmethod b/->benchmark :wide-tables [_ {:keys [n-big-tables n-big-rows n-small-tables n-small-rows n-columns n-rounds seed no-load?]
                                          :or {n-big-tables 10 n-big-rows 300000 n-small-tables 300 n-small-rows 1000
                                               n-columns 50 n-rounds 10 seed 0}}]
  (let [opts {:n-big-tables n-big-tables :n-big-rows n-big-rows
              :n-small-tables n-small-tables :n-small-rows n-small-rows
              :n-columns n-columns}
        tables (->tables opts)]
    (log/info (assoc opts :n-rounds n-rounds :seed seed :no-load? no-load?))

    {:title "Wide Tables"
     :benchmark-type :wide-tables
     :seed seed
     :parameters (assoc opts :n-rounds n-rounds :seed seed :no-load? no-load?)
     :tasks [(when-not no-load?
               {:t :do
                :stage :ingest
                :tasks [{:t :call
                         :stage :submit-docs
                         :f (fn [{:keys [node random]}] (load-tables! node random tables opts))}
                        {:t :call
                         :stage :await-transactions
                         :f (fn [{:keys [node]}] (b/sync-node node (Duration/ofMinutes 60)))}
                        {:t :call
                         :stage :flush-block
                         :f (fn [{:keys [node]}] (tu/flush-block! node))}]})

             {:t :call
              :stage :compact
              :f (fn [{:keys [node]}] (c/compact-all! node nil))}

             {:t :call
              :stage :profile-queries
              :f (fn [{:keys [node random] :as worker}]
                   (b/log-report worker {:profile (vec (profile node random tables n-rounds))}))}]}))
