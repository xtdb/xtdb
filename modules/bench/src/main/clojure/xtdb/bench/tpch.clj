(ns xtdb.bench.tpch
  (:require [clojure.tools.logging :as log]
            [xtdb.bench :as b]
            [xtdb.datasets.tpch :as tpch]
            [xtdb.datasets.tpch.ra :as tpch-ra]
            [xtdb.test-util :as tu])
  (:import (java.time Duration)))

(def qs (-> (into #{} (range (count tpch-ra/queries)))
            (disj 20)))

(defn query-tpch [stage-name i]
  (let [q (nth tpch-ra/queries i)
        stage-name (keyword (str (name stage-name) "-" (:name (meta q))))
        q @q
        {::tpch-ra/keys [args]} (meta q)]
    {:t :do, :stage stage-name
     :tasks [{:t :call
              :f (fn [{:keys [node]}]
                   (try
                     (count (tu/query-ra q {:node node, :args args}))
                     (catch Exception e
                       (.printStackTrace e))))}]}))

(defn queries-stage [stage-name]
  {:t :do, :stage stage-name
   :tasks (vec (concat [{:t :call :f (fn [{:keys [!state]}]
                                       (swap! !state assoc :bf-stats-start (System/currentTimeMillis)))}]

                       (for [i (range (count tpch-ra/queries))
                             :when (contains? qs i)]
                         (query-tpch stage-name i))

                       [{:t :call :f (fn [{:keys [!state] :as worker}]
                                       (let [report-name (str (name stage-name) " buffer pool stats")
                                             start-ms (get @!state :bf-stats-start)
                                             end-ms (System/currentTimeMillis)]
                                         (b/log-report worker {:stage report-name
                                                               :time-taken-ms (- end-ms start-ms)})))}]))})

(defmethod b/cli-flags :tpch [_]
  [["-s" "--scale-factor SCALE_FACTOR" "TPC-H scale factor to use"
    :parse-fn parse-double
    :default 0.01]

   ["-h" "--help"]])

(defmethod b/->benchmark :tpch [_ {:keys [scale-factor seed no-load?],
                                   :or {scale-factor 0.01, seed 0}}]
  (log/info {:scale-factor scale-factor :seed seed :no-load? no-load?})

  {:title "TPC-H (OLAP)", :seed seed
   :->state #(do {:!state (atom {})})
   :tasks [{:t :do
            :stage :ingest
            :tasks (concat (when-not no-load?
                             [{:t :call
                               :stage :submit-docs
                               :f (fn [{:keys [node]}] (tpch/submit-docs! node scale-factor))}])

                           [{:t :do
                             :stage :sync
                             :tasks [{:t :call :f (fn [{:keys [node]}] (b/sync-node node (Duration/ofHours 5)))}]}
                            {:t :do
                             :stage :finish-block
                             :tasks [{:t :call :f (fn [{:keys [node]}] (b/finish-block! node))}]}
                            {:t :do
                             :stage :compact
                             :tasks [{:t :call :f (fn [{:keys [node]}] (b/compact! node))}]}])}

           (queries-stage :cold-queries)

           (queries-stage :hot-queries)]})
