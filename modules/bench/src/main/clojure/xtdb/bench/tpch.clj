(ns xtdb.bench.tpch
  (:require [clojure.test :as t]
            [clojure.tools.logging :as log]
            [xtdb.bench :as b]
            [xtdb.datasets.tpch :as tpch]
            [xtdb.datasets.tpch.ra :as tpch-ra]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import (java.time Duration)))

(def qs (-> (into #{} (range (count tpch-ra/queries)))
            (disj 20)))

(defn query-tpch [stage-name i]
  (let [q (nth tpch-ra/queries i)
        stage-name (keyword (str (name stage-name) "-" (:name (meta q))))
        q @q
        {::tpch-ra/keys [args]} (meta q)]
    {:t :call, :stage stage-name
     :f (fn [{:keys [node]}]
          (try
            (count (tu/query-ra q {:node node, :args args}))
            (catch Exception e
              (.printStackTrace e)
              (throw e))))}))

(defn queries-stage [stage-name]
  {:t :do, :stage stage-name
   :tasks (vec (for [i (range (count tpch-ra/queries))
                     :when (contains? qs i)]
                 (query-tpch stage-name i)))})

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
            :tasks (when-not no-load?
                     [{:t :call, :stage :submit-docs
                       :f (fn [{:keys [node]}] (tpch/submit-docs! node scale-factor))}
                      
                      {:t :call, :stage :sync,
                       :f (fn [{:keys [node]}] (b/sync-node node (Duration/ofHours 5)))}

                      {:t :call, :stage :finish-block
                       :f (fn [{:keys [node]}] (b/finish-block! node))}

                      {:t :call, :stage :compact
                       :f (fn [{:keys [node]}] (b/compact! node))}])}

           (queries-stage :cold-queries)

           (queries-stage :hot-queries)]})

(t/deftest ^:bench tpch-benchmark
  (-> (b/->benchmark :tpch
                     {:scale-factor 1
                      ;; :no-load? true
                      :seed 42})
      (b/run-benchmark {:node-dir (util/->path (str (System/getProperty "user.home") "/tmp/tpch-1"))
                        ;; :no-load? true
                        })))
