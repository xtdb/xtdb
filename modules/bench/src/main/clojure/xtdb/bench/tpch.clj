(ns xtdb.bench.tpch
  (:require [clojure.tools.logging :as log]
            [xtdb.bench :as b]
            [xtdb.bench.xtdb2 :as bxt]
            [xtdb.datasets.tpch :as tpch]
            [xtdb.datasets.tpch.ra :as tpch-ra]
            [xtdb.test-util :as tu])
  (:import (java.time Duration)
           (java.util AbstractMap)))

(defn query-tpch [stage-name i]
  (let [q (nth tpch-ra/queries i)
        stage-name (keyword (str (name stage-name) "-" (:name (meta q))))
        q @q
        {::tpch-ra/keys [args]} (meta q)]
    {:t :do
     :stage stage-name
     :tasks [{:t :call
              :f (fn [{:keys [sut]}]
                   (try
                     (count (tu/query-ra q {:node sut, :args args}))
                     (catch Exception e
                       (.printStackTrace e))))}]}))


(defn queries-stage [stage-name]
  {:t :do
   :stage stage-name
   :tasks (vec (concat [{:t :call :f (fn [{:keys [^AbstractMap custom-state]}]
                                       (.put custom-state :bf-stats-start (System/currentTimeMillis)))}]

                       (for [i (range (count tpch-ra/queries))]
                         (query-tpch stage-name i))

                       [{:t :call :f (fn [{:keys [custom-state] :as worker}]
                                       (let [report-name (str (name stage-name) " buffer pool stats")
                                             start-ms (get custom-state :bf-stats-start)
                                             end-ms (System/currentTimeMillis)]
                                         (b/log-report worker {:stage report-name
                                                               :time-taken-ms (- end-ms start-ms)})))}]))})

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn benchmark [{:keys [scale-factor seed load-phase], :or {scale-factor 0.01, seed 0, load-phase true}}]
  (log/info {:scale-factor scale-factor})
  {:title "TPC-H (OLAP)"
   :seed seed
   :tasks
   [{:t :do
     :stage :ingest
     :tasks (into (if load-phase
                    [{:t :do
                      :stage :submit-docs
                      :tasks [{:t :call :f (fn [{:keys [sut]}] (tpch/submit-docs! sut scale-factor))}]}]
                    [])
                  [{:t :do
                    :stage :sync
                    :tasks [{:t :call :f (fn [{:keys [sut]}] (bxt/sync-node sut (Duration/ofHours 5)))}]}
                   {:t :do
                    :stage :finish-block
                    :tasks [{:t :call :f (fn [{:keys [sut]}] (bxt/finish-block! sut))}]}
                   {:t :do
                    :stage :compact
                    :tasks [{:t :call :f (fn [{:keys [sut]}] (bxt/compact! sut))}]}])}

    (queries-stage :cold-queries)

    (queries-stage :hot-queries)]})
