(ns xtdb.bench.readings
  (:require [xtdb.api :as xt]
            [xtdb.bench.xtdb2 :as bxt]
            [xtdb.time :as time]
            [xtdb.test-util :as tu])
  (:import (java.util AbstractMap)
           (java.time Duration Instant)))

(defn random-float [min max] (+ min (* (rand) (- max min))))

(defn docs
  ([n] (docs n 10000))
  ([n devices]
   (->> (tu/->instants :minute 5 #inst "2020-01-01")
        (partition 2 1)
        (mapcat (fn [[start end]]
                  (for [i (range 0 devices 1000)]
                    (into [:put-docs {:into :readings :valid-from start :valid-to end}]
                          (for [j (range i (min devices (+ i 1000)))]
                            {:xt/id j :value (random-float -100 100)})))))
        (take (* n (inc (quot devices 1000)))))))

(defn batch->largest-valid-time [batch]
  (-> batch last second :valid-to))

(def max-valid-time-q "SELECT max(_valid_from) AS max_valid_time FROM readings FOR ALL VALID_TIME
                       WHERE _id = 0")

(defn- subtract-period
  ([inst-like period] (subtract-period inst-like period 1))
  ([inst-like period len]
   (let [inst (time/->instant inst-like)]
     (case period
       :now inst
       :hour (.minusSeconds inst (* 60 60 len))
       :day (.minusSeconds inst (* 60 60 24 len))
       :week (.minusSeconds inst (* 60 60 24 7 len))
       :month (.minusSeconds inst (* 60 60 24 30 len))
       :quarter (.minusSeconds inst (* 60 60 24 30 3 len))
       :year (.minusSeconds inst (* 60 60 24 30 12 len))))))

(comment (subtract-period (Instant/now) :week))

(defn aggregate-query
  ([sut start end] (aggregate-query sut start end {}))
  ([sut start end opts]
   (xt/q sut "SELECT AVG(value) AS avg, MIN(VALUE) AS min, MAX(VALUE) AS max
              FROM readings FOR VALID_TIME BETWEEN ? AND ?
              GROUP BY _id"
         (assoc opts :args [start end]))))

(defn check-query
  "A query that returns the number 5 min intervals in the range. Good for sanity checking."
  ([sut start end] (aggregate-query sut start end {}))
  ([sut start end opts]
   (xt/q sut "SELECT COUNT(*) AS cnt, _id AS id
              FROM readings FOR VALID_TIME BETWEEN ? AND ?
              GROUP BY _id"
         (assoc opts :args [start end]))))


(defn ->query-stage
  ([interval] (->query-stage interval nil))
  ([interval offset] (->query-stage interval offset 1))
  ([interval offset len]
   {:t :do
    :stage (if offset
             (keyword (str "query-offset-" len "-" (name offset) "-interval-" (name interval)))
             (keyword (str "query-recent-interval-" (name interval))))
    :tasks [{:t :call :f (fn [{:keys [sut custom-state]}]
                           (let [{:keys [latest-completed-tx max-valid-time]} custom-state
                                 max-valid-time (cond-> max-valid-time
                                                  offset (subtract-period offset len))]
                             (aggregate-query sut (subtract-period max-valid-time interval) max-valid-time
                                              {:current-time (:system-time latest-completed-tx)})))}]}))

(defn ->ingestion-stage
  ([size devices] (->ingestion-stage size devices {}))
  ([size devices {:keys [backfill?] :or {backfill? true}}]
   [{:t :do
     :stage :ingest
     :tasks [{:t :call :f (fn [{:keys [sut]}]
                            ;; batching by day
                            (doseq [batch (partition-all (* 24 12) (docs size devices))]
                              (xt/submit-tx sut batch (when backfill?
                                                        {:system-time (batch->largest-valid-time batch)}))))}]}
    {:t :do
     :stage :sync
     :tasks [{:t :call
              :f (fn [{:keys [sut]}]
                   (bxt/sync-node sut (Duration/ofMinutes 5)))}]}

    {:t :do
     :stage :compact
     :tasks [{:t :call
              :f (fn [{:keys [sut]}]
                   (bxt/compact! sut))}]}]))

(defn benchmark [{:keys [size devices seed load-phase] :or {seed 0}}]
  {:title "Readings benchmarks"
   :seed seed
   :tasks (concat (if load-phase
                    (->ingestion-stage size devices)
                    [])

                  [{:t :call
                    :f (fn [{:keys [sut ^AbstractMap custom-state]}]
                         (let [{:keys [latest-completed-tx]} (xt/status sut)
                               max-valid-time (-> (xt/q sut max-valid-time-q)
                                                  first
                                                  :max-valid-time)]
                           (.putAll custom-state {:latest-completed-tx latest-completed-tx
                                                  :max-valid-time max-valid-time})))}]

                  ;; this accumulates over the most recent interval
                  (for [interval [:now :day :week :month :quarter :year]]
                    (->query-stage interval))

                  ;; running the same queries but a year (valid-time) in the past
                  (for [interval [:now :day :week :month :quarter :year]]
                    (->query-stage interval :year)))})


(defn overwritten-readings [{:keys [size devices seed load-phase] :or {seed 0}}]
  {:title "Degenerative backfill case"
   :seed seed
   :tasks
   (concat (if load-phase
             (concat (->ingestion-stage size devices) (->ingestion-stage size devices {:backfill? true}))
             [])
           [{:t :call :f (fn [{:keys [sut ^AbstractMap custom-state]}]
                           (let [{:keys [latest-completed-tx]} (xt/status sut)
                                 max-valid-time (-> (xt/q sut max-valid-time-q)
                                                    first
                                                    :max-valid-time)]
                             (.putAll custom-state {:latest-completed-tx latest-completed-tx
                                                    :max-valid-time max-valid-time})))}
            ;; a year and one device is sufficient to show the issue
            (->query-stage :year)])})

(comment
  (require '[clojure.java.io :as io]
           '[xtdb.bench :as bench]
           '[xtdb.util :as util]
           '[clojure.test :as t])

  (def node-dir (.toPath (io/file "dev/readings")))
  (def node (tu/->local-node {:node-dir node-dir}))
  (.close node)
  (def latest-completed-tx (:latest-completed-tx (xt/status node)))
  (def max-valid-time (time (-> (xt/q node max-valid-time-q) first :max-valid-time)))

  (time (aggregate-query node (subtract-period max-valid-time :now) max-valid-time
                         {:current-time (:system-time latest-completed-tx)}))


  (t/deftest run-benchmark
    (let [path (util/->path "/tmp/readings-bench")
          reload? false]
      (when reload?
        (util/delete-dir path))

      (let [f (bench/compile-benchmark (benchmark {:size 1000, :devices 10000, :load-phase reload?})
                                       @(requiring-resolve `xtdb.bench.measurement/wrap-task))]

        (with-open [node (tu/->local-node {:node-dir path})]
          (f node))

        #_
        (f dev/node)))))
