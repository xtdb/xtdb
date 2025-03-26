(ns xtdb.bench.readings
  (:require [clojure.test :as t]
            [clojure.tools.logging :as log]
            [xtdb.api :as xt]
            [xtdb.bench :as b]
            [xtdb.test-util :as tu]
            [xtdb.time :as time]
            [xtdb.util :as util])
  (:import (java.time Duration Instant LocalTime)
           (java.util AbstractMap)))

(defn random-float [min max] (+ min (* (rand) (- max min))))

(defn docs [devices readings]
  (->> (tu/->instants :minute 5 #inst "2020-01-01")
       (partition 2 1)
       (take readings)
       (map (fn [[start end]]
              (into [:put-docs {:into :readings :valid-from start :valid-to end}]
                    (for [i (range devices)]
                      {:xt/id i :value (random-float -100 100)}))))))

(def max-valid-time-q
  "SELECT max(_valid_from) AS max_valid_time
   FROM readings FOR ALL VALID_TIME
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
  ([devices readings] (->ingestion-stage devices readings {}))
  ([devices readings {:keys [backfill?], :or {backfill? true}}]
   [{:t :call, :stage :ingest
     :f (fn [{:keys [sut]}]
          (log/infof "Inserting %d readings for %d devices" readings devices)

          (doseq [[idx batch] (map vector (range) (docs devices readings))
                  :let [[_put-docs {:keys [^Instant valid-from, ^Instant valid-to]}] batch]]
            (when (zero? (mod idx 1000))
              (log/debugf "Submitting readings from %s (batch %d)" (str valid-from) idx))

            (xt/submit-tx sut [batch]
                          (when backfill?
                            {:system-time (.plus valid-to (Duration/ofNanos (* idx 1000)))}))))}
    {:t :call, :stage :sync
     :f (fn [{:keys [sut]}]
          (b/sync-node sut (Duration/ofMinutes 5)))}

    {:t :call, :stage :compact
     :f (fn [{:keys [sut]}]
          (b/finish-block! sut)
          (b/compact! sut))}]))

(defmethod b/cli-flags :readings [_]
  [[nil "--devices devices" "device count"
    :parse-fn parse-long
    :default 10000]
   [nil "--readings READINGS" "reading count per device"
    :parse-fn parse-long
    :default 10000]

   ["-h" "--help"]])

(defmethod b/->benchmark :readings [_ {:keys [readings devices seed no-load?] :or {seed 0}}]
  {:title "Readings benchmarks"
   :seed seed
   :tasks (concat (when-not no-load?
                    (->ingestion-stage devices readings))

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

;; not intended to be run as a test - more for ease of REPL dev
(t/deftest ^:benchmark run-readings
  (let [path (util/->path "/home/james/tmp/readings-bench")
        no-load? true]

    (-> (b/->benchmark :readings {:readings 100000, :devices 10000, :no-load? no-load?})
        (b/run-benchmark {:node-dir path, :no-load? no-load?}))))
