(ns dashboards
  (:require [clojure.walk :as w]
            [clojure.string :as str])
  (:import [software.amazon.awscdk.core
            App Stack$Builder Environment$Builder RemovalPolicy Duration]
           [software.amazon.awscdk.services.cloudwatch
            Dashboard$Builder GraphWidget$Builder AlarmWidget$Builder Metric$Builder
            MathExpression$Builder YAxisProps$Builder Alarm$Builder IAlarmAction
            CompositeAlarm$Builder AlarmRule AlarmState TreatMissingData]
           [software.amazon.awscdk.services.cloudwatch.actions
            SnsAction]
           [software.amazon.awscdk.services.sns
            Topic$Builder]
           [software.amazon.awscdk.services.sns.subscriptions
            EmailSubscription$Builder]))

;; Example: [[g1 g2] [g3 g4]] -> 2x2 grid of widgets where gX is a map
(def ingest-grid
  [[{:bench-type "ingest"
     :bench-ns "ts-devices"
     :crux-node-type "embedded-kafka-rocksdb"
     :threshold 0.1}
    {:bench-type "ingest"
     :bench-ns "ts-devices"
     :crux-node-type "standalone-rocksdb"
     :threshold 0.1}
    {:bench-type "ingest"
     :bench-ns "ts-devices"
     :crux-node-type "kafka-rocksdb"
     :threshold 0.1}]

   [{:bench-type "ingest"
     :bench-ns "ts-weather"
     :crux-node-type "embedded-kafka-rocksdb"
     :threshold 0.1}
    {:bench-type "ingest"
     :bench-ns "ts-weather"
     :crux-node-type "standalone-rocksdb"
     :threshold 0.1}
    {:bench-type "ingest"
     :bench-ns "ts-weather"
     :crux-node-type "kafka-rocksdb"
     :threshold 0.1}]

   [{:bench-type "ingest"
     :bench-ns "watdiv-crux"
     :crux-node-type "embedded-kafka-rocksdb"
     :threshold 0.45}
    {:bench-type "ingest"
     :bench-ns "watdiv-crux"
     :crux-node-type "standalone-rocksdb"
     :threshold 0.25}
    {:bench-type "ingest"
     :bench-ns "watdiv-crux"
     :crux-node-type "kafka-rocksdb"
     :threshold 0.1}]])

(def queries-grid
  [[{:bench-type "queries-warm"
     :bench-ns "tpch"
     :crux-node-type "embedded-kafka-rocksdb"
     :threshold 0.007}
    {:bench-type "queries-warm"
     :bench-ns "tpch"
     :crux-node-type "kafka-lmdb"
     :threshold 0.007}
    {:bench-type "queries-warm"
     :bench-ns "tpch"
     :crux-node-type "kafka-rocksdb"
     :threshold 0.023}]

   [{:bench-type "queries-warm"
     :bench-ns "tpch"
     :crux-node-type "standalone-lmdb"
     :threshold 0.0025}
    {:bench-type "queries-warm"
     :bench-ns "tpch"
     :crux-node-type "standalone-rocksdb"
     :threshold 0.003}
    {:bench-type "queries-warm"
     :bench-ns "tpch"
     :crux-node-type "standalone-rocksdb-with-metrics"
     :threshold 0.007}]])

(defn ->metric [m]
  (.. (Metric$Builder/create)
      (namespace "crux.bench")
      (dimensions {"bench-type" (:bench-type m)
                   "bench-ns" (:bench-ns m)
                   "crux-node-type" (:crux-node-type m)})
      (metricName "time-taken")
      (statistic "avg")
      (period (Duration/hours 30))
      (build)))

(defn ->math
  ([metric expr label]
   (->math metric "time" expr label)) ;; default label metric as time
  ([metric skey expr label]
   (.. (MathExpression$Builder/create)
       (expression expr)
       (usingMetrics {skey metric})
       (period (Duration/days 1))
       (label label)
       (build))))

(defn ->alarm [stack m metric-kw]
  (.. (Alarm$Builder/create stack (String/join "-" [(:bench-type m) (:bench-ns m) (:crux-node-type m)]))
      (metric (get m metric-kw))
      (threshold (:threshold m))
      (evaluationPeriods 1)
      (datapointsToAlarm 1)
      (actionsEnabled false)
      (treatMissingData TreatMissingData/BREACHING)
      (build)))

(defn ->alarm-widget [m alarm-kw]
  (.. (AlarmWidget$Builder/create)
      (title (str/join "-" [(:bench-type m) (:bench-ns m) (:crux-node-type m)]))
      (alarm (get m alarm-kw))
      (leftYAxis (.. (YAxisProps$Builder.)
                     (showUnits false)
                     (build)))
      (width 8)
      (build)))

(defn ->graph-widget [m metric-kws]
  (.. (GraphWidget$Builder/create)
      (title (str/join "-" [(:bench-type m) (:bench-ns m) (:crux-node-type m)]))
      (left (-> (select-keys m metric-kws) vals))
      (leftYAxis (.. (YAxisProps$Builder.)
                     (showUnits false)
                     (min 0)
                     (build)))
      (width 8)
      (build)))

(defn update-in-grid [grid k f]
  (w/postwalk #(if (map? %)
                 (assoc % k (f %))
                 %)
              grid))

(defn get-grid [grid k]
  (w/postwalk #(if (get % k)
                 (get % k)
                 %)
              grid))

(defn gen-alarm-composite-rule [grid]
  (->> (get-grid grid :alarm)
       flatten
       (map #(AlarmRule/fromAlarm % AlarmState/ALARM))
       (into-array)
       (AlarmRule/anyOf)))

(def app (App.))

(def env
  (.. (Environment$Builder.)
      (region "eu-west-2")
      (build)))

(def stack
  (.. (Stack$Builder/create app "crux-bench-dashboard-stack")
      (env env)
      (build)))

(defn -main []
  (let [ingest-grid (-> ingest-grid
                        (update-in-grid :minute-metric (fn [m]
                                                         (-> (->metric m)
                                                             (->math "time/6000" "time-taken(mins)"))))

                        (update-in-grid :rate-metric (fn [m]
                                                       (-> (->metric m)
                                                           (->math "RATE(time)^2" "rate(time-taken(ms))^2"))))

                        (update-in-grid :timing-widget (fn [m]
                                                         (->graph-widget m [:minute-metric])))

                        (update-in-grid :alarm (fn [m]
                                                 (->alarm stack m :rate-metric)))

                        (update-in-grid :alarm-widget (fn [m]
                                                        (->alarm-widget m :alarm))))

        queries-grid (-> queries-grid
                         (update-in-grid :cold-metric (fn [m]
                                                        (-> (->metric m)
                                                            (->math "time/6000" "warm time-taken(mins)"))))

                         (update-in-grid :warm-metric (fn [m]
                                                        (-> (->metric (assoc m :bench-type "queries"))
                                                            (->math "cold" "cold/6000" "cold time-taken(mins)"))))

                         (update-in-grid :timing-widget (fn [m]
                                                          (->graph-widget m [:cold-metric :warm-metric])))

                         (update-in-grid :rate-metric (fn [m]
                                                        (-> (->metric m) (->math "RATE(time)^2" "rate(time-taken(ms))^2"))))

                         (update-in-grid :alarm (fn [m]
                                                  (->alarm stack m :rate-metric)))

                         (update-in-grid :alarm-widget (fn [m]
                                                         (->alarm-widget m :alarm))))

        alert-dash (.. (Dashboard$Builder/create stack "Crux Bench Ingestion Alerting Dashboard")
                       (dashboardName "Crux-Benchmarks-Ingestion-Alerting")
                       (start "-P8W")
                       (widgets (get-grid ingest-grid :alarm-widget))
                       build)

        timing-dash (.. (Dashboard$Builder/create stack "Crux Bench Ingestion Timings Dashboard")
                        (dashboardName "Crux-Benchmarks-Ingestion-Timing")
                        (start "-P8W")
                        (widgets (get-grid ingest-grid :timing-widget))
                        build)

        queries-dash (.. (Dashboard$Builder/create stack "Crux Bench Query Timings Dashboard")
                         (dashboardName "Crux-Benchmarks-Queries-Timing")
                         (start "-P8W")
                         (widgets (get-grid queries-grid :timing-widget))
                         build)

        queries-alert-dash (.. (Dashboard$Builder/create stack "Crux Bench Query Alerting Dashboard")
                               (dashboardName "Crux-Benchmarks-Query-Alerting")
                               (start "-P8W")
                               (widgets (get-grid queries-grid :alarm-widget))
                               build)

        sns-email-sub (.. (EmailSubscription$Builder/create "crux-devs@juxt.pro")
                          (build))

        sns-topic (doto (.. (Topic$Builder/create stack "crux-bench-alerts")
                            (displayName "Crux Bench Alerts")
                            (build))
                    (.addSubscription sns-email-sub))

        sns-actions (into-array [(SnsAction. sns-topic)])

        ingest-alarm (doto (.. (CompositeAlarm$Builder/create stack "crux-bench-ingest-alert")
                               (alarmRule (gen-alarm-composite-rule ingest-grid))
                               (actionsEnabled true)
                               (build))
                       (.addAlarmAction sns-actions)
                       (.addOkAction sns-actions))

        query-alarm (doto (.. (CompositeAlarm$Builder/create stack "crux-bench-query-alert")
                              (alarmRule (gen-alarm-composite-rule queries-grid))
                              (actionsEnabled true)
                              (build))
                      (.addAlarmAction sns-actions)
                      (.addOkAction sns-actions))]

    (.synth app)))
