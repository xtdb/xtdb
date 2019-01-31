(ns crux.ts-test
  (:require [clojure.test :as t]
            [clojure.instant :as inst]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [crux.db :as db]
            [crux.tx :as tx]
            [crux.lru :as lru]
            [crux.query :as q]
            [crux.kafka :as k]
            [crux.fixtures :as f]))

(def ^:const weather-locations-csv-resource "ts/data/weather_med_locations.csv")
(def ^:const weather-conditions-csv-resource "ts/data/weather_med_conditions.csv")

(def run-ts-weather-tests? (boolean (and (io/resource weather-locations-csv-resource)
                                         (io/resource weather-conditions-csv-resource)
                                         (System/getenv "CRUX_TS_WEATHER"))))

(defn with-kv-backend-from-env [f]
  (binding [f/*kv-backend* (or (System/getenv "CRUX_TS_KV_BACKEND") f/*kv-backend*)]
    (when run-ts-weather-tests?
      (println "Using KV backend:" f/*kv-backend*))
    (f)))

(def ^:const conditions-chunk-size 100)

(defn submit-ts-weather-data
  ([tx-log]
   (submit-ts-weather-data tx-log (io/resource weather-locations-csv-resource) (io/resource weather-conditions-csv-resource)))
  ([tx-log locations-resource conditions-resource]
   (with-open [locations-in (io/reader locations-resource)
               conditions-in (io/reader conditions-resource)]
     (let [location-tx-ops (vec (for [location (line-seq locations-in)
                                      :let [[device-id location environment] (str/split location #",")
                                            id (keyword "location" device-id)
                                            device-id (keyword device-id)
                                            location (keyword location)
                                            environment (keyword environment)]]
                                  [:crux.tx/put
                                   id
                                   {:crux.db/id id
                                    :location/device-id device-id
                                    :location/location location
                                    :location/environment environment}]))]
       (db/submit-tx tx-log location-tx-ops)
       (->> (line-seq conditions-in)
            (partition conditions-chunk-size)
            (reduce
             (fn [n chunk]
               (db/submit-tx
                tx-log
                (vec (for [condition chunk
                           :let [[time device-id temprature humidity] (str/split condition #",")
                                 id (keyword "condition" device-id)
                                 decide-id (keyword device-id)
                                 time (inst/read-instant-date
                                       (-> time
                                           (str/replace " " "T")
                                           (str/replace #"-(\d\d)$" ".000-$1:00")))]]
                       [:crux.tx/put
                        id
                        {:crux.db/id id
                         :condition/time time
                         :condition/device-id device-id
                         :condition/temprature (Double/parseDouble temprature)
                         :condition/humidity (Double/parseDouble humidity)}
                        time])))
               (+ n (count chunk)))
             (count location-tx-ops)))))))

(defn with-ts-weather-data [f]
  (if run-ts-weather-tests?
    (let [tx-topic "test-ts-tx"
          doc-topic "test-ts-doc"
          tx-log (k/->KafkaTxLog f/*producer* tx-topic doc-topic {})
          object-store (lru/new-cached-object-store f/*kv*)
          indexer (tx/->KvIndexer f/*kv* tx-log object-store)]

      (k/create-topic f/*admin-client* tx-topic 1 1 k/tx-topic-config)
      (k/create-topic f/*admin-client* doc-topic 1 1 k/doc-topic-config)
      (k/subscribe-from-stored-offsets indexer f/*consumer* [tx-topic doc-topic])
      (let [submit-future (future
                            (submit-ts-weather-data tx-log))
            consume-args {:indexer indexer
                          :consumer f/*consumer*
                          :pending-txs-state (atom [])
                          :tx-topic tx-topic
                          :doc-topic doc-topic}]
        (k/consume-and-index-entities consume-args)
        (while (not= {:txs 0 :docs 0}
                     (k/consume-and-index-entities
                      (assoc consume-args :timeout 100))))
        (t/is (= 15001000 @submit-future))
        (tx/await-no-consumer-lag indexer {:crux.tx-log/await-tx-timeout 60000}))
      (f))
    (f)))

(t/use-fixtures :once f/with-embedded-kafka-cluster f/with-kafka-client with-kv-backend-from-env f/with-kv-store with-ts-weather-data)

;; https://docs.timescale.com/v1.2/tutorials/other-sample-datasets#in-depth-weather
;; Requires https://timescaledata.blob.core.windows.net/datasets/weather_med.tar.gz

;; TODO: Doesn't work, as this isn't really how Crux works.

(t/deftest weather-last-10-readings-test
  (if run-ts-weather-tests?
    (t/is (= [[#inst "2016-12-06T02:58:00.000-05:00" :weather-pro-000000 84.10000000000034 83.70000000000053]
              [#inst "2016-12-06T02:58:00.000-05:00" :weather-pro-000001 35.999999999999915 51.79999999999994]
              [#inst "2016-12-06T02:58:00.000-05:00" :weather-pro-000002 68.90000000000006 63.09999999999999]
              [#inst "2016-12-06T02:58:00.000-05:00" :weather-pro-000003 83.70000000000041 84.69999999999989]
              [#inst "2016-12-06T02:58:00.000-05:00" :weather-pro-000004 83.10000000000039 84.00000000000051]
              [#inst "2016-12-06T02:58:00.000-05:00" :weather-pro-000005 85.10000000000034 81.70000000000017]
              [#inst "2016-12-06T02:58:00.000-05:00" :weather-pro-000006 61.09999999999999 49.800000000000026]
              [#inst "2016-12-06T02:58:00.000-05:00" :weather-pro-000007 82.9000000000004 84.80000000000047]
              [#inst "2016-12-06T02:58:00.000-05:00" :weather-pro-000008 58.599999999999966 40.2]
              [#inst "2016-12-06T02:58:00.000-05:00" :weather-pro-000009 61.000000000000014 49.399999999999906]]
             (q/q (q/db f/*kv*)
                  '{:find [time device-id temprature humidity]
                    :where [[c :condition/time time]
                            [c :condition/device-id device-id]
                            [c :condition/temprature temprature]
                            [c :condition/humidity humidity]]
                    :order-by [[time :desc]]
                    :limit 10})))
    (t/is true "skipping")))

;; Last 10 readings

;; SELECT * FROM conditions c ORDER BY time DESC LIMIT 10;

;; time                   |     device_id      |    temperature     |      humidity
;; -----------------------+--------------------+--------------------+--------------------
;; 2016-12-06 02:58:00-05 | weather-pro-000000 |  84.10000000000034 |  83.70000000000053
;; 2016-12-06 02:58:00-05 | weather-pro-000001 | 35.999999999999915 |  51.79999999999994
;; 2016-12-06 02:58:00-05 | weather-pro-000002 |  68.90000000000006 |  63.09999999999999
;; 2016-12-06 02:58:00-05 | weather-pro-000003 |  83.70000000000041 |  84.69999999999989
;; 2016-12-06 02:58:00-05 | weather-pro-000004 |  83.10000000000039 |  84.00000000000051
;; 2016-12-06 02:58:00-05 | weather-pro-000005 |  85.10000000000034 |  81.70000000000017
;; 2016-12-06 02:58:00-05 | weather-pro-000006 |  61.09999999999999 | 49.800000000000026
;; 2016-12-06 02:58:00-05 | weather-pro-000007 |   82.9000000000004 |  84.80000000000047
;; 2016-12-06 02:58:00-05 | weather-pro-000008 | 58.599999999999966 |               40.2
;; 2016-12-06 02:58:00-05 | weather-pro-000009 | 61.000000000000014 | 49.399999999999906
;; (10 rows)

;; Last 10 readings from 'outside' locations

;; SELECT time, c.device_id, location,
;; trunc(temperature, 2) temperature, trunc(humidity, 2) humidity
;; FROM conditions c
;; INNER JOIN locations l ON c.device_id = l.device_id
;; WHERE l.environment = 'outside'
;; ORDER BY time DESC LIMIT 10;

;; time                   |     device_id      |   location    | temperature | humidity
;; -----------------------+--------------------+---------------+-------------+----------
;; 2016-12-06 02:58:00-05 | weather-pro-000000 | field-000000  |       84.10 |    83.70
;; 2016-12-06 02:58:00-05 | weather-pro-000001 | arctic-000000 |       35.99 |    51.79
;; 2016-12-06 02:58:00-05 | weather-pro-000003 | swamp-000000  |       83.70 |    84.69
;; 2016-12-06 02:58:00-05 | weather-pro-000004 | field-000001  |       83.10 |    84.00
;; 2016-12-06 02:58:00-05 | weather-pro-000005 | swamp-000001  |       85.10 |    81.70
;; 2016-12-06 02:58:00-05 | weather-pro-000007 | field-000002  |       82.90 |    84.80
;; 2016-12-06 02:58:00-05 | weather-pro-000014 | field-000003  |       84.50 |    83.90
;; 2016-12-06 02:58:00-05 | weather-pro-000015 | swamp-000002  |       85.50 |    66.00
;; 2016-12-06 02:58:00-05 | weather-pro-000017 | arctic-000001 |       35.29 |    50.59
;; 2016-12-06 02:58:00-05 | weather-pro-000019 | arctic-000002 |       36.09 |    48.80
;; (10 rows)

;; Hourly average, min, and max temperatures for "field" locations

;; SELECT date_trunc('hour', time) "hour",
;; trunc(avg(temperature), 2) avg_temp,
;; trunc(min(temperature), 2) min_temp,
;; trunc(max(temperature), 2) max_temp
;; FROM conditions c
;; WHERE c.device_id IN (
;;     SELECT device_id FROM locations
;;     WHERE location LIKE 'field-%'
;; ) GROUP BY "hour" ORDER BY "hour" ASC LIMIT 24;

;; hour                   | avg_temp | min_temp | max_temp
;; -----------------------+----------+----------+----------
;; 2016-11-15 07:00:00-05 |    73.80 |    68.00 |    79.09
;; 2016-11-15 08:00:00-05 |    74.80 |    68.69 |    80.29
;; 2016-11-15 09:00:00-05 |    75.75 |    69.39 |    81.19
;; 2016-11-15 10:00:00-05 |    76.75 |    70.09 |    82.29
;; 2016-11-15 11:00:00-05 |    77.77 |    70.79 |    83.39
;; 2016-11-15 12:00:00-05 |    78.76 |    71.69 |    84.49
;; 2016-11-15 13:00:00-05 |    79.73 |    72.69 |    85.29
;; 2016-11-15 14:00:00-05 |    80.72 |    73.49 |    86.99
;; 2016-11-15 15:00:00-05 |    81.73 |    74.29 |    88.39
;; 2016-11-15 16:00:00-05 |    82.70 |    75.09 |    88.89
;; 2016-11-15 17:00:00-05 |    83.70 |    76.19 |    89.99
;; 2016-11-15 18:00:00-05 |    84.67 |    77.09 |    90.00
;; 2016-11-15 19:00:00-05 |    85.64 |    78.19 |    90.00
;; 2016-11-15 20:00:00-05 |    86.53 |    78.59 |    90.00
;; 2016-11-15 21:00:00-05 |    86.40 |    78.49 |    90.00
;; 2016-11-15 22:00:00-05 |    85.39 |    77.29 |    89.30
;; 2016-11-15 23:00:00-05 |    84.40 |    76.19 |    88.70
;; 2016-11-16 00:00:00-05 |    83.39 |    75.39 |    87.90
;; 2016-11-16 01:00:00-05 |    82.40 |    74.39 |    87.10
;; 2016-11-16 02:00:00-05 |    81.40 |    73.29 |    86.29
;; 2016-11-16 03:00:00-05 |    80.38 |    71.89 |    85.40
;; 2016-11-16 04:00:00-05 |    79.41 |    70.59 |    84.40
;; 2016-11-16 05:00:00-05 |    78.39 |    69.49 |    83.60
;; 2016-11-16 06:00:00-05 |    78.42 |    69.49 |    84.40
;; (24 rows)

;; https://docs.timescale.com/v1.2/tutorials/other-sample-datasets#in-depth-devices
;; Requires https://timescaledata.blob.core.windows.net/datasets/devices_med.tar.gz

;; 10 most recent battery temperature readings for charging devices

;; SELECT time, device_id, battery_temperature
;; FROM readings
;; WHERE battery_status = 'charging'
;; ORDER BY time DESC LIMIT 10;

;; time                   | device_id  | battery_temperature
;; -----------------------+------------+---------------------
;; 2016-11-15 23:39:30-05 | demo004887 |                99.3
;; 2016-11-15 23:39:30-05 | demo004882 |               100.8
;; 2016-11-15 23:39:30-05 | demo004862 |                95.7
;; 2016-11-15 23:39:30-05 | demo004844 |                95.5
;; 2016-11-15 23:39:30-05 | demo004841 |                95.4
;; 2016-11-15 23:39:30-05 | demo004804 |               101.6
;; 2016-11-15 23:39:30-05 | demo004784 |               100.6
;; 2016-11-15 23:39:30-05 | demo004760 |                99.1
;; 2016-11-15 23:39:30-05 | demo004731 |                97.9
;; 2016-11-15 23:39:30-05 | demo004729 |                99.6
;; (10 rows)

;; Busiest devices (1 min avg) whose battery level is below 33% and is not charging

;; SELECT time, readings.device_id, cpu_avg_1min,
;; battery_level, battery_status, device_info.model
;; FROM readings
;; JOIN device_info ON readings.device_id = device_info.device_id
;; WHERE battery_level < 33 AND battery_status = 'discharging'
;; ORDER BY cpu_avg_1min DESC, time DESC LIMIT 5;

;; time                   | device_id  | cpu_avg_1min | battery_level | battery_status |  model
;; -----------------------+------------+--------------+---------------+----------------+---------
;; 2016-11-15 23:30:00-05 | demo003764 |        98.99 |            32 | discharging    | focus
;; 2016-11-15 22:54:30-05 | demo001935 |        98.99 |            30 | discharging    | pinto
;; 2016-11-15 19:10:30-05 | demo000695 |        98.99 |            23 | discharging    | focus
;; 2016-11-15 16:46:00-05 | demo002784 |        98.99 |            18 | discharging    | pinto
;; 2016-11-15 14:58:30-05 | demo004978 |        98.99 |            22 | discharging    | mustang
;; (5 rows)

;; SELECT date_trunc('hour', time) "hour",
;; min(battery_level) min_battery_level,
;; max(battery_level) max_battery_level
;; FROM readings r
;; WHERE r.device_id IN (
;;     SELECT DISTINCT device_id FROM device_info
;;     WHERE model = 'pinto' OR model = 'focus'
;; ) GROUP BY "hour" ORDER BY "hour" ASC LIMIT 12;

;; hour                   | min_battery_level | max_battery_level
;; -----------------------+-------------------+-------------------
;; 2016-11-15 07:00:00-05 |                17 |                99
;; 2016-11-15 08:00:00-05 |                11 |                98
;; 2016-11-15 09:00:00-05 |                 6 |                97
;; 2016-11-15 10:00:00-05 |                 6 |                97
;; 2016-11-15 11:00:00-05 |                 6 |                97
;; 2016-11-15 12:00:00-05 |                 6 |                97
;; 2016-11-15 13:00:00-05 |                 6 |                97
;; 2016-11-15 14:00:00-05 |                 6 |                98
;; 2016-11-15 15:00:00-05 |                 6 |               100
;; 2016-11-15 16:00:00-05 |                 6 |               100
;; 2016-11-15 17:00:00-05 |                 6 |               100
;; 2016-11-15 18:00:00-05 |                 6 |               100
;; (12 rows)
