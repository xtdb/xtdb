(ns crux.ts-weather-test
  (:require [clojure.instant :as inst]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :as t]
            [crux.api :as api]
            [crux.fixtures.api :refer [*api*]]
            [crux.fixtures.kafka :as fk]
            [crux.io :as cio])
  (:import java.math.RoundingMode
           java.time.temporal.ChronoUnit
           java.util.Date))

;; https://docs.timescale.com/v1.2/tutorials/other-sample-datasets#in-depth-weather
;; Requires https://timescaledata.blob.core.windows.net/datasets/weather_small.tar.gz

;; NOTE: Results in link above doesn't match actual data, test is
;; adjusted for this.

(def ^:const weather-locations-csv-resource "ts/data/weather_small_locations.csv")
(def ^:const weather-conditions-csv-resource "ts/data/weather_small_conditions.csv")

(def run-ts-weather-tests? (boolean (and (io/resource weather-locations-csv-resource)
                                         (io/resource weather-conditions-csv-resource)
                                         (Boolean/parseBoolean (System/getenv "CRUX_TS_WEATHER")))))

(def ^:const conditions-chunk-size 1000)

(defn submit-ts-weather-data
  ([node]
   (submit-ts-weather-data
     node
     (io/resource weather-locations-csv-resource)
     (io/resource weather-conditions-csv-resource)))
  ([node locations-resource conditions-resource]
   (with-open [locations-in (io/reader locations-resource)
               conditions-in (io/reader conditions-resource)]
     (let [location-tx-ops (vec (for [location (line-seq locations-in)
                                      :let [[device-id location environment] (str/split location #",")
                                            id (keyword "location" device-id)
                                            location (keyword location)
                                            environment (keyword environment)]]
                                  [:crux.tx/put
                                   {:crux.db/id id
                                    :location/location location
                                    :location/environment environment}]))]
       (api/submit-tx node location-tx-ops)
       (->> (line-seq conditions-in)
            (partition conditions-chunk-size)
            (reduce
             (fn [n chunk]
               (api/submit-tx
                node
                (vec (for [condition chunk
                           :let [[time device-id temperature humidity] (str/split condition #",")
                                 time (inst/read-instant-date
                                       (-> time
                                           (str/replace " " "T")
                                           (str/replace #"-(\d\d)$" ".000-$1:00")))
                                 condition-id (keyword "condition" device-id)
                                 location-device-id (keyword "location" device-id)]]
                       [:crux.tx/put
                        {:crux.db/id condition-id
                         :condition/time time
                         :condition/device-id location-device-id
                         :condition/temperature (Double/parseDouble temperature)
                         :condition/humidity (Double/parseDouble humidity)}
                        time])))
               (+ n (count chunk)))
             (count location-tx-ops)))))))


(defn with-ts-weather-data [f]
  (if run-ts-weather-tests?
    (let [submit-future (future (submit-ts-weather-data *api*))]
      (api/sync *api* (java.time.Duration/ofMinutes 20))
      (t/is (= 1001000 @submit-future))
      (f))
    (f)))

(t/use-fixtures :once
                fk/with-embedded-kafka-cluster
                fk/with-kafka-client
                fk/with-cluster-node
                with-ts-weather-data)

;; NOTE: Does not work with range, takes latest values.

;; NOTE: the AEV index is slower than necessary, as it could skip
;; straight to the right version as it knows this from the
;; entity. Currently this degrades with history size. This requires
;; swapping position of value and content hash in the index. Similar
;; issue would be there for AVE, but this already uses the content
;; hash to directly jump to the right version, if it exists.

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

(t/deftest weather-last-10-readings-test
  (if run-ts-weather-tests?
    (t/is (= [[#inst "2016-11-16T21:18:00.000-00:00"
               :location/weather-pro-000000
               42.0
               54.600000000000094]
              [#inst "2016-11-16T21:18:00.000-00:00"
               :location/weather-pro-000001
               42.0
               54.49999999999998]
              [#inst "2016-11-16T21:18:00.000-00:00"
               :location/weather-pro-000002
               42.0
               55.200000000000074]
              [#inst "2016-11-16T21:18:00.000-00:00"
               :location/weather-pro-000003
               42.0
               52.70000000000004]
              [#inst "2016-11-16T21:18:00.000-00:00"
               :location/weather-pro-000004
               70.00000000000004
               49.000000000000014]
              [#inst "2016-11-16T21:18:00.000-00:00"
               :location/weather-pro-000005
               70.30000000000014
               48.60000000000001]
              [#inst "2016-11-16T21:18:00.000-00:00"
               :location/weather-pro-000006
               42.0
               50.00000000000003]
              [#inst "2016-11-16T21:18:00.000-00:00"
               :location/weather-pro-000007
               69.89999999999998
               49.50000000000002]
              [#inst "2016-11-16T21:18:00.000-00:00"
               :location/weather-pro-000008
               42.0
               53.70000000000008]
              [#inst "2016-11-16T21:18:00.000-00:00"
               :location/weather-pro-000009
               91.0
               92.40000000000006]]
             (api/q (api/db *api*)
                  '{:find [time device-id temperature humidity]
                    :where [[c :condition/time time]
                            [c :condition/device-id device-id]
                            [c :condition/temperature temperature]
                            [c :condition/humidity humidity]]
                    :order-by [[time :desc] [device-id :asc]]
                    :limit 10})))
    (t/is true "skipping")))

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

;; NOTE: Does not work with range, takes latest values.

(defn trunc ^double [d ^long scale]
  (.doubleValue (.setScale (bigdec d) scale RoundingMode/HALF_UP)))

(t/deftest weather-last-10-readings-from-outside-locations-test
  (if run-ts-weather-tests?
    (t/is (= [[#inst "2016-11-16T21:18:00.000-00:00"
               :location/weather-pro-000000
               :arctic-000000
               42.0
               54.6]
              [#inst "2016-11-16T21:18:00.000-00:00"
               :location/weather-pro-000001
               :arctic-000001
               42.0
               54.5]
              [#inst "2016-11-16T21:18:00.000-00:00"
               :location/weather-pro-000002
               :arctic-000002
               42.0
               55.2]
              [#inst "2016-11-16T21:18:00.000-00:00"
               :location/weather-pro-000003
               :arctic-000003
               42.0
               52.7]
              [#inst "2016-11-16T21:18:00.000-00:00"
               :location/weather-pro-000006
               :arctic-000004
               42.0
               50.0]
              [#inst "2016-11-16T21:18:00.000-00:00"
               :location/weather-pro-000008
               :arctic-000005
               42.0
               53.7]
              [#inst "2016-11-16T21:18:00.000-00:00"
               :location/weather-pro-000009
               :swamp-000000
               91.0
               92.4]
              [#inst "2016-11-16T21:18:00.000-00:00"
               :location/weather-pro-000012
               :swamp-000001
               91.0
               94.3]
              [#inst "2016-11-16T21:18:00.000-00:00"
               :location/weather-pro-000017
               :swamp-000002
               91.0
               90.5]
              [#inst "2016-11-16T21:18:00.000-00:00"
               :location/weather-pro-000018
               :swamp-000003
               91.0
               96.9]]
             (for [[time device-id location temperature humidity]
                   (api/q (api/db *api*)
                        '{:find [time device-id location temperature humidity]
                          :where [[c :condition/time time]
                                  [c :condition/device-id device-id]
                                  [c :condition/temperature temperature]
                                  [c :condition/humidity humidity]
                                  [device-id :location/location location]
                                  [device-id :location/environment :outside]]
                          :order-by [[time :desc] [device-id :asc]]
                          :limit 10
                          :timeout 120000})]
               [time device-id location (trunc temperature 2) (trunc humidity 2)])))
    (t/is true "skipping")))

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

;; TODO: Reading of history and documents could be made lazy via merge
;; sort. Would require one iterator per entity though to be open for
;; its history seq.

;; NOTE: Assumes locations are stable over time.

(defn kw-starts-with? [kw prefix]
  (str/starts-with? (name kw) prefix))

(t/deftest weather-hourly-average-min-max-temperatures-for-field-locations
  (if run-ts-weather-tests?
    (t/is (= [[#inst "2016-11-15T12:00:00.000-00:00" 73.45 68.0 79.2]
              [#inst "2016-11-15T13:00:00.000-00:00" 74.43 68.7 80.4]
              [#inst "2016-11-15T14:00:00.000-00:00" 75.44 69.5 81.4]
              [#inst "2016-11-15T15:00:00.000-00:00" 76.47 70.5 82.7]
              [#inst "2016-11-15T16:00:00.000-00:00" 77.48 71.5 83.4]
              [#inst "2016-11-15T17:00:00.000-00:00" 78.46 72.5 84.6]
              [#inst "2016-11-15T18:00:00.000-00:00" 79.45 73.5 85.5]
              [#inst "2016-11-15T19:00:00.000-00:00" 80.42 74.8 86.5]
              [#inst "2016-11-15T20:00:00.000-00:00" 81.41 75.6 87.4]
              [#inst "2016-11-15T21:00:00.000-00:00" 82.42 76.1 88.4]
              [#inst "2016-11-15T22:00:00.000-00:00" 83.4 77.0 89.5]
              [#inst "2016-11-15T23:00:00.000-00:00" 84.38 78.3 90.0]
              [#inst "2016-11-16T00:00:00.000-00:00" 85.35 79.3 90.0]
              [#inst "2016-11-16T01:00:00.000-00:00" 85.27 78.8 90.0]
              [#inst "2016-11-16T02:00:00.000-00:00" 84.26 77.7 89.4]
              [#inst "2016-11-16T03:00:00.000-00:00" 83.25 76.5 88.5]
              [#inst "2016-11-16T04:00:00.000-00:00" 82.24 75.3 87.9]
              [#inst "2016-11-16T05:00:00.000-00:00" 81.23 74.1 87.2]
              [#inst "2016-11-16T06:00:00.000-00:00" 80.21 72.6 86.3]
              [#inst "2016-11-16T07:00:00.000-00:00" 79.19 71.5 84.9]
              [#inst "2016-11-16T08:00:00.000-00:00" 78.17 71.1 84.2]
              [#inst "2016-11-16T09:00:00.000-00:00" 77.17 70.1 83.2]
              [#inst "2016-11-16T10:00:00.000-00:00" 77.18 70.1 83.6]
              [#inst "2016-11-16T11:00:00.000-00:00" 78.17 71.0 84.8]]
             (let [condition-ids (->> (api/q (api/db *api*)
                                          '{:find [c]
                                            :where [[c :condition/device-id device-id]
                                                    [device-id :location/location location]
                                                    [(crux.ts-weather-test/kw-starts-with? location "field-")]]
                                            :timeout 120000})
                                      (reduce into []))
                   db (api/db *api* #inst "1970")]
               (with-open [snapshot (api/new-snapshot db)]
                 (->> (for [c condition-ids]
                        (for [entity-tx (api/history-ascending db snapshot c)]
                          (update entity-tx :crux.db/valid-time #(Date/from (.truncatedTo (.toInstant ^Date %) ChronoUnit/HOURS)))))
                      (cio/merge-sort (fn [a b]
                                        (compare (:crux.db/valid-time a) (:crux.db/valid-time b))))
                      (partition-by :crux.db/valid-time)
                      (take 24)
                      (mapv (fn [group]
                              (let [temperatures (sort (mapv (comp :condition/temperature :crux.db/doc) group))]
                                [(:crux.db/valid-time (first group))
                                 (trunc (/ (reduce + temperatures) (count group)) 2)
                                 (trunc (first temperatures) 2)
                                 (trunc (last temperatures) 2)]))))))))
    (t/is true "skipping")))
