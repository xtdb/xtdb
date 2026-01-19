(ns xtdb.bench.tsbs
  (:require [clojure.test :as t]
            [clojure.tools.logging :as log]
            [xtdb.api :as xt]
            [xtdb.bench :as b]
            [xtdb.time :as time]
            [xtdb.tsbs :as tsbs]
            [xtdb.util :as util])
  (:import (java.time Duration Instant LocalTime)
           (java.util Random)))

;; TSBS fleet choices (from pkg/data/usecases/iot/truck.go)
(def fleet-choices ["East" "West" "North" "South"])

(defn- random-fleet
  "Pick a random fleet using the benchmark seed"
  [^Random rng]
  (nth fleet-choices (.nextInt rng (count fleet-choices))))

(defn- random-trucks
  "Pick n random truck names from the available trucks"
  [^Random rng truck-names n]
  (let [available (vec truck-names)
        cnt (count available)]
    (if (<= cnt n)
      available
      (->> (repeatedly n #(.nextInt rng cnt))
           distinct
           (take n)
           (mapv #(nth available %))))))

(defn- random-window
  "Pick a random time window of given duration within the data range"
  [^Random rng ^Instant min-time ^Instant max-time ^Duration window-duration]
  (let [range-millis (- (.toEpochMilli max-time) (.toEpochMilli min-time))
        window-millis (.toMillis window-duration)
        max-start (max 0 (- range-millis window-millis))
        start-offset (if (pos? max-start) (.nextLong rng max-start) 0)
        window-start (Instant/ofEpochMilli (+ (.toEpochMilli min-time) start-offset))
        window-end (.plus window-start window-duration)]
    [window-start window-end]))

(defmethod b/cli-flags :tsbs-iot [_]
  [[nil "--devices DEVICES"
    :id :devices
    :parse-fn parse-long]

   [nil "--timestamp-start TIMESTAMP_START"
    :id :timestamp-start
    :parse-fn time/->instant]

   [nil "--timestamp-end TIMESTAMP_END"
    :id :timestamp-end
    :parse-fn time/->instant]

   [nil "--file TXS_FILE"
    :id :txs-file
    :parse-fn util/->path]

   ["-h" "--help"]])

(defn- estimate-row-count
  "Estimate total rows based on devices and time range.
   Each device generates 1 reading + 1 diagnostic every 5 minutes (PT5M).
   Uses TSBS defaults (1 day) if timestamps not provided."
  [devices timestamp-start timestamp-end]
  (when devices
    (let [;; Use TSBS defaults: 2016-01-01 to 2016-01-02 (1 day)
          start (time/->instant (or timestamp-start #inst "2016-01-01T00:00:00Z"))
          end (time/->instant (or timestamp-end #inst "2016-01-02T00:00:00Z"))
          duration-millis (.toMillis (Duration/between start end))
          interval-millis (.toMillis #xt/duration "PT5M")
          readings-per-device (quot duration-millis interval-millis)
          total-readings (* devices readings-per-device)
          total-diagnostics total-readings
          total-rows (+ total-readings total-diagnostics)]
      {:devices devices
       :days (/ duration-millis (* 1000 60 60 24.0))
       :readings-per-device readings-per-device
       :total-readings total-readings
       :total-diagnostics total-diagnostics
       :total-rows total-rows})))

(defmethod b/->benchmark :tsbs-iot [_ {:keys [seed txs-file devices timestamp-start timestamp-end], :or {seed 0}, :as opts}]
  (when-let [est (and (not txs-file) (estimate-row-count devices timestamp-start timestamp-end))]
    (log/info (format "TSBS-IoT scale: %d devices × %.1f days = %,d total rows (%,d readings + %,d diagnostics)"
                      (:devices est) (:days est) (:total-rows est) (:total-readings est) (:total-diagnostics est))))

  {:title "TSBS IoT"
   :benchmark-type :tsbs-iot
   :seed seed
   :parameters {:seed seed
                :txs-file txs-file
                :devices devices
                :timestamp-start timestamp-start
                :timestamp-end timestamp-end}
   :->state (fn [_] {:!state (atom {:rng (Random. seed)})})
   :tasks [{:t :do
            :stage :ingest
            :tasks [(letfn [(submit-txs [node txs]
                              (doseq [{:keys [ops system-time]} txs]
                                (when (Thread/interrupted) (throw (InterruptedException.)))

                                (when (= LocalTime/MIDNIGHT (.toLocalTime (time/->zdt system-time)))
                                  (log/debug "submitting" system-time))

                                (xt/submit-tx node ops {:system-time system-time})))]

                      (if txs-file
                        {:t :call
                         :stage :submit-docs
                         :f (fn [{:keys [node]}]
                              (tsbs/with-file-txs txs-file
                                (partial submit-txs node)))}

                        {:t :call
                         :stage :gen+submit-docs
                         :f (fn [{:keys [node]}]
                              (tsbs/make-gen)

                              (tsbs/with-generated-data (into {:seed seed
                                                               :use-case :iot,
                                                               :log-interval #xt/duration "PT5M"}
                                                              (if devices
                                                                ;; We pass devices as scale here to not confuse with scale-factor elsewhere
                                                                (assoc opts :scale devices)
                                                                opts))
                                (partial submit-txs node)))}))

                    {:t :call
                     :stage :sync
                     :f (fn [{:keys [node]}] (b/sync-node node (Duration/ofHours 5)))}

                    {:t :call
                     :stage :finish-block
                     :f (fn [{:keys [node]}] (b/finish-block! node))}

                    {:t :call
                     :stage :compact
                     :f (fn [{:keys [node]}] (b/compact! node))}]}

           ;; Query setup - discover data characteristics for parameterized queries
           {:t :call
            :stage :query-setup
            :f (fn [{:keys [node !state]}]
                 (let [rng (:rng @!state)
                       ;; Get all truck names for random selection
                       truck-names (mapv :name (xt/q node "SELECT name FROM trucks ORDER BY name"))
                       ;; Get time range from readings
                       time-bounds (first (xt/q node
                                                "SELECT MIN(_valid_from) AS min_time, MAX(_valid_from) AS max_time
                                                 FROM readings FOR ALL VALID_TIME"))
                       ;; Pick random fleet for this benchmark run
                       fleet (random-fleet rng)
                       ;; Pick some random trucks for specific queries
                       sample-trucks (random-trucks rng truck-names 5)
                       min-time (:min_time time-bounds)
                       max-time (:max_time time-bounds)]
                   (log/info (format "Query setup: %d trucks, fleet=%s, time=%s to %s"
                                     (count truck-names) fleet min-time max-time))
                   (swap! !state assoc
                          :truck-names truck-names
                          :fleet fleet
                          :sample-trucks sample-trucks
                          :min-time min-time
                          :max-time max-time)
                   {:truck-count (count truck-names)
                    :fleet fleet
                    :sample-trucks sample-trucks}))}

           ;; ============ TSBS IoT Queries (all 13) ============

           ;; 1. LastLocByTruck - last location for specific trucks
           {:t :call
            :stage :last-loc-by-truck
            :f (fn [{:keys [node !state]}]
                 (let [{:keys [sample-trucks]} @!state
                       results (xt/q node
                                     (into ["SELECT t.name, t.driver, r.longitude, r.latitude
                                             FROM trucks t
                                             JOIN readings r ON r._id = t._id
                                             WHERE t.name IN (SELECT * FROM UNNEST(?))
                                             ORDER BY t.name"]
                                           [sample-trucks]))]
                   (log/info (format "last-loc-by-truck: %d trucks queried, %d results"
                                     (count sample-trucks) (count results)))
                   {:queried-trucks (count sample-trucks) :result-count (count results)}))}

           ;; 2. LastLocPerTruck - last location for all trucks in a fleet
           {:t :call
            :stage :last-loc-per-truck
            :f (fn [{:keys [node !state]}]
                 (let [{:keys [fleet]} @!state
                       results (xt/q node
                                     ["SELECT t.name, t.driver, r.longitude, r.latitude
                                       FROM trucks t
                                       JOIN readings r ON r._id = t._id
                                       WHERE t.name IS NOT NULL AND t.fleet = ?
                                       ORDER BY t.name" fleet])]
                   (log/info (format "last-loc-per-truck (fleet=%s): %d trucks" fleet (count results)))
                   {:fleet fleet :truck-count (count results)}))}

           ;; 3. TrucksWithLowFuel - trucks with fuel < 10%
           {:t :call
            :stage :low-fuel
            :f (fn [{:keys [node !state]}]
                 (let [{:keys [fleet]} @!state
                       results (xt/q node
                                     ["SELECT t.name, t.driver, d.fuel_state
                                       FROM trucks t
                                       JOIN diagnostics d ON d._id = t._id
                                       WHERE t.name IS NOT NULL
                                         AND t.fleet = ?
                                         AND d.fuel_state < 0.1
                                       ORDER BY d.fuel_state" fleet])]
                   (log/info (format "low-fuel (fleet=%s): %d trucks" fleet (count results)))
                   {:fleet fleet :count (count results)}))}

           ;; 4. TrucksWithHighLoad - trucks with load > 90% capacity
           {:t :call
            :stage :high-load
            :f (fn [{:keys [node !state]}]
                 (let [{:keys [fleet]} @!state
                       results (xt/q node
                                     ["SELECT t.name, t.driver, d.current_load, t.load_capacity,
                                              (d.current_load / t.load_capacity) AS load_pct
                                       FROM trucks t
                                       JOIN diagnostics d ON d._id = t._id
                                       WHERE t.name IS NOT NULL
                                         AND t.fleet = ?
                                         AND d.current_load / t.load_capacity > 0.9
                                       ORDER BY load_pct DESC" fleet])]
                   (log/info (format "high-load (fleet=%s): %d trucks" fleet (count results)))
                   {:fleet fleet :count (count results)}))}

           ;; 5. StationaryTrucks - trucks with avg velocity < 1 in 10-min window
           {:t :call
            :stage :stationary-trucks
            :f (fn [{:keys [node !state]}]
                 (let [{:keys [fleet min-time max-time rng]} @!state
                       [window-start window-end] (random-window rng min-time max-time (Duration/ofMinutes 10))
                       results (xt/q node
                                     ["SELECT t.name, t.driver, AVG(r.velocity) AS avg_velocity
                                       FROM trucks t
                                       JOIN readings FOR VALID_TIME BETWEEN ? AND ? AS r ON r._id = t._id
                                       WHERE t.name IS NOT NULL AND t.fleet = ?
                                       GROUP BY t.name, t.driver
                                       HAVING AVG(r.velocity) < 1
                                       ORDER BY avg_velocity"
                                      window-start window-end fleet])]
                   (log/info (format "stationary-trucks (fleet=%s, %s to %s): %d trucks"
                                     fleet window-start window-end (count results)))
                   {:fleet fleet :window [window-start window-end] :count (count results)}))}

           ;; 6. TrucksWithLongDrivingSessions - drove > 220 mins in 4 hours (stopped < 20 mins)
           {:t :call
            :stage :long-driving-sessions
            :f (fn [{:keys [node !state]}]
                 (let [{:keys [fleet min-time max-time rng]} @!state
                       [window-start window-end] (random-window rng min-time max-time (Duration/ofHours 4))
                       ;; Count 10-min periods with avg velocity > 1, need > 22 (220 mins of driving)
                       results (xt/q node
                                     ["WITH driving_periods AS (
                                         SELECT t.name, t.driver,
                                                DATE_TRUNC(HOUR, r._valid_from) AS hour_bucket,
                                                AVG(r.velocity) AS avg_vel
                                         FROM trucks t
                                         JOIN readings FOR VALID_TIME BETWEEN ? AND ? AS r ON r._id = t._id
                                         WHERE t.name IS NOT NULL AND t.fleet = ?
                                         GROUP BY t.name, t.driver, DATE_TRUNC(HOUR, r._valid_from)
                                         HAVING AVG(r.velocity) > 1
                                       )
                                       SELECT name, driver, COUNT(*) AS driving_hours
                                       FROM driving_periods
                                       GROUP BY name, driver
                                       HAVING COUNT(*) >= 3
                                       ORDER BY driving_hours DESC"
                                      window-start window-end fleet])]
                   (log/info (format "long-driving-sessions (fleet=%s): %d trucks" fleet (count results)))
                   {:fleet fleet :count (count results)}))}

           ;; 7. TrucksWithLongDailySessions - drove > 10 hours in 24 hours
           {:t :call
            :stage :long-daily-sessions
            :f (fn [{:keys [node !state]}]
                 (let [{:keys [fleet min-time max-time rng]} @!state
                       [window-start window-end] (random-window rng min-time max-time (Duration/ofHours 24))
                       results (xt/q node
                                     ["WITH driving_periods AS (
                                         SELECT t.name, t.driver,
                                                DATE_TRUNC(HOUR, r._valid_from) AS hour_bucket,
                                                AVG(r.velocity) AS avg_vel
                                         FROM trucks t
                                         JOIN readings FOR VALID_TIME BETWEEN ? AND ? AS r ON r._id = t._id
                                         WHERE t.name IS NOT NULL AND t.fleet = ?
                                         GROUP BY t.name, t.driver, DATE_TRUNC(HOUR, r._valid_from)
                                         HAVING AVG(r.velocity) > 1
                                       )
                                       SELECT name, driver, COUNT(*) AS driving_hours
                                       FROM driving_periods
                                       GROUP BY name, driver
                                       HAVING COUNT(*) >= 10
                                       ORDER BY driving_hours DESC"
                                      window-start window-end fleet])]
                   (log/info (format "long-daily-sessions (fleet=%s): %d trucks" fleet (count results)))
                   {:fleet fleet :count (count results)}))}

           ;; 8. AvgVsProjectedFuelConsumption - actual vs nominal fuel per fleet
           {:t :call
            :stage :avg-vs-projected-fuel
            :f (fn [{:keys [node]}]
                 (let [results (xt/q node
                                     "SELECT t.fleet,
                                             AVG(r.fuel_consumption) AS avg_fuel_consumption,
                                             AVG(t.nominal_fuel_consumption) AS projected_fuel_consumption
                                      FROM trucks t
                                      JOIN readings FOR ALL VALID_TIME AS r ON r._id = t._id
                                      WHERE t.fleet IS NOT NULL
                                        AND t.nominal_fuel_consumption IS NOT NULL
                                        AND r.velocity > 1
                                      GROUP BY t.fleet
                                      ORDER BY t.fleet")]
                   (log/info (format "avg-vs-projected-fuel: %d fleets" (count results)))
                   {:fleet-count (count results) :results results}))}

           ;; 9. AvgDailyDrivingDuration - average hours driven per day per driver
           {:t :call
            :stage :avg-daily-driving-duration
            :f (fn [{:keys [node]}]
                 (let [results (xt/q node
                                     "WITH driving_hours AS (
                                        SELECT t.name, t.driver, t.fleet,
                                               DATE_TRUNC(DAY, r._valid_from) AS day,
                                               DATE_TRUNC(HOUR, r._valid_from) AS hour_bucket
                                        FROM trucks t
                                        JOIN readings FOR ALL VALID_TIME AS r ON r._id = t._id
                                        WHERE t.name IS NOT NULL
                                        GROUP BY t.name, t.driver, t.fleet,
                                                 DATE_TRUNC(DAY, r._valid_from),
                                                 DATE_TRUNC(HOUR, r._valid_from)
                                        HAVING AVG(r.velocity) > 1
                                      ),
                                      daily_hours AS (
                                        SELECT name, driver, fleet, day, COUNT(*) AS hours_driven
                                        FROM driving_hours
                                        GROUP BY name, driver, fleet, day
                                      )
                                      SELECT fleet, name, driver, AVG(hours_driven) AS avg_daily_hours
                                      FROM daily_hours
                                      GROUP BY fleet, name, driver
                                      ORDER BY fleet, name")]
                   (log/info (format "avg-daily-driving-duration: %d drivers" (count results)))
                   {:driver-count (count results)}))}

           ;; 10. AvgDailyDrivingSession - avg session length per driver per day (state transitions)
           {:t :call
            :stage :avg-daily-driving-session
            :f (fn [{:keys [node]}]
                 ;; Simplified: count driving periods (velocity > 5) per day
                 (let [results (xt/q node
                                     "WITH driver_states AS (
                                        SELECT t.name,
                                               DATE_TRUNC(DAY, r._valid_from) AS day,
                                               DATE_TRUNC(HOUR, r._valid_from) AS hour_bucket,
                                               AVG(r.velocity) > 5 AS driving
                                        FROM trucks t
                                        JOIN readings FOR ALL VALID_TIME AS r ON r._id = t._id
                                        WHERE t.name IS NOT NULL
                                        GROUP BY t.name, DATE_TRUNC(DAY, r._valid_from), DATE_TRUNC(HOUR, r._valid_from)
                                      )
                                      SELECT name, day, COUNT(*) FILTER (WHERE driving) AS driving_hours
                                      FROM driver_states
                                      GROUP BY name, day
                                      HAVING COUNT(*) FILTER (WHERE driving) > 0
                                      ORDER BY name, day")]
                   (log/info (format "avg-daily-driving-session: %d driver-days" (count results)))
                   {:driver-day-count (count results)}))}

           ;; 11. AvgLoad - average load per truck model per fleet
           {:t :call
            :stage :avg-load
            :f (fn [{:keys [node]}]
                 (let [results (xt/q node
                                     "SELECT t.fleet, t.model, t.load_capacity,
                                             AVG(d.current_load / t.load_capacity) AS avg_load_pct
                                      FROM trucks t
                                      JOIN diagnostics FOR ALL VALID_TIME AS d ON d._id = t._id
                                      WHERE t.name IS NOT NULL
                                      GROUP BY t.fleet, t.model, t.load_capacity
                                      ORDER BY t.fleet, t.model")]
                   (log/info (format "avg-load: %d fleet/model combinations" (count results)))
                   {:combination-count (count results)}))}

           ;; 12. DailyTruckActivity - hours active per day per fleet/model
           {:t :call
            :stage :daily-activity
            :f (fn [{:keys [node]}]
                 (let [results (xt/q node
                                     "WITH active_hours AS (
                                        SELECT t.fleet, t.model,
                                               DATE_TRUNC(DAY, d._valid_from) AS day,
                                               DATE_TRUNC(HOUR, d._valid_from) AS hour_bucket,
                                               AVG(d.status) AS avg_status
                                        FROM trucks t
                                        JOIN diagnostics FOR ALL VALID_TIME AS d ON d._id = t._id
                                        WHERE t.name IS NOT NULL
                                        GROUP BY t.fleet, t.model,
                                                 DATE_TRUNC(DAY, d._valid_from),
                                                 DATE_TRUNC(HOUR, d._valid_from)
                                        HAVING AVG(d.status) < 1
                                      )
                                      SELECT fleet, model, day, COUNT(*) AS active_hours
                                      FROM active_hours
                                      GROUP BY fleet, model, day
                                      ORDER BY day, fleet, model")]
                   (log/info (format "daily-activity: %d fleet/model/day combinations" (count results)))
                   {:combination-count (count results)}))}

           ;; 13. TruckBreakdownFrequency - breakdown events per model
           {:t :call
            :stage :breakdown-frequency
            :f (fn [{:keys [node]}]
                 ;; Count transitions from status > 0.5 to status < 0.5 per model
                 (let [results (xt/q node
                                     "WITH hourly_status AS (
                                        SELECT t.model, t._id AS truck_id,
                                               DATE_TRUNC(HOUR, d._valid_from) AS hour_bucket,
                                               AVG(d.status) >= 0.5 AS operational
                                        FROM trucks t
                                        JOIN diagnostics FOR ALL VALID_TIME AS d ON d._id = t._id
                                        WHERE t.name IS NOT NULL
                                        GROUP BY t.model, t._id, DATE_TRUNC(HOUR, d._valid_from)
                                      ),
                                      breakdown_events AS (
                                        SELECT model, COUNT(*) FILTER (WHERE NOT operational) AS breakdown_hours
                                        FROM hourly_status
                                        GROUP BY model
                                      )
                                      SELECT model, breakdown_hours
                                      FROM breakdown_events
                                      WHERE breakdown_hours > 0
                                      ORDER BY breakdown_hours DESC")]
                   (log/info (format "breakdown-frequency: %d models with breakdowns" (count results)))
                   {:model-count (count results) :results results}))}]})

;; not intended to be run as a test - more for ease of REPL dev
(t/deftest ^:benchmark run-iot
  (util/with-tmp-dirs #{node-tmp-dir}
    (-> (b/->benchmark :tsbs-iot {:devices 40, :timestamp-start #inst "2020-01-01", :timestamp-end #inst "2020-01-07"})
        (b/run-benchmark {:node-dir node-tmp-dir}))))

(t/deftest ^:benchmark run-iot-from-file
  (util/with-tmp-dirs #{node-tmp-dir}
    (log/debug "tmp-dir:" node-tmp-dir)

    (-> (b/->benchmark :tsbs-iot {:txs-file #xt/path "/home/james/tmp/tsbs.transit.json"})
        (b/run-benchmark {:node-dir node-tmp-dir}))))
