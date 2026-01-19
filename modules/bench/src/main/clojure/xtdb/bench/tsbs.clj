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
                       sample-trucks (random-trucks rng truck-names 5)]
                   (log/info (format "Query setup: %d trucks, fleet=%s, time=%s to %s"
                                     (count truck-names) fleet
                                     (:min_time time-bounds) (:max_time time-bounds)))
                   (swap! !state assoc
                          :truck-names truck-names
                          :fleet fleet
                          :sample-trucks sample-trucks
                          :min-time (:min_time time-bounds)
                          :max-time (:max_time time-bounds))
                   {:truck-count (count truck-names)
                    :fleet fleet
                    :sample-trucks sample-trucks}))}

           ;; Query phase - exercise different query patterns
           {:t :call
            :stage :query-counts
            :f (fn [{:keys [node]}]
                 (let [readings-count (:row-count (first (xt/q node "SELECT COUNT(*) row_count FROM readings FOR ALL VALID_TIME")))
                       diagnostics-count (:row-count (first (xt/q node "SELECT COUNT(*) row_count FROM diagnostics FOR ALL VALID_TIME")))
                       total-count (+ readings-count diagnostics-count)]
                   (log/info (format "TSBS-IoT counts: %,d readings + %,d diagnostics = %,d total"
                                     readings-count diagnostics-count total-count))
                   {:readings-count readings-count
                    :diagnostics-count diagnostics-count
                    :total-count total-count}))}

           ;; Last location per truck in a fleet (point query - latest valid-time per entity)
           {:t :call
            :stage :query-last-loc
            :f (fn [{:keys [node !state]}]
                 (let [{:keys [fleet]} @!state
                       results (xt/q node
                                     ["SELECT t.name, t.driver, r.longitude, r.latitude
                                       FROM trucks t
                                       JOIN readings r ON r._id = t._id
                                       WHERE t.fleet = ?
                                       ORDER BY t.name" fleet])]
                   (log/info (format "Last location query (fleet=%s): %d trucks" fleet (count results)))
                   {:fleet fleet :truck-count (count results)}))}

           ;; Last location for specific trucks (parameterized truck selection)
           {:t :call
            :stage :query-last-loc-trucks
            :f (fn [{:keys [node !state]}]
                 (let [{:keys [sample-trucks]} @!state
                       results (xt/q node
                                     (into ["SELECT t.name, t.driver, r.longitude, r.latitude
                                             FROM trucks t
                                             JOIN readings r ON r._id = t._id
                                             WHERE t.name IN (SELECT * FROM UNNEST(?))
                                             ORDER BY t.name"]
                                           [sample-trucks]))]
                   (log/info (format "Last location for %d specific trucks: %d results"
                                     (count sample-trucks) (count results)))
                   {:queried-trucks sample-trucks :result-count (count results)}))}

           ;; Trucks with low fuel (threshold query on latest diagnostic)
           {:t :call
            :stage :query-low-fuel
            :f (fn [{:keys [node !state]}]
                 (let [{:keys [fleet]} @!state
                       results (xt/q node
                                     ["SELECT t.name, t.driver, d.fuel_state
                                       FROM trucks t
                                       JOIN diagnostics d ON d._id = t._id
                                       WHERE t.fleet = ?
                                         AND d.fuel_state < 0.1
                                       ORDER BY d.fuel_state" fleet])]
                   (log/info (format "Low fuel query (fleet=%s): %d trucks with <10%% fuel" fleet (count results)))
                   {:fleet fleet :low-fuel-count (count results)}))}

           ;; Trucks with high load (threshold query - load vs capacity)
           {:t :call
            :stage :query-high-load
            :f (fn [{:keys [node !state]}]
                 (let [{:keys [fleet]} @!state
                       results (xt/q node
                                     ["SELECT t.name, t.driver, d.current_load, t.load_capacity,
                                              (d.current_load / t.load_capacity) AS load_pct
                                       FROM trucks t
                                       JOIN diagnostics d ON d._id = t._id
                                       WHERE t.fleet = ?
                                         AND d.current_load / t.load_capacity > 0.9
                                       ORDER BY load_pct DESC" fleet])]
                   (log/info (format "High load query (fleet=%s): %d trucks with >90%% load" fleet (count results)))
                   {:fleet fleet :high-load-count (count results)}))}

           ;; Stationary trucks in time window (time-windowed aggregation)
           {:t :call
            :stage :query-stationary
            :f (fn [{:keys [node !state]}]
                 (let [{:keys [fleet min-time max-time]} @!state
                       ;; Use a 10-minute window from the middle of the time range
                       mid-time (Instant/ofEpochMilli (/ (+ (.toEpochMilli ^Instant min-time)
                                                           (.toEpochMilli ^Instant max-time)) 2))
                       window-start (.minus mid-time (Duration/ofMinutes 5))
                       window-end (.plus mid-time (Duration/ofMinutes 5))
                       results (xt/q node
                                     ["SELECT t.name, t.driver, AVG(r.velocity) AS avg_velocity
                                       FROM trucks t
                                       JOIN readings FOR VALID_TIME BETWEEN ? AND ? AS r ON r._id = t._id
                                       WHERE t.fleet = ?
                                       GROUP BY t.name, t.driver
                                       HAVING AVG(r.velocity) < 1
                                       ORDER BY avg_velocity"
                                      window-start window-end fleet])]
                   (log/info (format "Stationary trucks (fleet=%s, window=%s to %s): %d trucks"
                                     fleet window-start window-end (count results)))
                   {:fleet fleet :window-start window-start :window-end window-end
                    :stationary-count (count results)}))}

           ;; Average load per fleet per model (analytics query - full scan)
           {:t :call
            :stage :query-avg-load
            :f (fn [{:keys [node]}]
                 (let [results (xt/q node
                                     "SELECT t.fleet, t.model, t.load_capacity,
                                             AVG(d.current_load / t.load_capacity) AS avg_load_pct
                                      FROM trucks t
                                      JOIN diagnostics FOR ALL VALID_TIME AS d ON d._id = t._id
                                      GROUP BY t.fleet, t.model, t.load_capacity
                                      ORDER BY t.fleet, t.model")]
                   (log/info (format "Avg load query: %d fleet/model combinations" (count results)))
                   {:fleet-model-count (count results)}))}]})

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
