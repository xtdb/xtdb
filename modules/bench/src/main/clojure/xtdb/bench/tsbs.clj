(ns xtdb.bench.tsbs
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [clojure.test :as t]
            [clojure.tools.logging :as log]
            [hugsql.core :as hugsql]
            [xtdb.api :as xt]
            [xtdb.bench :as b]
            [xtdb.bench.stats :as stats]
            [xtdb.time :as time]
            [xtdb.tsbs :as tsbs]
            [xtdb.util :as util])
  (:import (java.time Duration Instant LocalTime)
           (java.util Random)
           (java.util.concurrent Executors ExecutorService TimeUnit)))

(hugsql/def-sqlvec-fns "xtdb/bench/tsbs.sql")

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
  "Pick a random time window of given duration within the data range.
   Returns nil if window-duration > data range (can't fit window in available data)."
  [^Random rng ^Instant min-time ^Instant max-time ^Duration window-duration]
  (let [range-millis (- (.toEpochMilli max-time) (.toEpochMilli min-time))
        window-millis (.toMillis window-duration)]
    (when-not (> window-millis range-millis)
      (let [max-start (- range-millis window-millis)
            start-offset (if (pos? max-start) (.nextLong rng max-start) 0)
            window-start (Instant/ofEpochMilli (+ (.toEpochMilli min-time) start-offset))
            window-end (.plus window-start window-duration)]
        [window-start window-end]))))

(defn- gen-last-loc-by-truck
  "LastLocByTruck - last location for specific random trucks"
  [{:keys [rng truck-names]}]
  (let [trucks (random-trucks rng truck-names 5)]
    {:query-type :last-loc-by-truck
     :sqlvec (query-last-loc-by-truck-sqlvec {:truck-names trucks})}))

(defn- gen-last-loc-per-truck
  "LastLocPerTruck - last location for all trucks in a random fleet"
  [{:keys [rng]}]
  (let [fleet (random-fleet rng)]
    {:query-type :last-loc-per-truck
     :sqlvec (query-last-loc-per-truck-sqlvec {:fleet fleet})}))

(defn- gen-low-fuel
  "TrucksWithLowFuel - trucks with fuel < 10% in a random fleet"
  [{:keys [rng]}]
  (let [fleet (random-fleet rng)]
    {:query-type :low-fuel
     :sqlvec (query-low-fuel-sqlvec {:fleet fleet})}))

(defn- gen-high-load
  "TrucksWithHighLoad - trucks with load > 90% capacity in a random fleet"
  [{:keys [rng]}]
  (let [fleet (random-fleet rng)]
    {:query-type :high-load
     :sqlvec (query-high-load-sqlvec {:fleet fleet})}))

(defn- gen-stationary-trucks
  "StationaryTrucks - trucks with avg velocity < 1 in random 10-min window.
   Returns nil if data range < 10 minutes."
  [{:keys [rng min-time max-time]}]
  (when-let [[window-start window-end] (random-window rng min-time max-time (Duration/ofMinutes 10))]
    (let [fleet (random-fleet rng)]
      {:query-type :stationary-trucks
       :sqlvec (query-stationary-trucks-sqlvec {:window-start window-start
                                                :window-end window-end
                                                :fleet fleet})})))

(defn- gen-long-driving-sessions
  "TrucksWithLongDrivingSessions - drove > 220 mins in random 4-hour window.
   Returns nil if data range < 4 hours."
  [{:keys [rng min-time max-time]}]
  (when-let [[window-start window-end] (random-window rng min-time max-time (Duration/ofHours 4))]
    (let [fleet (random-fleet rng)]
      {:query-type :long-driving-sessions
       :sqlvec (query-long-driving-sessions-sqlvec {:window-start window-start
                                                    :window-end window-end
                                                    :fleet fleet})})))

(defn- gen-long-daily-sessions
  "TrucksWithLongDailySessions - drove > 10 hours in random 24-hour window.
   Returns nil if data range < 24 hours."
  [{:keys [rng min-time max-time]}]
  (when-let [[window-start window-end] (random-window rng min-time max-time (Duration/ofHours 24))]
    (let [fleet (random-fleet rng)]
      {:query-type :long-daily-sessions
       :sqlvec (query-long-daily-sessions-sqlvec {:window-start window-start
                                                  :window-end window-end
                                                  :fleet fleet})})))

(defn- gen-avg-vs-projected-fuel-consumption
  "AvgVsProjectedFuelConsumption - actual vs nominal fuel per fleet (no random params)"
  [_state]
  {:query-type :avg-vs-projected-fuel-consumption
   :sqlvec (query-avg-vs-projected-fuel-consumption-sqlvec {})})

(defn- gen-avg-daily-driving-duration
  "AvgDailyDrivingDuration - average hours driven per day per driver (no random params)"
  [_state]
  {:query-type :avg-daily-driving-duration
   :sqlvec (query-avg-daily-driving-duration-sqlvec {})})

(defn- gen-avg-daily-driving-session
  "AvgDailyDrivingSession - avg session duration per driver per day (no random params)
   Uses LAG/LEAD to detect driving session transitions (velocity > 5 = driving).
   Calculates average duration of driving sessions, matching TSBS semantics."
  [_state]
  {:query-type :avg-daily-driving-session
   :sqlvec (query-avg-daily-driving-session-sqlvec {})})

(defn- gen-avg-load
  "AvgLoad - average load per truck model per fleet (no random params)"
  [_state]
  {:query-type :avg-load
   :sqlvec (query-avg-load-sqlvec {})})

(defn- gen-daily-activity
  "DailyTruckActivity - fraction of day active per fleet/model (no random params)"
  [_state]
  {:query-type :daily-activity
   :sqlvec (query-daily-activity-sqlvec {})})

(defn- gen-breakdown-frequency
  "TruckBreakdownFrequency - breakdown events per model (no random params)
   Uses LEAD to detect transitions from working (status > 0) to broken (status = 0)."
  [_state]
  {:query-type :breakdown-frequency
   :sqlvec (query-breakdown-frequency-sqlvec {})})

;; All query generators in TSBS order
(def query-generators
  [gen-last-loc-by-truck
   gen-last-loc-per-truck
   gen-low-fuel
   gen-high-load
   gen-stationary-trucks
   gen-long-driving-sessions
   gen-long-daily-sessions
   gen-avg-vs-projected-fuel-consumption
   gen-avg-daily-driving-duration
   gen-avg-daily-driving-session
   gen-avg-load
   gen-daily-activity
   gen-breakdown-frequency])

;; All possible query types (for detecting skipped queries)
(def ^:private all-query-types
  #{:last-loc-by-truck :last-loc-per-truck :low-fuel :high-load
    :stationary-trucks :long-driving-sessions :long-daily-sessions
    :avg-vs-projected-fuel-consumption :avg-daily-driving-duration :avg-daily-driving-session
    :avg-load :daily-activity :breakdown-frequency})

(defn- generate-queries
  "Generate n query instances for each query type.
   Returns {:queries [...] :skipped #{...}} where skipped contains query types
   that couldn't be generated (e.g. window duration > data range)."
  [state n]
  (let [all-results (for [gen query-generators
                          _ (range n)]
                      (gen state))
        queries (filterv some? all-results)
        ;; Determine which types were skipped by comparing with what we got
        generated-types (into #{} (map :query-type) queries)
        skipped-types (set/difference all-query-types generated-types)]
    (when (seq skipped-types)
      (log/warn (format "Skipping query types (insufficient data range): %s"
                        (str/join ", " (map name skipped-types)))))
    {:queries queries
     :skipped skipped-types}))

(defn- run-query-batch
  "Run a batch of queries, return timings grouped by query-type.
   If progress-atom is provided, increments it after each query."
  [node queries progress-atom]
  (reduce
   (fn [acc query]
     (when (Thread/interrupted) (throw (InterruptedException.)))
     (let [query-type (:query-type query)
           start-ns (System/nanoTime)
           _ (xt/q node (:sqlvec query))
           timing-ns (- (System/nanoTime) start-ns)]
       (when progress-atom (swap! progress-atom inc))
       (update acc query-type (fnil conj []) timing-ns)))
   {}
   queries))

(defn- progress-logger
  "Start a background thread that logs progress every interval-ms.
   Returns a function to stop the logger."
  [progress-atom total interval-ms]
  (let [running (atom true)
        thread (Thread.
                (fn []
                  (while @running
                    (Thread/sleep interval-ms)
                    (when @running
                      (let [done @progress-atom
                            pct (if (pos? total) (* 100.0 (/ done total)) 0.0)]
                        (log/info (format "Progress: %d/%d queries (%.1f%%)" done total pct)))))))]
    (.start thread)
    (fn []
      (reset! running false)
      (.interrupt thread))))

(defn- run-queries-single-threaded
  "Run all queries single-threaded"
  [node queries progress-atom]
  (run-query-batch node queries progress-atom))

(defn- run-queries-parallel
  "Run queries with multiple workers using a thread pool"
  [node queries workers progress-atom]
  (let [^ExecutorService executor (Executors/newFixedThreadPool workers)
        ;; Partition queries across workers
        query-batches (partition-all (max 1 (quot (count queries) workers)) queries)
        futures (mapv #(.submit executor ^Callable (fn [] (run-query-batch node % progress-atom)))
                      query-batches)]
    (try
      ;; Collect results from all workers
      (reduce
       (fn [acc fut]
         (let [batch-result (.get fut)]
           (merge-with into acc batch-result)))
       {}
       futures)
      (finally
        (.shutdown executor)
        (.awaitTermination executor 1 TimeUnit/HOURS)))))

(defn- run-queries
  "Run all queries with the specified configuration"
  [node queries {:keys [workers burn-in]}]
  (let [burn-in-count (min burn-in (count queries))

        ;; Run burn-in queries (discard timings)
        _ (when (pos? burn-in-count)
            (log/info (format "Running %d burn-in queries..." burn-in-count))
            (run-query-batch node (take burn-in-count queries) nil))

        ;; Run remaining queries and collect timings
        queries-for-stats (drop burn-in-count queries)
        total (count queries-for-stats)
        _ (log/info (format "Running %d queries with %d workers..." total workers))

        ;; Set up progress tracking (log every 60 seconds)
        progress-atom (atom 0)
        stop-logger (progress-logger progress-atom total 60000)

        timings-by-type (try
                          (if (= 1 workers)
                            (run-queries-single-threaded node queries-for-stats progress-atom)
                            (run-queries-parallel node queries-for-stats workers progress-atom))
                          (finally
                            (stop-logger)))]
    timings-by-type))

(defn- format-stats-row
  "Format a single query type's stats as a string"
  [query-type stats]
  (format "%-25s %6d queries | min: %8.2f | mean: %8.2f | p50: %8.2f | p90: %8.2f | p95: %8.2f | p99: %8.2f | max: %8.2f ms"
          (name query-type)
          (:count stats)
          (:min-ms stats)
          (:mean-ms stats)
          (:p50-ms stats)
          (:p90-ms stats)
          (:p95-ms stats)
          (:p99-ms stats)
          (:max-ms stats)))

;; ============ CLI and Benchmark Definition ============

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

   ;; Query execution options (TSBS-style)
   [nil "--query-iterations ITERATIONS"
    :id :query-iterations
    :default 100
    :parse-fn parse-long]

   [nil "--workers WORKERS"
    :id :workers
    :default 1
    :parse-fn parse-long]

   [nil "--burn-in BURN_IN"
    :id :burn-in
    :default 0
    :parse-fn parse-long]

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

(defmethod b/->benchmark :tsbs-iot [_ {:keys [seed txs-file devices timestamp-start timestamp-end
                                              query-iterations workers burn-in]
                                       :or {seed 0
                                            query-iterations 100
                                            workers 1
                                            burn-in 0}
                                       :as opts}]
  (when-let [est (and (not txs-file) (estimate-row-count devices timestamp-start timestamp-end))]
    (log/info (format "TSBS-IoT scale: %d devices × %.1f days = %,d total rows (%,d readings + %,d diagnostics)"
                      (:devices est) (:days est) (:total-rows est) (:total-readings est) (:total-diagnostics est))))

  (log/info (format "Query config: %d iterations × %d query types = %,d total queries, %d workers, burn-in=%d"
                    query-iterations (count query-generators) (* query-iterations (count query-generators))
                    workers burn-in))

  {:title "TSBS IoT"
   :benchmark-type :tsbs-iot
   :seed seed
   :parameters {:seed seed
                :txs-file txs-file
                :devices devices
                :timestamp-start timestamp-start
                :timestamp-end timestamp-end
                :query-iterations query-iterations
                :workers workers
                :burn-in burn-in}
   :->state (fn [] {:!state (atom {:rng (Random. seed)})})
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
                     :f (fn [{:keys [node]}] (b/flush-block! node))}

                    {:t :call
                     :stage :compact
                     :f (fn [{:keys [node]}] (b/compact! node))}]}

           ;; Query setup - discover data characteristics for parameterized queries
           {:t :call
            :stage :query-setup
            :f (fn [{:keys [node !state]}]
                 (let [;; Get all truck names for random selection
                       truck-names (mapv :name (xt/q node "SELECT name FROM trucks ORDER BY name"))
                       ;; Debug: check a sample row
                       sample-row (first (xt/q node "SELECT * FROM readings FOR ALL VALID_TIME LIMIT 1"))
                       _ (log/info (format "Sample readings row: %s" (pr-str sample-row)))
                       ;; Get time range from readings
                       readings-count (-> (xt/q node "SELECT COUNT(*) AS cnt FROM readings FOR ALL VALID_TIME")
                                          first :cnt)
                       _ (log/info (format "Readings table has %d rows" readings-count))
                       ;; Get min/max valid_from
                       time-bounds (first (xt/q node
                                                "SELECT MIN(_valid_from) AS min_time, MAX(_valid_from) AS max_time
                                                 FROM readings FOR ALL VALID_TIME"))
                       _ (log/info (format "Time bounds from valid_from: %s" (pr-str time-bounds)))
                       ;; If valid_from is null, try system_from
                       time-bounds (if (nil? (:min-time time-bounds))
                                     (let [sys-bounds (first (xt/q node
                                                                   "SELECT MIN(_system_from) AS min_time, MAX(_system_from) AS max_time
                                                                    FROM readings FOR ALL SYSTEM_TIME"))]
                                       (log/info (format "Time bounds from system_from: %s" (pr-str sys-bounds)))
                                       sys-bounds)
                                     time-bounds)
                       ;; Convert to Instant if needed (query returns ZonedDateTime)
                       min-time (some-> (:min-time time-bounds) time/->instant)
                       max-time (some-> (:max-time time-bounds) time/->instant)]
                   (log/info (format "Query setup: %d trucks, time=%s to %s"
                                     (count truck-names) min-time max-time))
                   (when (nil? min-time)
                     (throw (ex-info "Could not determine time bounds from readings table"
                                     {:readings-count readings-count
                                      :time-bounds time-bounds
                                      :sample-row sample-row})))
                   (swap! !state assoc
                          :truck-names truck-names
                          :min-time min-time
                          :max-time max-time)
                   {:truck-count (count truck-names)
                    :time-range [min-time max-time]}))}

           ;; Generate all query instances
           {:t :call
            :stage :generate-queries
            :f (fn [{:keys [!state]}]
                 (let [state @!state
                       {:keys [queries skipped]} (generate-queries state query-iterations)
                       active-types (- (count query-generators) (count skipped))]
                   (log/info (format "Generated %d query instances (%d types × %d iterations, %d types skipped)"
                                     (count queries) active-types query-iterations (count skipped)))
                   (swap! !state assoc :queries queries)
                   {:query-count (count queries)
                    :query-types active-types
                    :skipped-types (vec skipped)
                    :iterations query-iterations}))}

           ;; Execute all queries and collect stats
           {:t :call
            :stage :run-queries
            :f (fn [{:keys [node !state] :as worker}]
                 (let [{:keys [queries]} @!state
                       timings-by-type (run-queries node queries
                                                    {:workers workers
                                                     :burn-in burn-in})
                       stats-by-type (into {}
                                           (map (fn [[qt timings]]
                                                  [qt (stats/timing-stats timings)]))
                                           timings-by-type)
                       ;; Calculate overall stats
                       all-timings (into [] cat (vals timings-by-type))
                       overall-stats (stats/timing-stats all-timings)]

                   ;; Log per-query-type stats to console
                   (log/info "Query execution complete. Results by query type:")
                   (doseq [[query-type stats] (sort-by key stats-by-type)]
                     (log/info (format-stats-row query-type stats)))
                   (log/info (str (apply str (repeat 120 "-"))))
                   (log/info (format-stats-row :OVERALL overall-stats))

                   ;; Output stats as JSON for consumption
                   (b/log-report worker {:stage :query-stats
                                         :stats-by-type stats-by-type
                                         :overall-stats overall-stats
                                         :total-queries (count all-timings)})

                   {:stats-by-type stats-by-type
                    :overall-stats overall-stats
                    :total-queries (count all-timings)
                    :total-time-ms (:total-ms overall-stats)}))}]})

;; not intended to be run as a test - more for ease of REPL dev
(t/deftest ^:benchmark run-iot
  (util/with-tmp-dirs #{node-tmp-dir}
    (-> (b/->benchmark :tsbs-iot {:devices 40
                                  :timestamp-start #inst "2020-01-01"
                                  :timestamp-end #inst "2020-01-07"
                                  :query-iterations 100
                                  :workers 4})
        (b/run-benchmark {:node-dir node-tmp-dir}))))

(t/deftest ^:benchmark run-iot-from-file
  (util/with-tmp-dirs #{node-tmp-dir}
    (log/debug "tmp-dir:" node-tmp-dir)

    (-> (b/->benchmark :tsbs-iot {:txs-file #xt/path "/home/james/tmp/tsbs.transit.json"
                                  :query-iterations 100})
        (b/run-benchmark {:node-dir node-tmp-dir}))))
