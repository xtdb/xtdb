(ns xtdb.bench.fusion
  (:require [clojure.test :as t]
            [clojure.tools.logging :as log]
            [xtdb.api :as xt]
            [xtdb.bench :as b]
            [xtdb.bench.random :as random]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import (java.time Duration Instant)
           (java.util UUID)))

;; ============================================================================
;; FUSION BENCHMARK
;; ============================================================================
;;
;; A benchmark based on actual design partner usage patterns to catch
;; regressions and enable optimization.
;;
;; ## Data Model (11 tables)
;;
;; Core tables:
;;   1. organisation    - Manufacturers (5 rows)
;;   2. device_series   - Device series per org (25 rows, 5 per org)
;;   3. device_model    - Reference data (50 rows, 2 per series)
;;   4. site            - Location data (NMI meter IDs, ~= system count)
;;   5. system          - Main table, constantly updated (configurable count, default 10k)
;;   6. device          - Links systems to device models (2× system count)
;;   7. readings        - High volume time-series (system_id, value, valid_time)
;;
;; Registration test tables (for cumulative registration query):
;;   8. test_suite      - Test suite definitions (1 suite)
;;   9. test_case       - Test cases within suites (5 cases per suite)
;;   10. test_suite_run - Suite executions per system (1 per system, 80% pass rate)
;;   11. test_case_run  - Case execution results (5 per suite run)
;;
;; ## Workload Characteristics (matching production patterns)
;;
;; Readings ingestion:
;;   - Batch size: ~1000
;;   - Insert frequency: every 5min of valid_time
;;   - System-time lag:
;;     * 80%: 0-2ms delay (near real-time)
;;     * 20%: 0-50ms delay (delayed batch processing)
;;
;; System updates:
;;   - Small-batch UPDATEs: batch size 30
;;   - Simulates constant trickle of system state changes (~every 5min)
;;   - Update frequency: configurable rounds (default 10)
;;
;; ## Query Suite (5 queries)
;;
;;   1. system-settings           - Point-in-time system lookup
;;   2. readings-for-system       - Time-series scan with temporal join
;;   3. system-count-over-time   - Complex 4-table temporal aggregation
;;   4. readings-range-bins       - Hourly aggregation using range_bins()
;;   5. cumulative-registration   - Multi-CTE query with window functions + complex temporal joins
;;                                 Tests registration status (Success/Failed/Pending)
;;
;; ## Key Production Pathologies Captured
;;
;; - Constant small-batch UPDATEs to frequently-queried table (system)
;; - Large-batch INSERT workload (readings)
;; - Temporal scatter: data arriving with variable system-time lag
;; - Complex multi-CTE queries with window functions
;; - range_bins temporal aggregation patterns
;; - Multi-table temporal joins with CONTAINS predicates
;;
;; ## Usage
;;
;; Default (10k systems × 1k readings):
;;   ./gradlew fusion
;;
;; Custom scale:
;;   ./gradlew fusion -PdeviceCount=1000 -PreadingCount=1000
;;
;; ============================================================================

(def organisation-names ["AlphaCorp" "BetaTech" "GammaGrid" "DeltaPower" "EpsilonEnergy"])
(def series-names ["Series-A" "Series-B" "Series-C" "Series-D" "Series-E"])
(def model-names ["Model-1" "Model-2"])

(defn random-float [rng min max]
  (+ min (* (.nextDouble rng) (- max min))))

(defn random-int [rng min max]
  (+ min (random/next-int rng (- max min))))

(defn generate-ids [rng n]
  (vec (repeatedly n #(random/next-uuid rng))))

(defn generate-organisation [org-id name]
  {:xt/id org-id
   :name name})

(defn generate-device-series [series-id org-id series-name]
  {:xt/id series-id
   :organisation-id org-id
   :name series-name})

(defn generate-device-model [rng model-id series-id model-name]
  {:xt/id model-id
   :device-series-id series-id
   :name model-name
   :capacity-kw (random-float rng 5 15)})

(defn generate-site [rng nmi]
  {:xt/id nmi
   :address (str (random-int rng 1 999) " Solar Street")
   :postcode (str (random-int rng 1000 9999))
   :state (random/uniform-nth rng ["NSW" "VIC" "QLD" "SA" "WA"])})

(defn generate-system-record [rng system-id nmi base-time]
  (let [modes-enabled ["default" "eco"]
        modes-supported ["default" "eco" "grid-charge" "grid-discharge"]
        doe-modes-enabled ["doe-mode-1"]
        doe-modes-supported ["doe-mode-1" "doe-mode-2" "doe-mode-3"]
        vpp-modes-enabled ["vpp-frequency" "vpp-voltage"]
        vpp-modes-supported ["vpp-frequency" "vpp-voltage" "vpp-reserve"]]
    {:xt/id system-id
     :nmi nmi
     :type (random/next-int rng 10)
     :created-at base-time
     :registration-date base-time

     ;; Rating limits (rtg_*)
     :rtg-max-w (random-float rng 1000 10000)
     :rtg-max-wh (random-float rng 5000 50000)
     :rtg-max-va (random-float rng 1000 11000)
     :rtg-max-var (random-float rng 500 5000)
     :rtg-max-var-neg (random-float rng 500 5000)
     :rtg-max-a (random-float rng 10 50)
     :rtg-max-v (random-float rng 400 500)
     :rtg-min-v (random-float rng 180 220)
     :rtg-v-nom (random-float rng 230 240)
     :rtg-max-charge-rate-w (random-float rng 2000 5000)
     :rtg-max-charge-rate-va (random-float rng 2000 5500)
     :rtg-max-discharge-rate-w (random-float rng 2000 5000)
     :rtg-max-discharge-rate-va (random-float rng 2000 5500)
     :rtg-min-pf-over-excited (random-float rng 0.8 0.95)
     :rtg-min-pf-under-excited (random-float rng 0.8 0.95)

     ;; Setpoint limits (set_*)
     :set-max-w (random-float rng 500 5000)
     :set-max-wh (random-float rng 2500 25000)
     :set-max-va (random-float rng 500 5500)
     :set-max-var (random-float rng 250 2500)
     :set-max-var-neg (random-float rng 250 2500)
     :set-max-charge-rate-w (random-float rng 1000 4000)
     :set-max-discharge-rate-w (random-float rng 1000 4000)
     :set-grad-w (random-float rng 100 1000)

     ;; Modes (both string and set representations like production)
     :modes-enabled (clojure.string/join "," modes-enabled)
     :modes-enabled-set modes-enabled
     :modes-supported (clojure.string/join "," modes-supported)
     :modes-supported-set modes-supported
     :doe-modes-enabled (clojure.string/join "," doe-modes-enabled)
     :doe-modes-enabled-set doe-modes-enabled
     :doe-modes-supported (clojure.string/join "," doe-modes-supported)
     :doe-modes-supported-set doe-modes-supported
     :vpp-modes-enabled (clojure.string/join "," vpp-modes-enabled)
     :vpp-modes-enabled-set vpp-modes-enabled
     :vpp-modes-supported (clojure.string/join "," vpp-modes-supported)
     :vpp-modes-supported-set vpp-modes-supported

     ;; Credential/controller fields (sparse, like production)
     :certificate-credential-id (when (random/chance? rng 0.3)
                                   (str "cert-" (random/next-uuid rng)))
     :controller-listing-id (when (random/chance? rng 0.4)
                              (str "ctrl-" (random/next-uuid rng)))

     :updated-time (double (System/currentTimeMillis))}))

(defn generate-device [rng device-id system-id device-model-id base-time]
  {:xt/id device-id
   :system-id system-id
   :device-model-id device-model-id
   :serial-number (str "SN-" (random/next-uuid rng))
   :installed-at base-time})

;; Registration test tables - for cumulative registration query

(defn generate-test-suite [suite-id]
  {:xt/id suite-id
   :purpose "REGISTRATION"
   :name "System Registration Test Suite"})

(defn generate-test-case [case-id suite-id case-num]
  {:xt/id case-id
   :test-suite-id suite-id
   :name (str "Test Case " case-num)
   :description (str "Registration check " case-num)})

(defn generate-test-suite-run [rng run-id system-id suite-id base-time]
  (let [passed? (random/chance? rng 0.8)
        test-start (.plusSeconds base-time 43200)] ; 12 hours after system creation
    {:xt/id run-id
     :system-id system-id
     :test-suite-id suite-id
     :status (if passed? "DONE" "FAILED")
     :passed? passed?
     :started-at test-start
     :completed-at (.plusSeconds test-start 300)}))

(defn generate-test-case-run [rng run-id suite-run-id case-id suite-passed? base-time]
  (let [case-passed? (or suite-passed? (random/chance? rng 0.7))
        test-start (.plusSeconds base-time 43200)] ; Same as suite run start
    {:xt/id run-id
     :test-suite-run-id suite-run-id
     :test-case-id case-id
     :status (if case-passed? "OK" "FAILED")
     :executed-at test-start}))

(defn ->init-tables-stage
  [system-ids site-ids organisation-ids device-series-ids device-model-ids device-ids test-suite-id test-case-ids batch-size base-time]
  {:t :call, :stage :init-tables
   :f (fn [{:keys [node random]}]
        (log/infof "Inserting %d organisation records" (count organisation-ids))
        (xt/submit-tx node [(into [:put-docs :organisation]
                                  (map-indexed (fn [idx org-id]
                                                 (generate-organisation org-id (nth organisation-names idx)))
                                               organisation-ids))])

        (log/infof "Inserting %d device_series records" (count device-series-ids))
        (xt/submit-tx node [(into [:put-docs :device_series]
                                  (map-indexed (fn [idx series-id]
                                                 (let [org-idx (quot idx (count series-names))
                                                       org-id (nth organisation-ids org-idx)
                                                       series-name (nth series-names (mod idx (count series-names)))]
                                                   (generate-device-series series-id org-id series-name)))
                                               device-series-ids))])

        (log/infof "Inserting %d device_model records" (count device-model-ids))
        (xt/submit-tx node [(into [:put-docs :device_model]
                                  (map-indexed (fn [idx model-id]
                                                 (let [series-idx (quot idx (count model-names))
                                                       series-id (nth device-series-ids series-idx)
                                                       model-name (nth model-names (mod idx (count model-names)))]
                                                   (generate-device-model random model-id series-id model-name)))
                                               device-model-ids))])

        (log/infof "Inserting %d site records" (count site-ids))
        (doseq [batch (partition-all batch-size site-ids)]
          (xt/submit-tx node [(into [:put-docs :site] (map #(generate-site random %) batch))]))

        (log/infof "Inserting %d system records" (count system-ids))
        (doseq [[sys-batch site-batch] (map vector
                                            (partition-all batch-size system-ids)
                                            (partition-all batch-size site-ids))]
          (xt/submit-tx node [(into [:put-docs :system]
                                    (map (fn [sid nmi] (generate-system-record random sid nmi base-time))
                                         sys-batch site-batch))]))

        (log/infof "Inserting %d device records" (count device-ids))
        (doseq [batch (partition-all batch-size device-ids)]
          (xt/submit-tx node [(into [:put-docs :device]
                                    (map #(generate-device random %
                                                           (random/uniform-nth random system-ids)
                                                           (random/uniform-nth random device-model-ids)
                                                           base-time)
                                         batch))]))

        ;; Insert test suite and test cases for registration tracking
        (log/infof "Inserting test suite and %d test cases" (count test-case-ids))
        (xt/submit-tx node [(into [:put-docs :test_suite] [(generate-test-suite test-suite-id)])])
        (xt/submit-tx node [(into [:put-docs :test_case]
                                  (map-indexed #(generate-test-case %2 test-suite-id %1) test-case-ids))])

        ;; Insert test suite runs and case runs for each system
        ;; Simulate 80% pass rate
        (log/infof "Inserting test suite runs for %d systems" (count system-ids))
        (doseq [system-id system-ids]
          (let [suite-run-id (random/next-uuid random)
                suite-run (generate-test-suite-run random suite-run-id system-id test-suite-id base-time)
                suite-passed? (:passed? suite-run)]
            (xt/submit-tx node [(into [:put-docs :test_suite_run]
                                      [(dissoc suite-run :passed?)])])
            (xt/submit-tx node [(into [:put-docs :test_case_run]
                                      (map (fn [case-id]
                                             (generate-test-case-run random
                                                                     (random/next-uuid random)
                                                                     suite-run-id
                                                                     case-id
                                                                     suite-passed?
                                                                     base-time))
                                           test-case-ids))]))))})

(defn ->readings-docs [rng system-ids reading-idx start end]
  (into [:put-docs {:into :readings :valid-from start :valid-to end}]
        (for [system-id system-ids]
          {:xt/id (str system-id "-" reading-idx)
           :system-id system-id
           :value (random-float rng -100 100)
           :duration 300})))

(defn ->ingest-readings-stage [system-ids readings batch-size base-time]
  {:t :call, :stage :ingest-readings
   :f (fn [{:keys [node random]}]
        (log/infof "Inserting %d readings for %d systems" readings (count system-ids))
        (let [intervals (->> (tu/->instants :minute 5 base-time)
                             (partition 2 1)
                             (take readings))
              batches (partition-all batch-size system-ids)
              base-system-time (Instant/now)]
          (doseq [[idx [start end]] (map-indexed vector intervals)]
            (when (zero? (mod idx 1000))
              (log/infof "Readings batch %d" idx))
            ;; Bimodal system-time lag matching production: 80% ~seconds, 20% ~5min
            ;; Calculate once per interval to maintain monotonic system-time
            (let [lag-seconds (if (random/chance? random 0.8)
                                (random/next-int random 6)
                                (+ 280 (random/next-int random 41)))
                  system-time (-> base-system-time
                                  (.plusSeconds (* idx 300))
                                  (.plusSeconds lag-seconds))]
              (doseq [batch batches]
                (xt/submit-tx node
                              [(->readings-docs random (vec batch) idx start end)]
                              {:system-time system-time}))))))})

(defn ->update-system-stage [system-ids updates-per-system update-batch-size]
  {:t :call, :stage :update-system
   :f (fn [{:keys [node random]}]
        (log/infof "Running %d UPDATE rounds" updates-per-system)
        (dotimes [round updates-per-system]
          (doseq [batch (partition-all update-batch-size system-ids)]
            (doseq [sid batch]
              (xt/execute-tx node
                             [[:sql "UPDATE system SET updated_time = ?, set_max_w = ? WHERE _id = ?"
                               [(double (System/currentTimeMillis))
                                (random-float random 500 5000)
                                sid]]])))))})

(defn ->sync-stage []
  {:t :call, :stage :sync
   :f (fn [{:keys [node]}] (b/sync-node node (Duration/ofMinutes 5)))})

(defn ->compact-stage []
  {:t :call, :stage :compact
   :f (fn [{:keys [node]}]
        (b/finish-block! node)
        (b/compact! node))})

(defn query-system-settings
  "Simple point-in-time system lookup - common query pattern"
  [node system-id opts]
  (xt/q node ["SELECT *, _valid_from, _system_from FROM system WHERE _id = ?" system-id] opts))

(defn query-readings-for-system
  "Time-series scan for specific system - production metabase query

  NOTE: Production query is missing temporal join constraint (CONTAINS predicate).
  Without 'system._valid_time CONTAINS readings._valid_from', this produces a cartesian
  product - each reading is joined with ALL versions of the system. If a system has been
  updated N times, each reading appears N times in the result. Benchmarking the actual
  production query to capture realistic performance characteristics."
  [node system-id start end opts]
  (xt/q node ["SELECT readings._valid_to as reading_time, readings.value::float AS reading_value
               FROM readings FOR ALL VALID_TIME
               JOIN system FOR ALL VALID_TIME ON system._id = readings.system_id
               WHERE system._id = ? AND readings._valid_from >= ? AND readings._valid_from < ?
               ORDER BY reading_time" system-id start end] opts))

(defn query-system-count-over-time
  "Complex temporal aggregation with multi-table joins - production analytics query

  Matches production 'System count over a period' query with full table hierarchy:
  system -> device -> device_model -> device_series -> organisation, plus site."
  [node start end opts]
  (xt/q node ["WITH dates AS (
                 SELECT d::timestamptz AS d
                 FROM generate_series(DATE_BIN(INTERVAL 'PT1H', ?::timestamptz), ?::timestamptz, INTERVAL 'PT1H') AS x(d)
               )
               SELECT dates.d, COUNT(DISTINCT system._id) AS c
               FROM dates
               LEFT OUTER JOIN system ON system._valid_time CONTAINS dates.d
               LEFT OUTER JOIN device ON device.system_id = system._id AND device._valid_time CONTAINS dates.d
               LEFT OUTER JOIN device_model ON device_model._id = device.device_model_id AND device_model._valid_time CONTAINS dates.d
               LEFT OUTER JOIN device_series ON device_series._id = device_model.device_series_id AND device_series._valid_time CONTAINS dates.d
               LEFT OUTER JOIN organisation ON organisation._id = device_series.organisation_id AND organisation._valid_time CONTAINS dates.d
               LEFT OUTER JOIN site ON site._id = system.nmi AND site._valid_time CONTAINS dates.d
               GROUP BY dates.d
               ORDER BY dates.d" start end] opts))

(defn query-readings-range-bins
  "Hourly aggregation using range_bins - production power readings query"
  [node start end opts]
  (xt/q node ["WITH corrected_readings AS (
                 SELECT r.*, r._valid_from, r._valid_to,
                        (bin)._from AS corrected_from,
                        (bin)._weight AS corrected_weight,
                        r.value * (bin)._weight AS corrected_portion
                 FROM readings AS r, UNNEST(range_bins(INTERVAL 'PT1H', r._valid_time)) AS b(bin)
                 WHERE r._valid_from >= ? AND r._valid_from < ?
               )
               SELECT corrected_from AS t, SUM(corrected_portion) / SUM(corrected_weight) AS value
               FROM corrected_readings
               GROUP BY corrected_from
               ORDER BY t" start end] opts))

(defn query-cumulative-registration
  "Complex registration tracking query with multiple CTEs, window functions, and multi-way temporal joins"
  [node start end opts]
  (xt/q node ["WITH gen AS (
                 SELECT d::timestamptz AS t
                 FROM generate_series(?::timestamptz, ?::timestamptz, INTERVAL 'PT1H') AS x(d)
               ),
               latest_test_suite_run AS (
                 SELECT ranked.* FROM (
                   SELECT gen.t,
                          test_suite_run.*,
                          ROW_NUMBER() OVER (
                            PARTITION BY gen.t, test_suite_run.system_id
                            ORDER BY test_suite_run._system_from DESC
                          ) AS rn
                   FROM gen
                   JOIN test_suite_run ON test_suite_run._valid_time CONTAINS gen.t
                   JOIN test_suite ON test_suite._id = test_suite_run.test_suite_id
                                   AND test_suite._valid_time CONTAINS gen.t
                 ) ranked WHERE ranked.rn = 1
               ),
               expected_test_cases AS (
                 SELECT latest_test_suite_run.t AS t,
                        latest_test_suite_run._id AS test_suite_run_id,
                        COUNT(*) AS count
                 FROM latest_test_suite_run
                 JOIN test_suite ON test_suite._id = latest_test_suite_run.test_suite_id
                                 AND test_suite._valid_time CONTAINS latest_test_suite_run.t
                 JOIN test_case ON test_case.test_suite_id = test_suite._id
                                AND test_case._valid_time CONTAINS latest_test_suite_run.t
                 GROUP BY latest_test_suite_run.t, latest_test_suite_run._id
               ),
               passing_test_cases AS (
                 SELECT latest_test_suite_run.t AS t,
                        latest_test_suite_run._id AS test_suite_run_id,
                        COUNT(*) AS count
                 FROM latest_test_suite_run
                 JOIN test_case_run ON test_case_run.test_suite_run_id = latest_test_suite_run._id
                                    AND test_case_run._valid_time CONTAINS latest_test_suite_run.t
                 WHERE test_case_run.status = 'OK'
                 GROUP BY latest_test_suite_run.t, latest_test_suite_run._id
               ),
               data AS (
                 SELECT gen.t,
                        system._id AS system_id,
                        system.created_at AS created_at,
                        site._id IS NOT NULL AS site_linked,
                        COUNT(device._id) >= 1 AS devices_linked,
                        COALESCE(latest_test_suite_run.status = 'DONE', FALSE) AS test_suite_run_ok,
                        COALESCE(expected_test_cases.count, 0) AS expected_test_cases,
                        COALESCE(passing_test_cases.count, 0) AS passing_test_cases
                 FROM gen
                 JOIN system ON system._valid_time CONTAINS gen.t
                 LEFT OUTER JOIN site ON site._id = system.nmi AND site._valid_time CONTAINS gen.t
                 LEFT OUTER JOIN device ON device.system_id = system._id AND device._valid_time CONTAINS gen.t
                 LEFT OUTER JOIN device_model ON device_model._id = device.device_model_id AND device_model._valid_time CONTAINS gen.t
                 LEFT OUTER JOIN latest_test_suite_run ON latest_test_suite_run.system_id = system._id
                                                       AND latest_test_suite_run.t = gen.t
                 LEFT OUTER JOIN expected_test_cases ON expected_test_cases.test_suite_run_id = latest_test_suite_run._id
                                                     AND expected_test_cases.t = gen.t
                 LEFT OUTER JOIN passing_test_cases ON passing_test_cases.test_suite_run_id = latest_test_suite_run._id
                                                    AND passing_test_cases.t = gen.t
                 GROUP BY gen.t, system._id, system.created_at, site._id, latest_test_suite_run.status,
                          expected_test_cases.count, passing_test_cases.count
               ),
               data_with_status AS (
                 SELECT t,
                        system_id,
                        CASE
                          WHEN (site_linked AND devices_linked AND test_suite_run_ok
                                AND expected_test_cases = passing_test_cases) THEN 'Success'
                          WHEN (created_at + INTERVAL 'PT48H' < t) THEN 'Failed'
                          ELSE 'Pending'
                        END AS registration_status
                 FROM data
               )
               SELECT gen.t, registration_status, COUNT(system_id) AS c
               FROM gen
               LEFT OUTER JOIN data_with_status ON data_with_status.t = gen.t
               GROUP BY gen.t, registration_status
               ORDER BY gen.t, registration_status" start end] opts))

(defn ->query-stage [query-type]
  {:t :call
   :stage (keyword (str "query-" (name query-type)))
   :f (fn [{:keys [node !state]}]
        (let [{:keys [latest-completed-tx max-valid-time min-valid-time system-ids]} @!state
              opts {:current-time (:system-time latest-completed-tx)}
              sample-system-id (first system-ids)]
          (case query-type
            ;; Production queries
            :system-settings (query-system-settings node sample-system-id opts)
            :readings-for-system (query-readings-for-system node sample-system-id min-valid-time max-valid-time opts)
            :system-count-over-time (query-system-count-over-time node min-valid-time max-valid-time opts)
            :readings-range-bins (query-readings-range-bins node min-valid-time max-valid-time opts)
            :cumulative-registration (query-cumulative-registration node min-valid-time max-valid-time opts))))})

(defmethod b/cli-flags :fusion [_]
  [[nil "--devices DEVICES" "Number of systems" :parse-fn parse-long :default 10000]
   [nil "--readings READINGS" "Readings per system" :parse-fn parse-long :default 1000]
   [nil "--batch-size BATCH" "Insert batch size" :parse-fn parse-long :default 1000]
   [nil "--update-batch-size BATCH" "Update batch size" :parse-fn parse-long :default 30]
   [nil "--updates-per-system UPDATES" "UPDATE rounds" :parse-fn parse-long :default 10]
   ["-h" "--help"]])

(defmethod b/->benchmark :fusion [_ {:keys [devices readings batch-size update-batch-size
                                            updates-per-system seed no-load?]
                                     :or {seed 0 batch-size 1000 update-batch-size 30
                                          updates-per-system 10}}]
  (let [setup-rng (java.util.Random. seed)
        base-time (.minus (Instant/now) (Duration/ofDays 3))
        system-ids (generate-ids setup-rng devices)
        site-ids (mapv #(str "NMI-" %) (range devices))
        organisation-ids (generate-ids setup-rng 5)
        device-series-ids (generate-ids setup-rng 25) ; 5 series per org
        device-model-ids (generate-ids setup-rng 50) ; 2 models per series
        device-ids (generate-ids setup-rng (* devices 2))
        test-suite-id (random/next-uuid setup-rng)
        test-case-ids (generate-ids setup-rng 5)] ; 5 test cases per suite
    (log/info {:devices devices :readings readings :batch-size batch-size
               :update-batch-size update-batch-size :updates-per-system updates-per-system})
    {:title "Fusion benchmark"
     :benchmark-type :fusion
     :seed seed
     :parameters {:devices devices :readings readings :batch-size batch-size
                  :update-batch-size update-batch-size :updates-per-system updates-per-system}
     :->state #(do {:!state (atom {:system-ids system-ids})})
     :tasks (concat
             (when-not no-load?
               [(->init-tables-stage system-ids site-ids organisation-ids device-series-ids device-model-ids device-ids test-suite-id test-case-ids update-batch-size base-time)
                (->ingest-readings-stage system-ids readings batch-size base-time)
                (->update-system-stage system-ids updates-per-system update-batch-size)
                (->sync-stage)
                (->compact-stage)])

             [{:t :call
               :stage :setup-state
               :f (fn [{:keys [node !state]}]
                    (let [latest-completed-tx (-> (xt/status node)
                                                  (get-in [:latest-completed-txs "xtdb" 0]))
                          max-vt (-> (xt/q node "SELECT max(_valid_from) AS m FROM readings FOR ALL VALID_TIME")
                                     first :m)
                          min-vt (-> (xt/q node "SELECT min(_valid_from) AS m FROM readings FOR ALL VALID_TIME")
                                     first :m)]
                      (log/infof "Valid time range: %s to %s" min-vt max-vt)
                      (swap! !state into {:latest-completed-tx latest-completed-tx
                                          :max-valid-time max-vt
                                          :min-valid-time min-vt})))}]

             [;; Production queries from design partner
              (->query-stage :system-settings)
              (->query-stage :readings-for-system)
              (->query-stage :system-count-over-time)
              (->query-stage :readings-range-bins)
              (->query-stage :cumulative-registration)])}))

(t/deftest ^:benchmark run-fusion
  (let [path (util/->path "/tmp/fusion-bench")]
    (-> (b/->benchmark :fusion {:devices 100 :readings 100 :batch-size 50
                                :update-batch-size 10 :updates-per-system 5})
        (b/run-benchmark {:node-dir path}))))
