(ns xtdb.bench.fusion
  (:require [clojure.test :as t]
            [clojure.tools.logging :as log]
            [xtdb.api :as xt]
            [xtdb.bench :as b]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import (java.time Duration Instant)
           (java.util UUID)))

;; ============================================================================
;; FUSION BENCHMARK
;; ============================================================================
;;
;; A benchmark based on actual design partner usage patterns (both intended
;; and emergent) to catch regressions and enable optimization.
;;
;; ## Data Model (9 tables)
;;
;; Core tables:
;;   1. device_model    - Reference data (10 rows)
;;   2. site            - Location data (NMI meter IDs, ~= system count)
;;   3. system          - Main table, constantly updated (configurable count, default 10k)
;;   4. device          - Links systems to device models (2× system count)
;;   5. readings        - High volume time-series (system_id, value, valid_time)
;;
;; Registration test tables (for cumulative registration query):
;;   6. test_suite      - Test suite definitions (1 suite)
;;   7. test_case       - Test cases within suites (5 cases per suite)
;;   8. test_suite_run  - Suite executions per system (1 per system, 80% pass rate)
;;   9. test_case_run   - Case execution results (5 per suite run)
;;
;; ## Workload Characteristics (matching production patterns)
;;
;; Readings ingestion:
;;   - Batch size: ~1000 (production pattern, was <30 historically)
;;   - Insert frequency: every 5min of valid_time
;;   - System-time lag: bimodal distribution
;;     * 80%: 0-2ms delay (near real-time)
;;     * 20%: 0-50ms delay (delayed batch processing)
;;   - Expected overhead: ~6s for 1k readings (~0.6% of total runtime)
;;
;; System updates:
;;   - Small-batch UPDATEs: batch size <30 (production pattern)
;;   - Simulates constant trickle of system state changes (~every 5min)
;;   - Update frequency: configurable rounds (default 10)
;;
;; ## Query Suite (9 queries: 4 simple + 5 production)
;;
;; Simple queries (sanity checks):
;;   1. readings-aggregate        - Basic GROUP BY aggregation
;;   2. system-count-over-time    - generate_series + CONTAINS
;;   3. system-device-join        - Multi-table temporal join
;;   4. readings-with-system      - Join readings with system
;;
;; Production-inspired queries (from design partner):
;;   5. system-settings           - Point-in-time system lookup
;;   6. readings-for-system       - Time-series scan with temporal join
;;   7. system-count-with-joins   - Complex 4-table temporal aggregation
;;   8. readings-range-bins       - Hourly aggregation using range_bins()
;;   9. cumulative-registration   - 5 CTEs + window functions + 6-way join
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
;; Large scale (needs 12GB JVM):
;;   ./gradlew fusion -PdeviceCount=10000 -PreadingCount=10000 -PtwelveGBJvm
;;
;; Expected runtime: ~17-25 minutes for 10k×1k scale
;; Memory: 6GB default (2GB heap + 3GB direct), 12GB for large scale
;;
;; ============================================================================

(defn random-float [min max] (+ min (* (rand) (- max min))))
(defn random-int [min max] (+ min (rand-int (- max min))))
(defn random-nth [coll] (nth coll (rand-int (count coll))))

(def manufacturers ["AlphaCorp" "BetaTech" "GammaGrid" "DeltaPower" "EpsilonEnergy"])
(def models ["Model-A" "Model-B" "Model-C" "Model-D" "Model-E"])

(defn generate-ids [n] (vec (repeatedly n #(UUID/randomUUID))))

(defn generate-device-model [model-id]
  {:xt/id model-id
   :manufacturer (random-nth manufacturers)
   :model (random-nth models)
   :capacity-kw (random-float 5 15)})

(defn generate-site [nmi]
  {:xt/id nmi
   :address (str (random-int 1 999) " Solar Street")
   :postcode (str (random-int 1000 9999))
   :state (random-nth ["NSW" "VIC" "QLD" "SA" "WA"])})

(defn generate-system-record [system-id nmi]
  {:xt/id system-id
   :nmi nmi
   :type (rand-int 10)
   :created-at (Instant/parse "2020-01-01T00:00:00Z")
   :registration-date (Instant/parse "2020-01-01T00:00:00Z")
   :rtg-max-w (random-float 1000 10000)
   :rtg-max-wh (random-float 5000 50000)
   :set-max-w (random-float 500 5000)
   :modes-enabled "default,eco"
   :updated-time (double (System/currentTimeMillis))})

(defn generate-device [device-id system-id device-model-id]
  {:xt/id device-id
   :system-id system-id
   :device-model-id device-model-id
   :serial-number (str "SN-" (UUID/randomUUID))
   :installed-at (Instant/parse "2020-01-01T00:00:00Z")})

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

(defn generate-test-suite-run [run-id system-id suite-id passed?]
  {:xt/id run-id
   :system-id system-id
   :test-suite-id suite-id
   :status (if passed? "DONE" "FAILED")
   :started-at (Instant/parse "2020-01-01T12:00:00Z")
   :completed-at (Instant/parse "2020-01-01T12:05:00Z")})

(defn generate-test-case-run [run-id suite-run-id case-id passed?]
  {:xt/id run-id
   :test-suite-run-id suite-run-id
   :test-case-id case-id
   :status (if passed? "OK" "FAILED")
   :executed-at (Instant/parse "2020-01-01T12:00:00Z")})

(defn ->init-tables-stage
  [system-ids site-ids device-model-ids device-ids test-suite-id test-case-ids batch-size]
  {:t :call, :stage :init-tables
   :f (fn [{:keys [node]}]
        (log/infof "Inserting %d device_model records" (count device-model-ids))
        (xt/submit-tx node [(into [:put-docs :device_model]
                                  (map generate-device-model device-model-ids))])

        (log/infof "Inserting %d site records" (count site-ids))
        (doseq [batch (partition-all batch-size site-ids)]
          (xt/submit-tx node [(into [:put-docs :site] (map generate-site batch))]))

        (log/infof "Inserting %d system records" (count system-ids))
        (doseq [[sys-batch site-batch] (map vector
                                            (partition-all batch-size system-ids)
                                            (partition-all batch-size site-ids))]
          (xt/submit-tx node [(into [:put-docs :system]
                                    (map generate-system-record sys-batch site-batch))]))

        (log/infof "Inserting %d device records" (count device-ids))
        (doseq [batch (partition-all batch-size device-ids)]
          (xt/submit-tx node [(into [:put-docs :device]
                                    (map #(generate-device % (random-nth system-ids)
                                                           (random-nth device-model-ids))
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
          (let [suite-run-id (UUID/randomUUID)
                passed? (< (rand) 0.8)]
            (xt/submit-tx node [(into [:put-docs :test_suite_run]
                                      [(generate-test-suite-run suite-run-id system-id test-suite-id passed?)])])
            (xt/submit-tx node [(into [:put-docs :test_case_run]
                                      (map-indexed (fn [idx case-id]
                                                     ;; If suite passed, all cases pass; else random fails
                                                     (let [case-passed? (or passed? (< (rand) 0.7))]
                                                       (generate-test-case-run (UUID/randomUUID)
                                                                               suite-run-id
                                                                               case-id
                                                                               case-passed?)))
                                                   test-case-ids))]))))})

(defn ->readings-docs [system-ids reading-idx start end]
  (into [:put-docs {:into :readings :valid-from start :valid-to end}]
        (for [system-id system-ids]
          {:xt/id (str system-id "-" reading-idx)
           :system-id system-id
           :value (random-float -100 100)
           :duration 300})))

(defn ->ingest-readings-stage [system-ids readings batch-size]
  {:t :call, :stage :ingest-readings
   :f (fn [{:keys [node]}]
        (log/infof "Inserting %d readings for %d systems" readings (count system-ids))
        (let [intervals (->> (tu/->instants :minute 5 #inst "2020-01-01")
                             (partition 2 1)
                             (take readings))
              batches (partition-all batch-size system-ids)]
          (doseq [[idx [start end]] (map-indexed vector intervals)]
            (when (zero? (mod idx 1000))
              (log/infof "Readings batch %d" idx))
            (doseq [batch batches]
              (xt/submit-tx node [(->readings-docs (vec batch) idx start end)]))
            ;; Bimodal system-time lag: 80% near real-time, 20% delayed
            ;; Creates temporal scatter without significant overhead (~6s for 1k readings)
            (let [delay-ms (if (< (rand) 0.8)
                             (rand-int 2)       ;; 80%: 0-2ms (barely noticeable)
                             (rand-int 50))]    ;; 20%: 0-50ms (cluster gaps)
              (Thread/sleep delay-ms)))))})

(defn ->update-system-stage [system-ids updates-per-device update-batch-size]
  {:t :call, :stage :update-system
   :f (fn [{:keys [node]}]
        (log/infof "Running %d UPDATE rounds" updates-per-device)
        (dotimes [round updates-per-device]
          (doseq [batch (partition-all update-batch-size system-ids)]
            (doseq [sid batch]
              (xt/execute-tx node
                             [[:sql "UPDATE system SET updated_time = ?, set_max_w = ? WHERE _id = ?"
                               [(double (System/currentTimeMillis))
                                (random-float 500 5000)
                                sid]]])))))})

(defn ->sync-stage []
  {:t :call, :stage :sync
   :f (fn [{:keys [node]}] (b/sync-node node (Duration/ofMinutes 5)))})

(defn ->compact-stage []
  {:t :call, :stage :compact
   :f (fn [{:keys [node]}]
        (b/finish-block! node)
        (b/compact! node))})

(defn query-readings-aggregate [node start end opts]
  (xt/q node ["SELECT system_id, AVG(value) AS avg, MIN(value) AS min, MAX(value) AS max
               FROM readings FOR VALID_TIME BETWEEN ? AND ?
               GROUP BY system_id" start end] opts))

(defn query-system-count-over-time [node start end opts]
  (xt/q node ["WITH dates AS (
                 SELECT d::timestamptz AS d
                 FROM generate_series(?::timestamptz, ?::timestamptz, INTERVAL 'PT1H') AS x(d)
               )
               SELECT dates.d, COUNT(DISTINCT system._id) AS c
               FROM dates
               LEFT OUTER JOIN system ON system._valid_time CONTAINS dates.d
               GROUP BY dates.d
               ORDER BY dates.d" start end] opts))

(defn query-system-device-join [node point-in-time opts]
  (xt/q node ["SELECT s._id AS system_id, s.nmi, d._id AS device_id, dm.manufacturer, dm.model
               FROM system s
               JOIN device d ON d.system_id = s._id AND d._valid_time CONTAINS ?
               JOIN device_model dm ON dm._id = d.device_model_id AND dm._valid_time CONTAINS ?
               WHERE s._valid_time CONTAINS ?" point-in-time point-in-time point-in-time] opts))

(defn query-readings-with-system [node start end opts]
  (xt/q node ["SELECT s._id AS system_id, s.nmi, r.value, r._valid_from, r._valid_to
               FROM readings r
               JOIN system s ON s._id = r.system_id AND s._valid_time CONTAINS r._valid_from
               WHERE r._valid_from >= ? AND r._valid_from < ?
               LIMIT 1000" start end] opts))

;; Production-inspired queries from design partner usage patterns

(defn query-system-settings [node system-id opts]
  "Simple point-in-time system lookup - common query pattern"
  (xt/q node ["SELECT *, _valid_from, _system_from FROM system WHERE _id = ?" system-id] opts))

(defn query-readings-for-system [node system-id start end opts]
  "Time-series scan for specific system - production metabase query

  NOTE: Production query is missing temporal join constraint (CONTAINS predicate).
  Without 'system._valid_time CONTAINS readings._valid_from', this produces a cartesian
  product - each reading is joined with ALL versions of the system. If a system has been
  updated N times, each reading appears N times in the result. Benchmarking the actual
  production query to capture realistic performance characteristics."
  (xt/q node ["SELECT readings._valid_to as reading_time, readings.value::float AS reading_value
               FROM readings FOR ALL VALID_TIME
               JOIN system FOR ALL VALID_TIME ON system._id = readings.system_id
               WHERE system._id = ? AND readings._valid_from >= ? AND readings._valid_from < ?
               ORDER BY reading_time" system-id start end] opts))

(defn query-system-count-with-joins [node start end opts]
  "Complex temporal aggregation with multi-table joins - production analytics query"
  (xt/q node ["WITH dates AS (
                 SELECT d::timestamptz AS d
                 FROM generate_series(?::timestamptz, ?::timestamptz, INTERVAL 'PT1H') AS x(d)
               )
               SELECT dates.d, COUNT(DISTINCT system._id) AS c
               FROM dates
               LEFT OUTER JOIN system ON system._valid_time CONTAINS dates.d
               LEFT OUTER JOIN device ON device.system_id = system._id AND device._valid_time CONTAINS dates.d
               LEFT OUTER JOIN device_model ON device_model._id = device.device_model_id AND device_model._valid_time CONTAINS dates.d
               LEFT OUTER JOIN site ON site._id = system.nmi AND site._valid_time CONTAINS dates.d
               GROUP BY dates.d
               ORDER BY dates.d" start end] opts))

(defn query-readings-range-bins [node start end opts]
  "Hourly aggregation using range_bins - production power readings query"
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

(defn query-cumulative-registration [node start end opts]
  "Complex registration tracking query with 4 CTEs, window functions, and multi-table temporal joins"
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
            ;; Original simple queries
            :readings-aggregate (query-readings-aggregate node min-valid-time max-valid-time opts)
            :system-count-over-time (query-system-count-over-time node min-valid-time max-valid-time opts)
            :system-device-join (query-system-device-join node max-valid-time opts)
            :readings-with-system (query-readings-with-system node min-valid-time max-valid-time opts)

            ;; Production-inspired queries
            :system-settings (query-system-settings node sample-system-id opts)
            :readings-for-system (query-readings-for-system node sample-system-id min-valid-time max-valid-time opts)
            :system-count-with-joins (query-system-count-with-joins node min-valid-time max-valid-time opts)
            :readings-range-bins (query-readings-range-bins node min-valid-time max-valid-time opts)
            :cumulative-registration (query-cumulative-registration node min-valid-time max-valid-time opts))))})

(defmethod b/cli-flags :fusion [_]
  [[nil "--devices DEVICES" "Number of systems" :parse-fn parse-long :default 10000]
   [nil "--readings READINGS" "Readings per system" :parse-fn parse-long :default 1000]
   [nil "--batch-size BATCH" "Insert batch size" :parse-fn parse-long :default 1000]
   [nil "--update-batch-size BATCH" "Update batch size" :parse-fn parse-long :default 30]
   [nil "--updates-per-device UPDATES" "UPDATE rounds" :parse-fn parse-long :default 10]
   ["-h" "--help"]])

(defmethod b/->benchmark :fusion [_ {:keys [devices readings batch-size update-batch-size
                                            updates-per-device seed no-load?]
                                     :or {seed 0 batch-size 1000 update-batch-size 30
                                          updates-per-device 10}}]
  (let [system-ids (generate-ids devices)
        site-ids (mapv #(str "NMI-" %) (range devices))
        device-model-ids (generate-ids 10)
        device-ids (generate-ids (* devices 2))
        test-suite-id (UUID/randomUUID)
        test-case-ids (generate-ids 5)] ; 5 test cases per suite
    (log/info {:devices devices :readings readings :batch-size batch-size
               :update-batch-size update-batch-size :updates-per-device updates-per-device})
    {:title "Fusion benchmark"
     :benchmark-type :fusion
     :seed seed
     :parameters {:devices devices :readings readings :batch-size batch-size
                  :update-batch-size update-batch-size :updates-per-device updates-per-device}
     :->state #(do {:!state (atom {:system-ids system-ids})})
     :tasks (concat
             (when-not no-load?
               [(->init-tables-stage system-ids site-ids device-model-ids device-ids test-suite-id test-case-ids update-batch-size)
                (->ingest-readings-stage system-ids readings batch-size)
                (->update-system-stage system-ids updates-per-device update-batch-size)
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

             [;; Original simple queries
              (->query-stage :readings-aggregate)
              (->query-stage :system-count-over-time)
              (->query-stage :system-device-join)
              (->query-stage :readings-with-system)

              ;; Production-inspired queries from design partner
              (->query-stage :system-settings)
              (->query-stage :readings-for-system)
              (->query-stage :system-count-with-joins)
              (->query-stage :readings-range-bins)
              (->query-stage :cumulative-registration)])}))

(t/deftest ^:benchmark run-fusion
  (let [path (util/->path "/tmp/fusion-bench")]
    (-> (b/->benchmark :fusion {:devices 100 :readings 100 :batch-size 50
                                :update-batch-size 10 :updates-per-device 5})
        (b/run-benchmark {:node-dir path}))))
