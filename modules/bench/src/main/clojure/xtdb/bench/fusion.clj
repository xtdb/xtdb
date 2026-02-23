(ns xtdb.bench.fusion
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [clojure.test :as t]
            [clojure.tools.logging :as log]
            [hugsql.core :as hugsql]
            [xtdb.api :as xt]
            [xtdb.bench :as b]
            [xtdb.bench.random :as random]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import (java.time Duration Instant)
           (java.util UUID)
           (java.util.concurrent.atomic AtomicLong)))

;; See fusion.md for benchmark documentation

(hugsql/def-sqlvec-fns "xtdb/bench/fusion.sql")

(def organisation-names ["AlphaCorp" "BetaTech" "GammaGrid" "DeltaPower" "EpsilonEnergy"])
(def series-names ["Series-A" "Series-B" "Series-C" "Series-D" "Series-E"])
(def model-names ["Model-1" "Model-2"])

(defn random-float [^java.util.Random rng min max]
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

(defn generate-site [rng site-id]
  {:xt/id site-id
   :address (str (random-int rng 1 999) " Solar Street")
   :postcode (str (random-int rng 1000 9999))
   :state (random/uniform-nth rng ["NSW" "VIC" "QLD" "SA" "WA"])})

(defn generate-system-record [rng system-id site-id base-time]
  (let [modes-enabled ["default" "eco"]
        modes-supported ["default" "eco" "grid-charge" "grid-discharge"]
        feature-a-modes-enabled ["feature-a-1"]
        feature-a-modes-supported ["feature-a-1" "feature-a-2" "feature-a-3"]
        feature-b-modes-enabled ["feature-b-1" "feature-b-2"]
        feature-b-modes-supported ["feature-b-1" "feature-b-2" "feature-b-3"]]
    {:xt/id system-id
     :site-id site-id
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
     :modes-enabled (str/join "," modes-enabled)
     :modes-enabled-set modes-enabled
     :modes-supported (str/join "," modes-supported)
     :modes-supported-set modes-supported
     :feature-a-modes-enabled (str/join "," feature-a-modes-enabled)
     :feature-a-modes-enabled-set feature-a-modes-enabled
     :feature-a-modes-supported (str/join "," feature-a-modes-supported)
     :feature-a-modes-supported-set feature-a-modes-supported
     :feature-b-modes-enabled (str/join "," feature-b-modes-enabled)
     :feature-b-modes-enabled-set feature-b-modes-enabled
     :feature-b-modes-supported (str/join "," feature-b-modes-supported)
     :feature-b-modes-supported-set feature-b-modes-supported

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

(defn generate-test-suite-run [rng run-id system-id suite-id ^java.time.Instant base-time]
  (let [passed? (random/chance? rng 0.8)
        test-start (.plusSeconds base-time 43200)] ; 12 hours after system creation
    {:xt/id run-id
     :system-id system-id
     :test-suite-id suite-id
     :status (if passed? "DONE" "FAILED")
     :passed? passed?
     :started-at test-start
     :completed-at (.plusSeconds ^Instant test-start 300)}))

(defn generate-test-case-run [rng run-id suite-run-id case-id suite-passed? ^java.time.Instant base-time]
  (let [case-passed? (or suite-passed? (random/chance? rng 0.7))
        test-start (.plusSeconds base-time 43200)] ; Same as suite run start
    {:xt/id run-id
     :test-suite-run-id suite-run-id
     :test-case-id case-id
     :status (if case-passed? "OK" "FAILED")
     :executed-at test-start}))
(def control-event-ids ["event-alpha" "event-beta" "event-gamma" "event-delta"])

(defn generate-control-plans
  "Generate sparse control plan records. Each event spans 1-2 hours of valid time,
   with control values distributed across systems."
  [rng system-ids base-time]
  (let [num-events (+ 3 (random/next-int rng 2))] ; 3-4 events
    (for [event-idx (range num-events)
          :let [event-id (nth control-event-ids event-idx)
                ;; Spread events across the time range
                ^Instant event-start (.plus ^Instant base-time (Duration/ofHours (* event-idx 12)))
                event-duration (Duration/ofHours (+ 1 (random/next-int rng 2)))
                event-end (.plus event-start event-duration)]
          system-id (take (+ 5 (random/next-int rng 10)) (random/shuffle rng system-ids))]
      {:xt/id (str system-id "-cp-" event-id)
       :control-value (random-float rng 500 5000)
       :event-id event-id
       :status "ACTIVE"
       :valid-from event-start
       :valid-to event-end})))


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
                                    (map (fn [sid site-id] (generate-system-record random sid site-id base-time))
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
                                           test-case-ids))])))

        ;; Insert control plan records for multi-reading aggregation query
        ;; Group by event since all plans within an event share the same temporal range
        (let [plans (generate-control-plans random system-ids base-time)
              by-event (group-by (juxt :valid-from :valid-to) plans)]
          (log/infof "Inserting %d control plan records across %d event windows"
                     (count plans) (count by-event))
          (doseq [[[vf vt] event-plans] by-event]
            (xt/submit-tx node [(into [:put-docs {:into :control_plan
                                                  :valid-from vf
                                                  :valid-to vt}]
                                      (map #(-> %
                                                (dissoc :valid-from :valid-to)
                                                (set/rename-keys {:control-value :control_value
                                                                          :event-id :event_id}))
                                           event-plans))]))))})

(defn ->reading-a-docs
  "System active power readings (watts). 5 min cadence.
   Used by both the existing per-system queries and the multi-reading aggregation query."
  [rng system-ids reading-idx start end]
  (into [:put-docs {:into :reading_a :valid-from start :valid-to end}]
        (for [system-id system-ids]
          {:xt/id (str system-id "-a-" reading-idx)
           :system-id system-id
           :value (random-float rng -10000 10000)
           :duration 300})))

(defn ->reading-b-docs
  "Site active power readings (watts). Same cadence as readings (5 min)."
  [rng site-ids reading-idx start end]
  (into [:put-docs {:into :reading_b :valid-from start :valid-to end}]
        (for [site-id site-ids]
          {:xt/id (str site-id "-b-" reading-idx)
           :value (random-float rng -10000 10000)})))

(defn ->reading-c-docs
  "System state of charge readings (watt-hours). Slower cadence (15 min)."
  [rng system-ids reading-idx start end]
  (into [:put-docs {:into :reading_c :valid-from start :valid-to end}]
        (for [system-id system-ids]
          {:xt/id (str system-id "-c-" reading-idx)
           :value (random-float rng 0 50000)})))

(defn generate-reading-system-times
  "Generate system-times for readings with bimodal lag distribution.
  Returns vector of [idx system-time] pairs."
  [random interval-count ^Instant base-system-time]
  (loop [idx 0
         ^Instant last-time base-system-time
         result []]
    (if (>= idx interval-count)
      result
      (let [lag-seconds (if (random/chance? random 0.8)
                          (random/next-int random 6)
                          (+ 280 (random/next-int random 41)))
            ^Instant calculated-time (.plusSeconds (.plusSeconds base-system-time (long (* idx 300)))
                                                  (long lag-seconds))
            system-time (if (.isAfter calculated-time last-time)
                          calculated-time
                          (.plusMillis last-time 1))]
        (recur (inc idx)
               system-time
               (conj result [idx system-time]))))))

(defn do-update-round
  "Execute a single update round for given systems. Returns remaining active systems (with attrition)."
  [node random active-systems update-batch-size]
  (doseq [batch (partition-all update-batch-size active-systems)]
    (doseq [sid batch]
      (xt/execute-tx node
                     [[:sql "UPDATE system SET updated_time = ?, set_max_w = ? WHERE _id = ?"
                       [(double (System/currentTimeMillis))
                        (random-float random 500 5000)
                        sid]]])))
  ;; Return 90% of systems for next round (attrition)
  (->> active-systems
       (random/shuffle random)
       (take (int (* 0.9 (count active-systems))))
       vec))

(defn ->ingest-interleaved-stage
  "Interleave readings ingestion with system updates.
   Updates happen every (readings/updates-per-system) intervals, simulating
   realistic mixed workload where readings and updates arrive concurrently.
   Also inserts reading_a (5 min), reading_b (5 min), and reading_c (15 min) for
   the multi-reading aggregation query."
  [system-ids site-ids readings updates-per-system batch-size update-batch-size base-time]
  {:t :call, :stage :ingest-interleaved
   :f (fn [{:keys [node random]}]
        (log/infof "Inserting %d readings with %d interleaved update rounds" readings updates-per-system)
        (let [intervals (->> (tu/->instants :minute 5 base-time)
                             (partition 2 1)
                             (take readings))
              sys-batches (partition-all batch-size system-ids)
              site-batches (partition-all batch-size site-ids)
              base-system-time (Instant/now)
              system-times (generate-reading-system-times random readings base-system-time)
              update-interval (max 1 (quot readings updates-per-system))]
          (loop [reading-data (map vector system-times intervals)
                 active-systems (vec system-ids)
                 update-round 0]
            (when-let [[[idx system-time] [start end]] (first reading-data)]
              ;; Insert reading_a (system power) + reading_b (site power) at 5 min cadence
              (when (zero? (mod idx 1000))
                (log/infof "Readings batch %d" idx))
              (doseq [batch sys-batches]
                (xt/submit-tx node
                              [(->reading-a-docs random (vec batch) idx start end)]
                              {:system-time system-time}))
              ;; reading_b keyed by site
              (doseq [batch site-batches]
                (xt/submit-tx node
                              [(->reading-b-docs random (vec batch) idx start end)]
                              {:system-time system-time}))
              ;; reading_c at 15 min cadence (every 3rd 5-min interval)
              (when (zero? (mod idx 3))
                (let [c-end (.plus ^Instant start (Duration/ofMinutes 15))]
                  (doseq [batch sys-batches]
                    (xt/submit-tx node
                                  [(->reading-c-docs random (vec batch) (quot idx 3) start c-end)]
                                  {:system-time system-time}))))
              ;; Do update round at intervals (if we have rounds remaining)
              (let [should-update? (and (pos? (count active-systems))
                                        (< update-round updates-per-system)
                                        (zero? (mod (inc idx) update-interval)))
                    new-active (if should-update?
                                 (do
                                   (log/infof "Update round %d at reading %d: %d active systems"
                                              (inc update-round) idx (count active-systems))
                                   (do-update-round node random active-systems update-batch-size))
                                 active-systems)
                    new-round (if should-update? (inc update-round) update-round)]
                (recur (rest reading-data) new-active new-round))))))})

;; Keep separate stages for backwards compat / testing
(defn ->ingest-readings-stage [system-ids readings batch-size base-time]
  {:t :call, :stage :ingest-readings
   :f (fn [{:keys [node random]}]
        (log/infof "Inserting %d readings for %d systems" readings (count system-ids))
        (let [intervals (->> (tu/->instants :minute 5 base-time)
                             (partition 2 1)
                             (take readings))
              batches (partition-all batch-size system-ids)
              base-system-time (Instant/now)
              system-times (generate-reading-system-times random readings base-system-time)]
          (doseq [[[idx system-time] [start end]] (map vector system-times intervals)]
            (when (zero? (mod idx 1000))
              (log/infof "Readings batch %d" idx))
            (doseq [batch batches]
              (xt/submit-tx node
                            [(->reading-a-docs random (vec batch) idx start end)]
                            {:system-time system-time})))))})

(defn ->update-system-stage [system-ids updates-per-system update-batch-size]
  {:t :call, :stage :update-system
   :f (fn [{:keys [node random]}]
        (log/infof "Running %d UPDATE rounds with system attrition (10%% dropout per round)" updates-per-system)
        (loop [active-systems (vec system-ids)
               rounds-remaining updates-per-system]
          (when (and (seq active-systems) (pos? rounds-remaining))
            (log/infof "Update round %d: %d active systems"
                       (- updates-per-system rounds-remaining -1)
                       (count active-systems))
            (let [remaining (do-update-round node random active-systems update-batch-size)]
              (recur remaining (dec rounds-remaining))))))})

(defn ->sync-stage []
  {:t :call, :stage :sync
   :f (fn [{:keys [node]}] (b/sync-node node (Duration/ofMinutes 5)))})

(defn ->compact-stage []
  {:t :call, :stage :compact
   :f (fn [{:keys [node]}]
        (b/flush-block! node)
        (b/compact! node))})

(defn exec-system-settings [node system-id opts]
  (xt/q node (query-system-settings-sqlvec {:system-id system-id}) opts))

(defn exec-readings-for-system [node system-id start end opts]
  (xt/q node (query-readings-for-system-sqlvec {:system-id system-id :start start :end end}) opts))

(defn exec-system-count-over-time [node start end opts]
  (xt/q node (query-system-count-over-time-sqlvec {:start start :end end}) opts))

(defn exec-readings-range-bins [node start end opts]
  (xt/q node (query-readings-range-bins-sqlvec {:start start :end end}) opts))

(defn exec-cumulative-registration [node start end opts]
  (xt/q node (query-cumulative-registration-sqlvec {:start start :end end}) opts))

(defn exec-multi-reading-aggregation [node system-ids start end opts]
  (xt/q node (query-multi-reading-aggregation-sqlvec {:system-ids system-ids :start start :end end}) opts))

;; OLTP mixed workload procs

(defn proc-insert-readings
  "Insert a batch of reading_a for random systems"
  [{:keys [node random !state]}]
  (let [{:keys [system-ids base-time ^AtomicLong !reading-idx]} @!state
        idx (.getAndIncrement !reading-idx)
        ^Instant start (.plus ^Instant base-time (Duration/ofMinutes (* 5 idx)))
        end (.plus start (Duration/ofMinutes 5))
        batch-size (min 100 (count system-ids))
        batch (vec (repeatedly batch-size #(random/uniform-nth random system-ids)))]
    (xt/submit-tx node [(->reading-a-docs random batch idx start end)])))

(defn proc-insert-readings-b
  "Insert a batch of reading_b (site power) for random sites"
  [{:keys [node random !state]}]
  (let [{:keys [site-ids base-time ^AtomicLong !reading-b-idx]} @!state
        idx (.getAndIncrement !reading-b-idx)
        ^Instant start (.plus ^Instant base-time (Duration/ofMinutes (* 5 idx)))
        end (.plus start (Duration/ofMinutes 5))
        batch-size (min 100 (count site-ids))
        batch (vec (repeatedly batch-size #(random/uniform-nth random site-ids)))]
    (xt/submit-tx node [(->reading-b-docs random batch idx start end)])))

(defn proc-insert-readings-c
  "Insert a batch of reading_c (state of charge) for random systems. 15 min intervals."
  [{:keys [node random !state]}]
  (let [{:keys [system-ids base-time ^AtomicLong !reading-c-idx]} @!state
        idx (.getAndIncrement !reading-c-idx)
        ^Instant start (.plus ^Instant base-time (Duration/ofMinutes (* 15 idx)))
        end (.plus start (Duration/ofMinutes 15))
        batch-size (min 100 (count system-ids))
        batch (vec (repeatedly batch-size #(random/uniform-nth random system-ids)))]
    (xt/submit-tx node [(->reading-c-docs random batch idx start end)])))

(defn proc-update-system
  "Update a random system's fields"
  [{:keys [node random !state]}]
  (let [{:keys [system-ids]} @!state
        sid (random/uniform-nth random system-ids)]
    (xt/execute-tx node
                   [[:sql "UPDATE system SET updated_time = ?, set_max_w = ? WHERE _id = ?"
                     [(double (System/currentTimeMillis))
                      (random-float random 500 5000)
                      sid]]])))

(defn proc-query-system-settings
  "Query a random system's settings"
  [{:keys [node random !state]}]
  (let [{:keys [system-ids]} @!state
        sid (random/uniform-nth random system-ids)]
    (exec-system-settings node sid {})))

(defn proc-query-readings
  "Query readings for a random system over a time window"
  [{:keys [node random !state]}]
  (let [{:keys [system-ids min-valid-time max-valid-time]} @!state]
    (when (and min-valid-time max-valid-time)
      (let [sid (random/uniform-nth random system-ids)]
        (exec-readings-for-system node sid min-valid-time max-valid-time {})))))

(defn proc-query-system-count
  "Run the system count over time aggregation"
  [{:keys [node !state]}]
  (let [{:keys [min-valid-time max-valid-time]} @!state]
    (when (and min-valid-time max-valid-time)
      (exec-system-count-over-time node min-valid-time max-valid-time {}))))

(defn proc-query-range-bins
  "Run the range_bins aggregation query"
  [{:keys [node !state]}]
  (let [{:keys [min-valid-time max-valid-time]} @!state]
    (when (and min-valid-time max-valid-time)
      (exec-readings-range-bins node min-valid-time max-valid-time {}))))

(defn proc-query-registration
  "Run the cumulative registration query"
  [{:keys [node !state]}]
  (let [{:keys [min-valid-time max-valid-time]} @!state]
    (when (and min-valid-time max-valid-time)
      (exec-cumulative-registration node min-valid-time max-valid-time {}))))

(defn proc-query-multi-reading-aggregation
  "Run the multi-table reading aggregation with deltas and control plans.
   Targets ~30 systems to match production query patterns."
  [{:keys [node random !state]}]
  (let [{:keys [system-ids min-valid-time max-valid-time]} @!state]
    (when (and min-valid-time max-valid-time)
      (let [sample (vec (take 30 (random/shuffle random system-ids)))]
        (exec-multi-reading-aggregation node sample min-valid-time max-valid-time {})))))

(defn ->query-stage [query-type]
  {:t :call
   :stage (keyword (str "query-" (name query-type)))
   :f (fn [{:keys [node !state]}]
        (let [{:keys [latest-completed-tx max-valid-time min-valid-time system-ids]} @!state
              opts {:current-time (:system-time latest-completed-tx)}
              sample-system-id (first system-ids)]
          (case query-type
            :system-settings (exec-system-settings node sample-system-id opts)
            :readings-for-system (exec-readings-for-system node sample-system-id min-valid-time max-valid-time opts)
            :system-count-over-time (exec-system-count-over-time node min-valid-time max-valid-time opts)
            :readings-range-bins (exec-readings-range-bins node min-valid-time max-valid-time opts)
            :cumulative-registration (exec-cumulative-registration node min-valid-time max-valid-time opts)
            :multi-reading-aggregation (exec-multi-reading-aggregation node (vec (take 30 system-ids)) min-valid-time max-valid-time opts))))})

(defmethod b/cli-flags :fusion [_]
  [[nil "--devices DEVICES" "Number of systems" :parse-fn parse-long :default 10000]
   [nil "--readings READINGS" "Readings per system (load phase)" :parse-fn parse-long :default 1000]
   [nil "--batch-size BATCH" "Insert batch size" :parse-fn parse-long :default 1000]
   [nil "--update-batch-size BATCH" "Update batch size" :parse-fn parse-long :default 30]
   [nil "--updates-per-system UPDATES" "UPDATE rounds (load phase)" :parse-fn parse-long :default 10]
   ["-d" "--duration DURATION" "OLTP phase duration" :parse-fn #(Duration/parse %) :default (Duration/parse "PT30S")]
   ["-t" "--threads THREADS" "OLTP thread count" :parse-fn parse-long :default 4]
   [nil "--staged-only" "Run staged workload only, skip OLTP" :id :staged-only?]
   ["-h" "--help"]])

(defmethod b/->benchmark :fusion [_ {:keys [devices readings batch-size update-batch-size
                                            updates-per-system seed no-load? duration threads staged-only?]
                                     :or {seed 0 batch-size 1000 update-batch-size 30
                                          updates-per-system 10 duration (Duration/parse "PT30S") threads 4}}]
  (let [^Duration duration (cond-> duration (string? duration) Duration/parse)
        setup-rng (java.util.Random. seed)
        base-time (.minus (Instant/now) (Duration/ofDays 3))
        system-ids (generate-ids setup-rng devices)
        site-ids (mapv #(str "SITE-" %) (range devices))
        organisation-ids (generate-ids setup-rng 5)
        device-series-ids (generate-ids setup-rng 25) ; 5 series per org
        device-model-ids (generate-ids setup-rng 50) ; 2 models per series
        device-ids (generate-ids setup-rng (* devices 2))
        test-suite-id (random/next-uuid setup-rng)
        test-case-ids (generate-ids setup-rng 5)] ; 5 test cases per suite
    (log/info {:devices devices :readings readings :batch-size batch-size
               :update-batch-size update-batch-size :updates-per-system updates-per-system
               :duration duration :threads threads :staged-only? staged-only?})
    {:title "Fusion benchmark"
     :benchmark-type :fusion
     :seed seed
     :parameters {:devices devices :readings readings :batch-size batch-size
                  :update-batch-size update-batch-size :updates-per-system updates-per-system
                  :duration duration :threads threads :staged-only? staged-only?}
     :->state #(do {:!state (atom {:system-ids system-ids
                                   :site-ids site-ids
                                   :base-time base-time
                                   :!reading-idx (AtomicLong. readings)
                                   :!reading-b-idx (AtomicLong. readings)
                                   :!reading-c-idx (AtomicLong. (quot readings 3))})})
     :tasks (concat
             (when-not no-load?
               [(->init-tables-stage system-ids site-ids organisation-ids device-series-ids device-model-ids device-ids test-suite-id test-case-ids update-batch-size base-time)
                (->ingest-interleaved-stage system-ids site-ids readings updates-per-system batch-size update-batch-size base-time)
                (->sync-stage)
                (->compact-stage)])

             [{:t :call
               :stage :setup-state
               :f (fn [{:keys [node !state]}]
                    (let [latest-completed-tx (-> (xt/status node)
                                                  (get-in [:latest-completed-txs "xtdb" 0]))
                          max-vt (-> (xt/q node "SELECT max(_valid_from) AS m FROM reading_a FOR ALL VALID_TIME")
                                     first :m)
                          min-vt (-> (xt/q node "SELECT min(_valid_from) AS m FROM reading_a FOR ALL VALID_TIME")
                                     first :m)]
                      (log/infof "Valid time range: %s to %s" min-vt max-vt)
                      (swap! !state into {:latest-completed-tx latest-completed-tx
                                          :max-valid-time max-vt
                                          :min-valid-time min-vt})))}]

             (when staged-only?
               [;; Staged queries (sequential, non-interleaved)
                (->query-stage :system-settings)
                (->query-stage :readings-for-system)
                (->query-stage :system-count-over-time)
                (->query-stage :readings-range-bins)
                (->query-stage :cumulative-registration)
                (->query-stage :multi-reading-aggregation)])

             (when-not staged-only?
               [;; OLTP mixed workload - interleaved reads and writes
                {:t :concurrently
                 :stage :oltp
                 :duration duration
                 :join-wait (Duration/ofMinutes 1)
                 :thread-tasks [{:t :pool
                                 :duration duration
                                 :join-wait (Duration/ofMinutes 1)
                                 :thread-count threads
                                 :think Duration/ZERO
                                 :pooled-task {:t :pick-weighted
                                               :choices [;; Writes (30%)
                                                         [12.0 {:t :call, :transaction :insert-readings, :f (b/wrap-in-catch proc-insert-readings)}]
                                                         [5.0 {:t :call, :transaction :insert-readings-b, :f (b/wrap-in-catch proc-insert-readings-b)}]
                                                         [3.0 {:t :call, :transaction :insert-readings-c, :f (b/wrap-in-catch proc-insert-readings-c)}]
                                                         [10.0 {:t :call, :transaction :update-system, :f (b/wrap-in-catch proc-update-system)}]
                                                         ;; Reads (70%)
                                                         [20.0 {:t :call, :transaction :query-system-settings, :f (b/wrap-in-catch proc-query-system-settings)}]
                                                         [20.0 {:t :call, :transaction :query-readings, :f (b/wrap-in-catch proc-query-readings)}]
                                                         [10.0 {:t :call, :transaction :query-system-count, :f (b/wrap-in-catch proc-query-system-count)}]
                                                         [10.0 {:t :call, :transaction :query-range-bins, :f (b/wrap-in-catch proc-query-range-bins)}]
                                                         [5.0 {:t :call, :transaction :query-registration, :f (b/wrap-in-catch proc-query-registration)}]
                                                         [5.0 {:t :call, :transaction :query-multi-reading-aggregation, :f (b/wrap-in-catch proc-query-multi-reading-aggregation)}]]}}]}
                {:t :call, :stage :sync-after-oltp, :f (fn [{:keys [node]}] (b/sync-node node))}]))}))

;; ============================================================================
;; Tests
;; ============================================================================

(t/deftest test-reading-system-times-distribution
  (let [rng (java.util.Random. 42)
        interval-count 200
        base-time (Instant/now)
        times (generate-reading-system-times rng interval-count base-time)

        ;; Calculate lags in seconds
        lags (for [[[idx1 t1] [idx2 t2]] (partition 2 1 times)
                   :let [expected-interval-gap (* (- idx2 idx1) 300)
                         actual-gap (Duration/between t1 t2)
                         lag (- (.getSeconds actual-gap) expected-interval-gap)]]
               lag)

        short-lags (filter #(<= % 5) lags)
        long-lags (filter #(>= % 280) lags)
        short-ratio (/ (count short-lags) (double (count lags)))
        long-ratio (/ (count long-lags) (double (count lags)))]

    ;; All times should be monotonically increasing
    (t/is (every? (fn [[[_ ^Instant t1] [_ ^Instant t2]]] (or (.isBefore t1 t2) (.equals t1 t2)))
                  (partition 2 1 times))
          "System times must be monotonically increasing")

    ;; Should have both short and long lags
    (t/is (pos? (count short-lags))
          "Should have some short lags (0-5s)")
    (t/is (pos? (count long-lags))
          "Should have some long lags (280-320s)")

    ;; Check distribution is roughly 80/20 (allow 10% margin)
    (t/is (> short-ratio 0.70)
          (format "Short lags should be ~80%% (got %.1f%%)" (* 100 short-ratio)))
    (t/is (< short-ratio 0.90)
          (format "Short lags should be ~80%% (got %.1f%%)" (* 100 short-ratio)))

    (t/is (> long-ratio 0.10)
          (format "Long lags should be ~20%% (got %.1f%%)" (* 100 long-ratio)))
    (t/is (< long-ratio 0.30)
          (format "Long lags should be ~20%% (got %.1f%%)" (* 100 long-ratio)))

    ;; Log summary for human verification
    (log/infof "System-time lag distribution: %.1f%% short (0-5s), %.1f%% long (280-320s)"
               (* 100 short-ratio) (* 100 long-ratio))))

(t/deftest ^:benchmark run-fusion
  (let [path (util/->path "/tmp/fusion-bench")]
    (-> (b/->benchmark :fusion {:devices 100 :readings 100 :batch-size 50
                                :update-batch-size 10 :updates-per-system 5})
        (b/run-benchmark {:node-dir path}))))
