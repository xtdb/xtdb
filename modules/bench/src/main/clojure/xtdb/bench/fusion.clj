(ns xtdb.bench.fusion
  (:require [clojure.test :as t]
            [clojure.tools.logging :as log]
            [xtdb.api :as xt]
            [xtdb.bench :as b]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import (java.time Duration Instant)
           (java.util UUID)))

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

(defn ->init-tables-stage
  [system-ids site-ids device-model-ids device-ids batch-size]
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
                                         batch))])))})

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
              (xt/submit-tx node [(->readings-docs (vec batch) idx start end)])))))})

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
  "Time-series scan for specific system - production metabase query"
  (xt/q node ["SELECT readings._valid_to as reading_time, readings.value::float AS reading_value
               FROM readings FOR ALL VALID_TIME
               JOIN system ON system._id = readings.system_id AND system._valid_time CONTAINS readings._valid_from
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
            :readings-range-bins (query-readings-range-bins node min-valid-time max-valid-time opts))))})

(defmethod b/cli-flags :fusion [_]
  [[nil "--devices DEVICES" "Number of systems" :parse-fn parse-long :default 10000]
   [nil "--readings READINGS" "Readings per system" :parse-fn parse-long :default 10000]
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
        device-ids (generate-ids (* devices 2))]
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
               [(->init-tables-stage system-ids site-ids device-model-ids device-ids update-batch-size)
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
              (->query-stage :readings-range-bins)])}))

(t/deftest ^:benchmark run-fusion
  (let [path (util/->path "/tmp/fusion-bench")]
    (-> (b/->benchmark :fusion {:devices 100 :readings 100 :batch-size 50
                                :update-batch-size 10 :updates-per-device 5})
        (b/run-benchmark {:node-dir path}))))
