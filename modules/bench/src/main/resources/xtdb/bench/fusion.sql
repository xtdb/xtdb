-- :name query-system-settings :? :*
-- Simple point-in-time system lookup
SELECT *, _valid_from, _system_from
FROM system
WHERE _id = :system-id

-- :name query-readings-for-system :? :*
-- Time-series scan for specific system
-- NOTE: Production query is missing temporal join constraint (CONTAINS predicate).
-- Without 'system._valid_time CONTAINS reading_a._valid_from', this produces a cartesian
-- product - each reading is joined with ALL versions of the system.
SELECT reading_a._valid_to as reading_time, reading_a.value::float AS reading_value
FROM reading_a FOR ALL VALID_TIME
JOIN system FOR ALL VALID_TIME ON system._id = reading_a.system_id
WHERE system._id = :system-id
  AND reading_a._valid_from >= :start
  AND reading_a._valid_from < :end
ORDER BY reading_time

-- :name query-system-count-over-time :? :*
-- Complex temporal aggregation with multi-table joins
WITH dates AS (
  SELECT d::timestamptz AS d
  FROM generate_series(DATE_BIN(INTERVAL 'PT1H', :start::timestamptz), :end::timestamptz, INTERVAL 'PT1H') AS x(d)
)
SELECT dates.d, COUNT(DISTINCT system._id) AS c
FROM dates
LEFT OUTER JOIN system ON system._valid_time CONTAINS dates.d
LEFT OUTER JOIN device ON device.system_id = system._id AND device._valid_time CONTAINS dates.d
LEFT OUTER JOIN device_model ON device_model._id = device.device_model_id AND device_model._valid_time CONTAINS dates.d
LEFT OUTER JOIN device_series ON device_series._id = device_model.device_series_id AND device_series._valid_time CONTAINS dates.d
LEFT OUTER JOIN organisation ON organisation._id = device_series.organisation_id AND organisation._valid_time CONTAINS dates.d
LEFT OUTER JOIN site ON site._id = system.site_id AND site._valid_time CONTAINS dates.d
GROUP BY dates.d
ORDER BY dates.d

-- :name query-readings-range-bins :? :*
-- Hourly aggregation using range_bins
WITH corrected_readings AS (
  SELECT r.*, r._valid_from, r._valid_to,
         (bin)._from AS corrected_from,
         (bin)._weight AS corrected_weight,
         r.value * (bin)._weight AS corrected_portion
  FROM reading_a AS r, UNNEST(range_bins(INTERVAL 'PT1H', r._valid_time)) AS b(bin)
  WHERE r._valid_from >= :start AND r._valid_from < :end
)
SELECT corrected_from AS t, SUM(corrected_portion) / SUM(corrected_weight) AS value
FROM corrected_readings
GROUP BY corrected_from
ORDER BY t

-- :name query-cumulative-registration :? :*
-- Complex registration tracking query with multiple CTEs and window functions
WITH gen AS (
  SELECT d::timestamptz AS t
  FROM generate_series(:start::timestamptz, :end::timestamptz, INTERVAL 'PT1H') AS x(d)
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
  LEFT OUTER JOIN site ON site._id = system.site_id AND site._valid_time CONTAINS gen.t
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
ORDER BY gen.t, registration_status

-- :name query-multi-reading-aggregation :? :*
-- Multi-table reading aggregation with range_bins, delta calculations, and control plan targets.
-- Exercises: multiple CTEs with range_bins, MATERIALIZED CTE, ROW_NUMBER window function,
-- AGE/EXTRACT for time delta, IN filter on system IDs, and UNION of heterogeneous result sets.
-- The IN filter mirrors production where queries target a subset of ~30 systems.
WITH
  systems AS (
    SELECT
      sys._id AS sid,
      sys.rtg_max_w AS rtg_max_w,
      sys.set_max_discharge_rate_w,
      sys.set_max_charge_rate_w
    FROM system AS sys
    WHERE sys._id IN (:v*:system-ids)
  ),
  ap_corrected_readings AS (
    SELECT
      r.system_id AS sid,
      r._valid_from AS r_vf,
      r._valid_to AS r_vt,
      (bin)._from AS corrected_from,
      (bin)._weight AS corrected_weight,
      r.value * (bin)._weight AS corrected_portion
    FROM reading_a FOR ALL VALID_TIME AS r,
         UNNEST(range_bins(INTERVAL 'PT5M', r._valid_time)) AS b(bin)
    WHERE r.system_id IN (:v*:system-ids)
      AND r._valid_from >= :start AND r._valid_from < :end
  ),
  ap_summed_by_system AS (
    SELECT
      sid,
      corrected_from AS t,
      SUM(corrected_portion) / SUM(corrected_weight) AS value
    FROM ap_corrected_readings
    GROUP BY sid, corrected_from
  ),
  sp_corrected_readings AS (
    SELECT
      r._id AS sid,
      r._valid_from AS r_vf,
      r._valid_to AS r_vt,
      (bin)._from AS corrected_from,
      (bin)._weight AS corrected_weight,
      r.value * (bin)._weight AS corrected_portion
    FROM reading_b FOR ALL VALID_TIME AS r,
         UNNEST(range_bins(INTERVAL 'PT5M', r._valid_time)) AS b(bin)
    WHERE r._valid_from >= :start AND r._valid_from < :end
  ),
  sp_summed_by_system AS (
    SELECT
      sid,
      corrected_from AS t,
      SUM(corrected_portion) / SUM(corrected_weight) AS value
    FROM sp_corrected_readings
    GROUP BY sid, corrected_from
  ),
  soc_corrected_readings AS (
    SELECT
      r.system_id AS sid,
      r._valid_from AS r_vf,
      r._valid_to AS r_vt,
      (bin)._from AS corrected_from,
      (bin)._weight AS corrected_weight,
      r.value * (bin)._weight AS corrected_portion
    FROM reading_c FOR ALL VALID_TIME AS r,
         UNNEST(range_bins(INTERVAL 'PT5M', r._valid_time)) AS b(bin)
    WHERE r.system_id IN (:v*:system-ids)
      AND r._valid_from >= :start AND r._valid_from < :end
  ),
  MATERIALIZED soc_summed_by_system AS (
    SELECT
      sid,
      corrected_from AS t,
      SUM(corrected_portion) / SUM(corrected_weight) AS value
    FROM soc_corrected_readings
    GROUP BY sid, corrected_from
  ),
  soc_reading AS (
    SELECT
      *,
      t,
      ROW_NUMBER() OVER (ORDER BY sid, t) AS rn
    FROM soc_summed_by_system
  ),
  previous_reading_number AS (
    SELECT
      sid,
      value,
      t,
      (rn + 1) AS prn
    FROM soc_reading
  ),
  all_reading AS (
    SELECT
      soc_reading.*,
      prn.prn AS prn_prn,
      prn.value AS prn_value,
      prn.t AS prn_valid_from
    FROM soc_reading
    JOIN previous_reading_number AS prn ON prn.sid = soc_reading.sid
    WHERE soc_reading.rn = prn.prn
  ),
  deltas AS (
    SELECT
      *,
      all_reading.value - all_reading.prn_value AS value_delta,
      (all_reading.t - all_reading.prn_valid_from) AS time_delta_interval,
      (
        EXTRACT(HOUR FROM AGE(all_reading.t, all_reading.prn_valid_from))
      ) + (
        EXTRACT(MINUTE FROM AGE(all_reading.t, all_reading.prn_valid_from)) / 60.0
      ) + (
        EXTRACT(SECOND FROM AGE(all_reading.t, all_reading.prn_valid_from)) / 3600.0
      ) AS time_delta
    FROM all_reading
  ),
  aggregated_control_plans AS (
    SELECT
      _valid_from,
      _valid_to,
      SUM(control_value) AS aggregated_control_value,
      event_id,
      status
    FROM control_plan FOR ALL VALID_TIME
    WHERE status = 'ACTIVE'
    GROUP BY event_id, status, _valid_from, _valid_to
    ORDER BY _valid_from
  )

SELECT
  t,
  SUM(value) AS reading_value,
  'active_power' AS type
FROM ap_summed_by_system
GROUP BY t

UNION

SELECT
  t,
  SUM(value) AS reading_value,
  'site_power' AS type
FROM sp_summed_by_system
GROUP BY t

UNION

SELECT
  t,
  SUM(value) AS reading_value,
  'state_of_charge' AS type
FROM soc_summed_by_system
GROUP BY t

UNION

SELECT
  t,
  -1 * SUM(value_delta / time_delta) AS reading_value,
  'charge_rate' AS type
FROM deltas
GROUP BY t

UNION

SELECT
  _valid_from AS t,
  COALESCE(
    MIN(CASE WHEN is_start THEN aggregated_control_value END),
    MIN(aggregated_control_value)
  ) AS reading_value,
  event_id AS type
FROM (
  SELECT
    event_id,
    aggregated_control_value,
    _valid_from,
    TRUE AS is_start
  FROM aggregated_control_plans
  UNION ALL
  SELECT
    event_id,
    aggregated_control_value,
    (_valid_to - INTERVAL 'PT1S') AS _valid_from,
    FALSE AS is_start
  FROM aggregated_control_plans
) AS cp
GROUP BY _valid_from, event_id

ORDER BY t
