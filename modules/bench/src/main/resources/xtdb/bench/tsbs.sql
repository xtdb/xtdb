-- :name query-last-loc-by-truck :? :*
SELECT t.name, t.driver, r.longitude, r.latitude
FROM trucks t
CROSS JOIN LATERAL (
  SELECT longitude, latitude FROM readings FOR ALL VALID_TIME
  WHERE _id = t._id ORDER BY _valid_from DESC FETCH FIRST 1 ROW ONLY
) AS r
WHERE t.name IN (:v*:truck-names)

-- :name query-last-loc-per-truck :? :*
SELECT t.name, t.driver, r.longitude, r.latitude
FROM trucks t
CROSS JOIN LATERAL (
  SELECT longitude, latitude FROM readings FOR ALL VALID_TIME
  WHERE _id = t._id ORDER BY _valid_from DESC FETCH FIRST 1 ROW ONLY
) AS r
WHERE t.name IS NOT NULL AND t.fleet = :fleet

-- :name query-low-fuel :? :*
SELECT t.name, t.driver, d.fuel_state
FROM trucks t
CROSS JOIN LATERAL (
  SELECT fuel_state FROM diagnostics FOR ALL VALID_TIME
  WHERE _id = t._id ORDER BY _valid_from DESC FETCH FIRST 1 ROW ONLY
) AS d
WHERE t.name IS NOT NULL
  AND t.fleet = :fleet
  AND d.fuel_state < 0.1

-- :name query-high-load :? :*
SELECT t.name, t.driver, d.current_load
FROM trucks t
CROSS JOIN LATERAL (
  SELECT current_load FROM diagnostics FOR ALL VALID_TIME
  WHERE _id = t._id ORDER BY _valid_from DESC FETCH FIRST 1 ROW ONLY
) AS d
WHERE t.name IS NOT NULL
  AND t.fleet = :fleet
  AND d.current_load / CAST(t.load_capacity AS DOUBLE PRECISION) > 0.9

-- :name query-stationary-trucks :? :*
SELECT t.name, t.driver, AVG(r.velocity) AS avg_velocity
FROM trucks t
JOIN readings FOR VALID_TIME BETWEEN :window-start AND :window-end AS r ON r._id = t._id
WHERE t.name IS NOT NULL AND t.fleet = :fleet
GROUP BY t.name, t.driver
HAVING AVG(r.velocity) < 1

-- :name query-long-driving-sessions :? :*
WITH base AS (
  SELECT t.name, t.driver, r.velocity,
         FLOOR(EXTRACT(EPOCH FROM r._valid_from) / 600) AS ten_min_bucket
  FROM trucks t
  JOIN readings FOR VALID_TIME BETWEEN :window-start AND :window-end AS r ON r._id = t._id
  WHERE t.name IS NOT NULL AND t.fleet = :fleet
),
driving_periods AS (
  SELECT name, driver, ten_min_bucket
  FROM base
  GROUP BY name, driver, ten_min_bucket
  HAVING AVG(velocity) > 1
)
SELECT name, driver
FROM driving_periods
GROUP BY name, driver
HAVING COUNT(*) > 22

-- :name query-long-daily-sessions :? :*
WITH base AS (
  SELECT t.name, t.driver, r.velocity,
         FLOOR(EXTRACT(EPOCH FROM r._valid_from) / 600) AS ten_min_bucket
  FROM trucks t
  JOIN readings FOR VALID_TIME BETWEEN :window-start AND :window-end AS r ON r._id = t._id
  WHERE t.name IS NOT NULL AND t.fleet = :fleet
),
driving_periods AS (
  SELECT name, driver, ten_min_bucket
  FROM base
  GROUP BY name, driver, ten_min_bucket
  HAVING AVG(velocity) > 1
)
SELECT name, driver
FROM driving_periods
GROUP BY name, driver
HAVING COUNT(*) > 60

-- :name query-avg-vs-projected-fuel-consumption :? :*
SELECT t.fleet,
       AVG(r.fuel_consumption) AS avg_fuel_consumption,
       AVG(t.nominal_fuel_consumption) AS projected_fuel_consumption
FROM trucks t
JOIN readings FOR ALL VALID_TIME AS r ON r._id = t._id
WHERE t.name IS NOT NULL
  AND t.fleet IS NOT NULL
  AND t.nominal_fuel_consumption IS NOT NULL
  AND r.velocity > 1
GROUP BY t.fleet

-- :name query-avg-daily-driving-duration :? :*
WITH base AS (
  SELECT t.name, t.driver, t.fleet, r.velocity,
         DATE_TRUNC(DAY, r._valid_from) AS day_bucket,
         FLOOR(EXTRACT(EPOCH FROM r._valid_from) / 600) AS ten_min_bucket
  FROM trucks t
  JOIN readings FOR ALL VALID_TIME AS r ON r._id = t._id
  WHERE t.name IS NOT NULL
),
ten_min_driving AS (
  SELECT name, driver, fleet, day_bucket, ten_min_bucket
  FROM base
  GROUP BY name, driver, fleet, day_bucket, ten_min_bucket
  HAVING AVG(velocity) > 1
),
daily_hours AS (
  SELECT name, driver, fleet, day_bucket,
         CAST(COUNT(*) AS DOUBLE PRECISION) / 6.0 AS hours_driven
  FROM ten_min_driving
  GROUP BY name, driver, fleet, day_bucket
)
SELECT fleet, name, driver, AVG(hours_driven) AS avg_daily_hours
FROM daily_hours
GROUP BY fleet, name, driver

-- :name query-avg-daily-driving-session :? :*
SELECT name, day_bucket, AVG(session_duration_min) AS avg_session_duration_min
FROM (
  WITH readings_with_bucket AS (
    SELECT t.name, t._id AS truck_id, r.velocity,
           FLOOR(EXTRACT(EPOCH FROM r._valid_from) / 600) AS ten_min_bucket
    FROM trucks t
    JOIN readings FOR ALL VALID_TIME AS r ON r._id = t._id
    WHERE t.name IS NOT NULL
  ),
  readings_bucketed AS (
    SELECT name, truck_id, ten_min_bucket,
           AVG(velocity) > 5 AS driving
    FROM readings_with_bucket
    GROUP BY name, truck_id, ten_min_bucket
  ),
  status_with_prev AS (
    SELECT name, truck_id, ten_min_bucket, driving,
           LAG(driving) OVER (PARTITION BY truck_id ORDER BY ten_min_bucket) AS prev_driving
    FROM readings_bucketed
  ),
  status_changes AS (
    SELECT name, truck_id, ten_min_bucket AS start_bucket, driving,
           LEAD(ten_min_bucket) OVER (PARTITION BY truck_id ORDER BY ten_min_bucket) AS stop_bucket
    FROM status_with_prev
    WHERE driving <> prev_driving OR prev_driving IS NULL
  )
  SELECT name,
         FLOOR(start_bucket / 144) AS day_bucket,
         (stop_bucket - start_bucket) * 10.0 AS session_duration_min
  FROM status_changes
  WHERE driving = true AND stop_bucket IS NOT NULL
) sessions
GROUP BY name, day_bucket
ORDER BY name, day_bucket

-- :name query-avg-load :? :*
SELECT t.fleet, t.model, t.load_capacity,
       AVG(d.current_load / CAST(t.load_capacity AS DOUBLE PRECISION)) AS avg_load_pct
FROM trucks t
JOIN diagnostics FOR ALL VALID_TIME AS d ON d._id = t._id
WHERE t.name IS NOT NULL
GROUP BY t.fleet, t.model, t.load_capacity

-- :name query-daily-activity :? :*
WITH base AS (
  SELECT t.fleet, t.model, t._id AS truck_id, d.status,
         DATE_TRUNC(DAY, d._valid_from) AS day_bucket,
         FLOOR(EXTRACT(EPOCH FROM d._valid_from) / 600) AS ten_min_bucket
  FROM trucks t
  JOIN diagnostics FOR ALL VALID_TIME AS d ON d._id = t._id
  WHERE t.name IS NOT NULL
),
active_periods AS (
  SELECT fleet, model, truck_id, day_bucket, ten_min_bucket
  FROM base
  GROUP BY fleet, model, truck_id, day_bucket, ten_min_bucket
  HAVING AVG(status) < 1
)
SELECT fleet, model, day_bucket,
       CAST(COUNT(*) AS DOUBLE PRECISION) / 144.0 AS daily_activity
FROM active_periods
GROUP BY fleet, model, day_bucket
ORDER BY day_bucket

-- :name query-breakdown-frequency :? :*
WITH diagnostics_with_next AS (
  SELECT t.model, t._id AS truck_id, d.status,
         LEAD(d.status) OVER (PARTITION BY t._id ORDER BY d._valid_from) AS next_status
  FROM trucks t
  JOIN diagnostics FOR ALL VALID_TIME AS d ON d._id = t._id
  WHERE t.name IS NOT NULL
),
breakdown_events AS (
  SELECT model, truck_id
  FROM diagnostics_with_next
  WHERE status > 0 AND next_status = 0
)
SELECT model, COUNT(*) AS breakdown_count
FROM breakdown_events
GROUP BY model
HAVING COUNT(*) > 0
