(ns xtdb.pgwire.grafana-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.test-util :as tu]))

(t/use-fixtures :each tu/with-node)

(t/deftest comment-only-query-test
  (t/testing "SQL comment-only queries don't throw (pgx driver sends '-- ping')"
    (t/is (some? (xt/q tu/*node* "-- ping")))))

(t/deftest version-detection-test
  (t/testing "Grafana version detection query returns PG 16 compatible version"
    (t/is (= [{:version 1600}]
             (xt/q tu/*node* "SELECT current_setting('server_version_num')::int/100 AS version")))))

(t/deftest grafana-table-discovery-test
  (t/testing "simplified table discovery query returns user tables"
    (xt/execute-tx tu/*node* [[:sql "INSERT INTO foo (_id) VALUES (1)"]])
    (let [results (xt/q tu/*node*
                    "SELECT
                       CASE WHEN quote_ident(table_schema) IN ('public')
                       THEN quote_ident(table_name)
                       ELSE quote_ident(table_schema) || '.' || quote_ident(table_name)
                       END AS \"table\"
                     FROM information_schema.tables
                     WHERE quote_ident(table_schema) NOT IN (
                       'information_schema', 'pg_catalog',
                       '_timescaledb_cache', '_timescaledb_catalog',
                       '_timescaledb_internal', '_timescaledb_config',
                       'timescaledb_information', 'timescaledb_experimental'
                     )
                     ORDER BY 1")]
      (t/is (some #(= "foo" (:table %)) results)))))

;; --- Tests for queries Grafana actually sends (requiring pgwire rewrites) ---
;; These test the raw SQL that Grafana's PostgreSQL plugin generates.
;; They should fail until we add the pgwire-level query rewrites.

(t/deftest ^:grafana-rewrite grafana-raw-table-discovery-test
  (t/testing "the actual table discovery query Grafana sends (with generate_series, array subscripts etc)"
    (xt/execute-tx tu/*node* [[:sql "INSERT INTO sensor_readings (_id, recorded_at, value) VALUES (1, TIMESTAMP '2026-03-04 10:00:00+00:00', 42.0)"]])
    (let [grafana-query "SELECT
    CASE WHEN
          quote_ident(table_schema) IN (
          SELECT
            CASE WHEN trim(s[i]) = '\"$user\"' THEN user ELSE trim(s[i]) END
          FROM
            generate_series(
              array_lower(string_to_array(current_setting('search_path'),','),1),
              array_upper(string_to_array(current_setting('search_path'),','),1)
            ) as i,
            string_to_array(current_setting('search_path'),',') s
          )
      THEN quote_ident(table_name)
      ELSE quote_ident(table_schema) || '.' || quote_ident(table_name)
    END AS \"table\"
    FROM information_schema.tables
    WHERE quote_ident(table_schema) NOT IN ('information_schema',
                             'pg_catalog',
                             '_timescaledb_cache',
                             '_timescaledb_catalog',
                             '_timescaledb_internal',
                             '_timescaledb_config',
                             'timescaledb_information',
                             'timescaledb_experimental')
    ORDER BY CASE WHEN
          quote_ident(table_schema) IN (
          SELECT
            CASE WHEN trim(s[i]) = '\"$user\"' THEN user ELSE trim(s[i]) END
          FROM
            generate_series(
              array_lower(string_to_array(current_setting('search_path'),','),1),
              array_upper(string_to_array(current_setting('search_path'),','),1)
            ) as i,
            string_to_array(current_setting('search_path'),',') s
          ) THEN 0 ELSE 1 END, 1"
          results (xt/q tu/*node* grafana-query)]
      (t/is (some #(= "sensor_readings" (:table %)) results)))))

(t/deftest ^:grafana-rewrite grafana-raw-column-discovery-test
  (t/testing "the actual column discovery query Grafana sends (with array subscripts, parse_ident)"
    (xt/execute-tx tu/*node* [[:sql "INSERT INTO sensor_readings (_id, recorded_at, value) VALUES (1, TIMESTAMP '2026-03-04 10:00:00+00:00', 42.0)"]])
    (let [grafana-query "SELECT quote_ident(column_name) AS \"column\", data_type AS \"type\"
    FROM information_schema.columns
    WHERE
      CASE WHEN array_length(parse_ident('sensor_readings'),1) = 2
        THEN quote_ident(table_schema) = (parse_ident('sensor_readings'))[1]
          AND quote_ident(table_name) = (parse_ident('sensor_readings'))[2]
        ELSE quote_ident(table_name) = 'sensor_readings'
          AND
          quote_ident(table_schema) IN (
          SELECT
            CASE WHEN trim(s[i]) = '\"$user\"' THEN user ELSE trim(s[i]) END
          FROM
            generate_series(
              array_lower(string_to_array(current_setting('search_path'),','),1),
              array_upper(string_to_array(current_setting('search_path'),','),1)
            ) as i,
            string_to_array(current_setting('search_path'),',') s
          )
      END"
          results (xt/q tu/*node* grafana-query)]
      (t/is (some #(= "_id" (:column %)) results))
      (t/is (some #(= "recorded_at" (:column %)) results))
      (t/is (some #(= "value" (:column %)) results)))))

(t/deftest ^:grafana-rewrite grafana-as-time-alias-test
  (t/testing "Grafana generates unquoted `AS time` which needs TIME to be handled as alias"
    (xt/execute-tx tu/*node* [[:sql "INSERT INTO readings (_id, recorded_at, value) VALUES (1, TIMESTAMP '2026-03-04 10:00:00+00:00', 42.0)"]])
    ;; This is what Grafana actually sends — bare `AS time`, not `AS "time"`
    (let [results (xt/q tu/*node* "SELECT recorded_at AS time, value FROM readings ORDER BY time")]
      (t/is (= 1 (count results)))
      (t/is (= 42.0 (:time (first results)))))))

(t/deftest ^:grafana-rewrite grafana-iso-timestamp-filter-test
  (t/testing "Grafana $__timeFilter expands to bare ISO strings — XTDB needs TIMESTAMP literals"
    (xt/execute-tx tu/*node* [[:sql "INSERT INTO readings (_id, recorded_at, value) VALUES (1, TIMESTAMP '2026-03-04 10:00:00+00:00', 42.0)"]])
    ;; This is what Grafana's $__timeFilter(recorded_at) macro expands to
    (let [results (xt/q tu/*node*
                    "SELECT recorded_at AS \"time\", value FROM readings WHERE recorded_at BETWEEN '2026-03-04T09:00:00Z' AND '2026-03-04T11:00:00Z' ORDER BY recorded_at")]
      (t/is (= 1 (count results)))
      (t/is (= 42.0 (:value (first results)))))))

(t/deftest grafana-time-series-query-test
  (t/testing "end-to-end time-series query with explicit TIMESTAMP literals and quoted alias (already works)"
    (xt/execute-tx tu/*node* [[:sql "INSERT INTO readings (_id, recorded_at, value) VALUES (1, TIMESTAMP '2026-03-04 10:00:00+00:00', 42.0)"]])
    (let [results (xt/q tu/*node*
                    "SELECT recorded_at AS \"time\", value FROM readings WHERE recorded_at BETWEEN TIMESTAMP '2026-03-04 09:00:00+00:00' AND TIMESTAMP '2026-03-04 11:00:00+00:00' ORDER BY recorded_at")]
      (t/is (= 1 (count results)))
      (t/is (= 42.0 (:value (first results)))))))
