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

(t/deftest current-setting-search-path-test
  (t/testing "current_setting('search_path') returns expected value"
    (t/is (= [{:sp "public"}]
             (xt/q tu/*node* "SELECT current_setting('search_path') AS sp")))))

(t/deftest quote-ident-test
  (t/testing "quote_ident returns its input"
    (t/is (= [{:x "foo"}]
             (xt/q tu/*node* "SELECT quote_ident('foo') AS x")))))

(t/deftest string-to-array-test
  (t/testing "string_to_array splits a string by delimiter"
    (t/is (= [{:x ["a" " b" " c"]}]
             (xt/q tu/*node* "SELECT string_to_array('a, b, c', ',') AS x")))))

(t/deftest array-lower-test
  (t/testing "array_lower returns 1 for dimension 1"
    (t/is (= [{:x 1}]
             (xt/q tu/*node* "SELECT array_lower(string_to_array('a,b', ','), 1) AS x")))))

(t/deftest array-length-test
  (t/testing "array_length returns the length of a list"
    (t/is (= [{:x 2}]
             (xt/q tu/*node* "SELECT array_length(string_to_array('a,b', ','), 1) AS x")))))

(t/deftest parse-ident-test
  (t/testing "parse_ident splits a qualified identifier"
    (t/is (= [{:x ["public" "foo"]}]
             (xt/q tu/*node* "SELECT parse_ident('public.foo') AS x")))))

(t/deftest generate-series-2-arg-test
  (t/testing "generate_series with 2 args defaults step to 1"
    (t/is (= [{:x 1} {:x 2} {:x 3}]
             (xt/q tu/*node* "SELECT x FROM generate_series(1, 3) AS s(x)")))))

(t/deftest current-user-test
  (t/testing "CURRENT_USER returns current user"
    (t/is (= [{:u "xtdb"}]
             (xt/q tu/*node* "SELECT CURRENT_USER AS u")))))

(t/deftest grafana-table-discovery-test
  (t/testing "Grafana's table discovery query (simplified) runs without error"
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
