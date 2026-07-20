(ns xtdb.pgwire.grafana-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.test-util :as tu]))

(t/use-fixtures :each tu/with-node)

(t/deftest comment-only-query-test
  (t/testing "SQL comment-only queries don't throw (pgx driver sends '-- ping')"
    (t/is (some? (xt/q tu/*node* "-- ping"))))

  (t/testing "double-dash inside string literals is not treated as a comment"
    (t/is (= [{:bar "--foo"}]
             (xt/q tu/*node* "SELECT '--foo' AS bar")))))

(t/deftest datasource-server-version-test
  (t/is (seq (xt/q tu/*node* "SELECT current_setting('server_version_num') AS v"))
        "Grafana probes the server version via current_setting"))

(t/deftest datasource-timescaledb-probe-test
  (t/is (= []
           (xt/q tu/*node* "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'"))
        "Grafana probes pg_extension for timescaledb - resolves to empty, not an error"))

(t/deftest datasource-table-listing-test
  (xt/execute-tx tu/*node* [[:sql "INSERT INTO foo (_id) VALUES (1)"]])
  (t/is (seq (xt/q tu/*node*
                   "SELECT quote_ident(table_name) AS \"table\", quote_ident(table_schema) AS \"schema\"
                    FROM information_schema.tables
                    WHERE quote_ident(table_schema) NOT IN ('information_schema', 'pg_catalog')"))
        "Grafana lists tables via information_schema.tables"))

(t/deftest datasource-column-listing-test
  (xt/execute-tx tu/*node* [[:sql "INSERT INTO foo (_id, name) VALUES (1, 'a')"]])
  (t/is (seq (xt/q tu/*node*
                   "SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE quote_ident(table_schema) = 'public' AND quote_ident(table_name) = 'foo'"))
        "Grafana lists columns via information_schema.columns"))
