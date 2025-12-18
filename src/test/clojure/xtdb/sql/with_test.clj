(ns xtdb.sql.with-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.sql :as sql]
            xtdb.sql-test
            [xtdb.test-util :as tu]))

(t/use-fixtures :each tu/with-allocator tu/with-mock-clock tu/with-node)

(t/deftest test-with-clause
  (let [sql "WITH foo AS (SELECT _id FROM bar WHERE _id = 5)
             SELECT foo._id foo_id, baz._id baz_id
             FROM foo, foo AS baz"]
    (t/is (=plan-file "with/test-with-clause"
                      (sql/plan sql {:table-info {#xt/table bar #{"_id"}}})))

    (xt/execute-tx tu/*node* [[:put-docs :bar {:xt/id 3} {:xt/id 5}]])

    (t/is (= [{:foo-id 5, :baz-id 5}]
             (xt/q tu/*node* sql {:table-info {#xt/table bar #{"_id"}}})))))

(t/deftest test-with-mat-clause
  (let [sql "WITH MATERIALIZED foo AS (SELECT _id FROM bar WHERE _id = 5)
             SELECT foo._id foo_id, baz._id baz_id
             FROM foo, foo AS baz"]
    (t/is (=plan-file
           "with/test-with-mat-clause"
           (sql/plan sql {:table-info {#xt/table bar #{"_id"}}})))

    (xt/execute-tx tu/*node* [[:put-docs :bar {:xt/id 3} {:xt/id 5}]])

    (t/is (= [{:foo-id 5, :baz-id 5}]
             (xt/q tu/*node* sql {:table-info {#xt/table bar #{"_id"}}})))))

(t/deftest disallow-period-specs-on-ctes-3440
  (xt/submit-tx tu/*node* [[:put-docs :docs {:xt/id 1}]])

  (t/is (anomalous? [:incorrect nil
                     #"Period specifications not allowed on CTE reference: my_cte"]
                    (xt/q tu/*node* "WITH my_cte AS (SELECT * FROM docs) SELECT * FROM my_cte FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-01'")))

  (t/is (anomalous? [:incorrect nil
                     #"Period specifications not allowed on CTE reference: my_cte"]
                    (xt/q tu/*node* "WITH my_cte AS (SELECT * FROM docs) SELECT * FROM my_cte FOR VALID_TIME AS OF TIMESTAMP '2024-01-01'"))))

(t/deftest cte-exporting-valid-time-4054
  (xt/submit-tx tu/*node* [[:put-docs {:into :docs :valid-from #inst "2020" :valid-to #inst "2050"} {:xt/id 1}]])

  (t/is (= [{:xt/valid-time #xt/tstz-range [#xt/zoned-date-time "2020-01-01T00:00Z" #xt/zoned-date-time "2050-01-01T00:00Z"]}]
           (xt/q tu/*node*
                 "SELECT _valid_time
                  FROM (SELECT _valid_time
                        FROM docs) t")))

  (t/is (= [{:xt/system-time #xt/tstz-range [#xt/zoned-date-time "2020-01-01T00:00Z" nil]}]
           (xt/q tu/*node*
                 "WITH data AS (
                     SELECT _system_time
                     FROM docs
                  )
                  SELECT _system_time
                  FROM data"))))

(t/deftest test-materialized-cte-ordering-with-many-ctes-5109
  (xt/execute-tx tu/*node* [[:put-docs :tbl {:xt/id 1}]])

  (t/is (= [{:xt/id 1}]
           (xt/q tu/*node*
                 "WITH MATERIALIZED
                    a AS (SELECT _id FROM tbl),
                    b AS (SELECT _id FROM a),
                    b2 AS (SELECT _id FROM b),
                    c AS (SELECT _id FROM a),
                    c2 AS (SELECT _id FROM c),
                    MATERIALIZED d AS (SELECT _id FROM a),
                    d2 AS (SELECT _id FROM d),
                    d3 AS (SELECT _id FROM d2),
                    d4 AS (SELECT _id FROM d3)
                  SELECT _id FROM b2
                  UNION SELECT _id FROM c2
                  UNION SELECT _id FROM d2"))))

(t/deftest iseq-from-symbol-bug-4378
  (t/is (anomalous? [:incorrect nil #"Subquery arity error"]
                    (sql/plan "
WITH dates AS (
  SELECT TIMESTAMP '2023-01-01T00:00:00Z' AS d
  UNION ALL
  SELECT TIMESTAMP '2023-01-02T00:00:00Z'
),
system_range AS (
  SELECT
    'a' AS _id,
    period(TIMESTAMP '2022-12-31T00:00:00Z', TIMESTAMP '2023-01-02T00:00:00Z') AS valid_time_intersection
  UNION ALL
  SELECT
    'b',
    period(TIMESTAMP '2023-01-02T00:00:00Z', TIMESTAMP '2023-01-03T00:00:00Z')
)

SELECT
  dates.d,
  (
    SELECT COUNT(DISTINCT v._id), dates.d, v._id
    FROM system_range AS v
    WHERE v.valid_time_intersection CONTAINS (dates.d + INTERVAL 'PT0M')
  ) AS member_count
FROM dates"))))
