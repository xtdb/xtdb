(ns xtdb.default-tz-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            xtdb.serde
            [xtdb.test-util :as tu])
  (:import (java.time ZoneId)))

(t/use-fixtures :each
  (tu/with-opts {:default-tz (ZoneId/of "Europe/London")})
  tu/with-mock-clock tu/with-node)

(t/deftest can-specify-default-tz-in-query-396
  (let [q (str "SELECT CAST(DATE '2020-08-01' AS TIMESTAMP WITH TIME ZONE) AS tstz "
               "FROM (VALUES (NULL)) a (a)")]
    ;; no per-query zone, so the node's configured one governs
    (t/is (= [{:tstz #xt/zdt "2020-08-01T00:00+01:00[Europe/London]"}]
             (xt/q tu/*node* q {})))

    (t/is (= [{:tstz #xt/zoned-date-time "2020-08-01T00:00-07:00[America/Los_Angeles]"}]
             (xt/q tu/*node* q {:default-tz #xt/zone "America/Los_Angeles"})))))

(t/deftest can-specify-default-tz-in-dml-396
  (let [q "INSERT INTO foo (_id, dt, tstz) VALUES (?, DATE '2020-08-01', CAST(DATE '2020-08-01' AS TIMESTAMP WITH TIME ZONE))"]
    (xt/submit-tx tu/*node* [[:sql q ["foo"]]]
                  {:default-tz #xt/zone "Europe/London"})
    (xt/submit-tx tu/*node* [[:sql q ["bar"]]]
                  {:default-tz #xt/zone "America/Los_Angeles"})

    (let [q "SELECT foo._id, foo.dt, CAST(foo.dt AS TIMESTAMP WITH TIME ZONE) cast_tstz, foo.tstz FROM foo"]

      ;; `cast_tstz` is cast at read time, so it takes the query's zone — the node's configured one here,
      ;; for both rows, whatever zone each was written under
      (t/is (= #{{:xt/id "foo", :dt #xt/date "2020-08-01",
                  :cast-tstz #xt/zdt "2020-08-01T00:00+01:00[Europe/London]"
                  :tstz #xt/zoned-date-time "2020-08-01T00:00+01:00[Europe/London]"}
                 {:xt/id "bar", :dt #xt/date "2020-08-01"
                  :cast-tstz #xt/zdt "2020-08-01T00:00+01:00[Europe/London]"
                  :tstz #xt/zoned-date-time "2020-08-01T00:00-07:00[America/Los_Angeles]"}}

               (set (xt/q tu/*node* q))))

      (t/is (= #{{:xt/id "foo", :dt #xt/date "2020-08-01",
                  :cast-tstz #xt/zoned-date-time "2020-08-01T00:00-07:00[America/Los_Angeles]"
                  :tstz #xt/zoned-date-time "2020-08-01T00:00+01:00[Europe/London]"}
                 {:xt/id "bar", :dt #xt/date "2020-08-01"
                  :cast-tstz #xt/zoned-date-time "2020-08-01T00:00-07:00[America/Los_Angeles]"
                  :tstz #xt/zoned-date-time "2020-08-01T00:00-07:00[America/Los_Angeles]"}}

               (set (xt/q tu/*node* q {:default-tz #xt/zone "America/Los_Angeles"})))))))

(t/deftest distinct-compares-timestamps-in-the-query-zone
  (xt/execute-tx tu/*node* [[:sql "INSERT INTO foo (_id, v) VALUES (1, TIMESTAMP '2020-08-01 00:00:00'), (2, TIMESTAMP '2020-08-01 00:00:00Z')"]])

  (let [distinct-q "SELECT COUNT(*) v_count FROM (SELECT DISTINCT v FROM foo) f"
        count-distinct-q "SELECT COUNT(DISTINCT v) v_count FROM foo"
        utc-opts {:default-tz (ZoneId/of "UTC")}
        la-opts {:default-tz (ZoneId/of "America/Los_Angeles")}]
    (t/is (= [{:v-count 1}] (xt/q tu/*node* distinct-q utc-opts)))
    (t/is (= [{:v-count 1}] (xt/q tu/*node* count-distinct-q utc-opts)))

    (t/is (= [{:v-count 2}] (xt/q tu/*node* distinct-q la-opts)))
    (t/is (= [{:v-count 2}] (xt/q tu/*node* count-distinct-q la-opts)))))

(t/deftest join-compares-timestamps-in-the-query-zone
  (xt/execute-tx tu/*node* [[:sql "INSERT INTO foo (_id, v) VALUES (1, TIMESTAMP '2020-08-01 00:00:00'), (2, TIMESTAMP '2020-08-01 00:00:00Z')"]])

  (let [join-q "SELECT COUNT(*) v_count FROM foo a JOIN foo b ON a.v = b.v"
        intersect-q "SELECT COUNT(*) v_count FROM (SELECT v FROM foo WHERE _id = 1 INTERSECT SELECT v FROM foo WHERE _id = 2) f"
        utc-opts {:default-tz (ZoneId/of "UTC")}
        la-opts {:default-tz (ZoneId/of "America/Los_Angeles")}]
    (t/is (= [{:v-count 4}] (xt/q tu/*node* join-q utc-opts)))
    (t/is (= [{:v-count 1}] (xt/q tu/*node* intersect-q utc-opts)))

    (t/is (= [{:v-count 2}] (xt/q tu/*node* join-q la-opts)))
    (t/is (= [{:v-count 0}] (xt/q tu/*node* intersect-q la-opts)))))

(t/deftest sorts-compare-timestamps-in-the-query-zone
  (xt/execute-tx tu/*node* [[:sql "INSERT INTO foo (_id, v) VALUES (1, TIMESTAMP '2020-08-01 00:00:00'), (2, TIMESTAMP '2020-08-01 03:00:00Z')"]])

  (let [order-by-q "SELECT _id FROM foo ORDER BY v"
        percentile-q "SELECT PERCENTILE_DISC(0) WITHIN GROUP (ORDER BY v) v FROM foo"
        array-agg-q "SELECT ARRAY_AGG(_id ORDER BY v) ids FROM foo"
        utc-opts {:default-tz (ZoneId/of "UTC")}
        la-opts {:default-tz (ZoneId/of "America/Los_Angeles")}]
    (t/is (= [{:xt/id 1} {:xt/id 2}] (xt/q tu/*node* order-by-q utc-opts)))
    (t/is (= [{:v #xt/ldt "2020-08-01T00:00"}] (xt/q tu/*node* percentile-q utc-opts)))
    (t/is (= [{:ids [1 2]}] (xt/q tu/*node* array-agg-q utc-opts)))

    (t/is (= [{:xt/id 2} {:xt/id 1}] (xt/q tu/*node* order-by-q la-opts)))
    (t/is (= [{:v #xt/zdt "2020-08-01T03:00Z"}] (xt/q tu/*node* percentile-q la-opts)))
    (t/is (= [{:ids [2 1]}] (xt/q tu/*node* array-agg-q la-opts)))))

(t/deftest test-xtql-default-tz
  #_ ; FIXME #3020
  (t/is (= [{:time #xt/time "16:00"}]
           (xt/q tu/*node* '(rel [{:time (local-time)}] [time])
                 {:current-time (time/->instant #inst "2024-01-01")
                  :default-tz #xt/zone "America/Los_Angeles"}))))
