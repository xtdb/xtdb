(ns xtdb.sql.generate-series-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.sql :as sql]
            [xtdb.test-util :as tu]))

(t/use-fixtures :each tu/with-mock-clock tu/with-node)

(t/deftest test-range-3212
  (t/is (= [{:xs [1 2 3]}]
           (xt/q tu/*node* "SELECT range(1, 4) xs")))

  (t/is (= [{:xs [1 4 7]}]
           (xt/q tu/*node* "SELECT range(1, 8, 3) xs")))

  (t/is (= [{:xs []}]
           (xt/q tu/*node* "SELECT range(10, 3) xs")))

  (t/is (= [{:xs []}]
           (xt/q tu/*node* "SELECT range(1, 1) xs")))

  (t/is (= [{:xs [1]}]
           (xt/q tu/*node* "SELECT range(1, 2, 2) xs")))

  (xt/submit-tx tu/*node* [[:sql "INSERT INTO foo RECORDS {_id: 1, start: -1, end: 3}"]])

  (t/is (= [{:xs [-1 0 1 2]}]
           (xt/q tu/*node* "SELECT range(start, end) xs FROM foo"))))

(t/deftest test-range-datetimes-4067
  (t/testing "DATE"
    (t/is (= [{:dates []}]
             (xt/q tu/*node* "SELECT range(DATE '2020-01-01', DATE '2020-01-01', INTERVAL 'P1D') dates"))
          "single date, should return empty")

    (t/is (= [{:dates [#xt/date-time "2020-01-01T00:00", #xt/date-time "2020-01-02T00:00", #xt/date-time "2020-01-03T00:00"]}]
             (xt/q tu/*node* "SELECT range(DATE '2020-01-01', DATE '2020-01-04', INTERVAL 'P1D') dates"))
          "date range")

    (t/is (= [{:date #xt/date-time "2020-01-01T00:00"}
              {:date #xt/date-time "2020-01-03T00:00"}
              {:date #xt/date-time "2020-01-05T00:00"}]
             (xt/q tu/*node* "FROM range(DATE '2020-01-01', DATE '2020-01-07', INTERVAL 'P2D') dates (`date`)"))
          "multi-day interval")

    (t/is (= [{:date #xt/date-time "2020-01-01T00:00", :idx 1}
              {:date #xt/date-time "2020-01-03T00:00", :idx 2}
              {:date #xt/date-time "2020-01-05T00:00", :idx 3}]
             (xt/q tu/*node* "FROM range(DATE '2020-01-01', DATE '2020-01-07', INTERVAL 'P2D') WITH ORDINALITY dates (`date`, idx)"))
          "multi-day interval, WITH ORDINALITY")

    (t/is (= [{:dates [#xt/date "2020-01-01", #xt/date "2020-02-01", #xt/date "2020-03-01"]}]
             (xt/q tu/*node* "SELECT range(DATE '2020-01-01', DATE '2020-04-01', INTERVAL '1' MONTH) dates"))
          "monthly interval")

    (t/is (= [{:dates [#xt/date "2020-01-01"
                       #xt/date "2020-02-01"
                       #xt/date "2020-03-01"]}]
             (xt/q tu/*node* "SELECT range(DATE '2020-01-01', DATE '2020-04-01', INTERVAL 'P1M') dates"))
          "monthly interval")

    (t/is (= [{:date #xt/date "2020-01-01"} {:date #xt/date "2021-01-01"}]
             (xt/q tu/*node* "FROM range(DATE '2020-01-01', DATE '2022-01-01', INTERVAL 'P1Y') dates (`date`)"))
          "yearly interval")

    (t/is (= [{:date #xt/date "2020-01-01"} {:date #xt/date "2021-01-01"}]
             (xt/q tu/*node* "FROM range(DATE '2020-01-01', DATE '2022-01-01', INTERVAL '1' YEAR) dates (`date`)"))
          "yearly interval")

    (t/is (= [{:dates [#xt/date-time "2020-01-01T00:00"
                       #xt/date-time "2020-01-01T06:00"
                       #xt/date-time "2020-01-01T12:00"
                       #xt/date-time "2020-01-01T18:00"]}]
             (xt/q tu/*node* "SELECT range(DATE '2020-01-01', DATE '2020-01-02', INTERVAL 'PT6H') dates"))
          "time interval")

    (t/is (= [{:dates [#xt/date-time "2020-01-01T00:00"
                       #xt/date-time "2020-04-02T23:59:57"
                       #xt/date-time "2020-07-04T23:59:54"
                       #xt/date-time "2020-10-06T23:59:51"]}]
             (xt/q tu/*node* "SELECT range(DATE '2020-01-01', DATE '2021-01-01', INTERVAL 'P3M2DT-3S') dates"))
          "mixed interval"))

  (t/testing "TIMESTAMP"
    (t/is (= [{:timestamps [#xt/date-time "2020-01-01T00:00"]}]
             (xt/q tu/*node* "SELECT range(TIMESTAMP '2020-01-01 00:00:00', TIMESTAMP '2020-01-02 00:00:00', INTERVAL 'PT24H') timestamps")))

    (t/is (= [{:timestamps [#xt/date-time "2020-01-01T00:00"
                            #xt/date-time "2020-01-01T01:00"
                            #xt/date-time "2020-01-01T02:00"]}]
             (xt/q tu/*node* "SELECT range(TIMESTAMP '2020-01-01 00:00:00', TIMESTAMP '2020-01-01 03:00:00', INTERVAL 'PT1H') timestamps"))
          "hour steps")

    (t/is (= [{:ts #xt/date-time "2020-01-01T00:00"}
              {:ts #xt/date-time "2020-01-01T00:00:00.500"}
              {:ts #xt/date-time "2020-01-01T00:00:01"}
              {:ts #xt/date-time "2020-01-01T00:00:01.500"}
              {:ts #xt/date-time "2020-01-01T00:00:02"}
              {:ts #xt/date-time "2020-01-01T00:00:02.500"}]
             (xt/q tu/*node* "FROM range(TIMESTAMP '2020-01-01 00:00:00', TIMESTAMP '2020-01-01 00:00:03', INTERVAL 'PT0.5S') timestamps (ts)"))
          "fractional seconds")

    (t/is (= [{:timestamps [#xt/date-time "2020-01-01T00:00", #xt/date-time "2020-02-01T00:00"]}]
             (xt/q tu/*node* "SELECT range(TIMESTAMP '2020-01-01 00:00:00', TIMESTAMP '2020-03-01 00:00:00', INTERVAL 'P1M') timestamps"))
          "monthly interval")

    (t/is (= [{:timestamps [#xt/date-time "2020-01-01T00:00", #xt/date-time "2021-01-01T00:00"]}]
             (xt/q tu/*node* "SELECT range(TIMESTAMP '2020-01-01 00:00:00', TIMESTAMP '2022-01-01 00:00:00', INTERVAL 'P1Y') timestamps"))
          "yearly interval")

    (t/is (= [{:timestamps [#xt/date-time "2020-01-01T00:00"
                            #xt/date-time "2020-04-03T00:00:03"
                            #xt/date-time "2020-07-05T00:00:06"
                            #xt/date-time "2020-10-07T00:00:09"]}]
             (xt/q tu/*node* "SELECT range(TIMESTAMP '2020-01-01T00:00:00', TIMESTAMP '2021-01-01T00:00:00', INTERVAL 'P3M2DT3S') timestamps"))
          "mixed interval"))

  (t/testing "TIMESTAMPTZ"
    (t/is (= [{:timestamps [#xt/zoned-date-time "2020-03-28T00:00Z[Europe/London]"
                            #xt/zoned-date-time "2020-03-29T00:00Z[Europe/London]"
                            #xt/zoned-date-time "2020-03-30T01:00+01:00[Europe/London]"]}]
             (xt/q tu/*node* "SELECT range(TIMESTAMP '2020-03-28T00:00:00Z[Europe/London]', TIMESTAMP '2020-03-31T00:00:00+01:00[Europe/London]', INTERVAL 'PT24H') timestamps")))

    (t/is (= [{:ts #xt/zoned-date-time "2020-03-28T00:00Z[Europe/London]"}
              {:ts #xt/zoned-date-time "2020-03-29T00:00Z[Europe/London]"}
              {:ts #xt/zoned-date-time "2020-03-30T00:00+01:00[Europe/London]"}]
             (xt/q tu/*node* "FROM range(TIMESTAMP '2020-03-28T00:00:00Z[Europe/London]', TIMESTAMP '2020-03-31T00:00:00+01:00[Europe/London]', INTERVAL 'P1D') timestamps (ts)")))

    (t/is (= [{:ts #xt/zoned-date-time "2020-03-28T00:00Z[Europe/London]", :idx 1}
              {:ts #xt/zoned-date-time "2020-03-29T00:00Z[Europe/London]", :idx 2}
              {:ts #xt/zoned-date-time "2020-03-30T00:00+01:00[Europe/London]", :idx 3}]
             (xt/q tu/*node* "FROM range(TIMESTAMP '2020-03-28T00:00:00Z[Europe/London]', TIMESTAMP '2020-03-31T00:00:00+01:00[Europe/London]', INTERVAL 'P1D') WITH ORDINALITY timestamps (ts, idx)")))))

(t/deftest test-generate-series-3212
  (t/is (= [{:xs [1 2 3 4]}]
           (xt/q tu/*node* "SELECT generate_series(1, 4) xs")))

  (t/is (= [{:xs [1 4 7]}]
           (xt/q tu/*node* "SELECT generate_series(1, 8, 3) xs")))

  (t/is (= [{:xs []}]
           (xt/q tu/*node* "SELECT generate_series(10, 3) xs")))

  (t/is (= [{:xs [1]}]
           (xt/q tu/*node* "SELECT generate_series(1, 1) xs")))

  (t/is (= [{:xs [1]}]
           (xt/q tu/*node* "SELECT generate_series(1, 2, 2) xs")))

  (xt/submit-tx tu/*node* [[:sql "INSERT INTO foo RECORDS {_id: 1, start: -1, end: 3}"]])

  (t/is (= [{:xs [-1 0 1 2 3]}]
           (xt/q tu/*node* "SELECT generate_series(start, end) xs FROM foo"))))

(t/deftest test-negative-step
  (t/is (= [{:xs [5 4 3 2]}]
           (xt/q tu/*node* "SELECT range(5, 1, -1) xs"))
        "range, negative step")

  (t/is (= [{:xs [5 4 3 2 1]}]
           (xt/q tu/*node* "SELECT generate_series(5, 1, -1) xs"))
        "generate_series, negative step")

  (t/is (= [{:xs [10 7 4]}]
           (xt/q tu/*node* "SELECT range(10, 3, -3) xs"))
        "range, negative step with stride")

  (t/is (= [{:xs []}]
           (xt/q tu/*node* "SELECT range(1, 5, -1) xs"))
        "negative step, wrong direction"))

(t/deftest test-generate-series-datetimes-4067
  (t/testing "DATE"
    (t/is (= [{:dates [#xt/date-time "2020-01-01T00:00"]}]
             (xt/q tu/*node* "SELECT generate_series(DATE '2020-01-01', DATE '2020-01-01', INTERVAL 'P1D') dates"))
          "start == end")

    (t/is (= [{:dates [#xt/date-time "2020-01-01T00:00", #xt/date-time "2020-01-02T00:00", #xt/date-time "2020-01-03T00:00", #xt/date-time "2020-01-04T00:00"]}]
             (xt/q tu/*node* "SELECT generate_series(DATE '2020-01-01', DATE '2020-01-04', INTERVAL 'P1D') dates"))
          "date range")

    (t/is (= [{:date #xt/date-time "2020-01-01T00:00"}
              {:date #xt/date-time "2020-01-03T00:00"}
              {:date #xt/date-time "2020-01-05T00:00"}
              {:date #xt/date-time "2020-01-07T00:00"}]
             (xt/q tu/*node* "FROM generate_series(DATE '2020-01-01', DATE '2020-01-07', INTERVAL 'P2D') dates (`date`)"))
          "multi-day interval")

    (t/is (= [{:dates [#xt/date "2020-01-01", #xt/date "2020-02-01", #xt/date "2020-03-01", #xt/date "2020-04-01"]}]
             (xt/q tu/*node* "SELECT generate_series(DATE '2020-01-01', DATE '2020-04-01', INTERVAL 'P1M') dates"))
          "monthly interval")

    (t/is (= [{:date #xt/date "2020-01-01"} {:date #xt/date "2021-01-01"} {:date #xt/date "2022-01-01"}]
             (xt/q tu/*node* "FROM generate_series(DATE '2020-01-01', DATE '2022-01-01', INTERVAL 'P1Y') dates (`date`)"))
          "yearly interval")

    (t/is (= [{:dates [#xt/date-time "2020-01-01T00:00"
                       #xt/date-time "2020-01-01T06:00"
                       #xt/date-time "2020-01-01T12:00"
                       #xt/date-time "2020-01-01T18:00"
                       #xt/date-time "2020-01-02T00:00"]}]
             (xt/q tu/*node* "SELECT generate_series(DATE '2020-01-01', DATE '2020-01-02', INTERVAL 'PT6H') dates"))
          "time interval"))

  (t/testing "TIMESTAMP"
    (t/is (= [{:timestamps [#xt/date-time "2020-01-01T00:00", #xt/date-time "2020-01-02T00:00"]}]
             (xt/q tu/*node* "SELECT generate_series(TIMESTAMP '2020-01-01 00:00:00', TIMESTAMP '2020-01-02 00:00:00', INTERVAL 'PT24H') timestamps")))

    (t/is (= [{:timestamps [#xt/date-time "2020-01-01T00:00"
                            #xt/date-time "2020-01-01T01:00"
                            #xt/date-time "2020-01-01T02:00"
                            #xt/date-time "2020-01-01T03:00"]}]
             (xt/q tu/*node* "SELECT generate_series(TIMESTAMP '2020-01-01 00:00:00', TIMESTAMP '2020-01-01 03:00:00', INTERVAL 'PT1H') timestamps"))
          "hour steps")

    (t/is (= [{:timestamps [#xt/date-time "2020-01-01T00:00", #xt/date-time "2020-02-01T00:00", #xt/date-time "2020-03-01T00:00"]}]
             (xt/q tu/*node* "SELECT generate_series(TIMESTAMP '2020-01-01 00:00:00', TIMESTAMP '2020-03-01 00:00:00', INTERVAL 'P1M') timestamps"))
          "monthly interval")

    (t/is (= [{:timestamps [#xt/date-time "2020-01-01T00:00", #xt/date-time "2021-01-01T00:00", #xt/date-time "2022-01-01T00:00"]}]
             (xt/q tu/*node* "SELECT generate_series(TIMESTAMP '2020-01-01 00:00:00', TIMESTAMP '2022-01-01 00:00:00', INTERVAL 'P1Y') timestamps"))
          "yearly interval"))

  (t/testing "TIMESTAMPTZ"
    (t/is (= [{:ts #xt/zoned-date-time "2020-03-28T00:00Z[Europe/London]"}
              {:ts #xt/zoned-date-time "2020-03-29T00:00Z[Europe/London]"}
              {:ts #xt/zoned-date-time "2020-03-30T00:00+01:00[Europe/London]"}
              {:ts #xt/zoned-date-time "2020-03-31T00:00+01:00[Europe/London]"}]
             (xt/q tu/*node* "FROM generate_series(TIMESTAMP '2020-03-28T00:00:00Z[Europe/London]', TIMESTAMP '2020-03-31T00:00:00+01:00[Europe/London]', INTERVAL 'P1D') timestamps (ts)")))))

(t/deftest test-end-exclusive-escape-hatch
  ;; XTDB_GENERATE_SERIES_END_EXCLUSIVE reverts GENERATE_SERIES to pre-2.2 (end-exclusive) behaviour,
  ;; so existing queries can keep working while being migrated to RANGE.
  ;;
  ;; The env var is read at namespace load (process start) — `xt/q` goes via pgwire on a separate thread
  ;; which won't see a `binding` in the test thread, so we test the planner output directly.
  (t/is (not= (sql/plan "SELECT generate_series(1, 4) xs")
              (sql/plan "SELECT range(1, 4) xs"))
        "without the flag, GENERATE_SERIES and RANGE produce different plans")

  (binding [sql/*generate-series-end-exclusive?* true]
    (t/is (= (sql/plan "SELECT generate_series(1, 4) xs")
             (sql/plan "SELECT range(1, 4) xs"))
          "with the flag, GENERATE_SERIES delegates to the same plan as RANGE")

    (t/is (not= (sql/plan "SELECT generate_series(1, 4) xs")
                (binding [sql/*generate-series-end-exclusive?* false]
                  (sql/plan "SELECT generate_series(1, 4) xs")))
          "the flag actually changes the GENERATE_SERIES plan")))

(t/deftest test-generate-series-limit-batching-4412
  (t/is (= [{:xt/id 1}
            {:xt/id 2}
            {:xt/id 3}
            {:xt/id 4}
            {:xt/id 5}]
           (xt/q tu/*node* "SELECT system._id FROM generate_series(1, 200000000) AS system(_id) LIMIT 5"))
        "generate_series with large range + LIMIT should not consume all memory")

  (t/is (= [{:xt/id 100000001}
            {:xt/id 100000002}
            {:xt/id 100000003}
            {:xt/id 100000004}
            {:xt/id 100000005}]
           (xt/q tu/*node* "
             SELECT system._id
             FROM generate_series(1, 200000000) AS system(_id)
             OFFSET 100000000
             LIMIT 5"))
        "generate_series with large range, OFFSET + LIMIT should not consume all memory"))
