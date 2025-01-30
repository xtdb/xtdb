(ns xtdb.sql.generate-series-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.test-util :as tu])
  (:import [java.time LocalDate]))

(t/use-fixtures :each tu/with-mock-clock tu/with-node)

(t/deftest test-generate-series-3212
  (t/is (= [{:xs [1 2 3]}]
           (xt/q tu/*node* "SELECT generate_series(1, 4) xs")))

  (t/is (= [{:xs [1 4 7]}]
           (xt/q tu/*node* "SELECT generate_series(1, 8, 3) xs")))

  (t/is (= [{:xs []}]
           (xt/q tu/*node* "SELECT generate_series(10, 3) xs")))

  (t/is (= [{:xs []}]
           (xt/q tu/*node* "SELECT generate_series(1, 1) xs")))

  (t/is (= [{:xs [1]}]
           (xt/q tu/*node* "SELECT generate_series(1, 2, 2) xs")))

  (xt/submit-tx tu/*node* [[:sql "INSERT INTO foo RECORDS {_id: 1, start: -1, end: 3}"]])

  (t/is (= [{:xs [-1 0 1 2]}]
           (xt/q tu/*node* "SELECT generate_series(start, end) xs FROM foo"))))

(t/deftest test-generate-series-datetimes-4067
  (t/testing "DATE"
    (t/is (= [{:dates []}]
             (xt/q tu/*node* "SELECT generate_series(DATE '2020-01-01', DATE '2020-01-01', INTERVAL 'P1D') dates"))
          "single date, should return empty")

    (t/is (= [{:dates [#xt/date-time "2020-01-01T00:00", #xt/date-time "2020-01-02T00:00", #xt/date-time "2020-01-03T00:00"]}]
             (xt/q tu/*node* "SELECT generate_series(DATE '2020-01-01', DATE '2020-01-04', INTERVAL 'P1D') dates"))
          "date range")

    #_ ; FIXME reverse order is not supported yet
    (t/is (= [{:dates [#inst "2020-01-07" #inst "2020-01-06" #inst "2020-01-05" #inst "2020-01-04" #inst "2020-01-03" #inst "2020-01-02"]}]
             (xt/q tu/*node* "SELECT generate_series(DATE '2020-01-07', DATE '2020-01-01', INTERVAL 'P-1D') dates"))
          "reverse order")

    (t/is (= [{:date #xt/date-time "2020-01-01T00:00"}
              {:date #xt/date-time "2020-01-03T00:00"}
              {:date #xt/date-time "2020-01-05T00:00"}]
             (xt/q tu/*node* "FROM generate_series(DATE '2020-01-01', DATE '2020-01-07', INTERVAL 'P2D') dates (`date`)"))
          "multi-day interval")

    (t/is (= [{:date #xt/date-time "2020-01-01T00:00", :idx 1}
              {:date #xt/date-time "2020-01-03T00:00", :idx 2}
              {:date #xt/date-time "2020-01-05T00:00", :idx 3}]
             (xt/q tu/*node* "FROM generate_series(DATE '2020-01-01', DATE '2020-01-07', INTERVAL 'P2D') WITH ORDINALITY dates (`date`, idx)"))
          "multi-day interval, WITH ORDINALITY")

    (t/is (= [{:dates [#xt/date "2020-01-01", #xt/date "2020-02-01", #xt/date "2020-03-01"]}]
             (xt/q tu/*node* "SELECT generate_series(DATE '2020-01-01', DATE '2020-04-01', INTERVAL '1' MONTH) dates"))
          "monthly interval")

    (t/is (= [{:dates [#xt/date-time "2020-01-01T00:00"
                       #xt/date-time "2020-02-01T00:00"
                       #xt/date-time "2020-03-01T00:00"]}]
             (xt/q tu/*node* "SELECT generate_series(DATE '2020-01-01', DATE '2020-04-01', INTERVAL 'P1M') dates"))
          "monthly interval")

    (t/is (= [{:date #xt/date-time "2020-01-01T00:00"} {:date #xt/date-time "2021-01-01T00:00"}]
             (xt/q tu/*node* "FROM generate_series(DATE '2020-01-01', DATE '2022-01-01', INTERVAL 'P1Y') dates (`date`)"))
          "yearly interval")

    (t/is (= [{:date #xt/date "2020-01-01"} {:date #xt/date "2021-01-01"}]
             (xt/q tu/*node* "FROM generate_series(DATE '2020-01-01', DATE '2022-01-01', INTERVAL '1' YEAR) dates (`date`)"))
          "yearly interval")

    (t/is (= [{:dates [#xt/date-time "2020-01-01T00:00"
                       #xt/date-time "2020-01-01T06:00"
                       #xt/date-time "2020-01-01T12:00"
                       #xt/date-time "2020-01-01T18:00"]}]
             (xt/q tu/*node* "SELECT generate_series(DATE '2020-01-01', DATE '2020-01-02', INTERVAL 'PT6H') dates"))
          "time interval")

    (t/is (= [{:dates [#xt/date-time "2020-01-01T00:00"
                       #xt/date-time "2020-04-02T23:59:57"
                       #xt/date-time "2020-07-04T23:59:54"
                       #xt/date-time "2020-10-06T23:59:51"]}]
             (xt/q tu/*node* "SELECT generate_series(DATE '2020-01-01', DATE '2021-01-01', INTERVAL 'P3M2DT-3S') dates"))
          "mixed interval"))

  (t/testing "TIMESTAMP"
    (t/is (= [{:timestamps [#xt/date-time "2020-01-01T00:00"]}]
             (xt/q tu/*node* "SELECT generate_series(TIMESTAMP '2020-01-01 00:00:00', TIMESTAMP '2020-01-02 00:00:00', INTERVAL 'PT24H') timestamps")))

    (t/is (= [{:timestamps [#xt/date-time "2020-01-01T00:00"
                            #xt/date-time "2020-01-01T01:00"
                            #xt/date-time "2020-01-01T02:00"]}]
             (xt/q tu/*node* "SELECT generate_series(TIMESTAMP '2020-01-01 00:00:00', TIMESTAMP '2020-01-01 03:00:00', INTERVAL 'PT1H') timestamps"))
          "hour steps")

    (t/is (= [{:ts #xt/date-time "2020-01-01T00:00"}
              {:ts #xt/date-time "2020-01-01T00:00:00.500"}
              {:ts #xt/date-time "2020-01-01T00:00:01"}
              {:ts #xt/date-time "2020-01-01T00:00:01.500"}
              {:ts #xt/date-time "2020-01-01T00:00:02"}
              {:ts #xt/date-time "2020-01-01T00:00:02.500"}]
             (xt/q tu/*node* "FROM generate_series(TIMESTAMP '2020-01-01 00:00:00', TIMESTAMP '2020-01-01 00:00:03', INTERVAL 'PT0.5S') timestamps (ts)"))
          "fractional seconds")

    (t/is (= [{:timestamps [#xt/date-time "2020-01-01T00:00", #xt/date-time "2020-02-01T00:00"]}]
             (xt/q tu/*node* "SELECT generate_series(TIMESTAMP '2020-01-01 00:00:00', TIMESTAMP '2020-03-01 00:00:00', INTERVAL 'P1M') timestamps"))
          "monthly interval")

    (t/is (= [{:timestamps [#xt/date-time "2020-01-01T00:00", #xt/date-time "2021-01-01T00:00"]}]
             (xt/q tu/*node* "SELECT generate_series(TIMESTAMP '2020-01-01 00:00:00', TIMESTAMP '2022-01-01 00:00:00', INTERVAL 'P1Y') timestamps"))
          "yearly interval")

    (t/is (= [{:timestamps [#xt/date-time "2020-01-01T00:00"
                            #xt/date-time "2020-04-03T00:00:03"
                            #xt/date-time "2020-07-05T00:00:06"
                            #xt/date-time "2020-10-07T00:00:09"]}]
             (xt/q tu/*node* "SELECT generate_series(TIMESTAMP '2020-01-01T00:00:00', TIMESTAMP '2021-01-01T00:00:00', INTERVAL 'P3M2DT3S') timestamps"))
          "mixed interval"))

  (t/testing "TIMESTAMPTZ"
    (t/is (= [{:timestamps [#xt/zoned-date-time "2020-03-28T00:00Z[Europe/London]"
                            #xt/zoned-date-time "2020-03-29T00:00Z[Europe/London]"
                            #xt/zoned-date-time "2020-03-30T01:00+01:00[Europe/London]"]}]
             (xt/q tu/*node* "SELECT generate_series(TIMESTAMP '2020-03-28T00:00:00Z[Europe/London]', TIMESTAMP '2020-03-31T00:00:00+01:00[Europe/London]', INTERVAL 'PT24H') timestamps")))

    (t/is (= [{:ts #xt/zoned-date-time "2020-03-28T00:00Z[Europe/London]"}
              {:ts #xt/zoned-date-time "2020-03-29T00:00Z[Europe/London]"}
              {:ts #xt/zoned-date-time "2020-03-30T00:00+01:00[Europe/London]"}]
             (xt/q tu/*node* "FROM generate_series(TIMESTAMP '2020-03-28T00:00:00Z[Europe/London]', TIMESTAMP '2020-03-31T00:00:00+01:00[Europe/London]', INTERVAL 'P1D') timestamps (ts)")))

    (t/is (= [{:ts #xt/zoned-date-time "2020-03-28T00:00Z[Europe/London]", :idx 1}
              {:ts #xt/zoned-date-time "2020-03-29T00:00Z[Europe/London]", :idx 2}
              {:ts #xt/zoned-date-time "2020-03-30T00:00+01:00[Europe/London]", :idx 3}]
             (xt/q tu/*node* "FROM generate_series(TIMESTAMP '2020-03-28T00:00:00Z[Europe/London]', TIMESTAMP '2020-03-31T00:00:00+01:00[Europe/London]', INTERVAL 'P1D') WITH ORDINALITY timestamps (ts, idx)")))))
