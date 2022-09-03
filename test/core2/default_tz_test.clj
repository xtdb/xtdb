(ns core2.default-tz-test
  (:require [clojure.test :as t]
            [core2.test-util :as tu]
            [core2.api :as c2]))

(t/use-fixtures :each
  (tu/with-opts {:core2/default-tz #time/zone "Europe/London"})
  (tu/with-each-api-implementation
    (-> {:in-memory (t/join-fixtures [tu/with-mock-clock tu/with-node]),
         :remote (t/join-fixtures [tu/with-mock-clock tu/with-http-client-node])}
        #_(select-keys [:in-memory])
        #_(select-keys [:remote]))))

(t/deftest can-specify-default-tz-in-query-396
  (let [q (str "SELECT CAST(DATE '2020-08-01' AS TIMESTAMP WITH TIME ZONE) AS tstz "
               "FROM (VALUES (NULL)) a (a)")]
    (t/is (= [{:tstz #time/zoned-date-time "2020-08-01T00:00+01:00[Europe/London]"}]
             (c2/sql-query tu/*node* q {})))

    (t/is (= [{:tstz #time/zoned-date-time "2020-08-01T00:00-07:00[America/Los_Angeles]"}]
             (c2/sql-query tu/*node* q {:default-tz #time/zone "America/Los_Angeles"})))))

(t/deftest can-specify-default-tz-in-dml-396
  (let [q "INSERT INTO foo (id, dt, tstz) VALUES (?, DATE '2020-08-01', CAST(DATE '2020-08-01' AS TIMESTAMP WITH TIME ZONE))"]
    @(c2/submit-tx tu/*node* [[:sql q [["foo"]]]])
    (let [tx @(c2/submit-tx tu/*node* [[:sql q [["bar"]]]]
                            {:default-tz #time/zone "America/Los_Angeles"})
          q "SELECT foo.id, foo.dt, CAST(foo.dt AS TIMESTAMP WITH TIME ZONE) cast_tstz, foo.tstz FROM foo"]

      (t/is (= [{:id "foo", :dt #time/date "2020-08-01",
                 :cast_tstz #time/zoned-date-time "2020-08-01T00:00+01:00[Europe/London]"
                 :tstz #time/zoned-date-time "2020-08-01T00:00+01:00[Europe/London]"}
                {:id "bar", :dt #time/date "2020-08-01"
                 :cast_tstz #time/zoned-date-time "2020-08-01T00:00+01:00[Europe/London]"
                 :tstz #time/zoned-date-time "2020-08-01T00:00-07:00[America/Los_Angeles]"}]

               (c2/sql-query tu/*node* q {:basis {:tx tx}})))

      (t/is (= [{:id "foo", :dt #time/date "2020-08-01",
                 :cast_tstz #time/zoned-date-time "2020-08-01T00:00-07:00[America/Los_Angeles]"
                 :tstz #time/zoned-date-time "2020-08-01T00:00+01:00[Europe/London]"}
                {:id "bar", :dt #time/date "2020-08-01"
                 :cast_tstz #time/zoned-date-time "2020-08-01T00:00-07:00[America/Los_Angeles]"
                 :tstz #time/zoned-date-time "2020-08-01T00:00-07:00[America/Los_Angeles]"}]

               (c2/sql-query tu/*node* q
                             {:basis {:tx tx}, :default-tz #time/zone "America/Los_Angeles"}))))))
