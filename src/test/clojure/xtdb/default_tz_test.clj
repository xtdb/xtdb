(ns xtdb.default-tz-test
  (:require [clojure.test :as t]
            [xtdb.datalog :as xt-datalog]
            [xtdb.sql :as xt]
            [xtdb.test-util :as tu]
            [xtdb.util :as util]))

(t/use-fixtures :each
  (tu/with-opts {:xtdb/default-tz #time/zone "Europe/London"})
  (tu/with-each-api-implementation
    (-> {:in-memory (t/join-fixtures [tu/with-mock-clock tu/with-node]),
         :remote (t/join-fixtures [tu/with-mock-clock tu/with-http-client-node])}
        #_(select-keys [:in-memory])
        #_(select-keys [:remote]))))

(t/deftest can-specify-default-tz-in-query-396
  (let [q (str "SELECT CAST(DATE '2020-08-01' AS TIMESTAMP WITH TIME ZONE) AS tstz "
               "FROM (VALUES (NULL)) a (a)")]
    (t/is (= [{:tstz #time/zoned-date-time "2020-08-01T00:00+01:00[Europe/London]"}]
             (xt/q tu/*node* q {})))

    (t/is (= [{:tstz #time/zoned-date-time "2020-08-01T00:00-07:00[America/Los_Angeles]"}]
             (xt/q tu/*node* q {:default-tz #time/zone "America/Los_Angeles"})))))

(t/deftest can-specify-default-tz-in-dml-396
  (let [q "INSERT INTO foo (xt$id, dt, tstz) VALUES (?, DATE '2020-08-01', CAST(DATE '2020-08-01' AS TIMESTAMP WITH TIME ZONE))"]
    (xt/submit-tx tu/*node* [[:sql [q "foo"]]])
    (let [tx (xt/submit-tx tu/*node* [[:sql [q "bar"]]]
                           {:default-tz #time/zone "America/Los_Angeles"})
          q "SELECT foo.xt$id, foo.dt, CAST(foo.dt AS TIMESTAMP WITH TIME ZONE) cast_tstz, foo.tstz FROM foo"]

      (t/is (= [{:xt$id "foo", :dt #time/date "2020-08-01",
                 :cast_tstz #time/zoned-date-time "2020-08-01T00:00+01:00[Europe/London]"
                 :tstz #time/zoned-date-time "2020-08-01T00:00+01:00[Europe/London]"}
                {:xt$id "bar", :dt #time/date "2020-08-01"
                 :cast_tstz #time/zoned-date-time "2020-08-01T00:00+01:00[Europe/London]"
                 :tstz #time/zoned-date-time "2020-08-01T00:00-07:00[America/Los_Angeles]"}]

               (xt/q tu/*node* q {:basis {:tx tx}})))

      (t/is (= [{:xt$id "foo", :dt #time/date "2020-08-01",
                 :cast_tstz #time/zoned-date-time "2020-08-01T00:00-07:00[America/Los_Angeles]"
                 :tstz #time/zoned-date-time "2020-08-01T00:00+01:00[Europe/London]"}
                {:xt$id "bar", :dt #time/date "2020-08-01"
                 :cast_tstz #time/zoned-date-time "2020-08-01T00:00-07:00[America/Los_Angeles]"
                 :tstz #time/zoned-date-time "2020-08-01T00:00-07:00[America/Los_Angeles]"}]

               (xt/q tu/*node* q
                     {:basis {:tx tx}
                      :default-tz #time/zone "America/Los_Angeles"}))))))

(t/deftest test-datalog-default-tz
  (t/is (= [{:time #time/time "16:00"}]
           (xt-datalog/q tu/*node*
                         (assoc '{:find [time]
                                  :where [[(local-time) time]]
                                  :default-tz #time/zone "America/Los_Angeles"}
                                :basis {:current-time (util/->instant #inst "2024-01-01")})))))
