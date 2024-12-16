(ns xtdb.default-tz-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            xtdb.serde
            [xtdb.test-util :as tu]))

(t/use-fixtures :each
  (tu/with-opts {:default-tz #xt/zone "Europe/London"})
  (tu/with-each-api-implementation
    (-> {:in-memory (t/join-fixtures [tu/with-mock-clock tu/with-node]),
         :remote (t/join-fixtures [tu/with-mock-clock tu/with-http-client-node])}
        #_(select-keys [:in-memory])
        #_(select-keys [:remote]))))

(t/deftest can-specify-default-tz-in-query-396
  (let [q (str "SELECT CAST(DATE '2020-08-01' AS TIMESTAMP WITH TIME ZONE) AS tstz "
               "FROM (VALUES (NULL)) a (a)")]
    (t/is (= [{:tstz #xt/zoned-date-time "2020-08-01T00:00+01:00[Europe/London]"}]
             (xt/q tu/*node* q {})))

    (t/is (= [{:tstz #xt/zoned-date-time "2020-08-01T00:00-07:00[America/Los_Angeles]"}]
             (xt/q tu/*node* q {:default-tz #xt/zone "America/Los_Angeles"})))))

(t/deftest can-specify-default-tz-in-dml-396
  (let [q "INSERT INTO foo (_id, dt, tstz) VALUES (?, DATE '2020-08-01', CAST(DATE '2020-08-01' AS TIMESTAMP WITH TIME ZONE))"]
    (xt/submit-tx tu/*node* [[:sql q ["foo"]]])
    (xt/submit-tx tu/*node* [[:sql q ["bar"]]]
                  {:default-tz #xt/zone "America/Los_Angeles"})

    (let [q "SELECT foo._id, foo.dt, CAST(foo.dt AS TIMESTAMP WITH TIME ZONE) cast_tstz, foo.tstz FROM foo"]

      (t/is (= #{{:xt/id "foo", :dt #xt/date "2020-08-01",
                  :cast-tstz #xt/zoned-date-time "2020-08-01T00:00+01:00[Europe/London]"
                  :tstz #xt/zoned-date-time "2020-08-01T00:00+01:00[Europe/London]"}
                 {:xt/id "bar", :dt #xt/date "2020-08-01"
                  :cast-tstz #xt/zoned-date-time "2020-08-01T00:00+01:00[Europe/London]"
                  :tstz #xt/zoned-date-time "2020-08-01T00:00-07:00[America/Los_Angeles]"}}

               (set (xt/q tu/*node* q))))

      (t/is (= #{{:xt/id "foo", :dt #xt/date "2020-08-01",
                  :cast-tstz #xt/zoned-date-time "2020-08-01T00:00-07:00[America/Los_Angeles]"
                  :tstz #xt/zoned-date-time "2020-08-01T00:00+01:00[Europe/London]"}
                 {:xt/id "bar", :dt #xt/date "2020-08-01"
                  :cast-tstz #xt/zoned-date-time "2020-08-01T00:00-07:00[America/Los_Angeles]"
                  :tstz #xt/zoned-date-time "2020-08-01T00:00-07:00[America/Los_Angeles]"}}

               (set (xt/q tu/*node* q {:default-tz #xt/zone "America/Los_Angeles"})))))))

(t/deftest test-xtql-default-tz
  #_ ; FIXME #3020
  (t/is (= [{:time #xt/time "16:00"}]
           (xt/q tu/*node* '(rel [{:time (local-time)}] [time])
                 {:current-time (time/->instant #inst "2024-01-01")
                  :default-tz #xt/zone "America/Los_Angeles"}))))
