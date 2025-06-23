(ns xtdb.pgwire.types-test
  (:require [clojure.test :as t]
            [xtdb.pgwire.types :as pg-types]
            [xtdb.test-util :as tu]))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-pg-datetime-binary-roundtrip
  (doseq [{:keys [type val]} [{:val #xt/date "2018-07-25" :type :date}
                              {:val #xt/date-time "1441-07-25T18:00:11.888842" :type :timestamp}
                              {:val #xt/offset-date-time "1441-07-25T18:00:11.211142Z" :type :timestamptz}]]
    (let [{:keys [write-binary read-binary]} (get pg-types/pg-types type)]

      (with-open [rdr (tu/open-vec "val" [val])]
        (t/is (= val (read-binary {} (write-binary {} rdr 0))))))))
