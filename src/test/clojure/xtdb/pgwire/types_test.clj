(ns xtdb.pgwire.types-test
  (:require [clojure.test :as t]
            [xtdb.test-util :as tu])
  (:import (java.time LocalDate)
           (xtdb.pgwire PgType)))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-pg-datetime-binary-roundtrip
  (doseq [{:keys [^PgType pg-type val]} [{:val #xt/date "2018-07-25" :pg-type PgType/PG_DATE}
                                         {:val #xt/date-time "1441-07-25T18:00:11.888842" :pg-type PgType/PG_TIMESTAMP}
                                         {:val #xt/offset-date-time "1441-07-25T18:00:11.211142Z" :pg-type PgType/PG_TIMESTAMPTZ}]]
    (with-open [rdr (tu/open-vec "val" [val])]
      (t/is (= val (.readBinary pg-type (.write (.binaryWriter pg-type rdr) {} (.next (.valueReader rdr)))))))))

(t/deftest test-pg-date-binary-from-date-milli
  (with-open [rdr (tu/open-vec "val" #xt/type [:date :milli] [#xt/date "2018-07-25" #xt/date "1969-12-31"])]
    (let [w (.binaryWriter PgType/PG_DATE rdr)
          vr (.valueReader rdr)]
      (t/is (= [#xt/date "2018-07-25" #xt/date "1969-12-31"]
               (vec (for [_ (range 2)]
                      (.readBinary PgType/PG_DATE (.write w {} (.next vr)))))))))

  (with-open [rdr (tu/open-vec "val" #xt/type [:date :milli] [(LocalDate/of 6000000 1 1)])]
    (t/is (thrown? ArithmeticException
                   (.write (.binaryWriter PgType/PG_DATE rdr) {} (.next (.valueReader rdr))))
          "a date past int4 days raises rather than wrapping")))
