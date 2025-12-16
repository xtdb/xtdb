(ns xtdb.pgwire.types-test
  (:require [clojure.test :as t]
            [xtdb.test-util :as tu])
  (:import (xtdb.pgwire PgType)))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-pg-datetime-binary-roundtrip
  (doseq [{:keys [^PgType pg-type val]} [{:val #xt/date "2018-07-25" :pg-type PgType/PG_DATE}
                                         {:val #xt/date-time "1441-07-25T18:00:11.888842" :pg-type PgType/PG_TIMESTAMP}
                                         {:val #xt/offset-date-time "1441-07-25T18:00:11.211142Z" :pg-type PgType/PG_TIMESTAMPTZ}]]
    (with-open [rdr (tu/open-vec "val" [val])]
      (t/is (= val (.readBinary pg-type (.writeBinary pg-type {} rdr 0)))))))
