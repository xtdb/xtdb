(ns xtdb.sql.interval-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.test-util :as tu]))

(t/use-fixtures :each tu/with-node)

(defn- q [expr]
  (-> (xt/q tu/*node* (format "SELECT %s r" expr) {})
      first :r))

(t/deftest test-interval-literals
  (t/is (= #xt/interval "P36M" (q "INTERVAL '3' YEAR")))
  (t/is (= #xt/interval "P-36M" (q "INTERVAL '-3' YEAR")))
  (t/is (= #xt/interval "P3M" (q "INTERVAL '3' MONTH")))
  (t/is (= #xt/interval "P-3M" (q "INTERVAL '-3' MONTH")))
  (t/is (= #xt/interval "P40M" (q "INTERVAL '3-4' YEAR TO MONTH")))

  (t/is (= #xt/interval "P3DT4H" (q "INTERVAL '3 4' DAY TO HOUR")))
  (t/is (= #xt/interval "P3DT4H" (q "INTERVAL '3 04' DAY TO HOUR")))
  (t/is (= #xt/interval "P3DT4H20M" (q "INTERVAL '3 04:20' DAY TO MINUTE")))
  (t/is (= #xt/interval "P3DT4H20M34S" (q "INTERVAL '3 04:20:34' DAY TO SECOND")))
  (t/is (= #xt/interval "P0DT4H20M" (q "INTERVAL '04:20' HOUR TO MINUTE")))
  (t/is (= #xt/interval "P0DT4H20M34S" (q "INTERVAL '04:20:34' HOUR TO SECOND")))
  (t/is (= #xt/interval "P0DT4H20M34.245S" (q "INTERVAL '04:20:34.245' HOUR TO SECOND")))
  (t/is (= #xt/interval "P0DT20M34S" (q "INTERVAL '20:34' MINUTE TO SECOND"))))

(t/deftest test-interval-fns
  (t/is (= #xt/interval "P-12M" (q "INTERVAL '-1' YEAR")))
  (t/is (= #xt/interval "P12M" (q "ABS(INTERVAL '-1' YEAR)"))))
