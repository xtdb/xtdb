(ns core2.sql.interval-test
  (:require [clojure.test :as t]
            [core2.sql :as c2]
            [core2.test-util :as tu]))

(t/use-fixtures :each tu/with-node)

(defn- q [expr]
  (-> (c2/q tu/*node* (format "SELECT %s r FROM (VALUES ('a')) a" expr) {})
      first :r))

(t/deftest test-interval-literals
    (t/is (= #c2/interval-ym "P36M" (q "INTERVAL '3' YEAR")))
    (t/is (= #c2/interval-ym "P-36M" (q "INTERVAL '-3' YEAR")))
    (t/is (= #c2/interval-ym "P3M" (q "INTERVAL '3' MONTH")))
    (t/is (= #c2/interval-ym "P-3M" (q "INTERVAL '-3' MONTH")))
    (t/is (= #c2/interval-ym "P40M" (q "INTERVAL '3-4' YEAR TO MONTH")))

    (t/is (= #c2/interval-mdn ["P3D" "PT4H"] (q "INTERVAL '3 4' DAY TO HOUR")))
    (t/is (= #c2/interval-mdn ["P3D" "PT4H"] (q "INTERVAL '3 04' DAY TO HOUR")))
    (t/is (= #c2/interval-mdn ["P3D" "PT4H20M"] (q "INTERVAL '3 04:20' DAY TO MINUTE")))
    (t/is (= #c2/interval-mdn ["P3D" "PT4H20M34S"] (q "INTERVAL '3 04:20:34' DAY TO SECOND")))
    (t/is (= #c2/interval-mdn ["P0D" "PT4H20M"] (q "INTERVAL '04:20' HOUR TO MINUTE")))
    (t/is (= #c2/interval-mdn ["P0D" "PT4H20M34S"] (q "INTERVAL '04:20:34' HOUR TO SECOND")))
    (t/is (= #c2/interval-mdn ["P0D" "PT4H20M34.245S"] (q "INTERVAL '04:20:34.245' HOUR TO SECOND")))
  (t/is (= #c2/interval-mdn ["P0D" "PT20M34S"] (q "INTERVAL '20:34' MINUTE TO SECOND"))))

(t/deftest test-interval-fns
  (t/is (= #c2/interval-ym "P-12M" (q "-1 YEAR")))
  (t/is (= #c2/interval-ym "P12M" (q "ABS(-1 YEAR)"))))
