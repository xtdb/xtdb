(ns xtdb.error-test
  (:require [clojure.test :as t]
            [clojure.pprint :as pprint])
  (:import [xtdb.error Busy]))

(t/deftest test-anomaly-pretty-printing
  (t/testing "Busy anomaly pretty-prints correctly"
    (let [ex (Busy. "System is busy" "BUSY" {"retry-after" 5000} nil)
          ex-2 (Busy. "System is busy" {"retry-after" 5000} nil)
          ex-3 (Busy. "System is busy" {"retry-after" 5000} (Exception. "Foo"))]
      (t/is (nil? (pprint/pprint ex)))
      (t/is (nil? (pprint/pprint ex-2)))
      (t/is (nil? (pprint/pprint ex-3))))))
