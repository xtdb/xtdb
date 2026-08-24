(ns xtdb.error-test
  (:require [clojure.test :as t]
            [clojure.pprint :as pprint]
            [xtdb.error :as err])
  (:import [java.nio.channels ClosedByInterruptException]
           [xtdb.api.error Busy Interrupted]))

(t/deftest test-anomaly-pretty-printing
  (t/testing "Busy anomaly pretty-prints correctly"
    (let [ex (Busy. "System is busy" "BUSY" {"retry-after" 5000} nil)
          ex-2 (Busy. "System is busy" {"retry-after" 5000} nil)
          ex-3 (Busy. "System is busy" {"retry-after" 5000} (Exception. "Foo"))]
      (t/is (nil? (pprint/pprint ex)))
      (t/is (nil? (pprint/pprint ex-2)))
      (t/is (nil? (pprint/pprint ex-3))))))

(t/deftest interrupts-escape-wrap-anomaly-unclassified
  (let [interrupt (InterruptedException. "stopping")]
    (t/is (identical? interrupt (try (err/wrap-anomaly {} (throw interrupt))
                                     (catch InterruptedException e e)))))

  (let [closed (ClosedByInterruptException.)]
    (t/is (identical? closed (ex-cause (try (err/wrap-anomaly {} (throw closed))
                                            (catch InterruptedException e e))))))

  (t/is (anomalous? [:incorrect] (err/wrap-anomaly {} (throw (IllegalArgumentException. "nope"))))))

(t/deftest ->anomaly-gives-an-interrupt-a-category-for-the-wire-boundaries
  (t/is (instance? Interrupted (err/->anomaly (InterruptedException. "stopping") {})))
  (t/is (instance? Interrupted (err/->anomaly (ClosedByInterruptException.) {}))))
