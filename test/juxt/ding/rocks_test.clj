(ns juxt.rocks-test
  (:require [clojure.test :as t]
            [juxt.rocks]))

(def ^:dynamic *rocks-db*)

(defn- start-system [f]
  (let [db-name :test]
    (binding [*rocks-db* (juxt.rocks/open-db db-name)]
      (try
        (f)
        (finally
          (.close *rocks-db*)
          (juxt.rocks/destroy-db db-name))))))

(t/use-fixtures :each start-system)

(t/deftest test-can-get-at-now
  (juxt.rocks/-put *rocks-db* :foo "Bar4")
  (juxt.rocks/-put *rocks-db* :foo "Bar5")
  (t/is (= "Bar5" (juxt.rocks/-get-at *rocks-db* :foo))))

(t/deftest test-can-get-at-t
  (juxt.rocks/-put *rocks-db* :foo "Bar3" (java.util.Date. 1 1 0))
  (juxt.rocks/-put *rocks-db* :foo "Bar4" (java.util.Date. 1 1 2))
  (juxt.rocks/-put *rocks-db* :foo "Bar5" (java.util.Date. 1 1 3))
  (juxt.rocks/-put *rocks-db* :foo "Bar6" (java.util.Date. 1 1 4))

  (t/is (= "Bar3" (juxt.rocks/-get-at *rocks-db* :foo (java.util.Date. 1 1 1))))
  (t/is (= "Bar4" (juxt.rocks/-get-at *rocks-db* :foo (java.util.Date. 1 1 2))))
  (t/is (= "Bar6" (juxt.rocks/-get-at *rocks-db* :foo (java.util.Date. 1 1 5)))))

(t/deftest test-can-get-nil-before-range
  (juxt.rocks/-put *rocks-db* :foo "Bar3" (java.util.Date. 1 1 2))
  (juxt.rocks/-put *rocks-db* :foo "Bar4" (java.util.Date. 1 1 3))
  (t/is (not (juxt.rocks/-get-at *rocks-db* :foo (java.util.Date. 1 1 0)))))
