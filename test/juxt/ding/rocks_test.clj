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

(def test-eid 1)

(t/deftest test-can-get-at-now
  (juxt.rocks/-put *rocks-db* test-eid :foo "Bar4")
  (juxt.rocks/-put *rocks-db* test-eid :foo "Bar5")
  (t/is (= "Bar5" (juxt.rocks/-get-at *rocks-db* test-eid :foo))))

(t/deftest test-can-get-at-t
  (juxt.rocks/-put *rocks-db* test-eid :foo "Bar3" (java.util.Date. 1 1 0))
  (juxt.rocks/-put *rocks-db* test-eid :foo "Bar4" (java.util.Date. 1 1 2))
  (juxt.rocks/-put *rocks-db* test-eid :foo "Bar5" (java.util.Date. 1 1 3))
  (juxt.rocks/-put *rocks-db* test-eid :foo "Bar6" (java.util.Date. 1 1 4))

  (t/is (= "Bar3" (juxt.rocks/-get-at *rocks-db* test-eid :foo (java.util.Date. 1 1 1))))
  (t/is (= "Bar4" (juxt.rocks/-get-at *rocks-db* test-eid :foo (java.util.Date. 1 1 2))))
  (t/is (= "Bar6" (juxt.rocks/-get-at *rocks-db* test-eid :foo (java.util.Date. 1 1 5)))))

(t/deftest test-can-get-nil-before-range
  (juxt.rocks/-put *rocks-db* test-eid :foo "Bar3" (java.util.Date. 1 1 2))
  (juxt.rocks/-put *rocks-db* test-eid :foo "Bar4" (java.util.Date. 1 1 3))
  (t/is (not (juxt.rocks/-get-at *rocks-db* test-eid :foo (java.util.Date. 1 1 0)))))

(t/deftest test-can-get-nil-outside-of-range
  (juxt.rocks/-put *rocks-db* test-eid :foo "Bar3" (java.util.Date. 1 1 1))
  (juxt.rocks/-put *rocks-db* test-eid :tar "Bar4" (java.util.Date. 1 1 1))
  (t/is (not (juxt.rocks/-get-at *rocks-db* test-eid :tar (java.util.Date. 1 1 0)))))

(t/deftest test-entity-ids
  (t/is (= 1 (juxt.rocks/next-entity-id *rocks-db*)))
  (t/is (= 2 (juxt.rocks/next-entity-id *rocks-db*)))

  (dotimes [n 1000]
    (juxt.rocks/next-entity-id *rocks-db*))

  (t/is (= 1003 (juxt.rocks/next-entity-id *rocks-db*))))

(t/deftest test-fetch-entity
  (juxt.rocks/-put *rocks-db* test-eid :foo "Bar3" (java.util.Date. 1 1 2))
  (juxt.rocks/-put *rocks-db* test-eid :tar "Bar4" (java.util.Date. 1 1 2))

  (t/is (= {:tar "Bar4" :foo "Bar3"}
           (juxt.rocks/entity *rocks-db* test-eid))))

;; Test TODOs:
;; test try diff data types for the value
;; ensure fetch-entity returns only one attribute per timestamp..
;; fetch-entity for a given timestamp
