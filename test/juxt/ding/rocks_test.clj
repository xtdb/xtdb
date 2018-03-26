(ns juxt.rocks-test
  (:require [clojure.test :as t]
            [juxt.rocks]
            [juxt.byte-utils :refer :all]
            [clj-time.core :as time]
            [clj-time.coerce :as c]))

(def ^:dynamic db)

(defn- start-system [f]
  (let [db-name :test]
    (binding [db (juxt.rocks/open-db db-name)]
      (try
        (juxt.rocks/transact-schema! db {:attr/ident :foo})
        (juxt.rocks/transact-schema! db {:attr/ident :tar})
        (f)
        (finally
          (.close db)
          (juxt.rocks/destroy-db db-name))))))

(t/use-fixtures :each start-system)

(def test-eid 1)

(t/deftest test-can-get-at-now
  (juxt.rocks/-put db [test-eid :foo "Bar4"])
  (t/is (= "Bar4" (juxt.rocks/-get-at db test-eid :foo)))
  (juxt.rocks/-put db [test-eid :foo "Bar5"])
  (t/is (= "Bar5" (juxt.rocks/-get-at db test-eid :foo)))

  ;; Insert into past
  (juxt.rocks/-put db [test-eid :foo "foo1"] (java.util.Date. 2000 1 2))
  (t/is (= "Bar5" (juxt.rocks/-get-at db test-eid :foo))))

(t/deftest test-can-get-at-now-for-old-entry
  (juxt.rocks/-put db [test-eid :foo "Bar3"] (java.util.Date. 110 1 2))
  (juxt.rocks/all-keys db)
  (t/is (= "Bar3" (juxt.rocks/-get-at db test-eid :foo))))

(t/deftest test-can-get-at-t
  (juxt.rocks/-put db [test-eid :foo "Bar3"] (java.util.Date. 1 1 0))
  (t/is (= "Bar3" (juxt.rocks/-get-at db test-eid :foo (java.util.Date. 1 1 1))))

  (juxt.rocks/-put db [test-eid :foo "Bar4"] (java.util.Date. 1 1 2))
  (juxt.rocks/-put db [test-eid :foo "Bar5"] (java.util.Date. 1 1 3))
  (juxt.rocks/-put db [test-eid :foo "Bar6"] (java.util.Date. 1 1 4))

  (t/is (= "Bar3" (juxt.rocks/-get-at db test-eid :foo (java.util.Date. 1 1 1))))
  (t/is (= "Bar4" (juxt.rocks/-get-at db test-eid :foo (java.util.Date. 1 1 2))))
  (t/is (= "Bar6" (juxt.rocks/-get-at db test-eid :foo (java.util.Date. 1 1 5)))))

(t/deftest test-can-get-nil-before-range
  (juxt.rocks/-put db [test-eid :foo "Bar3"] (java.util.Date. 1 1 2))
  (juxt.rocks/-put db [test-eid :foo "Bar4"] (java.util.Date. 1 1 3))
  (t/is (not (juxt.rocks/-get-at db test-eid :foo (java.util.Date. 1 1 0)))))

(t/deftest test-can-get-nil-outside-of-range
  (juxt.rocks/-put db [test-eid :foo "Bar3"] (c/to-date (time/date-time 1986 10 22)))
  (juxt.rocks/-put db [test-eid :tar "Bar4"] (c/to-date (time/date-time 1986 10 22)))
  (t/is (not (juxt.rocks/-get-at db test-eid :tar (c/to-date (time/date-time 1986 10 21))))))

(t/deftest test-entity-ids
  (t/is (= 3 (juxt.rocks/next-entity-id db)))
  (t/is (= 4 (juxt.rocks/next-entity-id db)))

  (dotimes [n 1000]
    (juxt.rocks/next-entity-id db))

  (t/is (= 1005 (juxt.rocks/next-entity-id db))))

(t/deftest test-fetch-entity
  (juxt.rocks/-put db [test-eid :foo "Bar3"] (c/to-date (time/date-time 1986 10 22)))
  (juxt.rocks/-put db [test-eid :tar "Bar4"] (c/to-date (time/date-time 1986 10 22)))
  (t/is (= {:tar "Bar4" :foo "Bar3"}
           (juxt.rocks/entity db test-eid))))

(t/deftest test-fetch-entity-at-t
  (juxt.rocks/-put db [test-eid :foo "foo1"] (c/to-date (time/date-time 1986 10 22)))
  (juxt.rocks/-put db [test-eid :tar "tar1"] (c/to-date (time/date-time 1986 10 22)))
  (juxt.rocks/-put db [test-eid :foo "foo2"] (c/to-date (time/date-time 1986 10 23)))
  (juxt.rocks/-put db [test-eid :tar "tar2"] (c/to-date (time/date-time 1986 10 23)))
  (t/is (= {:tar "tar2" :foo "foo2"}
           (juxt.rocks/entity db test-eid))))

(t/deftest test-invalid-attribute-exception
  (try
    (juxt.rocks/-put db [test-eid :unknown-attribute "foo1"] (c/to-date (time/date-time 1986 10 22)))
    (assert false "Exception expected")
    (catch IllegalArgumentException e
      (t/is (= "Unrecognised schema attribute: :unknown-attribute"
               (.getMessage e))))))

(t/deftest test-transact-schema-attribute
  (juxt.rocks/transact-schema! db {:attr/ident :new-ident})
  (juxt.rocks/-put db [test-eid :new-ident "foo1"])
  (t/is (= "foo1" (juxt.rocks/-get-at db test-eid :new-ident))))
