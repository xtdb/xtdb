(ns crux.rocks-test
  (:require [clojure.test :as t]
            [crux.rocks :as cr]
            [crux.byte-utils :refer :all]
            [clj-time.core :as time]
            [clj-time.coerce :as c]))

(def ^:dynamic db)

(defn- start-system [f]
  (let [db-name :test]
    (binding [db (cr/open-db db-name)]
      (try
        (cr/transact-schema! db {:attr/ident :foo :attr/type :string})
        (cr/transact-schema! db {:attr/ident :tar :attr/type :string})
        (f)
        (finally
          (.close db)
          (cr/destroy-db db-name))))))

(t/use-fixtures :each start-system)

(def test-eid 1)

(t/deftest test-can-get-at-now
  (cr/-put db [[test-eid :foo "Bar4"]])
  (t/is (= "Bar4" (cr/-get-at db test-eid :foo)))
  (cr/-put db [[test-eid :foo "Bar5"]])
  (t/is (= "Bar5" (cr/-get-at db test-eid :foo)))

  ;; Insert into past
  (cr/-put db [[test-eid :foo "foo1"]](java.util.Date. 2000 1 2))
  (t/is (= "Bar5" (cr/-get-at db test-eid :foo))))

(t/deftest test-can-get-at-now-for-old-entry
  (cr/-put db [[test-eid :foo "Bar3"]] (java.util.Date. 110 1 2))
  (t/is (= "Bar3" (cr/-get-at db test-eid :foo))))

(t/deftest test-can-get-at-t
  (cr/-put db [[test-eid :foo "Bar3"]] (java.util.Date. 1 1 0))
  (t/is (= "Bar3" (cr/-get-at db test-eid :foo (java.util.Date. 1 1 1))))

  (cr/-put db [[test-eid :foo "Bar4"]] (java.util.Date. 1 1 2))
  (cr/-put db [[test-eid :foo "Bar5"]] (java.util.Date. 1 1 3))
  (cr/-put db [[test-eid :foo "Bar6"]] (java.util.Date. 1 1 4))

  (t/is (= "Bar3" (cr/-get-at db test-eid :foo (java.util.Date. 1 1 1))))
  (t/is (= "Bar4" (cr/-get-at db test-eid :foo (java.util.Date. 1 1 2))))
  (t/is (= "Bar6" (cr/-get-at db test-eid :foo (java.util.Date. 1 1 5)))))

(t/deftest test-can-get-nil-before-range
  (cr/-put db [[test-eid :foo "Bar3"]] (java.util.Date. 1 1 2))
  (cr/-put db [[test-eid :foo "Bar4"]] (java.util.Date. 1 1 3))
  (t/is (not (cr/-get-at db test-eid :foo (java.util.Date. 1 1 0)))))

(t/deftest test-can-get-nil-outside-of-range
  (cr/-put db [[test-eid :foo "Bar3"]] (c/to-date (time/date-time 1986 10 22)))
  (cr/-put db [[test-eid :tar "Bar4"]] (c/to-date (time/date-time 1986 10 22)))
  (t/is (not (cr/-get-at db test-eid :tar (c/to-date (time/date-time 1986 10 21))))))

(.getTime (c/to-date (time/date-time 2030 12 30)))

(t/deftest test-entity-ids
  (t/is (= 3 (cr/next-entity-id db)))
  (t/is (= 4 (cr/next-entity-id db)))

  (dotimes [n 1000]
    (cr/next-entity-id db))

  (t/is (= 1005 (cr/next-entity-id db))))

(t/deftest test-write-and-fetch-entity
  (cr/-put db {:crux.rocks/id test-eid
               :foo "Bar3"
               :tar "Bar4"}
           (c/to-date (time/date-time 1986 10 22)))
  (t/is (= {:tar "Bar4" :foo "Bar3"}
           (cr/entity db test-eid))))

(t/deftest test-fetch-entity-at-t
  (cr/-put db [[test-eid :foo "foo1"]
               [test-eid :tar "tar1"]] (c/to-date (time/date-time 1986 10 22)))
  (cr/-put db [[test-eid :foo "foo2"]
               [test-eid :tar "tar2"]] (c/to-date (time/date-time 1986 10 23)))
  (t/is (= {:tar "tar2" :foo "foo2"}
           (cr/entity db test-eid)))

  ;; TODO NEEDS MORE TESTS, IGNORES T!
  ;; TODO validate date passed isn't above max-value for sanity reasons
  )

(t/deftest test-invalid-attribute-exception
  (try
    (cr/-put db [[test-eid :unknown-attribute "foo1"]] (c/to-date (time/date-time 1986 10 22)))
    (assert false "Exception expected")
    (catch IllegalArgumentException e
      (t/is (= "Unrecognised schema attribute: :unknown-attribute"
               (.getMessage e))))))

(t/deftest test-transact-schema-attribute
  (cr/transact-schema! db {:attr/ident :new-ident
                           :attr/type :string})
  (cr/-put db [[test-eid :new-ident "foo1"]])
  (t/is (= "foo1" (cr/-get-at db test-eid :new-ident)))


  (let [aid (cr/transact-schema! db {:attr/ident :new-ident2
                                     :attr/type :long})]
    (t/is (= :new-ident2 (:attr/ident (cr/attr-aid->schema db aid)))))

  (cr/-put db [[test-eid :new-ident2 1]])
  (t/is (= 1 (cr/-get-at db test-eid :new-ident2)))

  ;; test insertion of invalid type and consequent exception
  )

(t/deftest test-retract-attribute
  (cr/-put db [[test-eid :foo "foo1"]] (c/to-date (time/date-time 1986 10 22)))
  (cr/-put db [[test-eid :foo nil]])
  (t/is (not (cr/-get-at db test-eid :foo)))
  (t/is (= "foo1" (cr/-get-at db test-eid :foo (c/to-date (time/date-time 1986 10 22))))))

(t/deftest test-basic-query
  (cr/-put db {:crux.rocks/id 2 :foo "bar"})
  (cr/-put db {:crux.rocks/id 3 :foo "tar"})

  (t/is (= #{2} (cr/query db [[:foo "bar"]])))
  (t/is (= #{3} (cr/query db [[:foo "tar"]])))
  (t/is (= #{2 3} (cr/query db [[:foo]]))))

(t/deftest test-multiple-query-clauses
  (cr/-put db {:crux.rocks/id 2 :foo "bar"})
  (cr/-put db {:crux.rocks/id 2 :tar "zar"})
  (cr/-put db {:crux.rocks/id 3 :foo "bar"})

  (t/is (= #{2} (cr/query db [[:foo "bar"]
                              [:tar "zar"]]))))
