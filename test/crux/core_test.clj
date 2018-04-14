(ns crux.core-test
  (:require [clojure.test :as t]
            [crux.core :as cr]
            [crux.byte-utils :refer :all]
            [clj-time.core :as time]
            [clj-time.coerce :as c]
            [crux.fixtures :as f :refer [db]]
            [crux.kv :as kv]
            [crux.rocksdb]))

(t/use-fixtures :each f/start-system)

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

(t/deftest test-entity-ids
  (let [eid (cr/next-entity-id db)]
    (dotimes [n 1000]
      (cr/next-entity-id db))

    (t/is (= (+ eid 1001) (cr/next-entity-id db)))))

(t/deftest test-write-and-fetch-entity
  (let [person (first f/people)]
    (cr/-put db person (c/to-date (time/date-time 1986 10 22)))
    (t/is (= person
             (cr/entity db (:crux.core/id person))))))

(t/deftest test-fetch-entity-at-t
  (cr/-put db [[test-eid :foo "foo1"]
               [test-eid :tar "tar1"]] (c/to-date (time/date-time 1986 10 22)))
  (cr/-put db [[test-eid :foo "foo2"]
               [test-eid :tar "tar2"]] (c/to-date (time/date-time 1986 10 24)))

  (t/is (= {:tar "tar1" :foo "foo1"}
           (cr/entity db test-eid (c/to-date (time/date-time 1986 10 23)))))
  (t/is (= {:tar "tar2" :foo "foo2"}
           (cr/entity db test-eid))))

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
  (doseq [p (map #(assoc %1 :name %2) (take 2 f/people) ["Ivan" "Petr"])]
    (cr/-put db p))

  (t/is (= #{{:e 1001}} (cr/q db [[:e :name "Ivan"]])))
  (t/is (= #{{:e 1002}} (cr/q db [[:e :name "Petr"]])))
  (t/is (= #{{:e 1001} {:e 1002}} (cr/q db [[:e :name]]))))

(t/deftest test-multiple-query-clauses
  (cr/-put db {:crux.core/id 2 :foo "bar" :tar "zar"})
  (cr/-put db {:crux.core/id 3 :foo "bar"})

  (t/is (= #{{:e 2}} (cr/q db [[:e :foo "bar"]
                               [:e :tar "zar"]])))

  (t/is (= #{{:e 2}} (cr/q db [[:e :foo "bar"]
                               [:e :tar "zar"]])))

  (t/testing "Negate query based on subsequent non-matching clause"
    (t/is (= #{} (cr/q db [[:e :foo "bar"]
                           [:e :tar "BAH"]])))))

(t/deftest test-basic-query-at-t
  (cr/-put db [[test-eid :foo "foo"]] (c/to-date (time/date-time 1986 10 22)))
  (cr/-put db [[test-eid :tar "tar"]] (c/to-date (time/date-time 1986 10 24)))

  (t/is (= #{} (cr/q db [[:e :foo "foo"]
                         [:e :tar "tar"]]
                     (c/to-date (time/date-time 1986 10 23)))))

  (t/is (= #{{:e test-eid}} (cr/q db [[:e :foo "foo"]
                                      [:e :tar "tar"]]))))

(t/deftest test-query-across-entities
  (cr/-put db {:crux.core/id test-eid :foo "bar" :tar "tar"})
  (cr/-put db {:crux.core/id 2 :foo "bar" :tar "zar"})

  (t/is (= #{{:a test-eid :b 2}} (cr/q db [[:a :foo "bar"]
                                           [:a :tar "tar"]
                                           [:b :tar "zar"]])))

  (t/testing "Should not unify on empty set"
    (t/is (= #{} (cr/q db [[:a :foo "bar"]
                           [:b :tar "NUTTING"]])))))

(t/deftest test-query-across-entities-using-join

  ;; TODO deprecate this hard to read test code

  (cr/-put db {:crux.core/id 1 :foo "bar" :tar "tar"})
  (cr/-put db {:crux.core/id 2 :foo "baz" :tar "bar"})
  (cr/-put db {:crux.core/id 99 :foo "CONTROL" :tar "CONTROL2"})

  (t/is (= #{{:a 1 :b 2}} (cr/q db [[:a :foo 'v]
                                    [:b :tar 'v]])))

  (t/is (= #{{:a 1 :b 2}} (cr/q db [[:a :foo 'v]
                                    [:b :tar 'v]
                                    [:a :foo "bar"]])))

  (cr/-put db {:crux.core/id 3 :foo "bar2" :tar "tar2"})
  (cr/-put db {:crux.core/id 4 :foo "baz2" :tar "bar2"})

  (t/is (= #{{:a 1 :b 2}
             {:a 3 :b 4}} (cr/q db [[:a :foo 'v]
                                    [:b :tar 'v]])))

  ;; Five people, two of which share the same name:
  (doseq [p (map #(assoc %1 :name %2) (take 5 f/people) ["Ivan" "Petr" "Sergei" "Denis" "Denis"])]
    (cr/-put db p))

  (t/testing "Five people, without a join"
    (t/is (= 5 (count (cr/q db [[:p1 :name 'name]
                                [:p1 :age 'age]
                                [:p1 :salary 'salary]])))))

  (t/testing "Every person joins once, plus 2 more matches"
    (t/is (= 7 (count (cr/q db [[:p1 :name 'name]
                                [:p2 :name 'name]]))))))
