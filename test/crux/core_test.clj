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
  (let [person (first f/people)
        eid (first (vals (cr/-put db [person] (c/to-date (time/date-time 1986 10 22)))))]
    (t/is (= (dissoc person :crux.core/id)
             (dissoc (cr/entity db eid) :crux.core/id)))))

(t/deftest test-fetch-entity-at-t
  (let [person (first f/people)
        eid (first (vals (cr/-put db [(assoc person :name "Fred")] (c/to-date (time/date-time 1986 10 22)))))]
    (cr/-put db [(assoc person :name "Freda" :crux.core/id eid)] (c/to-date (time/date-time 1986 10 24)))
    (t/is (= "Fred"
             (:name (cr/entity db eid (c/to-date (time/date-time 1986 10 23))))))
    (t/is (= "Freda"
             (:name (cr/entity db eid))))))

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
  (let [[ivan petr :as people] (->> [{:name "Ivan"} {:name "Petr"}]
                                    (map #(merge %1 %2) (take 2 f/people)))
        ids (cr/-put db people)]

    (t/testing "Can query by single field"
      (t/is (= #{{'e (get ids (:crux.core/id ivan))}} (cr/q db [['e :name "Ivan"]])))
      (t/is (= #{{'e (get ids (:crux.core/id petr))}} (cr/q db [['e :name "Petr"]]))))

    (t/testing "Can query for multiple results"
      (t/is (= #{{'e (get ids (:crux.core/id ivan))}
                 {'e (get ids (:crux.core/id petr))}}
               (cr/q db [['e :name]]))))

    (let [[smith :as people] (->> [{:name "Smith" :last-name "Smith"}]
                                  (map #(merge %1 %2) (take 1 f/people)))
          ids (cr/-put db people)]

      (t/testing "Can query across fields for same value"
        (t/is (= #{{'p1 (get ids (:crux.core/id smith))}}
                 (cr/q db [['p1 :name 'name]
                           ['p1 :last-name 'name]]))))

      (t/testing "Can query across fields for same value when value is passed in"
        (t/is (= #{{'p1 (get ids (:crux.core/id smith))}}
                 (cr/q db [['p1 :name 'name]
                           ['p1 :last-name 'name]
                           ['p1 :name "Smith"]])))))))

(t/deftest test-multiple-query-clauses
  (cr/-put db [{:crux.core/id 2 :foo "bar" :tar "zar"}
               {:crux.core/id 3 :foo "bar"}])

  (t/is (= #{{'e 2}} (cr/q db [['e :foo "bar"]
                               ['e :tar "zar"]])))

  (t/is (= #{{'e 2}} (cr/q db [['e :foo "bar"]
                               ['e :tar "zar"]])))

  (t/testing "Negate query based on subsequent non-matching clause"
    (t/is (= #{} (cr/q db [['e :foo "bar"]
                           ['e :tar "BAH"]])))))

(t/deftest test-basic-query-at-t
  (cr/-put db [[test-eid :foo "foo"]] (c/to-date (time/date-time 1986 10 22)))
  (cr/-put db [[test-eid :tar "tar"]] (c/to-date (time/date-time 1986 10 24)))

  (t/is (= #{} (cr/q db [['e :foo "foo"]
                         ['e :tar "tar"]]
                     (c/to-date (time/date-time 1986 10 23)))))

  (t/is (= #{{'e test-eid}} (cr/q db [['e :foo "foo"]
                                      ['e :tar "tar"]]))))

(t/deftest test-query-across-entities
  (cr/-put db [{:crux.core/id test-eid :foo "bar" :tar "tar"}
               {:crux.core/id 2 :foo "bar" :tar "zar"}])

  (t/is (= #{{'a test-eid 'b 2}} (cr/q db [['a :foo "bar"]
                                           ['a :tar "tar"]
                                           ['b :tar "zar"]])))

  (t/testing "Should not unify on empty set"
    (t/is (= #{} (cr/q db [['a :foo "bar"]
                           ['b :tar "NUTTING"]])))))

(t/deftest test-query-across-entities-using-join
  ;; TODO cleanup this hard to read test code, in lieu of people fixtures

  (cr/-put db [{:crux.core/id 1 :foo "bar" :tar "tar"}
               {:crux.core/id 2 :foo "baz" :tar "bar"}
               {:crux.core/id 99 :foo "CONTROL" :tar "CONTROL2"}])

  (cr/-put db [{:crux.core/id 3 :foo "bar2" :tar "tar2"}
               {:crux.core/id 4 :foo "baz2" :tar "bar2"}])

  (t/is (= #{{'a 1 'b 2}
             {'a 3 'b 4}} (cr/q db [['a :foo 'v]
                                    ['b :tar 'v]])))

  ;; Five people, two of which share the same name:
  (->> [{:name "Ivan"} {:name "Petr"} {:name "Sergei"} {:name "Denis"} {:name "Denis"}]
       (map #(merge %1 %2) (take 5 f/people))
       (cr/-put db))

  (t/testing "Five people, without a join"
    (t/is (= 5 (count (cr/q db [['p1 :name 'name]
                                ['p1 :age 'age]
                                ['p1 :salary 'salary]])))))

  (t/testing "Every person joins once, plus 2 more matches"
    (t/is (= 7 (count (cr/q db [['p1 :name 'name]
                                ['p2 :name 'name]]))))))
