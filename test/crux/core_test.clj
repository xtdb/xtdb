(ns crux.core-test
  (:require [clj-time.coerce :as c]
            [clj-time.core :as time]
            [clojure.test :as t]
            [crux.core :as cr]
            [crux.fixtures :as f :refer [db]]
            [clojure.spec.alpha :as s]))

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
  (let [[ivan petr] (f/transact-people! db [{:name "Ivan" :last-name "Ivanov"}
                                            {:name "Petr" :last-name "Petrov"}])]

    (t/testing "Can query by single field"
      (t/is (= #{["Ivan"]} (cr/q db {:find ['name]
                                     :where [['e :name "Ivan"]
                                             ['e :name 'name]]})))
      (t/is (= #{["Petr"]} (cr/q db {:find ['name]
                                     :where [['e :name "Petr"]
                                             ['e :name 'name]]}))))

    (t/testing "Can query by single field"
      (t/is (= #{[(:crux.core/id ivan)]} (cr/q db {:find ['e]
                                                   :where [['e :name "Ivan"]]})))
      (t/is (= #{[(:crux.core/id petr)]} (cr/q db {:find ['e]
                                                   :where [['e :name "Petr"]]}))))

    (t/testing "Can query using multiple terms"
      (t/is (= #{["Ivan" "Ivanov"]} (cr/q db {:find ['name 'last-name]
                                              :where [['e :name 'name]
                                                      ['e :last-name 'last-name]
                                                      ['e :name "Ivan"]
                                                      ['e :last-name "Ivanov"]]}))))

    (t/testing "Negate query based on subsequent non-matching clause"
      (t/is (= #{} (cr/q db {:find ['e]
                             :where [['e :name "Ivan"]
                                     ['e :last-name "Ivanov-does-not-match"]]}))))

    (t/testing "Can query for multiple results"
      (t/is (= #{["Ivan"] ["Petr"]}
               (cr/q db {:find ['name] :where [['e :name 'name]]})))

      (let [[ivan2] (f/transact-people! db [{:name "Ivan" :last-name "Ivanov2"}])]
        (t/is (= #{[(:crux.core/id ivan)]
                   [(:crux.core/id ivan2)]}
                 (cr/q db {:find ['e] :where [['e :name "Ivan"]]})))))

    (let [[smith] (f/transact-people! db [{:name "Smith" :last-name "Smith"}])]
      (t/testing "Can query across fields for same value"
        (t/is (= #{[(:crux.core/id smith)]}
                 (cr/q db {:find ['p1] :where [['p1 :name 'name]
                                               ['p1 :last-name 'name]]}))))

      (t/testing "Can query across fields for same value when value is passed in"
        (t/is (= #{[(:crux.core/id smith)]}
                 (cr/q db {:find ['p1] :where [['p1 :name 'name]
                                               ['p1 :last-name 'name]
                                               ['p1 :name "Smith"]]})))))))

(t/deftest test-basic-query-at-t
  (let [[malcolm] (f/transact-people! db [{:name "Malcolm" :last-name "Sparks"}]
                                      (c/to-date (time/date-time 1986 10 22)))]
    (cr/-put db [[(:crux.core/id malcolm) :name "Malcolma"]] (c/to-date (time/date-time 1986 10 24)))
    (let [q {:find ['e]
             :where [['e :name "Malcolma"]
                     ['e :last-name "Sparks"]]}]
      (t/is (= #{} (cr/q db q (c/to-date (time/date-time 1986 10 23)))))
      (t/is (= #{[(:crux.core/id malcolm)]} (cr/q db q))))))

(t/deftest test-query-across-entities-using-join
  ;; Five people, two of which share the same name:
  (f/transact-people! db [{:name "Ivan"} {:name "Petr"} {:name "Sergei"} {:name "Denis"} {:name "Denis"}])

  (t/testing "Five people, without a join"
    (t/is (= 5 (count (cr/q db {:find ['p1]
                                :where [['p1 :name 'name]
                                        ['p1 :age 'age]
                                        ['p1 :salary 'salary]]})))))

  (t/testing "Five people, a cartesian product - joining without unification"
    (t/is (= 25 (count (cr/q db {:find ['p1 'p2]
                                 :where [['p1 :name]
                                         ['p2 :name]]})))))

  (t/testing "A single first result, joined to all possible subsequent results in next term"
    (t/is (= 5 (count (cr/q db {:find ['p1 'p2]
                                :where [['p1 :name "Ivan"]
                                        ['p2 :name]]})))))

  (t/testing "A single first result, with no subsequent results in next term"
    (t/is (= 0 (count (cr/q db {:find ['p1]
                                :where [['p1 :name "Ivan"]
                                        ['p2 :name "does-not-match"]]})))))

  (t/testing "Every person joins once, plus 2 more matches"
    (t/is (= 7 (count (cr/q db {:find ['p1 'p2]
                                :where [['p1 :name 'name]
                                        ['p2 :name 'name]]}))))))

(t/deftest test-blanks
  (f/transact-people! db [{:name "Ivan"} {:name "Petr"} {:name "Sergei"}])

  (t/is (= #{["Ivan"] ["Petr"] ["Sergei"]}
           (cr/q db {:find ['name]
                     :where [['_ :name 'name]]}))))

(t/deftest test-exceptions
  (t/testing "Unbound query variable"
    (try
      (cr/q db {:find ['bah]
                :where [['e :name]]})
      (t/is (= true false) "Expected exception"))
    (catch IllegalArgumentException e
      (t/is (= "Find clause references unbound variable: bah" (.getMessage e))))))

(t/deftest test-not-query
  (t/is (= [[:term ['e :name 'name]]
            [:term ['e :name "Ivan"]]
            [:not {:operator 'not, :term [:term ['e :last-name "Ivannotov"]]}]]

           (s/conform :crux.core/where [['e :name 'name]
                                        ['e :name "Ivan"]
                                        '(not [e :last-name "Ivannotov"])])))

  (f/transact-people! db [{:name "Ivan" :last-name "Ivanov"}
                          {:name "Ivan" :last-name "Ivanov"}
                          {:name "Ivan" :last-name "Ivannotov"}])

  (t/is (= 1 (count (cr/q db {:find ['e]
                              :where [['e :name 'name]
                                      ['e :name "Ivan"]
                                      '(not [e :last-name "Ivanov"])]}))))

  (t/is (= 2 (count (cr/q db {:find ['e]
                              :where [['e :name 'name]
                                      ['e :name "Ivan"]
                                      '(not [e :last-name "Ivannotov"])]}))))

  ;; test what happens if not contains a brand new var, uses diff entity etc
  )

(t/deftest test-or-query
  (f/transact-people! db [{:name "Ivan" :last-name "Ivanov"}
                          {:name "Ivan" :last-name "Ivanov"}
                          {:name "Ivan" :last-name "Ivannotov"}
                          {:name "Bob" :last-name "Controlguy"}])

  ;; Here for dev reasons, delete when appropiate
  (t/is (= '[[:term [e :name name]]
             [:term [e :name "Ivan"]]
             [:or {:operator or, :terms [[:term [e :last-name "Ivanov"]]]}]]
           (s/conform :crux.core/where [['e :name 'name]
                                        ['e :name "Ivan"]
                                        '(or [[e :last-name "Ivanov"]])])))

  (t/testing "Or works as expected"
    (t/is (= 3 (count (cr/q db {:find ['e]
                                :where [['e :name 'name]
                                        ['e :name "Ivan"]
                                        '(or [[e :last-name "Ivanov"]
                                              [e :last-name "Ivannotov"]])]}))))

    (t/is (= 2 (count (cr/q db {:find ['e]
                                :where [['e :name 'name]
                                        ['e :name "Ivan"]
                                        '(or [[e :last-name "Ivanov"]
                                              [e :last-name "Controlguy"]])]})))))

  (t/testing "Or edge case - can take a single clause"
    ;; Unsure of the utility
    (t/is (= 2 (count (cr/q db {:find ['e]
                                :where [['e :name 'name]
                                        ['e :name "Ivan"]
                                        '(or [[e :last-name "Ivanov"]])]})))))

  ;; TODO dig into edge cases some more:
  ;; "All clauses used in an or clause must use the same set of variables, which will unify with the surrounding query."
  )

(t/deftest test-not-join
  (f/transact-people! db [{:name "Ivan" :last-name "Ivanov"}
                          {:name "Malcolm" :last-name "Ofsparks"}
                          {:name "Dominic" :last-name "Monroe"}])

  (t/testing "Rudimentary or-join"
    (t/is (= #{["Ivan"] ["Malcolm"]}
             (cr/q db {:find ['name]
                       :where [['e :name 'name]
                               '(not-join [e]
                                          [[e :last-name "Monroe"]])]})))))
