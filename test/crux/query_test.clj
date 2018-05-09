(ns crux.query-test
  (:require [clj-time.coerce :as c]
            [clj-time.core :as time]
            [clojure.spec.alpha :as s]
            [clojure.test :as t]
            [crux.fixtures :as f :refer [*kv*]]
            [crux.kv :as cr]
            [crux.core :refer [db as-of]]
            [crux.query :as q]))

(t/use-fixtures :each f/start-system)

(t/deftest test-basic-query
  (let [[ivan petr] (f/transact-people! *kv* [{:name "Ivan" :last-name "Ivanov"}
                                              {:name "Petr" :last-name "Petrov"}])]

    (t/testing "Can query by single field"
      (t/is (= #{["Ivan"]} (q/q (db *kv*) {:find ['name]
                                           :where [['e :name "Ivan"]
                                                   ['e :name 'name]]})))
      (t/is (= #{["Petr"]} (q/q (db *kv*) {:find ['name]
                                           :where [['e :name "Petr"]
                                                   ['e :name 'name]]}))))

    (t/testing "Can query by single field"
      (t/is (= #{[(:crux.kv/id ivan)]} (q/q (db *kv*) {:find ['e]
                                                       :where [['e :name "Ivan"]]})))
      (t/is (= #{[(:crux.kv/id petr)]} (q/q (db *kv*) {:find ['e]
                                                       :where [['e :name "Petr"]]}))))

    (t/testing "Can query using multiple terms"
      (t/is (= #{["Ivan" "Ivanov"]} (q/q (db *kv*) {:find ['name 'last-name]
                                                    :where [['e :name 'name]
                                                            ['e :last-name 'last-name]
                                                            ['e :name "Ivan"]
                                                            ['e :last-name "Ivanov"]]}))))

    (t/testing "Negate query based on subsequent non-matching clause"
      (t/is (= #{} (q/q (db *kv*) {:find ['e]
                                   :where [['e :name "Ivan"]
                                           ['e :last-name "Ivanov-does-not-match"]]}))))

    (t/testing "Can query for multiple results"
      (t/is (= #{["Ivan"] ["Petr"]}
               (q/q (db *kv*) {:find ['name] :where [['e :name 'name]]})))

      (let [[ivan2] (f/transact-people! *kv* [{:name "Ivan" :last-name "Ivanov2"}])]
        (t/is (= #{[(:crux.kv/id ivan)]
                   [(:crux.kv/id ivan2)]}
                 (q/q (db *kv*) {:find ['e] :where [['e :name "Ivan"]]})))))

    (let [[smith] (f/transact-people! *kv* [{:name "Smith" :last-name "Smith"}])]
      (t/testing "Can query across fields for same value"
        (t/is (= #{[(:crux.kv/id smith)]}
                 (q/q (db *kv*) {:find ['p1] :where [['p1 :name 'name]
                                                     ['p1 :last-name 'name]]}))))

      (t/testing "Can query across fields for same value when value is passed in"
        (t/is (= #{[(:crux.kv/id smith)]}
                 (q/q (db *kv*) {:find ['p1] :where [['p1 :name 'name]
                                                     ['p1 :last-name 'name]
                                                     ['p1 :name "Smith"]]})))))))

(t/deftest test-multiple-results
  (f/transact-people! *kv* [{:name "Ivan" :last-name "1"}
                            {:name "Ivan" :last-name "2"}])
  (t/is (= 2
           (count (q/q (db *kv*) {:find ['e] :where [['e :name "Ivan"]]})))))

(t/deftest test-query-using-keywords
  (f/transact-people! *kv* [{:name "Ivan" :sex :male}
                            {:name "Petr" :sex :male}
                            {:name "Doris" :sex :female}
                            {:name "Jane" :sex :female}])

  (t/testing "Can query by single field"
    (t/is (= #{["Ivan"] ["Petr"]} (q/q (db *kv*) {:find ['name]
                                                  :where [['e :name 'name]
                                                          ['e :sex :male]]})))
    (t/is (= #{["Doris"] ["Jane"]} (q/q (db *kv*) {:find ['name]
                                                   :where [['e :name 'name]
                                                           ['e :sex :female]]})))))

(t/deftest test-basic-query-at-t
  (let [[malcolm] (f/transact-people! *kv* [{:name "Malcolm" :last-name "Sparks"}]
                                      (c/to-date (time/date-time 1986 10 22)))]
    (cr/-put *kv* [[(:crux.kv/id malcolm) :name "Malcolma"]] (c/to-date (time/date-time 1986 10 24)))
    (let [q {:find ['e]
             :where [['e :name "Malcolma"]
                     ['e :last-name "Sparks"]]}]
      (t/is (= #{} (q/q (as-of *kv* (c/to-date (time/date-time 1986 10 23)))
                        q)))
      (t/is (= #{[(:crux.kv/id malcolm)]} (q/q (db *kv*) q))))))

(t/deftest test-query-across-entities-using-join
  ;; Five people, two of which share the same name:
  (f/transact-people! *kv* [{:name "Ivan"} {:name "Petr"} {:name "Sergei"} {:name "Denis"} {:name "Denis"}])

  (t/testing "Five people, without a join"
    (t/is (= 5 (count (q/q (db *kv*) {:find ['p1]
                                      :where [['p1 :name 'name]
                                              ['p1 :age 'age]
                                              ['p1 :salary 'salary]]})))))

  (t/testing "Five people, a cartesian product - joining without unification"
    (t/is (= 25 (count (q/q (db *kv*) {:find ['p1 'p2]
                                       :where [['p1 :name]
                                               ['p2 :name]]})))))

  (t/testing "A single first result, joined to all possible subsequent results in next term"
    (t/is (= 5 (count (q/q (db *kv*) {:find ['p1 'p2]
                                      :where [['p1 :name "Ivan"]
                                              ['p2 :name]]})))))

  (t/testing "A single first result, with no subsequent results in next term"
    (t/is (= 0 (count (q/q (db *kv*) {:find ['p1]
                                      :where [['p1 :name "Ivan"]
                                              ['p2 :name "does-not-match"]]})))))

  (t/testing "Every person joins once, plus 2 more matches"
    (t/is (= 7 (count (q/q (db *kv*) {:find ['p1 'p2]
                                      :where [['p1 :name 'name]
                                              ['p2 :name 'name]]}))))))

(t/deftest test-blanks
  (f/transact-people! *kv* [{:name "Ivan"} {:name "Petr"} {:name "Sergei"}])

  (t/is (= #{["Ivan"] ["Petr"] ["Sergei"]}
           (q/q (db *kv*) {:find ['name]
                           :where [['_ :name 'name]]}))))

(t/deftest test-exceptions
  (t/testing "Unbound query variable"
    (try
      (q/q (db *kv*) {:find ['bah]
                      :where [['e :name]]})
      (t/is (= true false) "Expected exception"))
    (catch IllegalArgumentException e
      (t/is (= "Find clause references unbound variable: bah" (.getMessage e))))))

(t/deftest test-not-query
  (t/is (= [[:fact ['e :name 'name]]
            [:fact ['e :name "Ivan"]]
            [:not [:fact ['e :last-name "Ivannotov"]]]]

           (s/conform :crux.query/where [['e :name 'name]
                                         ['e :name "Ivan"]
                                         '(not [e :last-name "Ivannotov"])])))

  (f/transact-people! *kv* [{:name "Ivan" :last-name "Ivanov"}
                            {:name "Ivan" :last-name "Ivanov"}
                            {:name "Ivan" :last-name "Ivannotov"}])

  (t/is (= 1 (count (q/q (db *kv*) {:find ['e]
                                    :where [['e :name 'name]
                                            ['e :name "Ivan"]
                                            '(not [e :last-name "Ivanov"])]}))))

  (t/is (= 2 (count (q/q (db *kv*) {:find ['e]
                                    :where [['e :name 'name]
                                            ['e :name "Ivan"]
                                            '(not [e :last-name "Ivannotov"])]})))))

(t/deftest test-or-query
  (f/transact-people! *kv* [{:name "Ivan" :last-name "Ivanov"}
                            {:name "Ivan" :last-name "Ivanov"}
                            {:name "Ivan" :last-name "Ivannotov"}
                            {:name "Bob" :last-name "Controlguy"}])

  ;; Here for dev reasons, delete when appropiate
  (t/is (= '[[:fact [e :name name]]
             [:fact [e :name "Ivan"]]
             [:or [[:fact [e :last-name "Ivanov"]]]]]
           (s/conform :crux.query/where [['e :name 'name]
                                         ['e :name "Ivan"]
                                         '(or [[e :last-name "Ivanov"]])])))

  (t/testing "Or works as expected"
    (t/is (= 3 (count (q/q (db *kv*) {:find ['e]
                                      :where [['e :name 'name]
                                              ['e :name "Ivan"]
                                              '(or [[e :last-name "Ivanov"]
                                                    [e :last-name "Ivannotov"]])]}))))

    (t/is (= 3 (count (q/q (db *kv*) {:find ['e]
                                      :where [['e :name 'name]
                                              '(or [[e :last-name "Ivanov"]
                                                    [e :name "Bob"]])]})))))

  (t/testing "Or edge case - can take a single clause"
    ;; Unsure of the utility
    (t/is (= 2 (count (q/q (db *kv*) {:find ['e]
                                      :where [['e :name 'name]
                                              ['e :name "Ivan"]
                                              '(or [[e :last-name "Ivanov"]])]}))))))

(t/deftest test-or-query-can-use-and
  (f/transact-people! *kv* [{:name "Ivan" :sex :male}
                            {:name "Bob" :sex :male}
                            {:name "Ivana" :sex :female}])

  (t/is (= #{["Ivan"]
             ["Ivana"]}
           (q/q (db *kv*) {:find ['name]
                           :where [['e :name 'name]
                                   '(or [[e :sex :female]
                                         (and [[e :sex :male]
                                               [e :name "Ivan"]])])]}))))

(t/deftest test-ors-must-use-same-vars
  (try
    (q/q (db *kv*) {:find ['e]
                    :where [['e :name 'name]
                            '(or [[e1 :last-name "Ivanov"]
                                  [e2 :last-name "Ivanov"]])]})
    (t/is (= true false) "Expected assertion error")
    (catch java.lang.AssertionError e
      (t/is true))))

(t/deftest test-ors-can-introduce-new-bindings
  (let [[petr ivan ivanova] (f/transact-people! *kv* [{:name "Petr" :last-name "Smith" :sex :male}
                                                      {:name "Ivan" :last-name "Ivanov" :sex :male}
                                                      {:name "Ivanova" :last-name "Ivanov" :sex :female}])]

    (t/testing "?p2 introduced only inside of an Or"
      (t/is (= #{[(:crux.kv/id ivan)]} (q/q (db *kv*) {:find ['?p2]
                                                       :where ['(or [(and [[?p2 :name "Petr"]
                                                                           [?p2 :sex :female]])
                                                                     (and [[?p2 :last-name "Ivanov"]
                                                                           [?p2 :sex :male]])])]}))))))

(t/deftest test-not-join
  (f/transact-people! *kv* [{:name "Ivan" :last-name "Ivanov"}
                            {:name "Malcolm" :last-name "Ofsparks"}
                            {:name "Dominic" :last-name "Monroe"}])

  (t/testing "Rudimentary or-join"
    (t/is (= #{["Ivan"] ["Malcolm"]}
             (q/q (db *kv*) {:find ['name]
                             :where [['e :name 'name]
                                     '(not-join [e]
                                                [[e :last-name "Monroe"]])]})))))

(t/deftest test-mixing-expressions
  (f/transact-people! *kv* [{:name "Ivan" :last-name "Ivanov"}
                            {:name "Derek" :last-name "Ivanov"}
                            {:name "Bob" :last-name "Ivannotov"}
                            {:name "Fred" :last-name "Ivannotov"}])

  (t/testing "Or can use not expression"
    (t/is (= #{["Ivan"] ["Derek"] ["Fred"]}
             (q/q (db *kv*) {:find ['name]
                             :where [['e :name 'name]
                                     '(or [[e :last-name "Ivanov"]
                                           (not [e :name "Bob"])])]}))))

  (t/testing "Not can use Or expression"
    (t/is (= #{["Fred"]} (q/q (db *kv*) {:find ['name]
                                         :where [['e :name 'name]
                                                 '(not (or [[e :last-name "Ivanov"]
                                                            [e :name "Bob"]]))]})))))

(t/deftest test-predicate-expression
  (f/transact-people! *kv* [{:name "Ivan" :last-name "Ivanov" :age 30}
                            {:name "Bob" :last-name "Ivanov" :age 40 }
                            {:name "Dominic" :last-name "Monroe" :age 50}])

  (t/testing "< predicate expression"
    (t/is (= #{["Ivan"] ["Bob"]}
             (q/q (db *kv*) {:find ['name]
                             :where [['e :name 'name]
                                     ['e :age 'age]
                                     '(< age 50)]})))

    (t/is (= #{["Dominic"]}
             (q/q (db *kv*) {:find ['name]
                             :where [['e :name 'name]
                                     ['e :age 'age]
                                     '(>= age 50)]})))))

;; TODO write:
(t/deftest test-use-another-datasource)
