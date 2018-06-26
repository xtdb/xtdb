(ns crux.index-query-test
  (:require [clojure.spec.alpha :as s]
            [clojure.test :as t]
            [crux.fixtures :as f :refer [*kv*]]
            [crux.doc :as doc :refer [db]]
            [crux.query]))

(t/use-fixtures :each f/with-kv-store)

(t/deftest test-sanity-check
  (f/transact-people! *kv* [{:name "Ivan"}])
  (t/is (first (doc/q (db *kv*) {:find ['e]
                                 :where [['e :name "Ivan"]]}))))

(t/deftest test-basic-query
  (let [[ivan petr] (f/transact-people! *kv* [{:name "Ivan" :last-name "Ivanov"}
                                              {:name "Petr" :last-name "Petrov"}])]

    (t/testing "Can query by single field"
      (t/is (= #{["Ivan"]} (doc/q (db *kv*) {:find ['name]
                                             :where [['e :name "Ivan"]
                                                     ['e :name 'name]]})))
      (t/is (= #{["Petr"]} (doc/q (db *kv*) {:find ['name]
                                             :where [['e :name "Petr"]
                                                     ['e :name 'name]]}))))

    (t/testing "Can query by single field"
      (t/is (= #{[(:crux.db/id ivan)]} (doc/q (db *kv*) {:find ['e]
                                                         :where [['e :name "Ivan"]]})))
      (t/is (= #{[(:crux.db/id petr)]} (doc/q (db *kv*) {:find ['e]
                                                         :where [['e :name "Petr"]]}))))

    (t/testing "Can query using multiple terms"
      (t/is (= #{["Ivan" "Ivanov"]} (doc/q (db *kv*) {:find ['name 'last-name]
                                                      :where [['e :name 'name]
                                                              ['e :last-name 'last-name]
                                                              ['e :name "Ivan"]
                                                              ['e :last-name "Ivanov"]]}))))

    (t/testing "Negate query based on subsequent non-matching clause"
      (t/is (= #{} (doc/q (db *kv*) {:find ['e]
                                     :where [['e :name "Ivan"]
                                             ['e :last-name "Ivanov-does-not-match"]]}))))

    (t/testing "Can query for multiple results"
        (t/is (= #{["Ivan"] ["Petr"]}
                 (doc/q (db *kv*) {:find ['name] :where [['e :name 'name]]}))))

    (let [[smith] (f/transact-people! *kv* [{:name "Smith" :last-name "Smith"}])]
      (t/testing "Can query across fields for same value"
        (t/is (= #{[(:crux.db/id smith)]}
                 (doc/q (db *kv*) {:find ['p1] :where [['p1 :name 'name]
                                                       ['p1 :last-name 'name]]}))))

      (t/testing "Can query across fields for same value when value is passed in"
        (t/is (= #{[(:crux.db/id smith)]}
                 (doc/q (db *kv*) {:find ['p1] :where [['p1 :name 'name]
                                                       ['p1 :last-name 'name]
                                                       ['p1 :name "Smith"]]})))))))

(t/deftest test-multiple-results
  (f/transact-people! *kv* [{:name "Ivan" :last-name "1"}
                            {:name "Ivan" :last-name "2"}])
  (t/is (= 2
           (count (doc/q (db *kv*) {:find ['e] :where [['e :name "Ivan"]]})))))

(t/deftest test-query-using-keywords
  (f/transact-people! *kv* [{:name "Ivan" :sex :male}
                            {:name "Petr" :sex :male}
                            {:name "Doris" :sex :female}
                            {:name "Jane" :sex :female}])

  (t/testing "Can query by single field"
    (t/is (= #{["Ivan"] ["Petr"]} (doc/q (db *kv*) {:find ['name]
                                                    :where [['e :name 'name]
                                                            ['e :sex :male]]})))
    (t/is (= #{["Doris"] ["Jane"]} (doc/q (db *kv*) {:find ['name]
                                                     :where [['e :name 'name]
                                                             ['e :sex :female]]})))))

(t/deftest test-basic-query-at-t
  (let [[malcolm] (f/transact-people! *kv* [{:name "Malcolm" :last-name "Sparks"}]
                                      #inst "1986-10-22")]
    (f/transact-people! *kv* [{:name "Malcolma" :last-name "Sparks"}] #inst "1986-10-24")
    (let [q {:find ['e]
                 :where [['e :name "Malcolma"]
                         ['e :last-name "Sparks"]]}]
      (t/is (= #{} (doc/q (doc/db *kv* #inst "1986-10-23")
                          q)))
      (t/is (= #{[(:crux.db/id malcolm)]} (doc/q (db *kv*) q))))))

(t/deftest test-query-across-entities-using-join
  ;; Five people, two of which share the same name:
  (f/transact-people! *kv* [{:name "Ivan"} {:name "Petr"} {:name "Sergei"} {:name "Denis"} {:name "Denis"}])

  (t/testing "Five people, without a join"
    (t/is (= 5 (count (doc/q (db *kv*) {:find ['p1]
                                        :where [['p1 :name 'name]
                                                ['p1 :age 'age]
                                                ['p1 :salary 'salary]]})))))

  (t/testing "Five people, a cartesian product - joining without unification"
    (t/is (= 25 (count (doc/q (db *kv*) {:find ['p1 'p2]
                                         :where [['p1 :name]
                                                 ['p2 :name]]})))))

  (t/testing "A single first result, joined to all possible subsequent results in next term"
    (t/is (= 5 (count (doc/q (db *kv*) {:find ['p1 'p2]
                                        :where [['p1 :name "Ivan"]
                                                ['p2 :name]]})))))

  (t/testing "A single first result, with no subsequent results in next term"
    (t/is (= 0 (count (doc/q (db *kv*) {:find ['p1]
                                        :where [['p1 :name "Ivan"]
                                                ['p2 :name "does-not-match"]]})))))

  (t/testing "Every person joins once, plus 2 more matches"
    (t/is (= 7 (count (doc/q (db *kv*) {:find ['p1 'p2]
                                        :where [['p1 :name 'name]
                                                ['p2 :name 'name]]}))))))

(t/deftest test-join-over-two-attributes
  (f/transact-people! *kv* [{:crux.db/id :ivan :name "Ivan" :last-name "Ivanov"}
                            {:crux.db/id :petr :name "Petr" :follows #{"Ivanov"}}])

  (t/is (= #{[:petr]} (doc/q (db *kv*) '{:find [e2]
                                         :where [[e :last-name last-name]
                                                 [e2 :follows last-name]
                                                 [e :name "Ivan"]]}))))

(t/deftest test-blanks
    (f/transact-people! *kv* [{:name "Ivan"} {:name "Petr"} {:name "Sergei"}])

    (t/is (= #{["Ivan"] ["Petr"] ["Sergei"]}
             (doc/q (db *kv*) {:find ['name]
                               :where [['_ :name 'name]]}))))

(t/deftest test-exceptions
  (t/testing "Unbound query variable"
    (try
      (doc/q (db *kv*) {:find ['bah]
                        :where [['e :name]]})
      (t/is (= true false) "Expected exception")
      (catch IllegalArgumentException e
        (t/is (= "Find clause references unbound variable: bah" (.getMessage e)))))))

;; TODO: lacks not support.
#_(t/deftest test-not-query
    (t/is (= [[:bgp {:e 'e :a :name :v 'name}]
              [:bgp {:e 'e :a :name :v "Ivan"}]
              [:not [:bgp {:e 'e :a :last-name :v "Ivannotov"}]]]

             (s/conform :crux.query/where [['e :name 'name]
                                           ['e :name "Ivan"]
                                           '(not [e :last-name "Ivannotov"])])))

    (f/transact-people! *kv* [{:name "Ivan" :last-name "Ivanov"}
                              {:name "Ivan" :last-name "Ivanov"}
                              {:name "Ivan" :last-name "Ivannotov"}])

    (t/is (= 1 (count (doc/q (db *kv*) {:find ['e]
                                        :where [['e :name 'name]
                                                ['e :name "Ivan"]
                                                '(not [e :last-name "Ivanov"])]}))))

    (t/is (= 2 (count (doc/q (db *kv*) {:find ['e]
                                        :where [['e :name 'name]
                                                ['e :name "Ivan"]
                                                '(not [e :last-name "Ivannotov"])]})))))

;; TODO: lacks or support.
#_(t/deftest test-or-query
  (f/transact-people! *kv* [{:name "Ivan" :last-name "Ivanov"}
                            {:name "Ivan" :last-name "Ivanov"}
                            {:name "Ivan" :last-name "Ivannotov"}
                            {:name "Bob" :last-name "Controlguy"}])

  ;; Here for dev reasons, delete when appropiate
  (t/is (= '[[:bgp {:e e :a :name :v name}]
             [:bgp {:e e :a :name :v "Ivan"}]
             [:or [[:bgp {:e e :a :last-name :v "Ivanov"}]]]]
           (s/conform :crux.query/where [['e :name 'name]
                                         ['e :name "Ivan"]
                                         '(or [[e :last-name "Ivanov"]])])))

  (t/testing "Or works as expected"
    (t/is (= 3 (count (doc/q (db *kv*) {:find ['e]
                                        :where [['e :name 'name]
                                                ['e :name "Ivan"]
                                                '(or [[e :last-name "Ivanov"]
                                                      [e :last-name "Ivannotov"]])]}))))

    (t/is (= 3 (count (doc/q (db *kv*) {:find ['e]
                                        :where [['e :name 'name]
                                                '(or [[e :last-name "Ivanov"]
                                                      [e :name "Bob"]])]})))))

  (t/testing "Or edge case - can take a single clause"
    ;; Unsure of the utility
    (t/is (= 2 (count (doc/q (db *kv*) {:find ['e]
                                        :where [['e :name 'name]
                                                ['e :name "Ivan"]
                                                '(or [[e :last-name "Ivanov"]])]}))))))

;; TODO: lacks and support - might not be supported.
#_(t/deftest test-or-query-can-use-and
    (f/transact-people! *kv* [{:name "Ivan" :sex :male}
                              {:name "Bob" :sex :male}
                              {:name "Ivana" :sex :female}])

    (t/is (= #{["Ivan"]
               ["Ivana"]}
             (doc/q (db *kv*) {:find ['name]
                               :where [['e :name 'name]
                                       '(or [[e :sex :female]
                                             (and [[e :sex :male]
                                                   [e :name "Ivan"]])])]}))))

;; TODO: lacks or support.
#_(t/deftest test-ors-must-use-same-vars
    (try
      (doc/q (db *kv*) {:find ['e]
                        :where [['e :name 'name]
                                '(or [[e1 :last-name "Ivanov"]
                                      [e2 :last-name "Ivanov"]])]})
      (t/is (= true false) "Expected assertion error")
      (catch java.lang.AssertionError e
        (t/is true))))

;; TODO bring back
#_(t/deftest test-ors-can-introduce-new-bindings
    (let [[petr ivan ivanova] (f/transact-people! *kv* [{:name "Petr" :last-name "Smith" :sex :male}
                                                        {:name "Ivan" :last-name "Ivanov" :sex :male}
                                                        {:name "Ivanova" :last-name "Ivanov" :sex :female}])]

      (t/testing "?p2 introduced only inside of an Or"
        (t/is (= #{[(:crux.db/id ivan)]} (doc/q (db *kv*) '{:find [?p2]
                                                            :where [(or [(and [[?p2 :name "Petr"]
                                                                               [?p2 :sex :female]])
                                                                         (and [[?p2 :last-name "Ivanov"]
                                                                               [?p2 :sex :male]])])]}))))))

;; TODO: lacks not-join support - might not be supported.
#_(t/deftest test-not-join
    (f/transact-people! *kv* [{:name "Ivan" :last-name "Ivanov"}
                              {:name "Malcolm" :last-name "Ofsparks"}
                              {:name "Dominic" :last-name "Monroe"}])

    (t/testing "Rudimentary or-join"
      (t/is (= #{["Ivan"] ["Malcolm"]}
               (doc/q (db *kv*) {:find ['name]
                                 :where [['e :name 'name]
                                         '(not-join [e]
                                                    [[e :last-name "Monroe"]])]})))))

;; TODO: lacks nested expression support - might not be supported.
#_(t/deftest test-mixing-expressions
    (f/transact-people! *kv* [{:name "Ivan" :last-name "Ivanov"}
                              {:name "Derek" :last-name "Ivanov"}
                              {:name "Bob" :last-name "Ivannotov"}
                              {:name "Fred" :last-name "Ivannotov"}])

    (t/testing "Or can use not expression"
      (t/is (= #{["Ivan"] ["Derek"] ["Fred"]}
               (doc/q (db *kv*) {:find ['name]
                                 :where [['e :name 'name]
                                         '(or [[e :last-name "Ivanov"]
                                               (not [e :name "Bob"])])]}))))

    (t/testing "Not can use Or expression"
      (t/is (= #{["Fred"]} (doc/q (db *kv*) {:find ['name]
                                             :where [['e :name 'name]
                                                     '(not (or [[e :last-name "Ivanov"]
                                                                [e :name "Bob"]]))]})))))

(t/deftest test-predicate-expression
  (f/transact-people! *kv* [{:name "Ivan" :last-name "Ivanov" :age 30}
                            {:name "Bob" :last-name "Ivanov" :age 40 }
                            {:name "Dominic" :last-name "Monroe" :age 50}])

  (t/testing "< predicate expression"
      (t/is (= #{["Ivan"] ["Bob"]}
               (doc/q (db *kv*) {:find ['name]
                                 :where [['e :name 'name]
                                         ['e :age 'age]
                                         '(< age 50)]})))

      (t/is (= #{["Dominic"]}
               (doc/q (db *kv*) {:find ['name]
                                 :where [['e :name 'name]
                                         ['e :age 'age]
                                         '(>= age 50)]}))))

  (t/testing "clojure.core predicate"
    (t/is (= #{["Bob"] ["Dominic"]}
             (doc/q (db *kv*) {:find ['name]
                               :where [['e :name 'name]
                                       ['e :age 'age]
                                       '(re-find #"o" name)]})))

    (t/testing "No results"
      (t/is (empty? (doc/q (db *kv*) {:find ['name]
                                      :where [['e :name 'name]
                                              ['e :age 'age]
                                              '(re-find #"X" name)]}))))))

(t/deftest test-can-use-idents-as-entities
  (f/transact-people! *kv* [{:crux.db/id :ivan :name "Ivan" :last-name "Ivanov"}
                            {:crux.db/id :petr :name "Petr" :last-name "Petrov" :mentor :ivan}])

  (t/testing "Can query by single field"
    (t/is (= #{[:petr]} (doc/q (db *kv*) '{:find [p]
                                           :where [[i :name "Ivan"]
                                                   [p :mentor i]]})))

    (t/testing "Other direction"
      (t/is (= #{[:petr]} (doc/q (db *kv*) '{:find [p]
                                             :where [[p :mentor i]
                                                     [i :name "Ivan"]]})))))

  ;; TODO: bug with joins to literal entity fields.
  #_(t/testing "Can query by known entity"
      (t/is (= #{["Ivan"]} (doc/q (db *kv*) '{:find [n]
                                              :where [[:ivan :name n]]})))

      (t/is (= #{["Ivan"]} (doc/q (db *kv*) '{:find [n]
                                              :where [[:petr :mentor i]
                                                      [i :name n]]})))

      (t/testing "Other direction"
        (t/is (= #{["Ivan"]} (doc/q (db *kv*) '{:find [n]
                                                :where [[i :name n]
                                                        [:petr :mentor i]]}))))
      (t/testing "No matches"
        (t/is (= #{} (doc/q (db *kv*) '{:find [n]
                                        :where [[:ivan :mentor x]
                                                [x :name n]]})))

        (t/testing "Other direction"
          (t/is (= #{} (doc/q (db *kv*) '{:find [n]
                                          :where [[x :name n]
                                                  [:ivan :mentor x]]})))))))

(t/deftest test-simple-numeric-range-search
  (t/is (= [[:bgp {:e 'i :a :age :v 'age}]
            [:range {:op '<
                     :sym 'age
                     :val 20}]]
           (s/conform :crux.query/where '[[i :age age]
                                          (< age 20)])))

  (f/transact-people! *kv* [{:crux.db/id :ivan :name "Ivan" :last-name "Ivanov" :age 21}
                            {:crux.db/id :petr :name "Petr" :last-name "Petrov" :age 18}])

  (t/testing "Min search case"
    (t/is (= #{[:ivan]} (doc/q (db *kv*) '{:find [i]
                                           :where [[i :age age]
                                                   (> age 20)]})))
    (t/is (= #{} (doc/q (db *kv*) '{:find [i]
                                    :where [[i :age age]
                                            (> age 21)]})))

    (t/is (= #{[:ivan]} (doc/q (db *kv*) '{:find [i]
                                           :where [[i :age age]
                                                   (>= age 21)]}))))

  (t/testing "Max search case"
    (t/is (= #{[:petr]} (doc/q (db *kv*) '{:find [i]
                                           :where [[i :age age]
                                                   (< age 20)]})))
    (t/is (= #{} (doc/q (db *kv*) '{:find [i]
                                    :where [[i :age age]
                                            (< age 18)]})))
    (t/is (= #{[:petr]} (doc/q (db *kv*) '{:find [i]
                                           :where [[i :age age]
                                                   (<= age 18)]})))
    (t/is (= #{[18]} (doc/q (db *kv*) '{:find [age]
                                        :where [[:petr :age age]
                                                (<= age 18)]})))))

(t/deftest test-mutiple-values
  (f/transact-people! *kv* [{:crux.db/id :ivan :name "Ivan" }
                            {:crux.db/id :oleg :name "Oleg"}
                            {:crux.db/id :petr :name "Petr" :follows #{:ivan :oleg}}])

  (t/testing "One way"
    (t/is (= #{[:ivan] [:oleg]} (doc/q (db *kv*) '{:find [x]
                                                   :where [[i :name "Petr"]
                                                           [i :follows x]]}))))

  (t/testing "The other way"
    (t/is (= #{[:petr]} (doc/q (db *kv*) '{:find [i]
                                           :where [[x :name "Ivan"]
                                                   [i :follows x]]})))))

;; TODO write:
(t/deftest test-use-another-datasource)

(t/deftest test-sanitise-join
  (f/transact-people! *kv* [{:crux.db/id :ivan :name "Ivan" :last-name "Ivanov"}])
  (t/testing "Can query by single field"
    (t/is (= #{[:ivan]} (doc/q (db *kv*) '{:find [e2]
                                           :where [[e :last-name "Ivanov"]
                                                   [e :last-name name1]
                                                   [e2 :last-name name1]]})))))
