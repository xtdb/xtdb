(ns crux.query-test
  (:require [clojure.spec.alpha :as s]
            [clojure.test :as t]
            [crux.db :as db]
            [crux.fixtures :as f :refer [*kv*]]
            [crux.query :as q])
  (:import java.util.UUID))

(t/use-fixtures :each f/with-kv-store)

(t/deftest test-sanity-check
  (f/transact-people! *kv* [{:name "Ivan"}])
  (t/is (first (q/q (q/db *kv*) '{:find [e]
                                  :where [[e :name "Ivan"]]}))))

(t/deftest test-basic-query
  (f/transact-people! *kv* [{:crux.db/id :ivan :name "Ivan" :last-name "Ivanov"}
                            {:crux.db/id :petr :name "Petr" :last-name "Petrov"}])

  (t/testing "Can query value by single field"
    (t/is (= #{["Ivan"]} (q/q (q/db *kv*) '{:find [name]
                                            :where [[e :name "Ivan"]
                                                    [e :name name]]})))
    (t/is (= #{["Petr"]} (q/q (q/db *kv*) '{:find [name]
                                            :where [[e :name "Petr"]
                                                    [e :name name]]}))))

  (t/testing "Can query entity by single field"
    (t/is (= #{[:ivan]} (q/q (q/db *kv*) '{:find [e]
                                           :where [[e :name "Ivan"]]})))
    (t/is (= #{[:petr]} (q/q (q/db *kv*) '{:find [e]
                                           :where [[e :name "Petr"]]}))))

  (t/testing "Can query using multiple terms"
    (t/is (= #{["Ivan" "Ivanov"]} (q/q (q/db *kv*) '{:find [name last-name]
                                                     :where [[e :name name]
                                                             [e :last-name last-name]
                                                             [e :name "Ivan"]
                                                             [e :last-name "Ivanov"]]}))))

  (t/testing "Negate query based on subsequent non-matching clause"
    (t/is (= #{} (q/q (q/db *kv*) '{:find [e]
                                    :where [[e :name "Ivan"]
                                            [e :last-name "Ivanov-does-not-match"]]}))))

  (t/testing "Can query for multiple results"
    (t/is (= #{["Ivan"] ["Petr"]}
             (q/q (q/db *kv*) '{:find [name] :where [[e :name name]]}))))


  (f/transact-people! *kv* [{:crux.db/id :smith :name "Smith" :last-name "Smith"}])
  (t/testing "Can query across fields for same value"
    (t/is (= #{[:smith]}
             (q/q (q/db *kv*) '{:find [p1] :where [[p1 :name name]
                                                   [p1 :last-name name]]}))))

  (t/testing "Can query across fields for same value when value is passed in"
    (t/is (= #{[:smith]}
             (q/q (q/db *kv*) '{:find [p1] :where [[p1 :name name]
                                                   [p1 :last-name name]
                                                   [p1 :name "Smith"]]})))))

(t/deftest test-basic-query-returning-meta
  (f/transact-people! *kv* [{:crux.db/id :ivan :name "Ivan" :last-name "Ivanov"}])

  (t/testing "Can retrieve meta data via results"
    (let [m (-> (meta (first (q/q (q/db *kv*) '{:find [e]
                                                :where [[e :name "Ivan"]]}))))]
      (t/is (= {:var 'e :value :ivan}
               (select-keys (get m 'e) [:var :value])))
      (t/is (= "Ivanov" (get-in m ['e :doc :last-name])))))

  (t/testing "Can retrieve meta data directly"
    (t/is (= ['e 'first-name] (map :var (first (q/q (q/db *kv*)
                                                    '{:find [e first-name]
                                                      :where [[e :name first-name]]
                                                      :full-results? true})))))))

(t/deftest test-query-with-arguments
  (let [[ivan petr] (f/transact-people! *kv* [{:name "Ivan" :last-name "Ivanov"}
                                              {:name "Petr" :last-name "Petrov"}])]

    (t/testing "Can query entity by single field"
      (t/is (= #{[(:crux.db/id ivan)]} (q/q (q/db *kv*) '{:find [e]
                                                          :where [[e :name name]]
                                                          :args [{:name "Ivan"}]})))
      (t/is (= #{[(:crux.db/id petr)]} (q/q (q/db *kv*) '{:find [e]
                                                          :where [[e :name name]]
                                                          :args [{:name "Petr"}]}))))

    (t/testing "Can query entity by entity position"
      (t/is (= #{["Ivan"]
                 ["Petr"]} (q/q (q/db *kv*) {:find '[name]
                                             :where '[[e :name name]]
                                             :args [{:e (:crux.db/id ivan)}
                                                    {:e (:crux.db/id petr)}]})))

      (t/is (= #{["Ivan" "Ivanov"]
                 ["Petr" "Petrov"]} (q/q (q/db *kv*) {:find '[name last-name]
                                                      :where '[[e :name name]
                                                               [e :last-name last-name]]
                                                      :args [{:e (:crux.db/id ivan)}
                                                             {:e (:crux.db/id petr)}]}))))

    (t/testing "Can match on both entity and value position"
      (t/is (= #{["Ivan"]} (q/q (q/db *kv*) {:find '[name]
                                             :where '[[e :name name]]
                                             :args [{:e (:crux.db/id ivan)
                                                     :name "Ivan"}]})))

      (t/is (= #{} (q/q (q/db *kv*) {:find '[name]
                                     :where '[[e :name name]]
                                     :args [{:e (:crux.db/id ivan)
                                             :name "Petr"}]}))))

    (t/testing "Can query entity by single field with several arguments"
      (t/is (= #{[(:crux.db/id ivan)]
                 [(:crux.db/id petr)]} (q/q (q/db *kv*) '{:find [e]
                                                          :where [[e :name name]]
                                                          :args [{:name "Ivan"}
                                                                 {:name "Petr"}]}))))

    (t/testing "Can query entity by single field with literals"
      (t/is (= #{[(:crux.db/id ivan)]} (q/q (q/db *kv*) '{:find [e]
                                                          :where [[e :name name]
                                                                  [e :last-name "Ivanov"]]
                                                          :args [{:name "Ivan"}
                                                                 {:name "Petr"}]})))

      (t/is (= #{["Ivan"]} (q/q (q/db *kv*) {:find '[name]
                                             :where '[[e :name name]
                                                      [e :last-name "Ivanov"]]
                                             :args [{:e (:crux.db/id ivan)}
                                                    {:e (:crux.db/id petr)}]}))))

    (t/testing "Can query entity by non existent argument"
      (t/is (= #{} (q/q (q/db *kv*) '{:find [e]
                                      :where [[e :name name]]
                                      :args [{:name "Bob"}]}))))

    (t/testing "Can query entity with empty arguments"
      (t/is (= #{[(:crux.db/id ivan)]
                 [(:crux.db/id petr)]} (q/q (q/db *kv*) '{:find [e]
                                                          :where [[e :name name]]
                                                          :args []}))))

    (t/testing "Can query entity with tuple arguments"
      (t/is (= #{[(:crux.db/id ivan)]
                 [(:crux.db/id petr)]} (q/q (q/db *kv*) '{:find [e]
                                                          :where [[e :name name]
                                                                  [e :last-name last-name]]
                                                          :args [{:name "Ivan" :last-name "Ivanov"}
                                                                 {:name "Petr" :last-name "Petrov"}]}))))

    (t/testing "Can query predicates based on arguments alone"
      (t/is (= #{["Ivan"]} (q/q (q/db *kv*) '{:find [name]
                                              :where [[(re-find #"I" name)]]
                                              :args [{:name "Ivan"}
                                                     {:name "Petr"}]})))

      (t/is (= #{["Ivan"]} (q/q (q/db *kv*) '{:find [name]
                                              :where [[(re-find #"I" name)]
                                                      [(= last-name "Ivanov")]]
                                              :args [{:name "Ivan" :last-name "Ivanov"}
                                                     {:name "Petr" :last-name "Petrov"}]})))

      (t/is (= #{["Ivan"]
                 ["Petr"]} (q/q (q/db *kv*) '{:find [name]
                                              :where [[(string? name)]]
                                              :args [{:name "Ivan"}
                                                     {:name "Petr"}]})))

      (t/is (= #{["Ivan" "Ivanov"]
                 ["Petr" "Petrov"]} (q/q (q/db *kv*) '{:find [name
                                                              last-name]
                                                       :where [[(not= last-name name)]]
                                                       :args [{:name "Ivan" :last-name "Ivanov"}
                                                              {:name "Petr" :last-name "Petrov"}]})))

      (t/is (= #{["Ivan"]} (q/q (q/db *kv*) '{:find [name]
                                              :where [[(string? name)]
                                                      [(re-find #"I" name)]]
                                              :args [{:name "Ivan"}
                                                     {:name "Petr"}]})))

      (t/is (= #{} (q/q (q/db *kv*) '{:find [name]
                                      :where [[(number? name)]]
                                      :args [{:name "Ivan"}
                                             {:name "Petr"}]})))

      (t/is (= #{} (q/q (q/db *kv*) '{:find [name]
                                      :where [(not [(string? name)])]
                                      :args [{:name "Ivan"}
                                             {:name "Petr"}]})))

      (t/testing "Can use range constraints on arguments"
        (t/is (= #{} (q/q (q/db *kv*) '{:find [age]
                                        :where [[(>= age 21)]]
                                        :args [{:age 20}]})))

        (t/is (= #{[22]} (q/q (q/db *kv*) '{:find [age]
                                            :where [[(>= age 21)]]
                                            :args [{:age 22}]})))))))

(t/deftest test-multiple-results
  (f/transact-people! *kv* [{:name "Ivan" :last-name "1"}
                            {:name "Ivan" :last-name "2"}])
  (t/is (= 2
           (count (q/q (q/db *kv*) '{:find [e] :where [[e :name "Ivan"]]})))))

(t/deftest test-query-using-keywords
  (f/transact-people! *kv* [{:name "Ivan" :sex :male}
                            {:name "Petr" :sex :male}
                            {:name "Doris" :sex :female}
                            {:name "Jane" :sex :female}])

  (t/testing "Can query by single field"
    (t/is (= #{["Ivan"] ["Petr"]} (q/q (q/db *kv*) '{:find [name]
                                                     :where [[e :name name]
                                                             [e :sex :male]]})))
    (t/is (= #{["Doris"] ["Jane"]} (q/q (q/db *kv*) '{:find [name]
                                                      :where [[e :name name]
                                                              [e :sex :female]]})))))

(t/deftest test-basic-query-at-t
  (let [[malcolm] (f/transact-people! *kv* [{:crux.db/id :malcolm :name "Malcolm" :last-name "Sparks"}]
                                      #inst "1986-10-22")]
    (f/transact-people! *kv* [{:crux.db/id :malcolm :name "Malcolma" :last-name "Sparks"}] #inst "1986-10-24")
    (let [q '{:find [e]
              :where [[e :name "Malcolma"]
                      [e :last-name "Sparks"]]}]
      (t/is (= #{} (q/q (q/db *kv* #inst "1986-10-23")
                        q)))
      (t/is (= #{[(:crux.db/id malcolm)]} (q/q (q/db *kv*) q))))))

(t/deftest test-query-across-entities-using-join
  ;; Five people, two of which share the same name:
  (f/transact-people! *kv* [{:name "Ivan"} {:name "Petr"} {:name "Sergei"} {:name "Denis"} {:name "Denis"}])

  (t/testing "Five people, without a join"
    (t/is (= 5 (count (q/q (q/db *kv*) '{:find [p1]
                                         :where [[p1 :name name]
                                                 [p1 :age age]
                                                 [p1 :salary salary]]})))))

  (t/testing "Five people, a cartesian product - joining without unification"
    (t/is (= 25 (count (q/q (q/db *kv*) '{:find [p1 p2]
                                          :where [[p1 :name]
                                                  [p2 :name]]})))))

  (t/testing "A single first result, joined to all possible subsequent results in next term"
    (t/is (= 5 (count (q/q (q/db *kv*) '{:find [p1 p2]
                                         :where [[p1 :name "Ivan"]
                                                 [p2 :name]]})))))

  (t/testing "A single first result, with no subsequent results in next term"
    (t/is (= 0 (count (q/q (q/db *kv*) '{:find [p1]
                                         :where [[p1 :name "Ivan"]
                                                 [p2 :name "does-not-match"]]})))))

  (t/testing "Every person joins once, plus 2 more matches"
    (t/is (= 7 (count (q/q (q/db *kv*) '{:find [p1 p2]
                                         :where [[p1 :name name]
                                                 [p2 :name name]]}))))))

(t/deftest test-join-over-two-attributes
  (f/transact-people! *kv* [{:crux.db/id :ivan :name "Ivan" :last-name "Ivanov"}
                            {:crux.db/id :petr :name "Petr" :follows #{"Ivanov"}}])

  (t/is (= #{[:petr]} (q/q (q/db *kv*) '{:find [e2]
                                         :where [[e :last-name last-name]
                                                 [e2 :follows last-name]
                                                 [e :name "Ivan"]]}))))

(t/deftest test-blanks
  (f/transact-people! *kv* [{:name "Ivan"} {:name "Petr"} {:name "Sergei"}])

  (t/is (= #{["Ivan"] ["Petr"] ["Sergei"]}
           (q/q (q/db *kv*) '{:find [name]
                              :where [[_ :name name]]}))))

(t/deftest test-exceptions
  (t/testing "Unbound query variable"
    (try
      (q/q (q/db *kv*) '{:find [bah]
                         :where [[e :name]]})
      (t/is (= true false) "Expected exception")
      (catch IllegalArgumentException e
        (t/is (= "Find refers to unknown variable: bah" (.getMessage e)))))

    (try
      (q/q (q/db *kv*) '{:find [x]
                         :where [[x :foo]
                                 [(+ 1 bah)]]})
      (t/is (= true false) "Expected exception")
      (catch IllegalArgumentException e
        (t/is (re-find #"Clause refers to unknown variable: bah" (.getMessage e)))))

    (try
      (q/q (q/db *kv*) '{:find [x]
                         :where [[x :foo]
                                 [(+ 1 bah) bah]]})
      (t/is (= true false) "Expected exception")
      (catch RuntimeException e
        (t/is (re-find #"Circular dependency between bah and bah" (.getMessage e)))))

    (try
      (q/q (q/db *kv*) '{:find [foo]
                         :where [[(+ 1 bar) foo]
                                 [(+ 1 foo) bar]]})
      (t/is (= true false) "Expected exception")
      (catch RuntimeException e
        (t/is (re-find #"Circular dependency between bar and foo" (.getMessage e)))))

    (try
      (q/q (q/db *kv*) '{:find [foo]
                         :where [[(+ 1 foo) bar]
                                 [(+ 1 bar) foo]]})
      (t/is (= true false) "Expected exception")
      (catch RuntimeException e
        (t/is (re-find #"Circular dependency between foo and bar" (.getMessage e)))))))

(t/deftest test-not-query
  (t/is (= '[[:triple {:e e :a :name :v name}]
             [:triple {:e e :a :name :v "Ivan"}]
             [:not [[:triple {:e e :a :last-name :v "Ivannotov"}]]]]

           (s/conform :crux.query/where '[[e :name name]
                                          [e :name "Ivan"]
                                          (not [e :last-name "Ivannotov"])])))

  (f/transact-people! *kv* [{:crux.db/id :ivan-ivanov-1 :name "Ivan" :last-name "Ivanov"}
                            {:crux.db/id :ivan-ivanov-2 :name "Ivan" :last-name "Ivanov"}
                            {:crux.db/id :ivan-ivanovtov-1 :name "Ivan" :last-name "Ivannotov"}])

  (t/testing "literal v"
    (t/is (= 1 (count (q/q (q/db *kv*) '{:find [e]
                                         :where [[e :name name]
                                                 [e :name "Ivan"]
                                                 (not [e :last-name "Ivanov"])]}))))

    (t/is (= 1 (count (q/q (q/db *kv*) '{:find [e]
                                         :where [[e :name name]
                                                 (not [e :last-name "Ivanov"])]}))))

    (t/is (= 1 (count (q/q (q/db *kv*) '{:find [e]
                                         :where [[e :name "Ivan"]
                                                 (not [e :last-name "Ivanov"])]}))))

    (t/is (= 2 (count (q/q (q/db *kv*) '{:find [e]
                                         :where [[e :name name]
                                                 [e :name "Ivan"]
                                                 (not [e :last-name "Ivannotov"])]}))))

    (t/testing "multiple clauses in not"
      (t/is (= 2 (count (q/q (q/db *kv*) '{:find [e]
                                           :where [[e :name name]
                                                   [e :name "Ivan"]
                                                   (not [e :last-name "Ivannotov"]
                                                        [e :name "Ivan"])]}))))

      (t/is (= 2 (count (q/q (q/db *kv*) '{:find [e]
                                           :where [[e :name name]
                                                   [e :name "Ivan"]
                                                   (not [e :last-name "Ivannotov"]
                                                        [(string? name)])]}))))

      (t/is (= 3 (count (q/q (q/db *kv*) '{:find [e]
                                           :where [[e :name name]
                                                   [e :name "Ivan"]
                                                   (not [e :last-name "Ivannotov"]
                                                        [(number? name)])]}))))

      (t/is (= 3 (count (q/q (q/db *kv*) '{:find [e]
                                           :where [[e :name name]
                                                   [e :name "Ivan"]
                                                   (not [e :last-name "Ivannotov"]
                                                        [e :name "Bob"])]}))))))

  (t/testing "variable v"
    (t/is (= 0 (count (q/q (q/db *kv*) '{:find [e]
                                         :where [[e :name name]
                                                 [e :name "Ivan"]
                                                 (not [e :name name])]}))))

    (t/is (= 0 (count (q/q (q/db *kv*) '{:find [e]
                                         :where [[e :name name]
                                                 (not [e :name name])]}))))

    (t/is (= 2 (count (q/q (q/db *kv*) '{:find [e]
                                         :where [[e :name name]
                                                 [:ivan-ivanovtov-1 :last-name i-name]
                                                 (not [e :last-name i-name])]})))))

  (t/testing "literal entities"
    (t/is (= 0 (count (q/q (q/db *kv*) '{:find [e]
                                         :where [[e :name name]
                                                 (not [:ivan-ivanov-1 :name name])]}))))

    (t/is (= 1 (count (q/q (q/db *kv*) '{:find [e]
                                         :where [[e :last-name last-name]
                                                 (not [:ivan-ivanov-1 :last-name last-name])]}))))))

(t/deftest test-or-query
  (f/transact-people! *kv* [{:name "Ivan" :last-name "Ivanov"}
                            {:name "Ivan" :last-name "Ivanov"}
                            {:name "Ivan" :last-name "Ivannotov"}
                            {:name "Bob" :last-name "Controlguy"}])

  ;; Here for dev reasons, delete when appropiate
  (t/is (= '[[:triple {:e e :a :name :v name}]
             [:triple {:e e :a :name :v "Ivan"}]
             [:or [[:term [:triple {:e e :a :last-name :v "Ivanov"}]]]]]
           (s/conform :crux.query/where '[[e :name name]
                                          [e :name "Ivan"]
                                          (or [e :last-name "Ivanov"])])))

  (t/testing "Or works as expected"
    (t/is (= 3 (count (q/q (q/db *kv*) '{:find [e]
                                         :where [[e :name name]
                                                 [e :name "Ivan"]
                                                 (or [e :last-name "Ivanov"]
                                                     [e :last-name "Ivannotov"])]}))))

    (t/is (= 4 (count (q/q (q/db *kv*) '{:find [e]
                                         :where [(or [e :last-name "Ivanov"]
                                                     [e :last-name "Ivannotov"]
                                                     [e :last-name "Controlguy"])]}))))

    (t/is (= 0 (count (q/q (q/db *kv*) '{:find [e]
                                         :where [(or [e :last-name "Controlguy"])
                                                 (or [e :last-name "Ivanov"]
                                                     [e :last-name "Ivannotov"])]}))))


    (t/is (= 0 (count (q/q (q/db *kv*) '{:find [e]
                                         :where [(or [e :last-name "Ivanov"])
                                                 (or [e :last-name "Ivannotov"])]}))))

    (t/is (= 0 (count (q/q (q/db *kv*) '{:find [e]
                                         :where [[e :last-name "Controlguy"]
                                                 (or [e :last-name "Ivanov"]
                                                     [e :last-name "Ivannotov"])]}))))

    (t/is (= 3 (count (q/q (q/db *kv*) '{:find [e]
                                         :where [[e :name name]
                                                 (or [e :last-name "Ivanov"]
                                                     [e :name "Bob"])]})))))

  (t/testing "Or edge case - can take a single clause"
    ;; Unsure of the utility
    (t/is (= 2 (count (q/q (q/db *kv*) '{:find [e]
                                         :where [[e :name name]
                                                 [e :name "Ivan"]
                                                 (or [e :last-name "Ivanov"])]})))))

  (t/is (= #{["Ivan" "Ivanov"]
             ["Ivan" :optional]} (q/q (q/db *kv*) '{:find [name l]
                                                    :where [[e :name name]
                                                            [e :name "Ivan"]
                                                            (or (and [e :last-name "Ivanov"]
                                                                     [e :last-name l])
                                                                (and [(identity e)]
                                                                     [(identity :optional) l]))]}))))

(t/deftest test-or-query-can-use-and
  (let [[ivan] (f/transact-people! *kv* [{:name "Ivan" :sex :male}
                                         {:name "Bob" :sex :male}
                                         {:name "Ivana" :sex :female}])]

    (t/is (= #{["Ivan"]
               ["Ivana"]}
             (q/q (q/db *kv*) '{:find [name]
                                :where [[e :name name]
                                        (or [e :sex :female]
                                            (and [e :sex :male]
                                                 [e :name "Ivan"]))]})))

    (t/is (= #{[(:crux.db/id ivan)]}
             (q/q (q/db *kv*) '{:find [e]
                                :where [(or [e :name "Ivan"])]})))

    (t/is (= #{}
             (q/q (q/db *kv*) '{:find [name]
                                :where [[e :name name]
                                        (or (and [e :sex :female]
                                                 [e :name "Ivan"]))]})))))

(t/deftest test-ors-must-use-same-vars
  (try
    (q/q (q/db *kv*) '{:find [e]
                       :where [[e :name name]
                               (or [e1 :last-name "Ivanov"]
                                   [e2 :last-name "Ivanov"])]})
    (t/is (= true false) "Expected assertion error")
    (catch IllegalArgumentException e
      (t/is (re-find #"Or requires same logic variables"
                     (.getMessage e)))))

  (try
    (q/q (q/db *kv*) '{:find [x]
                       :where [(or-join [x]
                                        [e1 :last-name "Ivanov"])]})
    (t/is (= true false) "Expected assertion error")
    (catch IllegalArgumentException e
      (t/is (re-find #"Or join variable never used: x"
                     (.getMessage e))))))

(t/deftest test-ors-can-introduce-new-bindings
  (let [[petr ivan ivanova] (f/transact-people! *kv* [{:name "Petr" :last-name "Smith" :sex :male}
                                                      {:name "Ivan" :last-name "Ivanov" :sex :male}
                                                      {:name "Ivanova" :last-name "Ivanov" :sex :female}])]

    (t/testing "?p2 introduced only inside of an Or"
      (t/is (= #{[(:crux.db/id ivan)]} (q/q (q/db *kv*) '{:find [?p2]
                                                          :where [(or (and [?p2 :name "Petr"]
                                                                           [?p2 :sex :female])
                                                                      (and [?p2 :last-name "Ivanov"]
                                                                           [?p2 :sex :male]))]}))))))

(t/deftest test-not-join
  (f/transact-people! *kv* [{:name "Ivan" :last-name "Ivanov"}
                            {:name "Malcolm" :last-name "Ofsparks"}
                            {:name "Dominic" :last-name "Monroe"}])

  (t/testing "Rudimentary not-join"
    (t/is (= #{["Ivan"] ["Malcolm"]}
             (q/q (q/db *kv*) '{:find [name]
                                :where [[e :name name]
                                        (not-join [e]
                                                  [e :last-name "Monroe"])]})))

    (t/is (= #{["Ivan"] ["Malcolm"]}
             (q/q (q/db *kv*) '{:find [name]
                                :where [[e :name name]
                                        (not-join [e]
                                                  [e :last-name last-name]
                                                  [(= last-name "Monroe")])]})))

    (t/is (= #{["Dominic"]}
             (q/q (q/db *kv*) '{:find [name]
                                :where [[e :name name]
                                        (not-join [e]
                                                  [e :last-name last-name]
                                                  [(not= last-name "Monroe")])]})))))

(t/deftest test-mixing-expressions
  (f/transact-people! *kv* [{:name "Ivan" :last-name "Ivanov"}
                            {:name "Derek" :last-name "Ivanov"}
                            {:name "Bob" :last-name "Ivannotov"}
                            {:name "Fred" :last-name "Ivannotov"}])

  (t/testing "Or can use not expression"
    (t/is (= #{["Ivan"] ["Derek"] ["Fred"]}
             (q/q (q/db *kv*) '{:find [name]
                                :where [[e :name name]
                                        (or [e :last-name "Ivanov"]
                                            (not [e :name "Bob"]))]}))))

  (t/testing "Not can use Or expression"
    (t/is (= #{["Fred"]} (q/q (q/db *kv*) '{:find [name]
                                            :where [[e :name name]
                                                    (not (or [e :last-name "Ivanov"]
                                                             [e :name "Bob"]))]})))))

(t/deftest test-predicate-expression
  (f/transact-people! *kv* [{:crux.db/id :ivan :name "Ivan" :last-name "Ivanov" :age 30}
                            {:crux.db/id :bob :name "Bob" :last-name "Ivanov" :age 40}
                            {:crux.db/id :dominic :name "Dominic" :last-name "Monroe" :age 50}])

  (t/testing "range expressions"
    (t/is (= #{["Ivan"] ["Bob"]}
             (q/q (q/db *kv*) '{:find [name]
                                :where [[e :name name]
                                        [e :age age]
                                        [(< age 50)]]})))

    (t/is (= #{["Dominic"]}
             (q/q (q/db *kv*) '{:find [name]
                                :where [[e :name name]
                                        [e :age age]
                                        [(>= age 50)]]})))

    (t/testing "fallback to built in predicate for vars"
      (t/is (= #{["Ivan" 30 "Ivan" 30]
                 ["Ivan" 30 "Bob" 40]
                 ["Ivan" 30 "Dominic" 50]
                 ["Bob" 40 "Bob" 40]
                 ["Bob" 40 "Dominic" 50]
                 ["Dominic" 50 "Dominic" 50]}
               (q/q (q/db *kv*) '{:find [name age1 name2 age2]
                                  :where [[e :name name]
                                          [e :age age1]
                                          [e2 :name name2]
                                          [e2 :age age2]
                                          [(<= age1 age2)]]})))

      (t/is (= #{["Ivan" "Dominic"]
                 ["Ivan" "Bob"]
                 ["Dominic" "Bob"]}
               (q/q (q/db *kv*) '{:find [name1 name2]
                                  :where [[e :name name1]
                                          [e2 :name name2]
                                          [(> name1 name2)]]})))))

  (t/testing "clojure.core predicate"
    (t/is (= #{["Bob"] ["Dominic"]}
             (q/q (q/db *kv*) '{:find [name]
                                :where [[e :name name]
                                        [(re-find #"o" name)]]})))

    (t/testing "No results"
      (t/is (empty? (q/q (q/db *kv*) '{:find [name]
                                       :where [[e :name name]
                                               [(re-find #"X" name)]]}))))

    (t/testing "Not predicate"
      (t/is (= #{["Ivan"]}
               (q/q (q/db *kv*) '{:find [name]
                                  :where [[e :name name]
                                          (not [(re-find #"o" name)])]}))))

    (t/testing "Entity variable"
      (t/is (= #{["Ivan"]}
               (q/q (q/db *kv*) '{:find [name]
                                  :where [[e :name name]
                                          [(= :ivan e)]]})))

      (t/testing "Filtered by value"
        (t/is (= #{[:bob] [:ivan]}
                 (q/q (q/db *kv*) '{:find [e]
                                    :where [[e :last-name last-name]
                                            [(= "Ivanov" last-name)]]})))

        (t/is (= #{[:ivan]}
                 (q/q (q/db *kv*) '{:find [e]
                                    :where [[e :last-name last-name]
                                            [e :age age]
                                            [(= "Ivanov" last-name)]
                                            [(= 30 age)]]})))))

    (t/testing "Several variables"
      (t/is (= #{["Bob"]}
               (q/q (q/db *kv*) '{:find [name]
                                  :where [[e :name name]
                                          [e :age age]
                                          [(= 40 age)]
                                          [(re-find #"o" name)]
                                          [(not= age name)]]})))

      (t/is (= #{[:bob "Ivanov"]}
               (q/q (q/db *kv*) '{:find [e last-name]
                                  :where [[e :last-name last-name]
                                          [e :age age]
                                          [(re-find #"ov$" last-name)]
                                          (not [(= age 30)])]})))

      (t/testing "No results"
        (t/is (= #{}
                 (q/q (q/db *kv*) '{:find [name]
                                    :where [[e :name name]
                                            [e :age age]
                                            [(re-find #"o" name)]
                                            [(= age name)]]})))))

    (t/testing "Bind result to var"
      (t/is (= #{["Dominic" 25] ["Ivan" 15] ["Bob" 20]}
               (q/q (q/db *kv*) '{:find [name half-age]
                                  :where [[e :name name]
                                          [e :age age]
                                          [(quot age 2) half-age]]})))

      (t/testing "Order of joins is rearranged to ensure arguments are bound"
        (t/is (= #{["Dominic" 25] ["Ivan" 15] ["Bob" 20]}
                 (q/q (q/db *kv*) '{:find [name half-age]
                                    :where [[e :name name]
                                            [e :age real-age]
                                            [(quot real-age 2) half-age]]}))))

      (t/testing "Binding more than once intersects result"
        (t/is (= #{["Ivan" 15]}
                 (q/q (q/db *kv*) '{:find [name half-age]
                                    :where [[e :name name]
                                            [e :age real-age]
                                            [(quot real-age 2) half-age]
                                            [(- real-age 15) half-age]]}))))

      (t/testing "Binding can use range predicates"
        (t/is (= #{["Dominic" 25]}
                 (q/q (q/db *kv*) '{:find [name half-age]
                                    :where [[e :name name]
                                            [e :age real-age]
                                            [(quot real-age 2) half-age]
                                            [(> half-age 20)]]})))))))

(t/deftest test-attributes-with-multiple-values
  (f/transact-people! *kv* [{:crux.db/id :ivan :name "Ivan" :last-name "Ivanov" :age 30 :friends #{:bob :dominic}}
                            {:crux.db/id :bob :name "Bob" :last-name "Ivanov" :age 40 :friends #{:ivan :dominic}}
                            {:crux.db/id :dominic :name "Dominic" :last-name "Monroe" :age 50 :friends #{:bob}}])

  (t/testing "can find multiple values"
    (t/is (= #{[:bob] [:dominic]}
             (q/q (q/db *kv*) '{:find [f]
                                :where [[i :name "Ivan"]
                                        [i :friends f]]}))))

  (t/testing "can find based on single value"
    (t/is (= #{[:ivan]}
             (q/q (q/db *kv*) '{:find [i]
                                :where [[i :name "Ivan"]
                                        [i :friends :bob]]}))))

  (t/testing "join intersects values"
    (t/is (= #{[:bob]}
             (q/q (q/db *kv*) '{:find [f]
                                :where [[i :name "Ivan"]
                                        [i :friends f]
                                        [d :name "Dominic"]
                                        [d :friends f]]}))))

  (t/testing "clojure.core predicate filters values"
    (t/is (= #{[:bob]}
             (q/q (q/db *kv*) '{:find [f]
                                :where [[i :name "Ivan"]
                                        [i :friends f]
                                        [(= f :bob)]]})))

    (t/is (= #{[:dominic]}
             (q/q (q/db *kv*) '{:find [f]
                                :where [[i :name "Ivan"]
                                        [i :friends f]
                                        [(not= f :bob)]]}))))

  (t/testing "unification filters values"
    (t/is (= #{[:bob]}
             (q/q (q/db *kv*) '{:find [f]
                                :where [[i :name "Ivan"]
                                        [i :friends f]
                                        [(== f :bob)]]})))

    (t/is (= #{[:bob] [:dominic]}
             (q/q (q/db *kv*) '{:find [f]
                                :where [[i :name "Ivan"]
                                        [i :friends f]
                                        [(== f #{:bob :dominic})]]})))

    (t/is (= #{[:dominic]}
             (q/q (q/db *kv*) '{:find [f]
                                :where [[i :name "Ivan"]
                                        [i :friends f]
                                        [(!= f :bob)]]}))))

  (t/testing "not filters values"
    (t/is (= #{[:ivan :dominic]}
             (q/q (q/db *kv*) '{:find [i f]
                                :where [[i :name "Ivan"]
                                        [i :friends f]
                                        (not [(= f :bob)])]})))))

(t/deftest test-can-use-idents-as-entities
  (f/transact-people! *kv* [{:crux.db/id :ivan :name "Ivan" :last-name "Ivanov"}
                            {:crux.db/id :petr :name "Petr" :last-name "Petrov" :mentor :ivan}])

  (t/testing "Can query by single field"
    (t/is (= #{[:petr]} (q/q (q/db *kv*) '{:find [p]
                                           :where [[i :name "Ivan"]
                                                   [p :mentor i]]})))

    (t/testing "Other direction"
      (t/is (= #{[:petr]} (q/q (q/db *kv*) '{:find [p]
                                             :where [[p :mentor i]
                                                     [i :name "Ivan"]]})))))

  (t/testing "Can query by known entity"
    (t/is (= #{["Ivan"]} (q/q (q/db *kv*) '{:find [n]
                                            :where [[:ivan :name n]]})))

    (t/is (= #{["Ivan"]} (q/q (q/db *kv*) '{:find [n]
                                            :where [[:petr :mentor i]
                                                    [i :name n]]})))

    (t/is (= #{["Ivan"]} (q/q (q/db *kv*) '{:find [n]
                                            :where [[p :name "Petr"]
                                                    [p :mentor i]
                                                    [i :name n]]})))

    (t/is (= #{["Ivan"]} (q/q (q/db *kv*) '{:find [n]
                                            :where [[p :mentor i]
                                                    [i :name n]]})))

    (t/is (= #{[:ivan]} (q/q (q/db *kv*) '{:find [i]
                                           :where [[p :name "Petr"]
                                                   [p :mentor i]]})))

    (t/testing "Other direction"
      (t/is (= #{["Ivan"]} (q/q (q/db *kv*) '{:find [n]
                                              :where [[i :name n]
                                                      [:petr :mentor i]]}))))
    (t/testing "No matches"
      (t/is (= #{} (q/q (q/db *kv*) '{:find [n]
                                      :where [[:ivan :mentor x]
                                              [x :name n]]})))

      (t/testing "Other direction"
        (t/is (= #{} (q/q (q/db *kv*) '{:find [n]
                                        :where [[x :name n]
                                                [:ivan :mentor x]]})))))

    (t/testing "Literal entity and literal value"
      (t/is (= #{[true]} (q/q (q/db *kv*) '{:find [found?]
                                            :where [[:ivan :name "Ivan"]
                                                    [(identity true) found?]]})))

      (t/is (= #{} (q/q (q/db *kv*) '{:find [found?]
                                      :where [[:ivan :name "Bob"]
                                              [(identity true) found?]]}))))))

(t/deftest test-join-and-seek-bugs
  (f/transact-people! *kv* [{:crux.db/id :ivan :name "Ivan" :last-name "Ivanov"}
                            {:crux.db/id :petr :name "Petr" :last-name "Petrov" :mentor :ivan}])

  (t/testing "index seek bugs"
    (t/is (= #{} (q/q (q/db *kv*) '{:find [i]
                                    :where [[p :name "Petrov"]
                                            [p :mentor i]]})))


    (t/is (= #{} (q/q (q/db *kv*) '{:find [p]
                                    :where [[p :name "Pet"]]})))

    (t/is (= #{} (q/q (q/db *kv*) '{:find [p]
                                    :where [[p :name "I"]]})))

    (t/is (= #{} (q/q (q/db *kv*) '{:find [p]
                                    :where [[p :name "Petrov"]]})))

    (t/is (= #{} (q/q (q/db *kv*) '{:find [i]
                                    :where [[p :name "Pet"]
                                            [p :mentor i]]})))

    (t/is (= #{} (q/q (q/db *kv*) '{:find [i]
                                    :where [[p :name "Petrov"]
                                            [p :mentor i]]}))))

  (t/testing "join bugs"
    (t/is (= #{} (q/q (q/db *kv*) '{:find [p]
                                    :where [[p :name "Ivan"]
                                            [p :mentor i]]})))

    (t/is (= #{} (q/q (q/db *kv*) '{:find [i]
                                    :where [[p :name "Ivan"]
                                            [p :mentor i]]})))))

(t/deftest test-queries-with-variables-only
  (f/transact-people! *kv* [{:crux.db/id :ivan :name "Ivan" :mentor :petr}
                            {:crux.db/id :petr :name "Petr" :mentor :oleg}
                            {:crux.db/id :oleg :name "Oleg" :mentor :ivan}])

  (t/is (= #{[:oleg "Oleg" :petr "Petr"]
             [:ivan "Ivan" :oleg "Oleg"]
             [:petr "Petr" :ivan "Ivan"]} (q/q (q/db *kv*) '{:find [e1 n1 e2 n2]
                                                             :where [[e1 :name n1]
                                                                     [e2 :mentor e1]
                                                                     [e2 :name n2]]}))))

(t/deftest test-index-unification
  (f/transact-people! *kv* [{:crux.db/id :ivan :name "Ivan" :last-name "Ivanov"}
                            {:crux.db/id :petr :name "Petr" :last-name "Petrov" :mentor :ivan}])

  (t/is (= #{[:petr :petr]} (q/q (q/db *kv*) '{:find [p1 p2]
                                               :where [[p1 :name "Petr"]
                                                       [p2 :mentor i]
                                                       [(== p1 p2)]]})))

  (t/is (= #{} (q/q (q/db *kv*) '{:find [p1 p2]
                                  :where [[p1 :name "Petr"]
                                          [p2 :mentor i]
                                          [(== p1 i)]]})))

  (t/is (= #{} (q/q (q/db *kv*) '{:find [p1 p2]
                                  :where [[p1 :name "Petr"]
                                          [p2 :mentor i]
                                          [(== p1 i)]]})))

  (t/is (= #{[:petr :petr]} (q/q (q/db *kv*) '{:find [p1 p2]
                                               :where [[p1 :name "Petr"]
                                                       [p2 :mentor i]
                                                       [(!= p1 i)]]})))

  (t/is (= #{} (q/q (q/db *kv*) '{:find [p1 p2]
                                  :where [[p1 :name "Petr"]
                                          [p2 :mentor i]
                                          [(!= p1 p2)]]})))


  (t/is (= #{} (q/q (q/db *kv*) '{:find [p]
                                  :where [[p :name "Petr"]
                                          [p :mentor i]
                                          [(== p i)]]})))

  (t/testing "unify with literal"
    (t/is (= #{[:petr]} (q/q (q/db *kv*) '{:find [p]
                                           :where [[p :name n]
                                                   [(== n "Petr")]]})))

    (t/is (= #{[:ivan]} (q/q (q/db *kv*) '{:find [p]
                                           :where [[p :name n]
                                                   [(!= n "Petr")]]}))))

  (t/testing "unify with entity"
    (t/is (= #{["Petr"]} (q/q (q/db *kv*) '{:find [n]
                                            :where [[p :name n]
                                                    [(== p :petr)]]})))

    (t/is (= #{["Ivan"]} (q/q (q/db *kv*) '{:find [n]
                                            :where [[i :name n]
                                                    [(!= i :petr)]]}))))

  (t/testing "multiple literals in set"
    (t/is (= #{[:petr] [:ivan]} (q/q (q/db *kv*) '{:find [p]
                                                   :where [[p :name n]
                                                           [(== n #{"Petr" "Ivan"})]]})))

    (t/is (= #{[:ivan]} (q/q (q/db *kv*) '{:find [p]
                                           :where [[p :name n]
                                                   [(!= n #{"Petr"})]]})))

    (t/is (= #{} (q/q (q/db *kv*) '{:find [p]
                                    :where [[p :name n]
                                            [(== n #{})]]})))

    (t/is (= #{[:petr] [:ivan]} (q/q (q/db *kv*) '{:find [p]
                                                   :where [[p :name n]
                                                           [(!= n #{})]]})))))

(t/deftest test-simple-numeric-range-search
  (t/is (= '[[:triple {:e i, :a :age, :v age}]
             [:range [[:sym-val {:op <, :sym age, :val 20}]]]]
           (s/conform :crux.query/where '[[i :age age]
                                          [(< age 20)]])))

  (f/transact-people! *kv* [{:crux.db/id :ivan :name "Ivan" :last-name "Ivanov" :age 21}
                            {:crux.db/id :petr :name "Petr" :last-name "Petrov" :age 18}])

  (t/testing "Min search case"
    (t/is (= #{[:ivan]} (q/q (q/db *kv*) '{:find [i]
                                           :where [[i :age age]
                                                   [(> age 20)]]})))
    (t/is (= #{} (q/q (q/db *kv*) '{:find [i]
                                    :where [[i :age age]
                                            [(> age 21)]]})))

    (t/is (= #{[:ivan]} (q/q (q/db *kv*) '{:find [i]
                                           :where [[i :age age]
                                                   [(>= age 21)]]}))))

  (t/testing "Max search case"
    (t/is (= #{[:petr]} (q/q (q/db *kv*) '{:find [i]
                                           :where [[i :age age]
                                                   [(< age 20)]]})))
    (t/is (= #{} (q/q (q/db *kv*) '{:find [i]
                                    :where [[i :age age]
                                            [(< age 18)]]})))
    (t/is (= #{[:petr]} (q/q (q/db *kv*) '{:find [i]
                                           :where [[i :age age]
                                                   [(<= age 18)]]})))
    (t/is (= #{[18]} (q/q (q/db *kv*) '{:find [age]
                                        :where [[:petr :age age]
                                                [(<= age 18)]]}))))

  (t/testing "Reverse symbol and value"
    (t/is (= #{[:ivan]} (q/q (q/db *kv*) '{:find [i]
                                           :where [[i :age age]
                                                   [(<= 20 age)]]})))

    (t/is (= #{[:petr]} (q/q (q/db *kv*) '{:find [i]
                                           :where [[i :age age]
                                                   [(>= 20 age)]]})))))

(t/deftest test-mutiple-values
  (f/transact-people! *kv* [{:crux.db/id :ivan :name "Ivan" }
                            {:crux.db/id :oleg :name "Oleg"}
                            {:crux.db/id :petr :name "Petr" :follows #{:ivan :oleg}}])

  (t/testing "One way"
    (t/is (= #{[:ivan] [:oleg]} (q/q (q/db *kv*) '{:find [x]
                                                   :where [[i :name "Petr"]
                                                           [i :follows x]]}))))

  (t/testing "The other way"
    (t/is (= #{[:petr]} (q/q (q/db *kv*) '{:find [i]
                                           :where [[x :name "Ivan"]
                                                   [i :follows x]]})))))

(t/deftest test-sanitise-join
  (f/transact-people! *kv* [{:crux.db/id :ivan :name "Ivan" :last-name "Ivanov"}])
  (t/testing "Can query by single field"
    (t/is (= #{[:ivan]} (q/q (q/db *kv*) '{:find [e2]
                                           :where [[e :last-name "Ivanov"]
                                                   [e :last-name name1]
                                                   [e2 :last-name name1]]})))))

(t/deftest test-basic-rules
  (t/is (= '[[:triple {:e i, :a :age, :v age}]
             [:rule {:name over-twenty-one?, :args [age]}]]
           (s/conform :crux.query/where '[[i :age age]
                                          (over-twenty-one? age)])))

  (t/is (= [{:head '{:name over-twenty-one?, :args [age]},
             :body '[[:range [[:sym-val {:op >=, :sym age, :val 21}]]]]}
            '{:head {:name over-twenty-one?, :args [age]},
              :body [[:not [[:range [[:sym-val {:op <, :sym age, :val 21}]]]]]]}]
           (s/conform :crux.query/rules '[[(over-twenty-one? age)
                                           [(>= age 21)]]
                                          [(over-twenty-one? age)
                                           (not [(< age 21)])]])))

  (f/transact-people! *kv* [{:crux.db/id :ivan :name "Ivan" :last-name "Ivanov" :age 21}
                            {:crux.db/id :petr :name "Petr" :last-name "Petrov" :age 18}])

  (t/testing "without rule"
    (t/is (= #{[:ivan]} (q/q (q/db *kv*) '{:find [i]
                                           :where [[i :age age]
                                                   [(>= age 21)]]}))))

  (t/testing "rule using same variable name as body"
    (t/is (= #{[:ivan]} (q/q (q/db *kv*) '{:find [i]
                                           :where [[i :age age]
                                                   (over-twenty-one? age)]
                                           :rules [[(over-twenty-one? age)
                                                    [(>= age 21)]]]}))))

  (t/testing "rules directly on arguments"
    (t/is (= #{[21]} (q/q (q/db *kv*) '{:find [age]
                                        :where [(over-twenty-one? age)]
                                        :args [{:age 21}]
                                        :rules [[(over-twenty-one? age)
                                                 [(>= age 21)]]]})))

    (t/is (= #{} (q/q (q/db *kv*) '{:find [age]
                                    :where [(over-twenty-one? age)]
                                    :args [{:age 20}]
                                    :rules [[(over-twenty-one? age)
                                             [(>= age 21)]]]}))))

  (t/testing "rule using required bound args"
    (t/is (= #{[:ivan]} (q/q (q/db *kv*) '{:find [i]
                                           :where [[i :age age]
                                                   (over-twenty-one? age)]
                                           :rules [[(over-twenty-one? [age])
                                                    [(>= age 21)]]]}))))

  (t/testing "rule using different variable name from body"
    (t/is (= #{[:ivan]} (q/q (q/db *kv*) '{:find [i]
                                           :where [[i :age age]
                                                   (over-twenty-one? age)]
                                           :rules [[(over-twenty-one? x)
                                                    [(>= x 21)]]]}))))

  (t/testing "nested rules"
    (t/is (= #{[:ivan]} (q/q (q/db *kv*) '{:find [i]
                                           :where [[i :age age]
                                                   (over-twenty-one? age)]
                                           :rules [[(over-twenty-one? x)
                                                    (over-twenty-one-internal? x)]
                                                   [(over-twenty-one-internal? y)
                                                    [(>= y 21)]]]}))))

  (t/testing "rule using multiple arguments"
    (t/is (= #{[:ivan]} (q/q (q/db *kv*) '{:find [i]
                                           :where [[i :age age]
                                                   (over-age? age 21)]
                                           :rules [[(over-age? [age] required-age)
                                                    [(>= age required-age)]]]}))))

  (t/testing "rule using multiple branches"
    (t/is (= #{[:ivan]} (q/q (q/db *kv*) '{:find [i]
                                           :where [(is-ivan-or-bob? i)]
                                           :rules [[(is-ivan-or-bob? i)
                                                    [i :name "Ivan"]
                                                    [i :last-name "Ivanov"]]
                                                   [(is-ivan-or-bob? i)
                                                    [i :name "Bob"]]]})))

    (t/is (= #{["Petr"]} (q/q (q/db *kv*) '{:find [name]
                                            :where [[i :name name]
                                                    (not (is-ivan-or-bob? i))]
                                            :rules [[(is-ivan-or-bob? i)
                                                     [i :name "Ivan"]]
                                                    [(is-ivan-or-bob? i)
                                                     [i :name "Bob"]]]})))

    (t/is (= #{[:ivan]
               [:petr]} (q/q (q/db *kv*) '{:find [i]
                                           :where [(is-ivan-or-petr? i)]
                                           :rules [[(is-ivan-or-petr? i)
                                                    [i :name "Ivan"]]
                                                   [(is-ivan-or-petr? i)
                                                    [i :name "Petr"]]]}))))

  (try
    (q/q (q/db *kv*) '{:find [i]
                       :where [[i :age age]
                               (over-twenty-one? age)]})
    (t/is (= true false) "Expected exception")
    (catch IllegalArgumentException e
      (t/is (re-find #"Unknown rule: " (.getMessage e)))))

  (try
    (q/q (q/db *kv*) '{:find [i]
                       :where [[i :age age]
                               (over-twenty-one? i age)]
                       :rules [[(over-twenty-one? x)
                                [(>= x 21)]]]})
    (t/is (= true false) "Expected exception")
    (catch IllegalArgumentException e
      (t/is (re-find #"Rule invocation has wrong arity, expected: 1" (.getMessage e)))))

  (try
    (q/q (q/db *kv*) '{:find [i]
                       :where [[i :age age]
                               (is-ivan-or-petr? i name)]
                       :rules [[(is-ivan-or-petr? i name)
                                [i :name "Ivan"]]
                               [(is-ivan-or-petr? i)
                                [i :name "Petr"]]]})
    (t/is (= true false) "Expected exception")
    (catch IllegalArgumentException e
      (t/is (re-find #"Rule definitions require same arity:" (.getMessage e))))))

;; https://github.com/juxt/crux/issues/70

(t/deftest test-lookup-by-value-bug-70
  (f/transact-people! *kv* (cons {:crux.db/id :ivan :name "Ivan" :last-name "Ivanov" :age 30}
                                 (repeat 1000 {:age 20})))

  (let [n 10
        acceptable-limit-slowdown 0.1
        factors (->> #(let [direct-hit-ns-start (System/nanoTime)]
                        (t/is (= #{[:ivan]} (q/q (q/db *kv*) '{:find [i]
                                                               :where [[i :age 30]]})))
                        (let [direct-hit-ns (- (System/nanoTime) direct-hit-ns-start)
                              limited-hit-ns-start (System/nanoTime)]
                          (t/is (= 1 (count (q/q (q/db *kv*) '{:find [i]
                                                               :where [[i :age 20]]
                                                               :limit 1}))))
                          (let [limited-hit-ns (- (System/nanoTime) limited-hit-ns-start)]
                            (double (/ (min direct-hit-ns limited-hit-ns)
                                       (max direct-hit-ns limited-hit-ns))))))
                     (repeatedly n))]
    (t/is (>= (/ (reduce + factors) n) acceptable-limit-slowdown))))

;; https://github.com/juxt/crux/issues/71

(t/deftest test-query-limits-bug-71
  (dotimes [_ 10]
    (f/transact-people! *kv* [{:name "Ivan" :last-name "Ivanov"}
                              {:name "Petr" :last-name "Petrov"}
                              {:name "Petr" :last-name "Ivanov"}])

    (t/is (= 2 (count (q/q (q/db *kv*) '{:find [l]
                                         :where [[_ :last-name l]]
                                         :limit 2}))))))

;; https://github.com/juxt/crux/issues/93

(t/deftest test-self-join-bug-93
  (f/transact-people! *kv* [{:crux.db/id :ivan :name "Ivan" :friend :ivan :boss :petr}
                            {:crux.db/id :petr :name "Petr"}])

  (t/is (= #{[:petr]} (q/q (q/db *kv*)
                           '{:find [b]
                             :where [[e :friend e]
                                     [e :boss b]]}))))

(t/deftest test-or-bug-146
  (f/transact-people! *kv* [{:crux.db/id :ivan :name "Ivan" :extra "Petr" :age 20}
                            {:crux.db/id :oleg :name "Oleg" :extra #inst "1980" :age 30}
                            {:crux.db/id :petr :name "Petr" :age 40}])

  ;; This wasn't the bug, but a useful test, lead to fixes in SPARQL
  ;; translator, and an example on how to use this.
  (t/testing "Or with non existing attribute in one leg and different types "
    (t/is (= #{["Ivan" "Petr" 20 :ivan]
               ["Oleg" #inst "1980" 30 :oleg]
               ["Petr" :none 40 :petr]}
             (q/q (q/db *kv*) '{:find [n x a e]
                                :where [[e :name n]
                                        [e :age a]
                                        [e :crux.db/id e]
                                        (or-join [e x]
                                                 [e :extra x]
                                                 (and [(identity :none) x]
                                                      (not [e :extra])))]}))))

  ;; NOTE: This is the actual root cause of the bug, when editing a
  ;; message the id was transacted as a raw string.
  (t/testing "entity ids cannot be raw strings"
    (let [id (UUID/randomUUID)
          id-str (str id)]
      (t/is (thrown-with-msg?
             RuntimeException
             #"Spec assertion failed"
             (f/transact-people! *kv* [{:crux.db/id id-str :name "Ivan" :version 2}]))))))

(t/deftest test-query-and-cas
  (let [tx-log (f/kv-tx-log-w-cache *kv*)]

    (t/testing "can create new user"
      (let [{:crux.tx/keys [tx-time
                            tx-id] :as submitted-tx}
            @(db/submit-tx tx-log [[:crux.tx/cas :ivan nil {:crux.db/id :ivan
                                                            :name "Ivan 1st"}]])]

        (t/is (true? (q/submitted-tx-updated-entity? *kv* submitted-tx :ivan)))
        (t/is (false? (q/submitted-tx-updated-entity? *kv* submitted-tx :petr)))

        (t/is (= #{["Ivan 1st"]} (q/q (q/db *kv* tx-time tx-time)
                                      '{:find [n]
                                        :where [[:ivan :name n]]})))

        (t/is (= tx-id (:crux.tx/tx-id (q/entity-tx (q/db *kv* tx-time tx-time) :ivan))))

        (t/is (= {:crux.db/id :ivan
                  :name "Ivan 1st"} (q/entity (q/db *kv* tx-time tx-time) :ivan)))))

    (t/testing "cannot create existing user"
      (let [{:crux.tx/keys [tx-time
                            tx-id] :as submitted-tx}
            @(db/submit-tx tx-log [[:crux.tx/cas :ivan nil {:crux.db/id :ivan
                                                            :name "Ivan 2nd"}]])]

        (t/is (false? (q/submitted-tx-updated-entity? *kv* submitted-tx :ivan)))

        (t/is (= #{["Ivan 1st"]} (q/q (q/db *kv* tx-time tx-time)
                                      '{:find [n]
                                        :where [[:ivan :name n]]})))

        (t/is (not= tx-id (:tx-id (q/entity-tx (q/db *kv* tx-time tx-time) :ivan))))))

    (t/testing "can update existing user"
      (let [{:crux.tx/keys [tx-time] :as submitted-update-tx}
            @(db/submit-tx tx-log [[:crux.tx/cas :ivan
                                    {:crux.db/id :ivan
                                     :name "Ivan 1st"}
                                    {:crux.db/id :ivan
                                     :name "Ivan 2nd"}]])]

        (t/is (true? (q/submitted-tx-updated-entity? *kv* submitted-update-tx :ivan)))
        (t/is (= #{["Ivan 2nd"]} (q/q (q/db *kv* tx-time tx-time)
                                      '{:find [n]
                                        :where [[:ivan :name n]]})))

        (t/testing "last CAS command in tx wins"
          (let [{:crux.tx/keys [tx-time] :as submitted-tx}
                @(db/submit-tx tx-log [[:crux.tx/cas :ivan
                                        {:crux.db/id :ivan
                                         :name "Ivan 2nd"}
                                        {:crux.db/id :ivan
                                         :name "Ivan 3rd"}]
                                       [:crux.tx/cas :ivan
                                        {:crux.db/id :ivan
                                         :name "Ivan 2nd"}
                                        {:crux.db/id :ivan
                                         :name "Ivan 4th"}]])]

            (t/is (true? (q/submitted-tx-updated-entity? *kv* submitted-tx :ivan)))
            (t/is (= #{["Ivan 4th"]} (q/q (q/db *kv* tx-time
                                                tx-time) '{:find [n]
                                                :where [[:ivan :name n]]})))))

        (t/testing "normal put after CAS works"
          (let [{:crux.tx/keys [tx-time] :as submitted-tx}
                @(db/submit-tx tx-log [[:crux.tx/put :ivan
                                        {:crux.db/id :ivan
                                         :name "Ivan 5th"}]])]

            (t/is (true? (q/submitted-tx-updated-entity? *kv* submitted-tx :ivan)))
            (t/is (= #{["Ivan 5th"]} (q/q (q/db *kv* tx-time
                                                tx-time) '{:find [n]
                                                :where [[:ivan :name n]]})))

            (t/testing "earlier submitted txs can still be checked"
              (t/is (true? (q/submitted-tx-updated-entity? *kv* submitted-update-tx :ivan))))))))))

;; https://www.comp.nus.edu.sg/~ooibc/stbtree95.pdf
;; This test is based on section 7. Support for complex queries in
;; bitemporal databases

;; p1 NY [0,3] [4,now]
;; p1 LA [4,now] [4,now]
;; p2 SFO [0,now] [0,5]
;; p2 SFO [0,5] [5,now]
;; p3 LA [0,now] [0,4]
;; p3 LA [0,4] [4,7]
;; p3 LA [0,7] [7,now]
;; p3 SFO [8,0] [8,now]
;; p4 NY [2,now] [2,3]
;; p4 NY [2,3] [3,now]
;; p4 LA [8,now] [8,now]
;; p5 LA [1O,now] [1O,now]
;; p6 NY [12,now] [12,now]
;; p7 NY [11,now] [11,now]

;; Find all persons who are known to be present in the United States
;; on day 2 (valid time), as of day 3 (transaction time)
;; t2 p2 SFO, t5 p3 LA, t9 p4 NY, t10 p4 NY (?)

(t/deftest test-bitemp-query-from-indexing-temporal-data-using-existing-b+-trees-paper
  (let [tx-log (f/kv-tx-log-w-cache *kv*)]
    ;; Day 0, represented as #inst "2018-12-31"
    @(db/submit-tx tx-log [[:crux.tx/put :p2
                            {:crux.db/id :p2
                             :entry-pt :SFO
                             :arrival-time #inst "2018-12-31"
                             :departure-time :na}
                            #inst "2018-12-31"]
                           [:crux.tx/put :p3
                            {:crux.db/id :p3
                             :entry-pt :LA
                             :arrival-time #inst "2018-12-31"
                             :departure-time :na}
                            #inst "2018-12-31"]])
    ;; Day 1, nothing happens.
    @(db/submit-tx tx-log [])
    ;; Day 2
    @(db/submit-tx tx-log [[:crux.tx/put :p4
                            {:crux.db/id :p4
                             :entry-pt :NY
                             :arrival-time #inst "2019-01-02"
                             :departure-time :na}
                            #inst "2019-01-02"]])
    ;; Day 3
    (let [third-day-submitted-tx @(db/submit-tx tx-log [[:crux.tx/put :p4
                                                         {:crux.db/id :p4
                                                          :entry-pt :NY
                                                          :arrival-time #inst "2019-01-02"
                                                          :departure-time #inst "2019-01-03"}
                                                         #inst "2019-01-03"]])]
      ;; Day 4, correction, adding missing trip on new arrival.
      @(db/submit-tx tx-log [[:crux.tx/put :p1
                              {:crux.db/id :p1
                               :entry-pt :NY
                               :arrival-time #inst "2018-12-31"
                               :departure-time :na}
                              #inst "2018-12-31"]
                             [:crux.tx/put :p1
                              {:crux.db/id :p1
                               :entry-pt :NY
                               :arrival-time #inst "2018-12-31"
                               :departure-time #inst "2019-01-03"}
                              #inst "2019-01-03"]
                             [:crux.tx/put :p1
                              {:crux.db/id :p1
                               :entry-pt :LA
                               :arrival-time #inst "2019-01-04"
                               :departure-time :na}
                              #inst "2019-01-04"]
                             [:crux.tx/put :p3
                              {:crux.db/id :p3
                               :entry-pt :LA
                               :arrival-time #inst "2018-12-31"
                               :departure-time #inst "2019-01-04"}
                              #inst "2019-01-04"]])
      ;; Day 5
      @(db/submit-tx tx-log [[:crux.tx/put :p2
                              {:crux.db/id :p2
                               :entry-pt :SFO
                               :arrival-time #inst "2018-12-31"
                               :departure-time #inst "2018-12-31"}
                              #inst "2019-01-05"]])
      ;; Day 6, nothing happens.
      @(db/submit-tx tx-log [])
      ;; Day 7-12, correction of deletion/departure on day 4. Shows
      ;; how valid time cannot be the same as arrival time.
      @(db/submit-tx tx-log [[:crux.tx/put :p3
                              {:crux.db/id :p3
                               :entry-pt :LA
                               :arrival-time #inst "2018-12-31"
                               :departure-time :na}
                              #inst "2019-01-04"]
                             [:crux.tx/put :p3
                              {:crux.db/id :p3
                               :entry-pt :LA
                               :arrival-time #inst "2018-12-31"
                               :departure-time #inst "2019-01-07"}
                              #inst "2019-01-07"]])
      @(db/submit-tx tx-log [[:crux.tx/put :p3
                              {:crux.db/id :p3
                               :entry-pt :SFO
                               :arrival-time #inst "2019-01-08"
                               :departure-time :na}
                              #inst "2019-01-08"]
                             [:crux.tx/put :p4
                              {:crux.db/id :p4
                               :entry-pt :LA
                               :arrival-time #inst "2019-01-08"
                               :departure-time :na}
                              #inst "2019-01-08"]])
      @(db/submit-tx tx-log [[:crux.tx/put :p3
                              {:crux.db/id :p3
                               :entry-pt :SFO
                               :arrival-time #inst "2019-01-08"
                               :departure-time #inst "2019-01-08"}
                              #inst "2019-01-09"]])
      @(db/submit-tx tx-log [[:crux.tx/put :p5
                              {:crux.db/id :p5
                               :entry-pt :LA
                               :arrival-time #inst "2019-01-10"
                               :departure-time :na}
                              #inst "2019-01-10"]])
      @(db/submit-tx tx-log [[:crux.tx/put :p7
                              {:crux.db/id :p7
                               :entry-pt :NY
                               :arrival-time #inst "2019-01-11"
                               :departure-time :na}
                              #inst "2019-01-11"]])
      @(db/submit-tx tx-log [[:crux.tx/put :p6
                              {:crux.db/id :p6
                               :entry-pt :NY
                               :arrival-time #inst "2019-01-12"
                               :departure-time :na}
                              #inst "2019-01-12"]])

      (t/is (= #{[:p2 :SFO #inst "2018-12-31" :na]
                 [:p3 :LA #inst "2018-12-31" :na]
                 [:p4 :NY #inst "2019-01-02" :na]}
               (q/q (q/db f/*kv* #inst "2019-01-02" (:crux.tx/tx-time third-day-submitted-tx))
                    '{:find [p entry-pt arrival-time departure-time]
                      :where [[p :entry-pt entry-pt]
                              [p :arrival-time arrival-time]
                              [p :departure-time departure-time]]}))))))

;; Tests borrowed from Datascript:
;; https://github.com/tonsky/datascript/tree/master/test/datascript/test

(defn populate-datascript-test-db []
  (f/transact-entity-maps! *kv* [{:crux.db/id :1 :name "Ivan" :age 10}
                                 {:crux.db/id :2 :name "Ivan" :age 20}
                                 {:crux.db/id :3 :name "Oleg" :age 10}
                                 {:crux.db/id :4 :name "Oleg" :age 20}
                                 {:crux.db/id :5 :name "Ivan" :age 10}
                                 {:crux.db/id :6 :name "Ivan" :age 20} ]))

(t/deftest datascript-test-not
  (populate-datascript-test-db)
  (let [db (q/db *kv*)]
    (t/are [q res] (= (q/q db {:find '[?e] :where (quote q)})
                      (into #{} (map vector) res))
      [[?e :name]
       (not [?e :name "Ivan"])]
      #{:3 :4}

      [[?e :name]
       (not
        [?e :name "Ivan"]
        [?e :age  10])]
      #{:2 :3 :4 :6}

      [[?e :name]
       (not [?e :name "Ivan"])
       (not [?e :age 10])]
      #{:4}

      ;; full exclude
      [[?e :name]
       (not [?e :age])]
      #{}

      ;; not-intersecting rels
      [[?e :name "Ivan"]
       (not [?e :name "Oleg"])]
      #{:1 :2 :5 :6}

      ;; exclude empty set
      [[?e :name]
       (not [?e :name "Ivan"]
            [?e :name "Oleg"])]
      #{:1 :2 :3 :4 :5 :6}

      ;; nested excludes
      [[?e :name]
       (not [?e :name "Ivan"]
            (not [?e :age 10]))]
      #{:1 :3 :4 :5})))

(t/deftest datascript-test-not-join
  (populate-datascript-test-db)
  (let [db (q/db *kv*)]
    (t/is (= (q/q db
                  '{:find [?e ?a]
                    :where [[?e :name]
                            [?e :age  ?a]
                            (not-join [?e]
                                      [?e :name "Oleg"]
                                      [?e :age ?a])]})
             #{[:1 10] [:2 20] [:5 10] [:6 20]}))

    (t/is (= (q/q db
                  '{:find [?e ?a]
                    :where [[?e :name]
                            [?e :age  ?a]
                            [?e :age  10]
                            (not-join [?e]
                                      [?e :name "Oleg"]
                                      [?e :age  10]
                                      [?e :age ?a])]})
             #{[:1 10] [:5 10]}))))

(t/deftest datascript-test-not-impl-edge-cases
  (populate-datascript-test-db)
  (let [db (q/db *kv*)]
    (t/are [q res] (= (q/q db {:find '[?e] :where (quote q)})
                      (into #{} (map vector) res))
      ;; const \ empty
      [[?e :name "Oleg"]
       [?e :age  10]
       (not [?e :age 20])]
      #{:3}

      ;; const \ const
      [[?e :name "Oleg"]
       [?e :age  10]
       (not [?e :age 10])]
      #{}

      ;; rel \ const
      [[?e :name "Oleg"]
       (not [?e :age 10])]
      #{:4})

    ;; 2 rels \ 2 rels
    (t/is (= (q/q db
                  '{:find [?e ?e2]
                    :where [[?e  :name "Ivan"]
                            [?e2 :name "Ivan"]
                            (not [?e :age 10]
                                 [?e2 :age 20])]})
             #{[:2 :1] [:6 :5] [:1 :1] [:2 :2] [:5 :5] [:6 :6] [:2 :5] [:1 :5] [:2 :6] [:6 :1] [:5 :1] [:6 :2]}))

    ;; 2 rels \ rel + const
    (t/is (= (q/q db
                  '{:find [?e ?e2]
                    :where [[?e  :name "Ivan"]
                            [?e2 :name "Oleg"]
                            (not [?e :age 10]
                                 [?e2 :age 20])]})
             #{[:2 :3] [:1 :3] [:2 :4] [:6 :3] [:5 :3] [:6 :4]}))

    ;; 2 rels \ 2 consts
    (t/is (= (q/q db
                  '{:find [?e ?e2]
                    :where [[?e  :name "Oleg"]
                            [?e2 :name "Oleg"]
                            (not [?e :age 10]
                                 [?e2 :age 20])]})
             #{[:4 :3] [:3 :3] [:4 :4]}))))

(t/deftest datascript-test-or
  (populate-datascript-test-db)
  (let [db (q/db *kv*)]
    (t/are [q res]  (= (q/q db {:find '[?e] :where (quote q)})
                       (into #{} (map vector) res))

      ;; intersecting results
      [(or [?e :name "Oleg"]
           [?e :age 10])]
      #{:1 :3 :4 :5}

      ;; one branch empty
      [(or [?e :name "Oleg"]
           [?e :age 30])]
      #{:3 :4}

      ;; both empty
      [(or [?e :name "Petr"]
           [?e :age 30])]
      #{}

      ;; join with 1 var
      [[?e :name "Ivan"]
       (or [?e :name "Oleg"]
           [?e :age 10])]
      #{:1 :5}

      ;; join with 2 vars
      [[?e :age ?a]
       (or (and [?e :name "Ivan"]
                [:1  :age  ?a])
           (and [?e :name "Oleg"]
                [:2  :age  ?a]))]
      #{:1 :5 :4})))

(t/deftest datascript-test-or-join
  (populate-datascript-test-db)
  (let [db (q/db *kv*)]
    (t/are [q res] (= (q/q db {:find '[?e] :where (quote q)})
                      (into #{} (map vector) res))
      [(or-join [?e]
                [?e :name ?n]
                (and [?e :age ?a]
                     [?e :name ?n]))]
      #{:1 :2 :3 :4 :5 :6}

      [[?e  :name ?a]
       [?e2 :name ?a]
       (or-join [?e]
                (and [?e  :age ?a]
                     [?e2 :age ?a]))]
      #{:1 :2 :3 :4 :5 :6})))

(defn even-kw? [x]
  (even? (Long/parseLong (name x))))

(t/deftest test-rules
  (f/transact-entity-maps! f/*kv* [{:crux.db/id :5 :follow :3}
                                   {:crux.db/id :1 :follow :2}
                                   {:crux.db/id :2 :follow #{:3 :4}}
                                   {:crux.db/id :3 :follow :4}
                                   {:crux.db/id :4 :follow :6}])
  (let [db (q/db *kv*)]
    (t/is (= (q/q db
                  '{:find  [?e1 ?e2]
                    :where [(follow ?e1 ?e2)]
                    :rules [[(follow ?x ?y)
                             [?x :follow ?y]]]})
             #{[:1 :2] [:2 :3] [:3 :4] [:2 :4] [:5 :3] [:4 :6]}))

    ;; NOTE: Crux does not support vars in attribute position, so
    ;; :follow is explicit.
    (t/testing "Joining regular clauses with rule"
      (t/is (= (q/q db
                    '{:find [?y ?x]
                      :where [[_ :follow ?x]
                              (rule ?x ?y)
                              [(crux.query-test/even-kw? ?x)]]
                      :rules [[(rule ?a ?b)
                               [?a :follow ?b]]]})
               #{[:3 :2] [:6 :4] [:4 :2]})))

    ;; NOTE: Crux does not support vars in attribute position.
    #_(t/testing "Rule context is isolated from outer context"
        (t/is (= (q/q db
                      '{:find [?x]
                        :where [[?e _ _]
                                (rule ?x)]
                        :rules [[(rule ?e)
                                 [_ ?e _]]]})
                 #{[:follow]})))

    (t/testing "Rule with branches"
      (t/is (= (q/q db
                    '{:find [?e2]
                      :where [(follow ?e1 ?e2)]
                      :args [{:?e1 :1}]
                      :rules [[(follow ?e2 ?e1)
                               [?e2 :follow ?e1]]
                              [(follow ?e2 ?e1)
                               [?e2 :follow ?t]
                               [?t  :follow ?e1]]]})
               #{[:2] [:3] [:4]})))


    (t/testing "Recursive rules"
      (t/is (= (q/q db
                    '{:find  [?e2]
                      :where [(follow ?e1 ?e2)]
                      :args [{:?e1 :1}]
                      :rules [[(follow ?e1 ?e2)
                               [?e1 :follow ?e2]]
                              [(follow ?e1 ?e2)
                               [?e1 :follow ?t]
                               (follow ?t ?e2)]]})
               #{[:2] [:3] [:4] [:6]}))

      (f/with-kv-store
        (fn []
          (f/transact-entity-maps! f/*kv* [{:crux.db/id :1 :follow :2}
                                           {:crux.db/id :2 :follow :3}])
          (let [db (q/db *kv*)]
            (t/is (= (q/q db
                          '{:find [?e1 ?e2]
                            :where [(follow ?e1 ?e2)]
                            :rules [[(follow ?e1 ?e2)
                                     [?e1 :follow ?e2]]
                                    [(follow ?e1 ?e2)
                                     (follow ?e2 ?e1)]]})
                     #{[:1 :2] [:2 :3] [:2 :1] [:3 :2]})))))

      (f/with-kv-store
        (fn []
          (f/transact-entity-maps! f/*kv* [{:crux.db/id :1 :follow :2}
                                           {:crux.db/id :2 :follow :3}
                                           {:crux.db/id :3 :follow :1}])
          (let [db (q/db *kv*)]
            (t/is (= (q/q db
                          '{:find [?e1 ?e2]
                            :where [(follow ?e1 ?e2)]
                            :rules [[(follow ?e1 ?e2)
                                     [?e1 :follow ?e2]]
                                    [(follow ?e1 ?e2)
                                     (follow ?e2 ?e1)]]})
                     #{[:1 :2] [:2 :3] [:3 :1] [:2 :1] [:3 :2] [:1 :3]}))))))

    (t/testing "Mutually recursive rules"
      (f/with-kv-store
        (fn []
          (f/transact-entity-maps! f/*kv* [{:crux.db/id :0 :f1 :1}
                                           {:crux.db/id :1 :f2 :2}
                                           {:crux.db/id :2 :f1 :3}
                                           {:crux.db/id :3 :f2 :4}
                                           {:crux.db/id :4 :f1 :5}
                                           {:crux.db/id :5 :f2 :6}])
          (let [db (q/db *kv*)]
            (t/is (= (q/q db
                          '{:find [?e1 ?e2]
                            :where [(f1 ?e1 ?e2)]
                            :rules [[(f1 ?e1 ?e2)
                                     [?e1 :f1 ?e2]]
                                    [(f1 ?e1 ?e2)
                                     [?t :f1 ?e2]
                                     (f2 ?e1 ?t)]
                                    [(f2 ?e1 ?e2)
                                     [?e1 :f2 ?e2]]
                                    [(f2 ?e1 ?e2)
                                     [?t :f2 ?e2]
                                     (f1 ?e1 ?t)]]})
                     #{[:0 :1] [:0 :3] [:0 :5]
                       [:1 :3] [:1 :5]
                       [:2 :3] [:2 :5]
                       [:3 :5]
                       [:4 :5]}))))))

    (t/testing "Passing ins to rule"
      (t/is (= (q/q db
                    {:find '[?x ?y]
                     :where '[(match ?even ?x ?y)]
                     :rules '[[(match ?pred ?e ?e2)
                               [?e :follow ?e2]
                               [(?pred ?e)]
                               [(?pred ?e2)]]]
                     :args [{:?even even-kw?}]})
               #{[:4 :6] [:2 :4]})))

    (t/testing "Using built-ins inside rule"
      (t/is (= (q/q db
                    '{:find [?x ?y]
                      :where [(match ?x ?y)]
                      :rules [[(match ?e ?e2)
                               [?e :follow ?e2]
                               [(crux.query-test/even-kw? ?e)]
                               [(crux.query-test/even-kw? ?e2)]]]})
               #{[:4 :6] [:2 :4]})))

    (t/testing "Calling rule twice (#44)"
      (f/with-kv-store
        (fn []
          (f/transact-entity-maps! f/*kv* [{:crux.db/id :1 :attr "a"}])
          (let [db (q/db *kv*)]
            (q/q db
                 {:find '[?p]
                  :where '[(rule ?p ?fn "a")
                           (rule ?p ?fn "b")]
                  :rules '[[(rule ?p ?fn ?x)
                            [?p :attr ?x]
                            [(?fn ?x)]]]
                  :args [{:?fn (constantly true)}]})))))))

;; https://github.com/tonsky/datascript/issues/218
(t/deftest datascript-test-rules-false-arguments
  (f/transact-entity-maps! f/*kv* [{:crux.db/id :1 :attr true}
                                   {:crux.db/id :2 :attr false}])
  (let [db (q/db *kv*)
        rules '[[(is ?id ?val)
                 [?id :attr ?val]]]]
    (t/is (= (q/q db
                  {:find '[?id]
                   :where '[(is ?id true)]
                   :rules rules})
             #{[:1]}))
    (t/is (= (q/q db
                  {:find '[?id]
                   :where '[(is ?id false)]
                   :rules rules})
             #{[:2]}))))

(defn- even-or-nil? [x]
  (when (even? x)
    x))

(t/deftest data-script-test-query-fns
  (f/transact-entity-maps! f/*kv* [{:crux.db/id :1 :name "Ivan" :age 15}
                                   {:crux.db/id :2 :name "Petr" :age 22 :height 240 :parent :1}
                                   {:crux.db/id :3 :name "Slava" :age 37 :parent :2}])
  (let [db (q/db *kv*)]
    (t/testing "predicate without free variables"
      (t/is (= (q/q db
                    '{:find [?x]
                      :args [{:?x :a}
                             {:?x :b}
                             {:?x :c}]
                      :where [[(> 2 1)]]})
               #{[:a] [:b] [:c]})))

    ;; NOTE: Crux does not support these functions.
    #_(t/testing "ground"
        (t/is (= (d/q '[:find ?vowel
                        :where [(ground [:a :e :i :o :u]) [?vowel ...]]])
                 #{[:a] [:e] [:i] [:o] [:u]})))

    #_(t/testing "get-else"
        (t/is (= (d/q '[:find ?e ?age ?height
                        :where [?e :age ?age]
                        [(get-else $ ?e :height 300) ?height]] db)
                 #{[1 15 300] [2 22 240] [3 37 300]}))

        (t/is (thrown-with-msg? ExceptionInfo #"get-else: nil default value is not supported"
                                (d/q '[:find ?e ?height
                                       :where [?e :age]
                                       [(get-else $ ?e :height nil) ?height]] db))))

    #_(t/testing "get-some"
        (t/is (= (d/q '[:find ?e ?a ?v
                        :where [?e :name _]
                        [(get-some $ ?e :height :age) [?a ?v]]] db)
                 #{[1 :age 15]
                   [2 :height 240]
                   [3 :age 37]})))

    #_(t/testing "missing?"
        (t/is (= (q/q '[:find ?e ?age
                        :in $
                        :where [?e :age ?age]
                        [(missing? $ ?e :height)]] db)
                 #{[1 15] [3 37]})))

    #_(t/testing "missing? back-ref"
        (t/is (= (q/q '[:find ?e
                        :in $
                        :where [?e :age ?age]
                        [(missing? $ ?e :_parent)]] db)
                 #{[3]})))

    (t/testing "Built-ins"
      (t/is (= (q/q db
                    '{:find [?e1 ?e2]
                      :where [[?e1 :age ?a1]
                              [?e2 :age ?a2]
                              [(< ?a1 18 ?a2)]]})
               #{[:1 :2] [:1 :3]}))

      (t/is (= (q/q db
                    '{:find [?x ?c]
                      :args [{:?x "a"}
                             {:?x "abc"}]
                      :where [[(count ?x) ?c]]})
               #{["a" 1] ["abc" 3]})))

    (t/testing "Built-in vector, hashmap"
      (t/is (= (q/q db
                    '{:find [?tx-data]
                      :where [[(identity :db/add) ?op]
                              [(vector ?op -1 :attr 12) ?tx-data]]})
               #{[[:db/add -1 :attr 12]]}))

      (t/is (= (q/q db
                    '{:find [?tx-data]
                      :where
                      [[(hash-map :db/id -1 :age 92 :name "Aaron") ?tx-data]]})
               #{[{:db/id -1 :age 92 :name "Aaron"}]})))


    (t/testing "Passing predicate as source"
      (t/is (= (q/q db
                    {:find '[?e]
                     :where '[[?e :age ?a]
                              [(?adult ?a)]]
                     :args [{:?adult #(> % 18)}]})
               #{[:2] [:3]})))

    (t/testing "Calling a function"
      (t/is (= (q/q db
                    '{:find [?e1 ?e2 ?e3]
                      :where [[?e1 :age ?a1]
                              [?e2 :age ?a2]
                              [?e3 :age ?a3]
                              [(+ ?a1 ?a2) ?a12]
                              [(= ?a12 ?a3)]]})
               #{[:1 :2 :3] [:2 :1 :3]})))

    (t/testing "Two conflicting function values for one binding."
      (t/is (= (q/q db
                    '{:find [?n]
                      :where [[(identity 1) ?n]
                              [(identity 2) ?n]]})
               #{})))

    ;; NOTE: Crux does not currently support destructuring.
    #_(t/testing "Destructured conflicting function values for two bindings."
        (t/is (= (d/q '[:find  ?n ?x
                        :where [(identity [3 4]) [?n ?x]]
                        [(identity [1 2]) [?n ?x]]]
                      db)
                 #{})))

    (t/testing "Rule bindings interacting with function binding. (fn, rule)"
      (t/is (= (q/q db
                    '{:find [?n]
                      :where [[(identity 2) ?n]
                              (my-vals ?n)]
                      :rules [[(my-vals ?x)
                               [(identity 1) ?x]]
                              [(my-vals ?x)
                               [(identity 2) ?x]]
                              [(my-vals ?x)
                               [(identity 3) ?x]]]})
               #{[2]})))

    (t/testing "Rule bindings interacting with function binding. (rule, fn)"
      (t/is (= (q/q db
                    '{:find [?n]
                      :where [(my-vals ?n)
                              [(identity 2) ?n]]
                      :rules [[(my-vals ?x)
                               [(identity 1) ?x]]
                              [(my-vals ?x)
                               [(identity 2) ?x]]
                              [(my-vals ?x)
                               [(identity 3) ?x]]]})
               #{[2]})))

    (t/testing "Conflicting relational bindings with function binding. (rel, fn)"
      (t/is (= (q/q db
                    '{:find [?age]
                      :where [[_ :age ?age]
                              [(identity 100) ?age]]})
               #{})))

    (t/testing "Conflicting relational bindings with function binding. (fn, rel)"
      (t/is (= (q/q db
                    '{:find [?age]
                      :where [[(identity 100) ?age]
                              [_ :age ?age]]})
               #{})))

    (t/testing "Function on empty rel"
      (t/is (= (q/q db
                    '{:find [?e ?y]
                      :where [[?e :salary ?x]
                              [(+ ?x 100) ?y]
                              [:0 :age 15]
                              [:1 :age 35]]})
               #{})))

    (t/testing "Returning nil from function filters out tuple from result"
      (t/is (= (q/q db
                    {:find '[?x]
                     :where '[[(crux.query-test/even-or-nil? ?in) ?x]]
                     :args [{:?in 1}
                            {:?in 2}
                            {:?in 3}
                            {:?in 4}]})
               #{[2] [4]})))

    ;; NOTE: Crux does not currently support destructuring.
    #_(t/testing "Result bindings"
        (t/is (= (d/q '[:find ?a ?c
                        :in ?in
                        :where [(ground ?in) [?a _ ?c]]]
                      [:a :b :c])
                 #{[:a :c]}))

        (t/is (= (d/q '[:find ?in
                        :in ?in
                        :where [(ground ?in) _]]
                      :a)
                 #{[:a]}))

        (t/is (= (d/q '[:find ?x ?z
                        :in ?in
                        :where [(ground ?in) [[?x _ ?z]...]]]
                      [[:a :b :c] [:d :e :f]])
                 #{[:a :c] [:d :f]}))

        (t/is (= (d/q '[:find ?in
                        :in [?in ...]
                        :where [(ground ?in) _]]
                      [])
                 #{})))))


(defn kw-less-than? [x y]
  (< (Long/parseLong (name x))
     (Long/parseLong (name y))))

(t/deftest datascript-test-predicates
  (f/transact-entity-maps! f/*kv* [{:crux.db/id :1 :name "Ivan" :age 10}
                                   {:crux.db/id :2 :name "Ivan" :age 20}
                                   {:crux.db/id :3 :name "Oleg" :age 10}
                                   {:crux.db/id :4 :name "Oleg" :age 20}])
  (let [db (q/db *kv*)]
    (t/are [q res] (= (q/q db (quote q)) res)
      ;; plain predicate
      {:find [?e ?a]
       :where [[?e :age ?a]
               [(> ?a 10)]]}
      #{[:2 20] [:4 20]}

      ;; join in predicate
      {:find [?e ?e2]
       :where [[?e  :name]
               [?e2 :name]
               [(crux.query-test/kw-less-than? ?e ?e2)]]}
      #{[:1 :2] [:1 :3] [:1 :4] [:2 :3] [:2 :4] [:3 :4]}

      ;; join with extra symbols
      {:find [?e ?e2]
       :where [[?e  :age ?a]
               [?e2 :age ?a2]
               [(crux.query-test/kw-less-than? ?e ?e2)]]}
      #{[:1 :2] [:1 :3] [:1 :4] [:2 :3] [:2 :4] [:3 :4]}

      ;; empty result
      {:find [?e ?e2]
       :where [[?e  :name "Ivan"]
               [?e2 :name "Oleg"]
               [(= ?e ?e2)]]}
      #{}

      ;; pred over const, true
      {:find [?e]
       :where [[?e :name "Ivan"]
               [?e :age 20]
               [(= ?e :2)]]}
      #{[:2]}

      ;; pred over const, false
      {:find [?e]
       :where [[?e :name "Ivan"]
               [?e :age 20]
               [(= ?e :1)]]}
      #{})

    ;; NOTE: Crux does not support source vars.
    #_(let [pred (fn [db e a]
                   (= a (:age (d/entity db e))))]
        (t/is (= (q/q '[:find ?e
                        :in $ ?pred
                        :where [?e :age ?a]
                        [(?pred $ ?e 10)]]
                      db pred)
                 #{[1] [3]})))))


(t/deftest datascript-test-issue-180
  (f/transact-entity-maps! f/*kv* [{:crux.db/id :1 :age 20}])
  (let [db (q/db *kv*)]
    (t/is (= #{}
             (q/q db
                  '{:find [?e ?a]
                    :where [[_ :pred ?pred]
                            [?e :age ?a]
                            [(?pred ?a)]]})))))

(defn sample-query-fn [] 42)

(t/deftest datascript-test-symbol-resolution
  (let [db (q/db *kv*)]
    (t/is (= #{[42]} (q/q db
                          '{:find [?x]
                            :where [[(crux.query-test/sample-query-fn) ?x]]})))))

;; Tests from Racket Datalog
;; https://github.com/racket/datalog/tree/master/tests/examples

(t/deftest test-racket-datalog-tutorial
  ;; parent(john,douglas).
  (f/transact-entity-maps! f/*kv* [{:crux.db/id :john :parent :douglas}])
  ;; parent(john,douglas)?
  (t/is (= #{[true]}
           (q/q (q/db *kv*)
                '{:find [found]
                  :where [[:john :parent :douglas]
                          [(identity true) found]]})))

  ;; parent(john,ebbon)?
  (t/is (= #{}
           (q/q (q/db *kv*)
                '{:find [found]
                  :where [[:john :parent :ebbon]
                          [(identity true) found]]})))

  ;; parent(bob,john).
  ;; parent(ebbon,bob).
  (f/transact-entity-maps! f/*kv* [{:crux.db/id :bob :parent :john}
                                   {:crux.db/id :ebbon :parent :bob}])

  ;; parent(A,B)?
  (t/is (= #{[:john :douglas]
             [:bob :john]
             [:ebbon :bob]}
           (q/q (q/db *kv*)
                '{:find [a b]
                  :where [[a :parent b]]})))

  ;; parent(john,B)?
  (t/is (= #{[:douglas]}
           (q/q (q/db *kv*)
                '{:find [ b]
                  :where [[:john :parent b]]})))

  ;; parent(A,A)?
  (t/is (= #{}
           (q/q (q/db *kv*)
                '{:find [a]
                  :where [[a :parent a]]})))

  ;; ancestor(A,B) :- parent(A,B).
  ;; ancestor(A,B) :- parent(A,C), ancestor(C, B).
  ;; ancestor(A, B)?
  (t/is (= #{[:ebbon :bob]
             [:bob :john]
             [:john :douglas]
             [:bob :douglas]
             [:ebbon :john]
             [:ebbon :douglas]}
           (q/q (q/db *kv*)
                '{:find [a b]
                  :where [(ancestor a b)]
                  :rules [[(ancestor a b)
                           [a :parent b]]
                          [(ancestor a b)
                           [a :parent c]
                           (ancestor c b)]]})))

  ;; ancestor(X,john)?
  (t/is (= #{[:bob]
             [:ebbon]}
           (q/q (q/db *kv*)
                '{:find [x]
                  :where [(ancestor x :john)]
                  :rules [[(ancestor a b)
                           [a :parent b]]
                          [(ancestor a b)
                           [a :parent c]
                           (ancestor c b)]]})))

  (let [db-before (q/db *kv*)]
    ;; parent(bob, john)-
    (f/delete-entities! f/*kv* [:bob])
    ;; parent(A,B)?
    (t/is (= #{[:john :douglas]
               [:ebbon :bob]}
             (q/q (q/db *kv*)
                  '{:find [a b]
                    :where [[a :parent b]]})))

    ;; ancestor(A,B)?
    (t/is (= #{[:ebbon :bob]
               [:john :douglas]}
             (q/q (q/db *kv*)
                  '{:find [a b]
                    :where [(ancestor a b)]
                    :rules [[(ancestor a b)
                             [a :parent b]]
                            [(ancestor a b)
                             [a :parent c]
                             (ancestor c b)]]})))

    (t/testing "can query previous state"
      (t/is (= #{[:ebbon :bob]
                 [:bob :john]
                 [:john :douglas]
                 [:bob :douglas]
                 [:ebbon :john]
                 [:ebbon :douglas]}
               (q/q db-before
                    '{:find [a b]
                      :where [(ancestor a b)]
                      :rules [[(ancestor a b)
                               [a :parent b]]
                              [(ancestor a b)
                               [a :parent c]
                               (ancestor c b)]]}))))))

(t/deftest test-racket-datalog-path
  ;; edge(a, b). edge(b, c). edge(c, d). edge(d, a).
  (f/transact-entity-maps! f/*kv* [{:crux.db/id :a :edge :b}
                                   {:crux.db/id :b :edge :c}
                                   {:crux.db/id :c :edge :d}
                                   {:crux.db/id :d :edge :a}])

  ;; path(X, Y) :- edge(X, Y).
  ;; path(X, Y) :- edge(X, Z), path(Z, Y).
  ;; path(X, Y)?
  (t/is (= #{[:a :a]
             [:a :d]
             [:a :c]
             [:a :b]
             [:b :a]
             [:b :d]
             [:b :c]
             [:b :b]
             [:c :a]
             [:c :d]
             [:c :c]
             [:c :b]
             [:d :b]
             [:d :c]
             [:d :d]
             [:d :a]}
           (q/q (q/db *kv*)
                '{:find [x y]
                  :where [(path x y)]
                  :rules [[(path x y)
                           [x :edge y]]
                          [(path x y)
                           [x :edge z]
                           (path z y)]]}))))

(t/deftest test-racket-datalog-revpath
  ;; edge(a, b). edge(b, c). edge(c, d). edge(d, a).
  (f/transact-entity-maps! f/*kv* [{:crux.db/id :a :edge :b}
                                   {:crux.db/id :b :edge :c}
                                   {:crux.db/id :c :edge :d}
                                   {:crux.db/id :d :edge :a}])
  ;; path(X, Y) :- edge(X, Y).
  ;; path(X, Y) :- path(X, Z), edge(Z, Y).
  ;; path(X, Y)?
  (t/is (= #{[:a :a]
             [:a :d]
             [:a :c]
             [:a :b]
             [:b :a]
             [:b :d]
             [:b :c]
             [:b :b]
             [:c :a]
             [:c :d]
             [:c :c]
             [:c :b]
             [:d :b]
             [:d :c]
             [:d :d]
             [:d :a]}
           (q/q (q/db *kv*)
                '{:find [x y]
                  :where [(path x y)]
                  :rules [[(path x y)
                           [x :edge y]]
                          [(path x y)
                           (path x z)
                           [z :edge y]]]}))))

(t/deftest test-racket-datalog-bidipath
  ;; edge(a, b). edge(b, c). edge(c, d). edge(d, a).
  (f/transact-entity-maps! f/*kv* [{:crux.db/id :a :edge :b}
                                   {:crux.db/id :b :edge :c}
                                   {:crux.db/id :c :edge :d}
                                   {:crux.db/id :d :edge :a}])

  ;; path(X, Y) :- edge(X, Y).
  ;; path(X, Y) :- edge(X, Z), path(Z, Y).
  ;; path(X, Y) :- path(X, Z), edge(Z, Y).
  ;; path(X, Y)?
  (t/is (= #{[:a :a]
             [:a :d]
             [:a :c]
             [:a :b]
             [:b :a]
             [:b :d]
             [:b :c]
             [:b :b]
             [:c :a]
             [:c :d]
             [:c :c]
             [:c :b]
             [:d :b]
             [:d :c]
             [:d :d]
             [:d :a]}
           (q/q (q/db *kv*)
                '{:find [x y]
                  :where [(path x y)]
                  :rules [[(path x y)
                           [x :edge y]]
                          [(path x y)
                           (path x z)
                           [z :edge y]]
                          [(path x y)
                           (path x z)
                           [z :edge y]]]}))))


(t/deftest test-racket-datalog-sym
  ;; sym(a).
  ;; sym(b).
  ;; sym(c).
  (f/transact-entity-maps! f/*kv* [{:crux.db/id :a}
                                   {:crux.db/id :b}
                                   {:crux.db/id :c}])

  ;; perm(X,Y) :- sym(X), sym(Y), X != Y.
  ;; perm(X, Y)?
  (t/is (= #{[:a :c]
             [:a :b]
             [:c :a]
             [:b :a]
             [:b :c]
             [:c :b]}
           (q/q (q/db *kv*)
                '{:find [x y]
                  :where [(perm x y)]
                  :rules [[(perm x y)
                           [x :crux.db/id]
                           [y :crux.db/id]
                           [(!= x y)]]]}))))

;; Tests from
;; https://pdfs.semanticscholar.org/9374/f0da312f3ba77fa840071d68935a28cba364.pdf

(t/deftest test-datalog-paper-sgc
  (f/transact-entity-maps! f/*kv* [{:crux.db/id :ann :parent #{:dorothy :hilary}}
                                   {:crux.db/id :bertrand :parent :dorothy}
                                   {:crux.db/id :charles :parent :evelyn}
                                   {:crux.db/id :dorothy :parent :george}
                                   {:crux.db/id :evelyn :parent :george}
                                   {:crux.db/id :fred}
                                   {:crux.db/id :george}
                                   {:crux.db/id :hilary}])

  ;; rl: sgc(X, X) :- person(X).
  ;; r2: sgc(X, Y) :- par(X, X1), sgc(X1, Y1), par(Y, Y1).
  (t/is (= #{[:ann :ann]
             [:bertrand :bertrand]
             [:charles :charles]
             [:dorothy :dorothy]
             [:evelyn :evelyn]
             [:fred :fred]
             [:george :george]
             [:hilary :hilary]
             [:dorothy :evelyn]
             [:evelyn :dorothy]
             [:charles :ann]
             [:ann :charles]
             [:ann :bertrand]
             [:bertrand :ann]
             [:charles :bertrand]
             [:bertrand :charles]}
           (q/q (q/db *kv*)
                '{:find [x y]
                  :where [(sgc x y)]
                  :rules [[(sgc x y)
                           [x :crux.db/id y]]
                          [(sgc x y)
                           [x :parent x1]
                           (sgc x1 y1)
                           [y :parent y1]]]}))))

(t/deftest test-datalog-paper-stratified-datalog
  ;; d(a, b), d(b, c) d(e, e)
  (f/transact-entity-maps! f/*kv* [{:crux.db/id :a :d :b}
                                   {:crux.db/id :b :d :c}
                                   {:crux.db/id :e :d :e}])

  ;; rl: p(X, Y) :- not q(X, Y), s(X, Y).
  ;; r2: q(K, Y) :- q(X, Z), q(Z, Y).
  ;; r3: q(X, Y) :- d(X, Y), not r(X, Y).
  ;; r4: r(X, Y) :- d(Y, X).
  ;; r5: s(X, Y) :- q(X, Z), q(Y, T), X != Y
  (let [rules '[[(p x y)
                 (not (q x y))
                 (s x y)]
                [(q x y)
                 (q x z)
                 (q z y)]
                [(q x y)
                 [x :d y]
                 (not (r x y))]
                [(r x y)
                 [y :d x]]
                [(s x y)
                 (q x z)
                 (q y t)
                 [(!= x y)]]]]

    (t/testing "stratum 1"
      (t/is (= #{[:b :a]
                 [:c :b]
                 [:e :e]}
               (q/q (q/db *kv*)
                    {:find '[x y]
                     :where '[(r x y)]
                     :rules rules}))))

    (t/testing "stratum 2"
      (t/is (= #{[:a :b]
                 [:b :c]
                 [:a :c]
                 [:b :a]}
               (q/q (q/db *kv*)
                    {:find '[x y]
                     :where '[(or (q x y)
                                  (s x y))]
                     :rules rules}))))

    (t/testing "stratum 3"
      (t/is (= #{[:b :a]}
               (q/q (q/db *kv*)
                    {:find '[x y]
                     :where '[(p x y)]
                     :rules rules}))))))
