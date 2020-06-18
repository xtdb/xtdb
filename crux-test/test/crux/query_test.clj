(ns crux.query-test
  (:require [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [clojure.walk :as w]
            [clojure.test :as t]
            [crux.api :as api]
            [crux.codec :as c]
            [crux.db :as db]
            [crux.fixtures :as fix :refer [*api*]]
            [crux.query :as q])
  (:import java.util.UUID))

(t/use-fixtures :each fix/with-kv-dir fix/with-standalone-topology fix/with-node)

(t/deftest test-sanity-check
  (fix/transact! *api* (fix/people [{:name "Ivan"}]))
  (t/is (first (api/q (api/db *api*) '{:find [e]
                                       :where [[e :name "Ivan"]]}))))

(t/deftest test-basic-query
  (fix/transact! *api* (fix/people [{:crux.db/id :ivan :name "Ivan" :last-name "Ivanov"}
                                    {:crux.db/id :petr :name "Petr" :last-name "Petrov"}]))

  (t/testing "Can query value by single field"
    (t/is (= #{["Ivan"]} (api/q (api/db *api*) '{:find [name]
                                                 :where [[e :name "Ivan"]
                                                         [e :name name]]})))
    (t/is (= #{["Petr"]} (api/q (api/db *api*) '{:find [name]
                                                 :where [[e :name "Petr"]
                                                         [e :name name]]}))))

  (t/testing "Can query entity by single field"
    (t/is (= #{[:ivan]} (api/q (api/db *api*) '{:find [e]
                                                :where [[e :name "Ivan"]]})))
    (t/is (= #{[:petr]} (api/q (api/db *api*) '{:find [e]
                                                :where [[e :name "Petr"]]}))))

  (t/testing "Can query using multiple terms"
    (t/is (= #{["Ivan" "Ivanov"]} (api/q (api/db *api*) '{:find [name last-name]
                                                          :where [[e :name name]
                                                                  [e :last-name last-name]
                                                                  [e :name "Ivan"]
                                                                  [e :last-name "Ivanov"]]}))))

  (t/testing "Negate query based on subsequent non-matching clause"
    (t/is (= #{} (api/q (api/db *api*) '{:find [e]
                                         :where [[e :name "Ivan"]
                                                 [e :last-name "Ivanov-does-not-match"]]}))))

  (t/testing "Can query for multiple results"
    (t/is (= #{["Ivan"] ["Petr"]}
             (api/q (api/db *api*) '{:find [name] :where [[e :name name]]}))))


  (fix/transact! *api* (fix/people [{:crux.db/id :smith :name "Smith" :last-name "Smith"}]))
  (t/testing "Can query across fields for same value"
    (t/is (= #{[:smith]}
             (api/q (api/db *api*) '{:find [p1] :where [[p1 :name name]
                                                        [p1 :last-name name]]}))))

  (t/testing "Can query across fields for same value when value is passed in"
    (t/is (= #{[:smith]}
             (api/q (api/db *api*) '{:find [p1] :where [[p1 :name name]
                                                        [p1 :last-name name]
                                                        [p1 :name "Smith"]]})))))

(t/deftest test-basic-query-returning-full-results
  (fix/transact! *api* [{:crux.db/id :ivan :name "Ivan" :last-name "Ivanov"}])

  (t/testing "Can retrieve full results"
    (t/is (= [{:crux.db/id :ivan :name "Ivan" :last-name "Ivanov"}
              "Ivan"] (first (api/q (api/db *api*)
                                    '{:find [e first-name]
                                      :where [[e :name first-name]]
                                      :full-results? true})))))

  (t/testing "Can retrieve full for a vector-style query"
    (t/is (= [{:crux.db/id :ivan :name "Ivan" :last-name "Ivanov"}
              "Ivan"] (first (api/q (api/db *api*)
                                    '[:find e first-name
                                      :where [e :name first-name]
                                      :full-results? true])))))

  (t/testing "Can retrieve full results in or-join"
    (t/is (= [{:crux.db/id :ivan :name "Ivan" :last-name "Ivanov"}
              "Ivan"] (first (api/q (api/db *api*)
                                    '{:find [e first-name]
                                      :where [(or-join [e first-name]
                                                       [e :name first-name])]
                                      :full-results? true})))))

  (t/testing "Can retrieve full results in or"
    (t/is (= [{:crux.db/id :ivan :name "Ivan" :last-name "Ivanov"}
              "Ivan"] (first (api/q (api/db *api*)
                                    '{:find [e first-name]
                                      :where [(or [e :name first-name])]
                                      :rules [[(my-rule e first-name)
                                               (or [e :name first-name])]]
                                      :full-results? true})))))

  (t/testing "Can retrieve full results in rule"
    (t/is (= [{:crux.db/id :ivan :name "Ivan" :last-name "Ivanov"}
              "Ivan"] (first (api/q (api/db *api*)
                                    '{:find [e first-name]
                                      :where [(my-rule e first-name)]
                                      :rules [[(my-rule e first-name)
                                               [e :name first-name]]]
                                      :full-results? true}))))))

(t/deftest test-query-with-arguments
  (let [[ivan petr] (fix/transact! *api* (fix/people [{:name "Ivan" :last-name "Ivanov"}
                                                      {:name "Petr" :last-name "Petrov"}]))]

    (t/testing "Can query entity by single field"
      (t/is (= #{[(:crux.db/id ivan)]} (api/q (api/db *api*) '{:find [e]
                                                               :where [[e :name name]]
                                                               :args [{:name "Ivan"}]})))
      (t/is (= #{[(:crux.db/id petr)]} (api/q (api/db *api*) '{:find [e]
                                                               :where [[e :name name]]
                                                               :args [{:name "Petr"}]}))))

    (t/testing "Can query entity by entity position"
      (t/is (= #{["Ivan"]
                 ["Petr"]} (api/q (api/db *api*) {:find '[name]
                                                  :where '[[e :name name]]
                                                  :args [{:e (:crux.db/id ivan)}
                                                         {:e (:crux.db/id petr)}]})))

      (t/is (= #{["Ivan" "Ivanov"]
                 ["Petr" "Petrov"]} (api/q (api/db *api*) {:find '[name last-name]
                                                           :where '[[e :name name]
                                                                    [e :last-name last-name]]
                                                           :args [{:e (:crux.db/id ivan)}
                                                                  {:e (:crux.db/id petr)}]}))))

    (t/testing "Can match on both entity and value position"
      (t/is (= #{["Ivan"]} (api/q (api/db *api*) {:find '[name]
                                                  :where '[[e :name name]]
                                                  :args [{:e (:crux.db/id ivan)
                                                          :name "Ivan"}]})))

      (t/is (= #{} (api/q (api/db *api*) {:find '[name]
                                          :where '[[e :name name]]
                                          :args [{:e (:crux.db/id ivan)
                                                  :name "Petr"}]}))))

    (t/testing "Can query entity by single field with several arguments"
      (t/is (= #{[(:crux.db/id ivan)]
                 [(:crux.db/id petr)]} (api/q (api/db *api*) '{:find [e]
                                                               :where [[e :name name]]
                                                               :args [{:name "Ivan"}
                                                                      {:name "Petr"}]}))))

    (t/testing "Can query entity by single field with literals"
      (t/is (= #{[(:crux.db/id ivan)]} (api/q (api/db *api*) '{:find [e]
                                                               :where [[e :name name]
                                                                       [e :last-name "Ivanov"]]
                                                               :args [{:name "Ivan"}
                                                                      {:name "Petr"}]})))

      (t/is (= #{["Ivan"]} (api/q (api/db *api*) {:find '[name]
                                                  :where '[[e :name name]
                                                           [e :last-name "Ivanov"]]
                                                  :args [{:e (:crux.db/id ivan)}
                                                         {:e (:crux.db/id petr)}]}))))

    (t/testing "Can query entity by non existent argument"
      (t/is (= #{} (api/q (api/db *api*) '{:find [e]
                                           :where [[e :name name]]
                                           :args [{:name "Bob"}]}))))

    (t/testing "Can query entity with empty arguments"
      (t/is (= #{[(:crux.db/id ivan)]
                 [(:crux.db/id petr)]} (api/q (api/db *api*) '{:find [e]
                                                               :where [[e :name name]]
                                                               :args []}))))

    (t/testing "Can query entity with tuple arguments"
      (t/is (= #{[(:crux.db/id ivan)]
                 [(:crux.db/id petr)]} (api/q (api/db *api*) '{:find [e]
                                                               :where [[e :name name]
                                                                       [e :last-name last-name]]
                                                               :args [{:name "Ivan" :last-name "Ivanov"}
                                                                      {:name "Petr" :last-name "Petrov"}]}))))

    (t/testing "Can query predicates based on arguments alone"
      (t/is (= #{["Ivan"]} (api/q (api/db *api*) '{:find [name]
                                                   :where [[(re-find #"I" name)]]
                                                   :args [{:name "Ivan"}
                                                          {:name "Petr"}]})))

      (t/is (= #{["Ivan"]} (api/q (api/db *api*) '{:find [name]
                                                   :where [[(re-find #"I" name)]
                                                           [(= last-name "Ivanov")]]
                                                   :args [{:name "Ivan" :last-name "Ivanov"}
                                                          {:name "Petr" :last-name "Petrov"}]})))

      (t/is (= #{["Ivan"]
                 ["Petr"]} (api/q (api/db *api*) '{:find [name]
                                                   :where [[(string? name)]]
                                                   :args [{:name "Ivan"}
                                                          {:name "Petr"}]})))

      (t/is (= #{["Ivan" "Ivanov"]
                 ["Petr" "Petrov"]} (api/q (api/db *api*) '{:find [name
                                                                   last-name]
                                                            :where [[(not= last-name name)]]
                                                            :args [{:name "Ivan" :last-name "Ivanov"}
                                                                   {:name "Petr" :last-name "Petrov"}]})))

      (t/is (= #{["Ivan"]} (api/q (api/db *api*) '{:find [name]
                                                   :where [[(string? name)]
                                                           [(re-find #"I" name)]]
                                                   :args [{:name "Ivan"}
                                                          {:name "Petr"}]})))

      (t/is (= #{} (api/q (api/db *api*) '{:find [name]
                                           :where [[(number? name)]]
                                           :args [{:name "Ivan"}
                                                  {:name "Petr"}]})))

      (t/is (= #{} (api/q (api/db *api*) '{:find [name]
                                           :where [(not [(string? name)])]
                                           :args [{:name "Ivan"}
                                                  {:name "Petr"}]})))

      (t/testing "Can use range constraints on arguments"
        (t/is (= #{} (api/q (api/db *api*) '{:find [age]
                                             :where [[(>= age 21)]]
                                             :args [{:age 20}]})))

        (t/is (= #{[22]} (api/q (api/db *api*) '{:find [age]
                                                 :where [[(>= age 21)]]
                                                 :args [{:age 22}]})))))))

(t/deftest test-multiple-results
  (fix/transact! *api* (fix/people [{:name "Ivan" :last-name "1"}
                                    {:name "Ivan" :last-name "2"}]))
  (t/is (= 2
           (count (api/q (api/db *api*) '{:find [e] :where [[e :name "Ivan"]]})))))

(t/deftest test-query-using-keywords
  (fix/transact! *api* (fix/people [{:name "Ivan" :sex :male}
                                    {:name "Petr" :sex :male}
                                    {:name "Doris" :sex :female}
                                    {:name "Jane" :sex :female}]))

  (t/testing "Can query by single field"
    (t/is (= #{["Ivan"] ["Petr"]} (api/q (api/db *api*) '{:find [name]
                                                          :where [[e :name name]
                                                                  [e :sex :male]]})))
    (t/is (= #{["Doris"] ["Jane"]} (api/q (api/db *api*) '{:find [name]
                                                           :where [[e :name name]
                                                                   [e :sex :female]]})))))

(t/deftest test-basic-query-at-t
  (let [[malcolm] (fix/transact! *api* (fix/people [{:crux.db/id :malcolm :name "Malcolm" :last-name "Sparks"}])
                                 #inst "1986-10-22")]
    (fix/transact! *api* (fix/people [{:crux.db/id :malcolm :name "Malcolma" :last-name "Sparks"}]) #inst "1986-10-24")
    (let [q '{:find [e]
              :where [[e :name "Malcolma"]
                      [e :last-name "Sparks"]]}]
      (t/is (= #{} (api/q (api/db *api* #inst "1986-10-23")
                          q)))
      (t/is (= #{[(:crux.db/id malcolm)]} (api/q (api/db *api*) q))))))

(t/deftest test-query-across-entities-using-join
  ;; Five people, two of which share the same name:
  (fix/transact! *api* (fix/people [{:name "Ivan"} {:name "Petr"} {:name "Sergei"} {:name "Denis"} {:name "Denis"}]))

  (t/testing "Five people, without a join"
    (t/is (= 5 (count (api/q (api/db *api*) '{:find [p1]
                                              :where [[p1 :name name]
                                                      [p1 :age age]
                                                      [p1 :salary salary]]})))))

  (t/testing "Five people, a cartesian product - joining without unification"
    (t/is (= 25 (count (api/q (api/db *api*) '{:find [p1 p2]
                                               :where [[p1 :name]
                                                       [p2 :name]]})))))

  (t/testing "A single first result, joined to all possible subsequent results in next term"
    (t/is (= 5 (count (api/q (api/db *api*) '{:find [p1 p2]
                                              :where [[p1 :name "Ivan"]
                                                      [p2 :name]]})))))

  (t/testing "A single first result, with no subsequent results in next term"
    (t/is (= 0 (count (api/q (api/db *api*) '{:find [p1]
                                              :where [[p1 :name "Ivan"]
                                                      [p2 :name "does-not-match"]]})))))

  (t/testing "Every person joins once, plus 2 more matches"
    (t/is (= 7 (count (api/q (api/db *api*) '{:find [p1 p2]
                                              :where [[p1 :name name]
                                                      [p2 :name name]]}))))))

(t/deftest test-join-over-two-attributes
  (fix/transact! *api* (fix/people [{:crux.db/id :ivan :name "Ivan" :last-name "Ivanov"}
                                    {:crux.db/id :petr :name "Petr" :follows #{"Ivanov"}}]))

  (t/is (= #{[:petr]} (api/q (api/db *api*) '{:find [e2]
                                              :where [[e :last-name last-name]
                                                      [e2 :follows last-name]
                                                      [e :name "Ivan"]]}))))

(t/deftest test-blanks
  (fix/transact! *api* (fix/people [{:name "Ivan"} {:name "Petr"} {:name "Sergei"}]))

  (t/is (= #{["Ivan"] ["Petr"] ["Sergei"]}
           (api/q (api/db *api*) '{:find [name]
                                   :where [[_ :name name]]}))))

(t/deftest test-exceptions
  (t/testing "Unbound query variable"
    (try
      (api/q (api/db *api*) '{:find [bah]
                              :where [[e :name]]})
      (t/is (= true false) "Expected exception")
      (catch IllegalArgumentException e
        (t/is (= "Find refers to unknown variable: bah" (.getMessage e)))))

    (try
      (api/q (api/db *api*) '{:find [x]
                              :where [[x :foo]
                                      [(+ 1 bah)]]})
      (t/is (= true false) "Expected exception")
      (catch IllegalArgumentException e
        (t/is (re-find #"Clause refers to unknown variable: bah" (.getMessage e)))))

    (try
      (api/q (api/db *api*) '{:find [x]
                              :where [[x :foo]
                                      [(+ 1 bah) bah]]})
      (t/is (= true false) "Expected exception")
      (catch RuntimeException e
        (t/is (re-find #"Circular dependency between bah and bah" (.getMessage e)))))

    (try
      (api/q (api/db *api*) '{:find [foo]
                              :where [[(+ 1 bar) foo]
                                      [(+ 1 foo) bar]]})
      (t/is (= true false) "Expected exception")
      (catch RuntimeException e
        (t/is (re-find #"Circular dependency between bar and foo" (.getMessage e)))))

    (try
      (api/q (api/db *api*) '{:find [foo]
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

  (fix/transact! *api* (fix/people [{:crux.db/id :ivan-ivanov-1 :name "Ivan" :last-name "Ivanov"}
                                    {:crux.db/id :ivan-ivanov-2 :name "Ivan" :last-name "Ivanov"}
                                    {:crux.db/id :ivan-ivanovtov-1 :name "Ivan" :last-name "Ivannotov"}]))

  (t/testing "literal v"
    (t/is (= 1 (count (api/q (api/db *api*) '{:find [e]
                                              :where [[e :name name]
                                                      [e :name "Ivan"]
                                                      (not [e :last-name "Ivanov"])]}))))

    (t/is (= 1 (count (api/q (api/db *api*) '{:find [e]
                                              :where [[e :name name]
                                                      (not [e :last-name "Ivanov"])]}))))

    (t/is (= 1 (count (api/q (api/db *api*) '{:find [e]
                                              :where [[e :name "Ivan"]
                                                      (not [e :last-name "Ivanov"])]}))))

    (t/is (= 2 (count (api/q (api/db *api*) '{:find [e]
                                              :where [[e :name name]
                                                      [e :name "Ivan"]
                                                      (not [e :last-name "Ivannotov"])]}))))

    (t/testing "multiple clauses in not"
      (t/is (= 2 (count (api/q (api/db *api*) '{:find [e]
                                                :where [[e :name name]
                                                        [e :name "Ivan"]
                                                        (not [e :last-name "Ivannotov"]
                                                             [e :name "Ivan"])]}))))

      (t/is (= 2 (count (api/q (api/db *api*) '{:find [e]
                                                :where [[e :name name]
                                                        [e :name "Ivan"]
                                                        (not [e :last-name "Ivannotov"]
                                                             [(string? name)])]}))))

      (t/is (= 3 (count (api/q (api/db *api*) '{:find [e]
                                                :where [[e :name name]
                                                        [e :name "Ivan"]
                                                        (not [e :last-name "Ivannotov"]
                                                             [(number? name)])]}))))

      (t/is (= 3 (count (api/q (api/db *api*) '{:find [e]
                                                :where [[e :name name]
                                                        [e :name "Ivan"]
                                                        (not [e :last-name "Ivannotov"]
                                                             [e :name "Bob"])]}))))))

  (t/testing "variable v"
    (t/is (= 0 (count (api/q (api/db *api*) '{:find [e]
                                              :where [[e :name name]
                                                      [e :name "Ivan"]
                                                      (not [e :name name])]}))))

    (t/is (= 0 (count (api/q (api/db *api*) '{:find [e]
                                              :where [[e :name name]
                                                      (not [e :name name])]}))))

    (t/is (= 2 (count (api/q (api/db *api*) '{:find [e]
                                              :where [[e :name name]
                                                      [:ivan-ivanovtov-1 :last-name i-name]
                                                      (not [e :last-name i-name])]})))))

  (t/testing "literal entities"
    (t/is (= 0 (count (api/q (api/db *api*) '{:find [e]
                                              :where [[e :name name]
                                                      (not [:ivan-ivanov-1 :name name])]}))))

    (t/is (= 1 (count (api/q (api/db *api*) '{:find [e]
                                              :where [[e :last-name last-name]
                                                      (not [:ivan-ivanov-1 :last-name last-name])]}))))))

(t/deftest test-or-query
  (fix/transact! *api* (fix/people [{:name "Ivan" :last-name "Ivanov"}
                                    {:name "Ivan" :last-name "Ivanov"}
                                    {:name "Ivan" :last-name "Ivannotov"}
                                    {:name "Bob" :last-name "Controlguy"}]))

  ;; Here for dev reasons, delete when appropiate
  (t/is (= '[[:triple {:e e :a :name :v name}]
             [:triple {:e e :a :name :v "Ivan"}]
             [:or [[:term [:triple {:e e :a :last-name :v "Ivanov"}]]]]]
           (s/conform :crux.query/where '[[e :name name]
                                          [e :name "Ivan"]
                                          (or [e :last-name "Ivanov"])])))

  (t/testing "Or works as expected"
    (t/is (= 3 (count (api/q (api/db *api*) '{:find [e]
                                              :where [[e :name name]
                                                      [e :name "Ivan"]
                                                      (or [e :last-name "Ivanov"]
                                                          [e :last-name "Ivannotov"])]}))))

    (t/is (= 4 (count (api/q (api/db *api*) '{:find [e]
                                              :where [(or [e :last-name "Ivanov"]
                                                          [e :last-name "Ivannotov"]
                                                          [e :last-name "Controlguy"])]}))))

    (t/is (= 0 (count (api/q (api/db *api*) '{:find [e]
                                              :where [(or [e :last-name "Controlguy"])
                                                      (or [e :last-name "Ivanov"]
                                                          [e :last-name "Ivannotov"])]}))))

    (t/is (= 0 (count (api/q (api/db *api*) '{:find [e]
                                              :where [(or [e :last-name "Ivanov"])
                                                      (or [e :last-name "Ivannotov"])]}))))

    (t/is (= 0 (count (api/q (api/db *api*) '{:find [e]
                                              :where [[e :last-name "Controlguy"]
                                                      (or [e :last-name "Ivanov"]
                                                          [e :last-name "Ivannotov"])]}))))

    (t/is (= 3 (count (api/q (api/db *api*) '{:find [e]
                                              :where [[e :name name]
                                                      (or [e :last-name "Ivanov"]
                                                          [e :name "Bob"])]})))))

  (t/testing "Or edge case - can take a single clause"
    ;; Unsure of the utility
    (t/is (= 2 (count (api/q (api/db *api*) '{:find [e]
                                              :where [[e :name name]
                                                      [e :name "Ivan"]
                                                      (or [e :last-name "Ivanov"])]})))))

  (t/is (= #{["Ivan" "Ivanov"]
             ["Ivan" :optional]} (api/q (api/db *api*) '{:find [name l]
                                                         :where [[e :name name]
                                                                 [e :name "Ivan"]
                                                                 (or (and [e :last-name "Ivanov"]
                                                                          [e :last-name l])
                                                                     (and [(identity e)]
                                                                          [(identity :optional) l]))]}))))

(t/deftest test-or-query-can-use-and
  (let [[ivan] (fix/transact! *api* (fix/people [{:name "Ivan" :sex :male}
                                                 {:name "Bob" :sex :male}
                                                 {:name "Ivana" :sex :female}]))]

    (t/is (= #{["Ivan"]
               ["Ivana"]}
             (api/q (api/db *api*) '{:find [name]
                                     :where [[e :name name]
                                             (or [e :sex :female]
                                                 (and [e :sex :male]
                                                      [e :name "Ivan"]))]})))

    (t/is (= #{[(:crux.db/id ivan)]}
             (api/q (api/db *api*) '{:find [e]
                                     :where [(or [e :name "Ivan"])]})))

    (t/is (= #{}
             (api/q (api/db *api*) '{:find [name]
                                     :where [[e :name name]
                                             (or (and [e :sex :female]
                                                      [e :name "Ivan"]))]})))))

(t/deftest test-ors-must-use-same-vars
  (try
    (api/q (api/db *api*) '{:find [e]
                            :where [[e :name name]
                                    (or [e1 :last-name "Ivanov"]
                                        [e2 :last-name "Ivanov"])]})
    (t/is (= true false) "Expected assertion error")
    (catch IllegalArgumentException e
      (t/is (re-find #"Or requires same logic variables"
                     (.getMessage e)))))

  (try
    (api/q (api/db *api*) '{:find [x]
                            :where [(or-join [x]
                                             [e1 :last-name "Ivanov"])]})
    (t/is (= true false) "Expected assertion error")
    (catch IllegalArgumentException e
      (t/is (re-find #"Or join variable never used: x"
                     (.getMessage e))))))

(t/deftest test-ors-can-introduce-new-bindings
  (let [[petr ivan ivanova] (fix/transact! *api* (fix/people [{:name "Petr" :last-name "Smith" :sex :male}
                                                              {:name "Ivan" :last-name "Ivanov" :sex :male}
                                                              {:name "Ivanova" :last-name "Ivanov" :sex :female}]))]

    (t/testing "?p2 introduced only inside of an Or"
      (t/is (= #{[(:crux.db/id ivan)]} (api/q (api/db *api*) '{:find [?p2]
                                                               :where [(or (and [?p2 :name "Petr"]
                                                                                [?p2 :sex :female])
                                                                           (and [?p2 :last-name "Ivanov"]
                                                                                [?p2 :sex :male]))]}))))))

(t/deftest test-not-join
  (fix/transact! *api* (fix/people [{:name "Ivan" :last-name "Ivanov"}
                                    {:name "Malcolm" :last-name "Ofsparks"}
                                    {:name "Dominic" :last-name "Monroe"}]))

  (t/testing "Rudimentary not-join"
    (t/is (= #{["Ivan"] ["Malcolm"]}
             (api/q (api/db *api*) '{:find [name]
                                     :where [[e :name name]
                                             (not-join [e]
                                                       [e :last-name "Monroe"])]})))

    (t/is (= #{["Ivan"] ["Malcolm"]}
             (api/q (api/db *api*) '{:find [name]
                                     :where [[e :name name]
                                             (not-join [e]
                                                       [e :last-name last-name]
                                                       [(= last-name "Monroe")])]})))

    (t/is (= #{["Dominic"]}
             (api/q (api/db *api*) '{:find [name]
                                     :where [[e :name name]
                                             (not-join [e]
                                                       [e :last-name last-name]
                                                       [(not= last-name "Monroe")])]})))))

(t/deftest test-mixing-expressions
  (fix/transact! *api* (fix/people [{:name "Ivan" :last-name "Ivanov"}
                                    {:name "Derek" :last-name "Ivanov"}
                                    {:name "Bob" :last-name "Ivannotov"}
                                    {:name "Fred" :last-name "Ivannotov"}]))

  (t/testing "Or can use not expression"
    (t/is (= #{["Ivan"] ["Derek"] ["Fred"]}
             (api/q (api/db *api*) '{:find [name]
                                     :where [[e :name name]
                                             (or [e :last-name "Ivanov"]
                                                 (not [e :name "Bob"]))]}))))

  (t/testing "Not can use Or expression"
    (t/is (= #{["Fred"]} (api/q (api/db *api*) '{:find [name]
                                                 :where [[e :name name]
                                                         (not (or [e :last-name "Ivanov"]
                                                                  [e :name "Bob"]))]})))))

(t/deftest test-predicate-expression
  (fix/transact! *api* (fix/people [{:crux.db/id :ivan :name "Ivan" :last-name "Ivanov" :age 30}
                                    {:crux.db/id :bob :name "Bob" :last-name "Ivanov" :age 40}
                                    {:crux.db/id :dominic :name "Dominic" :last-name "Monroe" :age 50}]))

  (t/testing "range expressions"
    (t/is (= #{["Ivan"] ["Bob"]}
             (api/q (api/db *api*) '{:find [name]
                                     :where [[e :name name]
                                             [e :age age]
                                             [(< age 50)]]})))

    (t/is (= #{["Dominic"]}
             (api/q (api/db *api*) '{:find [name]
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
               (api/q (api/db *api*) '{:find [name age1 name2 age2]
                                       :where [[e :name name]
                                               [e :age age1]
                                               [e2 :name name2]
                                               [e2 :age age2]
                                               [(<= age1 age2)]]})))

      (t/is (= #{["Ivan" "Dominic"]
                 ["Ivan" "Bob"]
                 ["Dominic" "Bob"]}
               (api/q (api/db *api*) '{:find [name1 name2]
                                       :where [[e :name name1]
                                               [e2 :name name2]
                                               [(> name1 name2)]]})))))

  (t/testing "clojure.core predicate"
    (t/is (= #{["Bob"] ["Dominic"]}
             (api/q (api/db *api*) '{:find [name]
                                     :where [[e :name name]
                                             [(re-find #"o" name)]]})))

    (t/testing "No results"
      (t/is (empty? (api/q (api/db *api*) '{:find [name]
                                            :where [[e :name name]
                                                    [(re-find #"X" name)]]}))))

    (t/testing "Not predicate"
      (t/is (= #{["Ivan"]}
               (api/q (api/db *api*) '{:find [name]
                                       :where [[e :name name]
                                               (not [(re-find #"o" name)])]}))))

    (t/testing "Entity variable"
      (t/is (= #{["Ivan"]}
               (api/q (api/db *api*) '{:find [name]
                                       :where [[e :name name]
                                               [(= :ivan e)]]})))

      (t/testing "Filtered by value"
        (t/is (= #{[:bob] [:ivan]}
                 (api/q (api/db *api*) '{:find [e]
                                         :where [[e :last-name last-name]
                                                 [(= "Ivanov" last-name)]]})))

        (t/is (= #{[:ivan]}
                 (api/q (api/db *api*) '{:find [e]
                                         :where [[e :last-name last-name]
                                                 [e :age age]
                                                 [(= "Ivanov" last-name)]
                                                 [(= 30 age)]]})))))

    (t/testing "Several variables"
      (t/is (= #{["Bob"]}
               (api/q (api/db *api*) '{:find [name]
                                       :where [[e :name name]
                                               [e :age age]
                                               [(= 40 age)]
                                               [(re-find #"o" name)]
                                               [(not= age name)]]})))

      (t/is (= #{[:bob "Ivanov"]}
               (api/q (api/db *api*) '{:find [e last-name]
                                       :where [[e :last-name last-name]
                                               [e :age age]
                                               [(re-find #"ov$" last-name)]
                                               (not [(= age 30)])]})))

      (t/testing "No results"
        (t/is (= #{}
                 (api/q (api/db *api*) '{:find [name]
                                         :where [[e :name name]
                                                 [e :age age]
                                                 [(re-find #"o" name)]
                                                 [(= age name)]]})))))

    (t/testing "Bind result to var"
      (t/is (= #{["Dominic" 25] ["Ivan" 15] ["Bob" 20]}
               (api/q (api/db *api*) '{:find [name half-age]
                                       :where [[e :name name]
                                               [e :age age]
                                               [(quot age 2) half-age]]})))

      (t/testing "Order of joins is rearranged to ensure arguments are bound"
        (t/is (= #{["Dominic" 25] ["Ivan" 15] ["Bob" 20]}
                 (api/q (api/db *api*) '{:find [name half-age]
                                         :where [[e :name name]
                                                 [e :age real-age]
                                                 [(quot real-age 2) half-age]]}))))

      (t/testing "Binding more than once intersects result"
        (t/is (= #{["Ivan" 15]}
                 (api/q (api/db *api*) '{:find [name half-age]
                                         :where [[e :name name]
                                                 [e :age real-age]
                                                 [(quot real-age 2) half-age]
                                                 [(- real-age 15) half-age]]}))))

      (t/testing "Binding can use range predicates"
        (t/is (= #{["Dominic" 25]}
                 (api/q (api/db *api*) '{:find [name half-age]
                                         :where [[e :name name]
                                                 [e :age real-age]
                                                 [(quot real-age 2) half-age]
                                                 [(> half-age 20)]]})))))))

(t/deftest test-attributes-with-multiple-values
  (fix/transact! *api* (fix/people [{:crux.db/id :ivan :name "Ivan" :last-name "Ivanov" :age 30 :friends #{:bob :dominic}}
                                    {:crux.db/id :bob :name "Bob" :last-name "Ivanov" :age 40 :friends #{:ivan :dominic}}
                                    {:crux.db/id :dominic :name "Dominic" :last-name "Monroe" :age 50 :friends #{:bob}}]))

  (t/testing "can find multiple values"
    (t/is (= #{[:bob] [:dominic]}
             (api/q (api/db *api*) '{:find [f]
                                     :where [[i :name "Ivan"]
                                             [i :friends f]]}))))

  (t/testing "can find based on single value"
    (t/is (= #{[:ivan]}
             (api/q (api/db *api*) '{:find [i]
                                     :where [[i :name "Ivan"]
                                             [i :friends :bob]]}))))

  (t/testing "join intersects values"
    (t/is (= #{[:bob]}
             (api/q (api/db *api*) '{:find [f]
                                     :where [[i :name "Ivan"]
                                             [i :friends f]
                                             [d :name "Dominic"]
                                             [d :friends f]]}))))

  (t/testing "clojure.core predicate filters values"
    (t/is (= #{[:bob]}
             (api/q (api/db *api*) '{:find [f]
                                     :where [[i :name "Ivan"]
                                             [i :friends f]
                                             [(= f :bob)]]})))

    (t/is (= #{[:dominic]}
             (api/q (api/db *api*) '{:find [f]
                                     :where [[i :name "Ivan"]
                                             [i :friends f]
                                             [(not= f :bob)]]}))))

  (t/testing "unification filters values"
    (t/is (= #{[:bob]}
             (api/q (api/db *api*) '{:find [f]
                                     :where [[i :name "Ivan"]
                                             [i :friends f]
                                             [(== f :bob)]]})))

    (t/is (= #{[:bob] [:dominic]}
             (api/q (api/db *api*) '{:find [f]
                                     :where [[i :name "Ivan"]
                                             [i :friends f]
                                             [(== f #{:bob :dominic})]]})))

    (t/is (= #{[:dominic]}
             (api/q (api/db *api*) '{:find [f]
                                     :where [[i :name "Ivan"]
                                             [i :friends f]
                                             [(!= f :bob)]]}))))

  (t/testing "not filters values"
    (t/is (= #{[:ivan :dominic]}
             (api/q (api/db *api*) '{:find [i f]
                                     :where [[i :name "Ivan"]
                                             [i :friends f]
                                             (not [(= f :bob)])]})))))

(t/deftest test-can-use-idents-as-entities
  (fix/transact! *api* (fix/people [{:crux.db/id :ivan :name "Ivan" :last-name "Ivanov"}
                                    {:crux.db/id :petr :name "Petr" :last-name "Petrov" :mentor :ivan}]))

  (t/testing "Can query by single field"
    (t/is (= #{[:petr]} (api/q (api/db *api*) '{:find [p]
                                                :where [[i :name "Ivan"]
                                                        [p :mentor i]]})))

    (t/testing "Other direction"
      (t/is (= #{[:petr]} (api/q (api/db *api*) '{:find [p]
                                                  :where [[p :mentor i]
                                                          [i :name "Ivan"]]})))))

  (t/testing "Can query by known entity"
    (t/is (= #{["Ivan"]} (api/q (api/db *api*) '{:find [n]
                                                 :where [[:ivan :name n]]})))

    (t/is (= #{["Ivan"]} (api/q (api/db *api*) '{:find [n]
                                                 :where [[:petr :mentor i]
                                                         [i :name n]]})))

    (t/is (= #{["Ivan"]} (api/q (api/db *api*) '{:find [n]
                                                 :where [[p :name "Petr"]
                                                         [p :mentor i]
                                                         [i :name n]]})))

    (t/is (= #{["Ivan"]} (api/q (api/db *api*) '{:find [n]
                                                 :where [[p :mentor i]
                                                         [i :name n]]})))

    (t/is (= #{[:ivan]} (api/q (api/db *api*) '{:find [i]
                                                :where [[p :name "Petr"]
                                                        [p :mentor i]]})))

    (t/testing "Other direction"
      (t/is (= #{["Ivan"]} (api/q (api/db *api*) '{:find [n]
                                                   :where [[i :name n]
                                                           [:petr :mentor i]]}))))
    (t/testing "No matches"
      (t/is (= #{} (api/q (api/db *api*) '{:find [n]
                                           :where [[:ivan :mentor x]
                                                   [x :name n]]})))

      (t/testing "Other direction"
        (t/is (= #{} (api/q (api/db *api*) '{:find [n]
                                             :where [[x :name n]
                                                     [:ivan :mentor x]]})))))

    (t/testing "Literal entity and literal value"
      (t/is (= #{[true]} (api/q (api/db *api*) '{:find [found?]
                                                 :where [[:ivan :name "Ivan"]
                                                         [(identity true) found?]]})))

      (t/is (= #{} (api/q (api/db *api*) '{:find [found?]
                                           :where [[:ivan :name "Bob"]
                                                   [(identity true) found?]]}))))))

(t/deftest test-join-and-seek-bugs
  (fix/transact! *api* (fix/people [{:crux.db/id :ivan :name "Ivan" :last-name "Ivanov"}
                                    {:crux.db/id :petr :name "Petr" :last-name "Petrov" :mentor :ivan}]))

  (t/testing "index seek bugs"
    (t/is (= #{} (api/q (api/db *api*) '{:find [i]
                                         :where [[p :name "Petrov"]
                                                 [p :mentor i]]})))


    (t/is (= #{} (api/q (api/db *api*) '{:find [p]
                                         :where [[p :name "Pet"]]})))

    (t/is (= #{} (api/q (api/db *api*) '{:find [p]
                                         :where [[p :name "I"]]})))

    (t/is (= #{} (api/q (api/db *api*) '{:find [p]
                                         :where [[p :name "Petrov"]]})))

    (t/is (= #{} (api/q (api/db *api*) '{:find [i]
                                         :where [[p :name "Pet"]
                                                 [p :mentor i]]})))

    (t/is (= #{} (api/q (api/db *api*) '{:find [i]
                                         :where [[p :name "Petrov"]
                                                 [p :mentor i]]}))))

  (t/testing "join bugs"
    (t/is (= #{} (api/q (api/db *api*) '{:find [p]
                                         :where [[p :name "Ivan"]
                                                 [p :mentor i]]})))

    (t/is (= #{} (api/q (api/db *api*) '{:find [i]
                                         :where [[p :name "Ivan"]
                                                 [p :mentor i]]})))))

(t/deftest test-queries-with-variables-only
  (fix/transact! *api* (fix/people [{:crux.db/id :ivan :name "Ivan" :mentor :petr}
                                    {:crux.db/id :petr :name "Petr" :mentor :oleg}
                                    {:crux.db/id :oleg :name "Oleg" :mentor :ivan}]))

  (t/is (= #{[:oleg "Oleg" :petr "Petr"]
             [:ivan "Ivan" :oleg "Oleg"]
             [:petr "Petr" :ivan "Ivan"]} (api/q (api/db *api*) '{:find [e1 n1 e2 n2]
                                                                  :where [[e1 :name n1]
                                                                          [e2 :mentor e1]
                                                                          [e2 :name n2]]}))))

(t/deftest test-index-unification
  (fix/transact! *api* (fix/people [{:crux.db/id :ivan :name "Ivan" :last-name "Ivanov"}
                                    {:crux.db/id :petr :name "Petr" :last-name "Petrov" :mentor :ivan}]))

  (t/is (= #{[:petr :petr]} (api/q (api/db *api*) '{:find [p1 p2]
                                                    :where [[p1 :name "Petr"]
                                                            [p2 :mentor i]
                                                            [(== p1 p2)]]})))

  (t/is (= #{} (api/q (api/db *api*) '{:find [p1 p2]
                                       :where [[p1 :name "Petr"]
                                               [p2 :mentor i]
                                               [(== p1 i)]]})))

  (t/is (= #{} (api/q (api/db *api*) '{:find [p1 p2]
                                       :where [[p1 :name "Petr"]
                                               [p2 :mentor i]
                                               [(== p1 i)]]})))

  (t/is (= #{[:petr :petr]} (api/q (api/db *api*) '{:find [p1 p2]
                                                    :where [[p1 :name "Petr"]
                                                            [p2 :mentor i]
                                                            [(!= p1 i)]]})))

  (t/is (= #{} (api/q (api/db *api*) '{:find [p1 p2]
                                       :where [[p1 :name "Petr"]
                                               [p2 :mentor i]
                                               [(!= p1 p2)]]})))


  (t/is (= #{} (api/q (api/db *api*) '{:find [p]
                                       :where [[p :name "Petr"]
                                               [p :mentor i]
                                               [(== p i)]]})))

  (t/testing "unify with literal"
    (t/is (= #{[:petr]} (api/q (api/db *api*) '{:find [p]
                                                :where [[p :name n]
                                                        [(== n "Petr")]]})))

    (t/is (= #{[:ivan]} (api/q (api/db *api*) '{:find [p]
                                                :where [[p :name n]
                                                        [(!= n "Petr")]]}))))

  (t/testing "unify with entity"
    (t/is (= #{["Petr"]} (api/q (api/db *api*) '{:find [n]
                                                 :where [[p :name n]
                                                         [(== p :petr)]]})))

    (t/is (= #{["Ivan"]} (api/q (api/db *api*) '{:find [n]
                                                 :where [[i :name n]
                                                         [(!= i :petr)]]}))))

  (t/testing "multiple literals in set"
    (t/is (= #{[:petr] [:ivan]} (api/q (api/db *api*) '{:find [p]
                                                        :where [[p :name n]
                                                                [(== n #{"Petr" "Ivan"})]]})))

    (t/is (= #{[:ivan]} (api/q (api/db *api*) '{:find [p]
                                                :where [[p :name n]
                                                        [(!= n #{"Petr"})]]})))

    (t/is (= #{} (api/q (api/db *api*) '{:find [p]
                                         :where [[p :name n]
                                                 [(== n #{})]]})))

    (t/is (= #{[:petr] [:ivan]} (api/q (api/db *api*) '{:find [p]
                                                        :where [[p :name n]
                                                                [(!= n #{})]]})))))

(t/deftest test-get-attr
  (fix/transact! *api* (fix/people [{:crux.db/id :ivan :name "Ivan" :age 21 :friends #{:petr :oleg}}]))
  (t/testing "existing attribute"
    (t/is (= #{[:ivan 21]}
             (api/q (api/db *api*) '{:find [e age]
                                     :where [[e :name "Ivan"]
                                             [(get-attr e :age) age]]})))

    (t/is (empty? (api/q (api/db *api*) '{:find [e age]
                                          :where [[e :name "Oleg"]
                                                  [(get-attr e :age) age]]})))

    (t/is (= #{}
             (api/q (api/db *api*) '{:find [e age]
                                     :where [[e :name "Ivan"]
                                             [(get-attr e :age) age]
                                             [(> age 30)]]}))))

  (t/testing "many valued attribute"
    (t/is (= #{[:ivan :petr]
               [:ivan :oleg]}
             (api/q (api/db *api*) '{:find [e friend]
                                     :where [[e :name "Ivan"]
                                             [(get-attr e :friends) friend]]}))))

  (t/testing "unknown attribute"
    (t/is (empty? (api/q (api/db *api*) '{:find [e email]
                                          :where [[e :name "Ivan"]
                                                  [(get-attr e :email) email]]}))))

  (t/testing "optional found attribute"
    (t/is (= #{[:ivan 21]}
             (api/q (api/db *api*) '{:find [e age]
                                     :where [[e :name "Ivan"]
                                             [(get-attr e :age 0) age]]}))))

  (t/testing "optional not found attribute"
    (t/is (= #{[:ivan "N/A"]}
             (api/q (api/db *api*) '{:find [e email]
                                     :where [[e :name "Ivan"]
                                             [(get-attr e :email "N/A") email]]})))

    (t/is (= #{[:ivan "ivan@example.com"]
               [:ivan "ivanov@example.com"]}
             (api/q (api/db *api*) '{:find [e email]
                                     :where [[e :name "Ivan"]
                                             [(get-attr e :email #{"ivan@example.com"
                                                                   "ivanov@example.com"}) email]]})))

    (t/is (empty? (api/q (api/db *api*) '{:find [e email]
                                          :where [[e :name "Ivan"]
                                                  [(get-attr e :email #{}) email]]})))))

(t/deftest test-multiple-values-literals
  (fix/transact! *api* (fix/people [{:crux.db/id :ivan :name "Ivan" :age 21 :friends #{:petr :oleg}}
                                    {:crux.db/id :petr :name "Petr" :age 30 :friends #{:ivan}}]))
  (t/testing "set"
    (t/is (empty? (api/q (api/db *api*) '{:find [e]
                                          :where [[e :name #{}]]})))

    (t/is (empty? (api/q (api/db *api*) '{:find [e]
                                          :where [[e :name #{"Oleg"}]]})))

    (t/is (= #{[:ivan]}
             (api/q (api/db *api*) '{:find [e]
                                     :where [[e :name #{"Ivan" "Oleg"}]]})))

    (t/is (= #{[:ivan] [:petr]}
             (api/q (api/db *api*) '{:find [e]
                                     :where [[e :name #{"Ivan" "Petr"}]]})))

    (t/is (= #{[:ivan]}
             (api/q (api/db *api*) '{:find [e]
                                     :where [[e :friends #{:petr :oleg}]]})))

    (t/is (= #{[:ivan] [:petr]}
             (api/q (api/db *api*) '{:find [e]
                                     :where [[e :friends #{:petr :ivan}]]}))))

  (t/testing "entity position"
    (t/is (empty? (api/q (api/db *api*) '{:find [name]
                                          :where [[#{} :name name]]})))

    (t/is (empty? (api/q (api/db *api*) '{:find [name]
                                          :where [[#{:oleg} :name name]]})))

    (t/is (= #{["Ivan"]}
             (api/q (api/db *api*) '{:find [name]
                                     :where [[#{:ivan :oleg} :name name]]})))

    (t/is (= #{["Ivan"] ["Petr"]}
             (api/q (api/db *api*) '{:find [name]
                                     :where [[#{:ivan :petr} :name name]]}))))

  (t/testing "vectors are not supported"
    (t/is (thrown-with-msg?
           RuntimeException
           #"Spec assertion failed"
           (api/q (api/db *api*) '{:find [e]
                                   :where [[e :name [:ivan]]]})))))

(t/deftest test-simple-numeric-range-search
  (t/is (= '[[:triple {:e i, :a :age, :v age}]
             [:range [[:sym-val {:op <, :sym age, :val 20}]]]]
           (s/conform :crux.query/where '[[i :age age]
                                          [(< age 20)]])))

  (fix/transact! *api* (fix/people [{:crux.db/id :ivan :name "Ivan" :last-name "Ivanov" :age 21}
                                    {:crux.db/id :petr :name "Petr" :last-name "Petrov" :age 18}]))

  (t/testing "Min search case"
    (t/is (= #{[:ivan]} (api/q (api/db *api*) '{:find [i]
                                                :where [[i :age age]
                                                        [(> age 20)]]})))
    (t/is (= #{} (api/q (api/db *api*) '{:find [i]
                                         :where [[i :age age]
                                                 [(> age 21)]]})))

    (t/is (= #{[:ivan]} (api/q (api/db *api*) '{:find [i]
                                                :where [[i :age age]
                                                        [(>= age 21)]]}))))

  (t/testing "Max search case"
    (t/is (= #{[:petr]} (api/q (api/db *api*) '{:find [i]
                                                :where [[i :age age]
                                                        [(< age 20)]]})))
    (t/is (= #{} (api/q (api/db *api*) '{:find [i]
                                         :where [[i :age age]
                                                 [(< age 18)]]})))
    (t/is (= #{[:petr]} (api/q (api/db *api*) '{:find [i]
                                                :where [[i :age age]
                                                        [(<= age 18)]]})))
    (t/is (= #{[18]} (api/q (api/db *api*) '{:find [age]
                                             :where [[:petr :age age]
                                                     [(<= age 18)]]}))))

  (t/testing "Reverse symbol and value"
    (t/is (= #{[:ivan]} (api/q (api/db *api*) '{:find [i]
                                                :where [[i :age age]
                                                        [(<= 20 age)]]})))
    (t/is (= #{[:petr]} (api/q (api/db *api*) '{:find [i]
                                                :where [[i :age age]
                                                        [(>= 20 age)]]})))

    (t/testing "Range inversion edge cases, #612"
      (t/is (= #{[:ivan]} (api/q (api/db *api*) '{:find [i]
                                                  :where [[i :age age]
                                                          [(<= 21 age)]]})))
      (t/is (= #{} (api/q (api/db *api*) '{:find [i]
                                           :where [[i :age age]
                                                   [(> 18 age)]]}))))))

(t/deftest test-mutiple-values
  (fix/transact! *api* (fix/people [{:crux.db/id :ivan :name "Ivan"}
                                    {:crux.db/id :oleg :name "Oleg"}
                                    {:crux.db/id :petr :name "Petr" :follows #{:ivan :oleg}}]))

  (t/testing "One way"
    (t/is (= #{[:ivan] [:oleg]} (api/q (api/db *api*) '{:find [x]
                                                        :where [[i :name "Petr"]
                                                                [i :follows x]]}))))

  (t/testing "The other way"
    (t/is (= #{[:petr]} (api/q (api/db *api*) '{:find [i]
                                                :where [[x :name "Ivan"]
                                                        [i :follows x]]})))))

(t/deftest test-sanitise-join
  (fix/transact! *api* (fix/people [{:crux.db/id :ivan :name "Ivan" :last-name "Ivanov"}]))
  (t/testing "Can query by single field"
    (t/is (= #{[:ivan]} (api/q (api/db *api*) '{:find [e2]
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

  (fix/transact! *api* (fix/people [{:crux.db/id :ivan :name "Ivan" :last-name "Ivanov" :age 21}
                                    {:crux.db/id :petr :name "Petr" :last-name "Petrov" :age 18}]))

  (t/testing "without rule"
    (t/is (= #{[:ivan]} (api/q (api/db *api*) '{:find [i]
                                                :where [[i :age age]
                                                        [(>= age 21)]]}))))

  (t/testing "rule using same variable name as body"
    (t/is (= #{[:ivan]} (api/q (api/db *api*) '{:find [i]
                                                :where [[i :age age]
                                                        (over-twenty-one? age)]
                                                :rules [[(over-twenty-one? age)
                                                         [(>= age 21)]]]}))))

  (t/testing "rules directly on arguments"
    (t/is (= #{[21]} (api/q (api/db *api*) '{:find [age]
                                             :where [(over-twenty-one? age)]
                                             :args [{:age 21}]
                                             :rules [[(over-twenty-one? age)
                                                      [(>= age 21)]]]})))

    (t/is (= #{} (api/q (api/db *api*) '{:find [age]
                                         :where [(over-twenty-one? age)]
                                         :args [{:age 20}]
                                         :rules [[(over-twenty-one? age)
                                                  [(>= age 21)]]]}))))

  (t/testing "rule using required bound args"
    (t/is (= #{[:ivan]} (api/q (api/db *api*) '{:find [i]
                                                :where [[i :age age]
                                                        (over-twenty-one? age)]
                                                :rules [[(over-twenty-one? [age])
                                                         [(>= age 21)]]]}))))

  (t/testing "rule using different variable name from body"
    (t/is (= #{[:ivan]} (api/q (api/db *api*) '{:find [i]
                                                :where [[i :age age]
                                                        (over-twenty-one? age)]
                                                :rules [[(over-twenty-one? x)
                                                         [(>= x 21)]]]}))))

  (t/testing "nested rules"
    (t/is (= #{[:ivan]} (api/q (api/db *api*) '{:find [i]
                                                :where [[i :age age]
                                                        (over-twenty-one? age)]
                                                :rules [[(over-twenty-one? x)
                                                         (over-twenty-one-internal? x)]
                                                        [(over-twenty-one-internal? y)
                                                         [(>= y 21)]]]}))))

  (t/testing "rule using multiple arguments"
    (t/is (= #{[:ivan]} (api/q (api/db *api*) '{:find [i]
                                                :where [[i :age age]
                                                        (over-age? age 21)]
                                                :rules [[(over-age? [age] required-age)
                                                         [(>= age required-age)]]]}))))

  (t/testing "rule using multiple branches"
    (t/is (= #{[:ivan]} (api/q (api/db *api*) '{:find [i]
                                                :where [(is-ivan-or-bob? i)]
                                                :rules [[(is-ivan-or-bob? i)
                                                         [i :name "Ivan"]
                                                         [i :last-name "Ivanov"]]
                                                        [(is-ivan-or-bob? i)
                                                         [i :name "Bob"]]]})))

    (t/is (= #{["Petr"]} (api/q (api/db *api*) '{:find [name]
                                                 :where [[i :name name]
                                                         (not (is-ivan-or-bob? i))]
                                                 :rules [[(is-ivan-or-bob? i)
                                                          [i :name "Ivan"]]
                                                         [(is-ivan-or-bob? i)
                                                          [i :name "Bob"]]]})))

    (t/is (= #{[:ivan]
               [:petr]} (api/q (api/db *api*) '{:find [i]
                                                :where [(is-ivan-or-petr? i)]
                                                :rules [[(is-ivan-or-petr? i)
                                                         [i :name "Ivan"]]
                                                        [(is-ivan-or-petr? i)
                                                         [i :name "Petr"]]]}))))

  (try
    (api/q (api/db *api*) '{:find [i]
                            :where [[i :age age]
                                    (over-twenty-one? age)]})
    (t/is (= true false) "Expected exception")
    (catch IllegalArgumentException e
      (t/is (re-find #"Unknown rule: " (.getMessage e)))))

  (try
    (api/q (api/db *api*) '{:find [i]
                            :where [[i :age age]
                                    (over-twenty-one? i age)]
                            :rules [[(over-twenty-one? x)
                                     [(>= x 21)]]]})
    (t/is (= true false) "Expected exception")
    (catch IllegalArgumentException e
      (t/is (re-find #"Rule invocation has wrong arity, expected: 1" (.getMessage e)))))

  (try
    (api/q (api/db *api*) '{:find [i]
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
  (fix/transact! *api* (fix/people (cons {:crux.db/id :ivan :name "Ivan" :last-name "Ivanov" :age 30}
                                         (repeat 1000 {:age 20}))))

  (let [n 10
        acceptable-limit-slowdown 0.1
        factors (->> #(let [direct-hit-ns-start (System/nanoTime)]
                        (t/is (= #{[:ivan]} (api/q (api/db *api*) '{:find [i]
                                                                    :where [[i :age 30]]})))
                        (let [direct-hit-ns (- (System/nanoTime) direct-hit-ns-start)
                              limited-hit-ns-start (System/nanoTime)]
                          (t/is (= 1 (count (api/q (api/db *api*) '{:find [i]
                                                                    :where [[i :age 20]]
                                                                    :limit 1}))))
                          (let [limited-hit-ns (- (System/nanoTime) limited-hit-ns-start)]
                            (double (/ (min direct-hit-ns limited-hit-ns)
                                       (max direct-hit-ns limited-hit-ns))))))
                     (repeatedly n))]
    (t/is (>= (/ (reduce + factors) n) acceptable-limit-slowdown))))

;; https://github.com/juxt/crux/issues/348

(t/deftest test-range-join-order-bug-348
  (fix/transact! *api* (fix/people
                        (for [n (range 100)]
                          {:crux.db/id (keyword (str "ivan-" n))
                           :name "Ivan"
                           :name1 "Ivan"
                           :number-1 n})))

  (fix/transact! *api* (fix/people
                        (for [n (range 10000)]
                          {:crux.db/id (keyword (str "oleg-" n))
                           :name "Oleg"
                           :name1 "Oleg"
                           :number-2 n})))

  (let [n 10
        acceptable-limit-slowdown 0.1
        factors (->> #(let [small-set-ns-start (System/nanoTime)]
                        (t/is (= #{[:ivan-50]}
                                 (api/q (api/db *api*) '{:find [e]
                                                         :where [[e :number-1 a]
                                                                 [e :name n]
                                                                 [(<= a 50)]
                                                                 [(>= a 50)]]})))
                        (let [small-set-ns (- (System/nanoTime) small-set-ns-start)
                              large-set-ns-start (System/nanoTime)]
                          (t/is (= #{[:oleg-5000]}
                                   (api/q (api/db *api*) '{:find [e]
                                                           :where [[e :number-2 a]
                                                                   [e :name n]
                                                                   [(<= a 5000)]
                                                                   [(>= a 5000)]]})))
                          (let [large-set-ns (- (System/nanoTime) large-set-ns-start)]
                            (double (/ (min small-set-ns large-set-ns)
                                       (max small-set-ns large-set-ns))))))
                     (repeatedly n))]
    (t/is (>= (/ (reduce + factors) n) acceptable-limit-slowdown))))

;; TODO: validate this test.
(t/deftest test-range-args-performance-906
  (fix/transact! *api* (fix/people
                        (for [n (range 10000)]
                          {:crux.db/id (keyword (str "oleg-" n))
                           :name "Oleg"
                           :number n})))

  (let [acceptable-slowdown-factor 1.5
        literal-ns-start (System/nanoTime)]
    (t/is (= #{[:oleg-9999]}
             (api/q (api/db *api*) '{:find [e]
                                     :where [[e :number a]
                                             [e :name n]
                                             [(>= a 9999)]]})))
    (let [literal-ns (- (System/nanoTime) literal-ns-start)
          args-ns-start (System/nanoTime)]
      (t/is (= #{[:oleg-9999]}
               (api/q (api/db *api*) '{:find [e]
                                       :where [[e :number a]
                                               [e :name n]
                                               [(>= a b)]]
                                       :args [{:b 9999}]})))
      (let [args-ns (- (System/nanoTime) args-ns-start)]
        (t/is (< (double (/ args-ns literal-ns)) acceptable-slowdown-factor)
              (pr-str args-ns " " literal-ns))))))

(t/deftest test-or-range-vars-bug-949
  (fix/transact! *api* (fix/people [{:crux.db/id :ivan :name "Ivan" :age 30}]))
  (t/is (= #{[:ivan "Ivan"]}
           (api/q (api/db *api*)
                  '{:find [e name]
                    :where [[e :name name]
                            [(get-attr e :age) age]
                            (or [(= x y)])
                            [(str age) x]
                            [(str age) y]]}))))

;; https://github.com/juxt/crux/issues/71

(t/deftest test-query-limits-bug-71
  (dotimes [i 10]
    (fix/transact! *api* (fix/people [{:name "Ivan" :last-name "Ivanov"}
                                      {:name "Petr" :last-name "Petrov"}
                                      {:name "Petr" :last-name "Ivanov"}]))

    (t/is (= 2 (count (api/q (api/db *api*) '{:find [l]
                                              :where [[_ :last-name l]]
                                              :limit 2}))))))

;; https://github.com/juxt/crux/issues/93

(t/deftest test-self-join-bug-93
  (fix/transact! *api* (fix/people [{:crux.db/id :ivan :name "Ivan" :friend :ivan :boss :petr}
                                    {:crux.db/id :petr :name "Petr"}]))

  (t/is (= #{[:petr]} (api/q (api/db *api*)
                             '{:find [b]
                               :where [[e :friend e]
                                       [e :boss b]]}))))

(t/deftest test-or-bug-146
  (fix/transact! *api* (fix/people [{:crux.db/id :ivan :name "Ivan" :extra "Petr" :age 20}
                                    {:crux.db/id :oleg :name "Oleg" :extra #inst "1980" :age 30}
                                    {:crux.db/id :petr :name "Petr" :age 40}]))

  ;; This wasn't the bug, but a useful test, lead to fixes in SPARQL
  ;; translator, and an example on how to use this.
  (t/testing "Or with non existing attribute in one leg and different types "
    (t/is (= #{["Ivan" "Petr" 20 :ivan]
               ["Oleg" #inst "1980" 30 :oleg]
               ["Petr" :none 40 :petr]}
             (api/q (api/db *api*) '{:find [n x a e]
                                     :where [[e :name n]
                                             [e :age a]
                                             [e :crux.db/id e]
                                             (or-join [e x]
                                                      [e :extra x]
                                                      (and [(identity :none) x]
                                                           (not [e :extra])))]})))))

(t/deftest test-arguments-bug-247
  (t/is (= #{} (api/q (api/db *api*)
                      '{:find [?x]
                        :where [[?x :name]]
                        :args [{:?x "Clojure"}]}))))

(t/deftest test-npe-arguments-bug-314
  (t/is (= #{} (api/q (api/db *api*)
                      '{:find [e]
                        :where
                        [[e :crux.db/id _]]
                        :args [{}]}))))

;; NOTE: Range constraints only apply when one of the arguments to the
;; constraint is a literal value. So when using :args to supply it, it
;; will always be a predicate which hence cannot jump directly to the
;; expected place in the index.
(t/deftest test-optimise-range-constraints-bug-505
  (let [tx (vec (for [i (range 5000)]
                  [:crux.tx/put
                   {:crux.db/id (keyword (str i))
                    :offer i}]))
        submitted-tx (doto (api/submit-tx *api* tx)
                       (->> (api/await-tx *api*)))])

  (let [db (api/db *api*)
        ;; Much higher than needed, but catches the bug without flakiness.
        range-slowdown-factor 100
        entity-time (let [start-time (System/nanoTime)]
                      (t/testing "entity id lookup"
                        (t/is (= :0 (:crux.db/id (api/entity db :0)))))
                      (- (System/nanoTime) start-time))
        base-query '{:find [i]
                     :where [[_ :offer i]
                             [(test-pred i test-val)]]
                     :limit 1}
        inputs '[[#{[2]} = 2]
                 [#{[0]} < 10]
                 [#{[0]} < 9223372036854775807]
                 [#{} < -100]
                 [#{[50]} >= 50]
                 [#{[0]} <= 5]
                 [#{[0]} > -100]
                 [#{[0]} >= -100]]]
    (doseq [[expected p v :as input] inputs]
      (dotimes [_ 5]
        (let [q (w/postwalk
                 #(case %
                    test-pred p
                    test-val v
                    %)
                 base-query)
              start-time (System/nanoTime)
              actual (api/q db q)
              query-time (- (System/nanoTime) start-time)]
          (t/is (= expected actual) (str input))
          (t/is (< query-time (* entity-time range-slowdown-factor)) (str input)))))))

;; NOTE: Micro-benchmark that shows relative bounds, acceptable
;; slowdown factors can be tweaked to force it to fail.
(t/deftest test-non-entity-id-lookup-issue-287
  (let [ivan {:crux.db/id :ivan :name "Ivan"}
        number-of-docs 500
        id-slowdown-factor 2
        entity-slowdown-factor 10
        tx (api/submit-tx *api* (vec (for [n (range number-of-docs)]
                                       [:crux.tx/put (assoc ivan :crux.db/id (keyword (str "ivan-" n)) :id n)])))
        _ (api/await-tx *api* tx)
        db (api/db *api*)
        entity-time (let [start-time (System/nanoTime)]
                      (t/testing "entity id lookup"
                        (t/is (= :ivan-2 (:crux.db/id (api/entity db :ivan-2)))))
                      (- (System/nanoTime) start-time))
        id-time (let [start-time (System/nanoTime)]
                  (t/testing "query based on primary key"
                    (t/is (= #{[:ivan-1]} (api/q db
                                                 '{:find [e]
                                                   :where [[e :crux.db/id :ivan-1]]}))))
                  (- (System/nanoTime) start-time))
        secondary-time (let [start-time (System/nanoTime)]
                         (t/testing "query based on secondary attribute"
                           (t/is (= #{[:ivan-3]} (api/q db
                                                        '{:find [e]
                                                          :where [[e :id 3]]}))))
                         (- (System/nanoTime) start-time))]
    (t/is (<= secondary-time (* entity-slowdown-factor entity-time)))
    (t/is (<= secondary-time (* id-slowdown-factor id-time)))))

(t/deftest test-query-and-match
  (t/testing "can create new user"
    (let [{:crux.tx/keys [tx-time tx-id] :as submitted-tx}
          (api/submit-tx *api* [[:crux.tx/match :ivan nil]
                                [:crux.tx/put {:crux.db/id :ivan, :name "Ivan 1st"}]])]
      (api/await-tx *api* submitted-tx)
      (t/is (true? (api/tx-committed? *api* submitted-tx)))

      (t/is (= #{["Ivan 1st"]} (api/q (api/db *api* tx-time tx-time)
                                      '{:find [n]
                                        :where [[:ivan :name n]]})))

      (t/is (= tx-id (:crux.tx/tx-id (api/entity-tx (api/db *api* tx-time tx-time) :ivan))))

      (t/is (= {:crux.db/id :ivan
                :name "Ivan 1st"} (api/entity (api/db *api* tx-time tx-time) :ivan)))))

  (t/testing "cannot create existing user"
    (let [{:crux.tx/keys [tx-time tx-id] :as submitted-tx}
          (api/submit-tx *api* [[:crux.tx/match :ivan nil]
                                [:crux.tx/put {:crux.db/id :ivan, :name "Ivan 2nd"}]])]

      (api/await-tx *api* submitted-tx)
      (t/is (false? (api/tx-committed? *api* submitted-tx)))

      (t/is (= #{["Ivan 1st"]} (api/q (api/db *api* tx-time tx-time)
                                      '{:find [n]
                                        :where [[:ivan :name n]]})))

      (t/is (not= tx-id (:tx-id (api/entity-tx (api/db *api* tx-time tx-time) :ivan))))))

  (t/testing "can update existing user"
    (let [{:crux.tx/keys [tx-time] :as submitted-update-tx}
          (api/submit-tx *api* [[:crux.tx/match :ivan {:crux.db/id :ivan, :name "Ivan 1st"}]
                                [:crux.tx/put {:crux.db/id :ivan, :name "Ivan 2nd"}]])]
      (api/await-tx *api* submitted-update-tx)
      (t/is (true? (api/tx-committed? *api* submitted-update-tx)))
      (t/is (= #{["Ivan 2nd"]} (api/q (api/db *api* tx-time tx-time)
                                      '{:find [n]
                                        :where [[:ivan :name n]]})))

      (t/testing "match sees interim state through the transaction"
        (let [{:crux.tx/keys [tx-time] :as submitted-tx}
              (api/submit-tx *api* [[:crux.tx/match :ivan {:crux.db/id :ivan, :name "Ivan 2nd"}]
                                    [:crux.tx/put {:crux.db/id :ivan, :name "Ivan 3rd"}]
                                    [:crux.tx/match :ivan {:crux.db/id :ivan, :name "Ivan 3rd"}]
                                    [:crux.tx/put {:crux.db/id :ivan :name "Ivan 4th"}]])]
          (api/await-tx *api* submitted-tx)

          (t/is (api/tx-committed? *api* submitted-tx))

          (t/is (= #{["Ivan 4th"]}
                   (api/q (api/db *api* tx-time tx-time)
                          '{:find [n]
                            :where [[:ivan :name n]]})))))

      (t/testing "normal put works after match"
        (let [{:crux.tx/keys [tx-time] :as submitted-tx}
              (api/submit-tx *api* [[:crux.tx/put
                                     {:crux.db/id :ivan
                                      :name "Ivan 5th"}]])]

          (api/await-tx *api* submitted-tx)
          (t/is (true? (api/tx-committed? *api* submitted-tx)))
          (t/is (= #{["Ivan 5th"]} (api/q (api/db *api* tx-time
                                                  tx-time) '{:find [n]
                                                             :where [[:ivan :name n]]})))

          (t/testing "earlier submitted txs can still be checked"
            (t/is (true? (api/tx-committed? *api* submitted-update-tx)))))))))

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
  ;; Day 0, represented as #inst "2018-12-31"
  (api/submit-tx *api* [[:crux.tx/put
                         {:crux.db/id :p2
                          :entry-pt :SFO
                          :arrival-time #inst "2018-12-31"
                          :departure-time :na}
                         #inst "2018-12-31"]
                        [:crux.tx/put
                         {:crux.db/id :p3
                          :entry-pt :LA
                          :arrival-time #inst "2018-12-31"
                          :departure-time :na}
                         #inst "2018-12-31"]])
  ;; Day 1, nothing happens.
  (api/submit-tx *api* [])
  ;; Day 2
  (api/submit-tx *api* [[:crux.tx/put
                         {:crux.db/id :p4
                          :entry-pt :NY
                          :arrival-time #inst "2019-01-02"
                          :departure-time :na}
                         #inst "2019-01-02"]])
  ;; Day 3
  (let [third-day-submitted-tx (api/submit-tx *api* [[:crux.tx/put
                                                      {:crux.db/id :p4
                                                       :entry-pt :NY
                                                       :arrival-time #inst "2019-01-02"
                                                       :departure-time #inst "2019-01-03"}
                                                      #inst "2019-01-03"]])]
    ;; this introduces enough delay s.t. tx3 and tx4 don't happen in the same millisecond
    ;; (see test further down)
    ;; because we only ask for the DB at the tx-time, currently, not the tx-id
    ;; see #421
    (api/await-tx *api* third-day-submitted-tx)

    ;; Day 4, correction, adding missing trip on new arrival.
    (api/submit-tx *api* [[:crux.tx/put
                           {:crux.db/id :p1
                            :entry-pt :NY
                            :arrival-time #inst "2018-12-31"
                            :departure-time :na}
                           #inst "2018-12-31"]
                          [:crux.tx/put
                           {:crux.db/id :p1
                            :entry-pt :NY
                            :arrival-time #inst "2018-12-31"
                            :departure-time #inst "2019-01-03"}
                           #inst "2019-01-03"]
                          [:crux.tx/put
                           {:crux.db/id :p1
                            :entry-pt :LA
                            :arrival-time #inst "2019-01-04"
                            :departure-time :na}
                           #inst "2019-01-04"]
                          [:crux.tx/put
                           {:crux.db/id :p3
                            :entry-pt :LA
                            :arrival-time #inst "2018-12-31"
                            :departure-time #inst "2019-01-04"}
                           #inst "2019-01-04"]])
    ;; Day 5
    (api/submit-tx *api* [[:crux.tx/put
                           {:crux.db/id :p2
                            :entry-pt :SFO
                            :arrival-time #inst "2018-12-31"
                            :departure-time #inst "2018-12-31"}
                           #inst "2019-01-05"]])
    ;; Day 6, nothing happens.
    (api/submit-tx *api* [])
    ;; Day 7-12, correction of deletion/departure on day 4. Shows
    ;; how valid time cannot be the same as arrival time.
    (api/submit-tx *api* [[:crux.tx/put
                           {:crux.db/id :p3
                            :entry-pt :LA
                            :arrival-time #inst "2018-12-31"
                            :departure-time :na}
                           #inst "2019-01-04"]
                          [:crux.tx/put
                           {:crux.db/id :p3
                            :entry-pt :LA
                            :arrival-time #inst "2018-12-31"
                            :departure-time #inst "2019-01-07"}
                           #inst "2019-01-07"]])
    (api/submit-tx *api* [[:crux.tx/put
                           {:crux.db/id :p3
                            :entry-pt :SFO
                            :arrival-time #inst "2019-01-08"
                            :departure-time :na}
                           #inst "2019-01-08"]
                          [:crux.tx/put
                           {:crux.db/id :p4
                            :entry-pt :LA
                            :arrival-time #inst "2019-01-08"
                            :departure-time :na}
                           #inst "2019-01-08"]])
    (api/submit-tx *api* [[:crux.tx/put
                           {:crux.db/id :p3
                            :entry-pt :SFO
                            :arrival-time #inst "2019-01-08"
                            :departure-time #inst "2019-01-08"}
                           #inst "2019-01-09"]])
    (api/submit-tx *api* [[:crux.tx/put
                           {:crux.db/id :p5
                            :entry-pt :LA
                            :arrival-time #inst "2019-01-10"
                            :departure-time :na}
                           #inst "2019-01-10"]])
    (api/submit-tx *api* [[:crux.tx/put
                           {:crux.db/id :p7
                            :entry-pt :NY
                            :arrival-time #inst "2019-01-11"
                            :departure-time :na}
                           #inst "2019-01-11"]])

    (doto (api/submit-tx *api* [[:crux.tx/put
                                 {:crux.db/id :p6
                                  :entry-pt :NY
                                  :arrival-time #inst "2019-01-12"
                                  :departure-time :na}
                                 #inst "2019-01-12"]])
      (->> (api/await-tx *api*)))

    (t/is (= #{[:p2 :SFO #inst "2018-12-31" :na]
               [:p3 :LA #inst "2018-12-31" :na]
               [:p4 :NY #inst "2019-01-02" :na]}
             (api/q (api/db *api* #inst "2019-01-02" (:crux.tx/tx-time third-day-submitted-tx))
                    '{:find [p entry-pt arrival-time departure-time]
                      :where [[p :entry-pt entry-pt]
                              [p :arrival-time arrival-time]
                              [p :departure-time departure-time]]})))))

;; Tests borrowed from Datascript:
;; https://github.com/tonsky/datascript/tree/master/test/datascript/test

(defn- populate-datascript-test-db []
  (fix/transact! *api* [{:crux.db/id :1 :name "Ivan" :age 10}
                        {:crux.db/id :2 :name "Ivan" :age 20}
                        {:crux.db/id :3 :name "Oleg" :age 10}
                        {:crux.db/id :4 :name "Oleg" :age 20}
                        {:crux.db/id :5 :name "Ivan" :age 10}
                        {:crux.db/id :6 :name "Ivan" :age 20}]))

(t/deftest datascript-test-not
  (populate-datascript-test-db)
  (let [db (api/db *api*)]
    (t/are [q res] (= (api/q db {:find '[?e] :where (quote q)})
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
  (let [db (api/db *api*)]
    (t/is (= (api/q db
                    '{:find [?e ?a]
                      :where [[?e :name]
                              [?e :age  ?a]
                              (not-join [?e]
                                        [?e :name "Oleg"]
                                        [?e :age ?a])]})
             #{[:1 10] [:2 20] [:5 10] [:6 20]}))

    (t/is (= (api/q db
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
  (let [db (api/db *api*)]
    (t/are [q res] (= (api/q db {:find '[?e] :where (quote q)})
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
    (t/is (= (api/q db
                    '{:find [?e ?e2]
                      :where [[?e  :name "Ivan"]
                              [?e2 :name "Ivan"]
                              (not [?e :age 10]
                                   [?e2 :age 20])]})
             #{[:2 :1] [:6 :5] [:1 :1] [:2 :2] [:5 :5] [:6 :6] [:2 :5] [:1 :5] [:2 :6] [:6 :1] [:5 :1] [:6 :2]}))

    ;; 2 rels \ rel + const
    (t/is (= (api/q db
                    '{:find [?e ?e2]
                      :where [[?e  :name "Ivan"]
                              [?e2 :name "Oleg"]
                              (not [?e :age 10]
                                   [?e2 :age 20])]})
             #{[:2 :3] [:1 :3] [:2 :4] [:6 :3] [:5 :3] [:6 :4]}))

    ;; 2 rels \ 2 consts
    (t/is (= (api/q db
                    '{:find [?e ?e2]
                      :where [[?e  :name "Oleg"]
                              [?e2 :name "Oleg"]
                              (not [?e :age 10]
                                   [?e2 :age 20])]})
             #{[:4 :3] [:3 :3] [:4 :4]}))))

(t/deftest datascript-test-or
  (populate-datascript-test-db)
  (let [db (api/db *api*)]
    (t/are [q res]  (= (api/q db {:find '[?e] :where (quote q)})
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
  (let [db (api/db *api*)]
    (t/are [q res] (= (api/q db {:find '[?e] :where (quote q)})
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
  (fix/transact! *api* [{:crux.db/id :5 :follow :3}
                        {:crux.db/id :1 :follow :2}
                        {:crux.db/id :2 :follow #{:3 :4}}
                        {:crux.db/id :3 :follow :4}
                        {:crux.db/id :4 :follow :6}])
  (let [db (api/db *api*)]
    (t/is (= (api/q db
                    '{:find  [?e1 ?e2]
                      :where [(follow ?e1 ?e2)]
                      :rules [[(follow ?x ?y)
                               [?x :follow ?y]]]})
             #{[:1 :2] [:2 :3] [:3 :4] [:2 :4] [:5 :3] [:4 :6]}))

    ;; NOTE: Crux does not support vars in attribute position, so
    ;; :follow is explicit.
    (t/testing "Joining regular clauses with rule"
      (t/is (= (api/q db
                      '{:find [?y ?x]
                        :where [[_ :follow ?x]
                                (rule ?x ?y)
                                [(crux.query-test/even-kw? ?x)]]
                        :rules [[(rule ?a ?b)
                                 [?a :follow ?b]]]})
               #{[:3 :2] [:6 :4] [:4 :2]})))

    ;; NOTE: Crux does not support vars in attribute position.
    #_(t/testing "Rule context is isolated from outer context"
        (t/is (= (api/q db
                        '{:find [?x]
                          :where [[?e _ _]
                                  (rule ?x)]
                          :rules [[(rule ?e)
                                   [_ ?e _]]]})
                 #{[:follow]})))

    (t/testing "Rule with branches"
      (t/is (= (api/q db
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
      (t/is (= (api/q db
                      '{:find  [?e2]
                        :where [(follow ?e1 ?e2)]
                        :args [{:?e1 :1}]
                        :rules [[(follow ?e1 ?e2)
                                 [?e1 :follow ?e2]]
                                [(follow ?e1 ?e2)
                                 [?e1 :follow ?t]
                                 (follow ?t ?e2)]]})
               #{[:2] [:3] [:4] [:6]})))

    (t/testing "Passing ins to rule"
      (t/is (= (api/q db
                      {:find '[?x ?y]
                       :where '[(match ?even ?x ?y)]
                       :rules '[[(match ?pred ?e ?e2)
                                 [?e :follow ?e2]
                                 [(?pred ?e)]
                                 [(?pred ?e2)]]]
                       :args [{:?even even-kw?}]})
               #{[:4 :6] [:2 :4]})))

    (t/testing "Using built-ins inside rule"
      (t/is (= (api/q db
                      '{:find [?x ?y]
                        :where [(match ?x ?y)]
                        :rules [[(match ?e ?e2)
                                 [?e :follow ?e2]
                                 [(crux.query-test/even-kw? ?e)]
                                 [(crux.query-test/even-kw? ?e2)]]]})
               #{[:4 :6] [:2 :4]})))))

(t/deftest test-rules-with-recursion-1
  (fix/transact! *api* [{:crux.db/id :1 :follow :2}
                        {:crux.db/id :2 :follow :3}
                        {:crux.db/id :3 :follow :1}])
  (t/is (= (api/q (api/db *api*)
                  '{:find [?e1 ?e2]
                    :where [(follow ?e1 ?e2)]
                    :rules [[(follow ?e1 ?e2)
                             [?e1 :follow ?e2]]
                            [(follow ?e1 ?e2)
                             (follow ?e2 ?e1)]]})
           #{[:1 :2] [:2 :3] [:3 :1] [:2 :1] [:3 :2] [:1 :3]})))

(t/deftest test-rules-with-recursion-2
  (fix/transact! *api* [{:crux.db/id :1 :follow :2}
                        {:crux.db/id :2 :follow :3}])
  (t/is (= (api/q (api/db *api*)
                  '{:find [?e1 ?e2]
                    :where [(follow ?e1 ?e2)]
                    :rules [[(follow ?e1 ?e2)
                             [?e1 :follow ?e2]]
                            [(follow ?e1 ?e2)
                             (follow ?e2 ?e1)]]})
           #{[:1 :2] [:2 :3] [:2 :1] [:3 :2]})))

(t/deftest test-calling-rule-twice-44
  (fix/transact! *api* [{:crux.db/id :1 :attr "a"}])
  (let [db (api/db *api*)]
    (t/is (api/q db
                 {:find '[?p]
                  :where '[(rule ?p ?fn "a")
                           (rule ?p ?fn "b")]
                  :rules '[[(rule ?p ?fn ?x)
                            [?p :attr ?x]
                            [(?fn ?x)]]]
                  :args [{:?fn (constantly true)}]}))))

(t/deftest test-mutually-recursive-rules
  (fix/transact! *api* [{:crux.db/id :0 :f1 :1}
                        {:crux.db/id :1 :f2 :2}
                        {:crux.db/id :2 :f1 :3}
                        {:crux.db/id :3 :f2 :4}
                        {:crux.db/id :4 :f1 :5}
                        {:crux.db/id :5 :f2 :6}])
  (let [db (api/db *api*)]
    (t/is (= (api/q db
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
               [:4 :5]}))))

;; https://github.com/tonsky/datascript/issues/218
(t/deftest datascript-test-rules-false-arguments
  (fix/transact! *api* [{:crux.db/id :1 :attr true}
                        {:crux.db/id :2 :attr false}])
  (let [db (api/db *api*)
        rules '[[(is ?id ?val)
                 [?id :attr ?val]]]]
    (t/is (= (api/q db
                    {:find '[?id]
                     :where '[(is ?id true)]
                     :rules rules})
             #{[:1]}))
    (t/is (= (api/q db
                    {:find '[?id]
                     :where '[(is ?id false)]
                     :rules rules})
             #{[:2]}))))

(defn- even-or-nil? [x]
  (when (even? x)
    x))

(t/deftest data-script-test-query-fns
  (fix/transact! *api* [{:crux.db/id :1 :name "Ivan" :age 15}
                        {:crux.db/id :2 :name "Petr" :age 22 :height 240 :parent :1}
                        {:crux.db/id :3 :name "Slava" :age 37 :parent :2}])
  (let [db (api/db *api*)]
    (t/testing "predicate without free variables"
      (t/is (= (api/q db
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
        (t/is (= (api/q '[:find ?e ?age
                          :in $
                          :where [?e :age ?age]
                          [(missing? $ ?e :height)]] db)
                 #{[1 15] [3 37]})))

    #_(t/testing "missing? back-ref"
        (t/is (= (api/q '[:find ?e
                          :in $
                          :where [?e :age ?age]
                          [(missing? $ ?e :_parent)]] db)
                 #{[3]})))

    (t/testing "Built-ins"
      (t/is (= (api/q db
                      '{:find [?e1 ?e2]
                        :where [[?e1 :age ?a1]
                                [?e2 :age ?a2]
                                [(< ?a1 18 ?a2)]]})
               #{[:1 :2] [:1 :3]}))

      (t/is (= (api/q db
                      '{:find [?x ?c]
                        :args [{:?x "a"}
                               {:?x "abc"}]
                        :where [[(count ?x) ?c]]})
               #{["a" 1] ["abc" 3]})))

    (t/testing "Built-in vector, hashmap"
      ;; Crux differs here by treating vectors and sets as multiple
      ;; results.
      #_(t/is (= (api/q db
                        '{:find [?tx-data]
                          :where [[(identity :db/add) ?op]
                                  [(vector ?op -1 :attr 12) ?tx-data]]})
                 #{[[:db/add -1 :attr 12]]}))

      (t/is (= (api/q db
                      '{:find [?tx-data]
                        :where
                        [[(hash-map :db/id -1 :age 92 :name "Aaron") ?tx-data]]})
               #{[{:db/id -1 :age 92 :name "Aaron"}]})))


    (t/testing "Passing predicate as source"
      (t/is (= (api/q db
                      {:find '[?e]
                       :where '[[?e :age ?a]
                                [(?adult ?a)]]
                       :args [{:?adult #(> % 18)}]})
               #{[:2] [:3]})))

    (t/testing "Calling a function"
      (t/is (= (api/q db
                      '{:find [?e1 ?e2 ?e3]
                        :where [[?e1 :age ?a1]
                                [?e2 :age ?a2]
                                [?e3 :age ?a3]
                                [(+ ?a1 ?a2) ?a12]
                                [(= ?a12 ?a3)]]})
               #{[:1 :2 :3] [:2 :1 :3]})))

    (t/testing "Two conflicting function values for one binding."
      (t/is (= (api/q db
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
      (t/is (= (api/q db
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
      (t/is (= (api/q db
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
      (t/is (= (api/q db
                      '{:find [?age]
                        :where [[_ :age ?age]
                                [(identity 100) ?age]]})
               #{})))

    (t/testing "Conflicting relational bindings with function binding. (fn, rel)"
      (t/is (= (api/q db
                      '{:find [?age]
                        :where [[(identity 100) ?age]
                                [_ :age ?age]]})
               #{})))

    (t/testing "Function on empty rel"
      (t/is (= (api/q db
                      '{:find [?e ?y]
                        :where [[?e :salary ?x]
                                [(+ ?x 100) ?y]
                                [:0 :age 15]
                                [:1 :age 35]]})
               #{})))

    ;; Crux supports nil and false results.
    #_(t/testing "Returning nil from function filters out tuple from result"
        (t/is (= (api/q db
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
  (fix/transact! *api* [{:crux.db/id :1 :name "Ivan" :age 10}
                        {:crux.db/id :2 :name "Ivan" :age 20}
                        {:crux.db/id :3 :name "Oleg" :age 10}
                        {:crux.db/id :4 :name "Oleg" :age 20}])
  (let [db (api/db *api*)]
    (t/are [q res] (= (api/q db (quote q)) res)
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
        (t/is (= (api/q '[:find ?e
                          :in $ ?pred
                          :where [?e :age ?a]
                          [(?pred $ ?e 10)]]
                        db pred)
                 #{[1] [3]})))))


(t/deftest datascript-test-issue-180
  (fix/transact! *api* [{:crux.db/id :1 :age 20}])
  (let [db (api/db *api*)]
    (t/is (= #{}
             (api/q db
                    '{:find [?e ?a]
                      :where [[_ :pred ?pred]
                              [?e :age ?a]
                              [(?pred ?a)]]})))))

(defn sample-query-fn [] 42)

(t/deftest datascript-test-symbol-resolution
  (let [db (api/db *api*)]
    (t/is (= #{[42]} (api/q db
                            '{:find [?x]
                              :where [[(crux.query-test/sample-query-fn) ?x]]})))))

(t/deftest test-can-bind-function-returns-to-falsy
  ;; Datomic does allow binding falsy values, DataScript doesn't
  ;; see "Returning nil from function filters out tuple from result"
  (t/is (= #{[false]}
           (api/q (api/db *api*)
                  '{:find [b]
                    :where [[(identity false) b]]})))

  (t/is (= #{[nil]}
           (api/q (api/db *api*)
                  '{:find [b]
                    :where [[(identity nil) b]]})))

  (t/is (= #{[true]}
           (api/q (api/db *api*)
                  '{:find [b]
                    :where [[(identity true) b]]}))))

(t/deftest test-can-bind-function-returns-to-multiple-values
  (t/testing "vectors"
    (t/is (= #{[1]
               [2]}
             (api/q (api/db *api*)
                    '{:find [x]
                      :where [[(vector 1 2) x]]})))

    (t/is (= #{}
           (api/q (api/db *api*)
                  '{:find [x]
                    :where [[(vector) x]]}))))

  (t/testing "sets"
    (t/is (= #{[1]
               [2]}
             (api/q (api/db *api*)
                    '{:find [x]
                      :where [[(sorted-set 1 2) x]]})))



    (t/is (= #{}
             (api/q (api/db *api*)
                    '{:find [x]
                      :where [[(sorted-set) x]]}))))

  (t/testing "maps are treated as single values"
    (t/is (= #{[{1 2}]}
             (api/q (api/db *api*)
                    '{:find [x]
                      :where [[(hash-map 1 2) x]]})))

    (t/is (= #{[{}]}
             (api/q (api/db *api*)
                    '{:find [x]
                      :where [[(hash-map) x]]})))))

(t/deftest test-can-use-any-value-as-entity-id
  (fix/transact! *api* [{:crux.db/id "ivan@example.com" :name "Ivan"}
                        {:crux.db/id 42 :name "Petr"}
                        {:crux.db/id true :name "Oleg" :friends ["ivan@example.com" 42 3.14]}
                        {:crux.db/id 3.14 :name "Pi" :boss "ivan@example.com"}])
  (t/is (= #{["Ivan"]}
           (api/q (api/db *api*)
                  '{:find [name]
                    :where [["ivan@example.com" :name name]]})))
  (t/is (= #{["Petr"]}
           (api/q (api/db *api*)
                  '{:find [name]
                    :where [[42 :name name]]})))
  (t/is (= #{["Oleg"]}
           (api/q (api/db *api*)
                  '{:find [name]
                    :where [[true :name name]]})))
  (t/is (= #{["Pi"]}
           (api/q (api/db *api*)
                  '{:find [name]
                    :where [[3.14 :name name]]})))

  (t/testing "join"
    (t/is (= #{["ivan@example.com" "Ivan"]}
             (api/q (api/db *api*)
                    '{:find [boss name]
                      :where [[boss :name name]
                              [pi :boss boss]
                              [pi :name "Pi"]]})))

    (t/is (= #{["Ivan"]
               ["Petr"]
               ["Pi"]}
             (api/q (api/db *api*)
                    '{:find [name]
                      :where [[true :friends f]
                              [f :name name]]})))))

;; Tests from Racket Datalog
;; https://github.com/racket/datalog/tree/master/tests/examples

(t/deftest test-racket-datalog-tutorial
  ;; parent(john,douglas).
  (fix/transact! *api* [{:crux.db/id :john :parent :douglas}])
  ;; parent(john,douglas)?
  (t/is (= #{[true]}
           (api/q (api/db *api*)
                  '{:find [found]
                    :where [[:john :parent :douglas]
                            [(identity true) found]]})))

  ;; parent(john,ebbon)?
  (t/is (= #{}
           (api/q (api/db *api*)
                  '{:find [found]
                    :where [[:john :parent :ebbon]
                            [(identity true) found]]})))

  ;; parent(bob,john).
  ;; parent(ebbon,bob).
  (fix/transact! *api* [{:crux.db/id :bob :parent :john}
                        {:crux.db/id :ebbon :parent :bob}])

  ;; parent(A,B)?
  (t/is (= #{[:john :douglas]
             [:bob :john]
             [:ebbon :bob]}
           (api/q (api/db *api*)
                  '{:find [a b]
                    :where [[a :parent b]]})))

  ;; parent(john,B)?
  (t/is (= #{[:douglas]}
           (api/q (api/db *api*)
                  '{:find [ b]
                    :where [[:john :parent b]]})))

  ;; parent(A,A)?
  (t/is (= #{}
           (api/q (api/db *api*)
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
           (api/q (api/db *api*)
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
           (api/q (api/db *api*)
                  '{:find [x]
                    :where [(ancestor x :john)]
                    :rules [[(ancestor a b)
                             [a :parent b]]
                            [(ancestor a b)
                             [a :parent c]
                             (ancestor c b)]]})))

  (let [db-before (api/db *api*)]
    ;; parent(bob, john)-
    (fix/submit+await-tx [[:crux.tx/delete :bob]])
    ;; parent(A,B)?
    (t/is (= #{[:john :douglas]
               [:ebbon :bob]}
             (api/q (api/db *api*)
                    '{:find [a b]
                      :where [[a :parent b]]})))

    ;; ancestor(A,B)?
    (t/is (= #{[:ebbon :bob]
               [:john :douglas]}
             (api/q (api/db *api*)
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
               (api/q db-before
                      '{:find [a b]
                        :where [(ancestor a b)]
                        :rules [[(ancestor a b)
                                 [a :parent b]]
                                [(ancestor a b)
                                 [a :parent c]
                                 (ancestor c b)]]}))))))

(t/deftest test-racket-datalog-path
  ;; edge(a, b). edge(b, c). edge(c, d). edge(d, a).
  (fix/transact! *api* [{:crux.db/id :a :edge :b}
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
           (api/q (api/db *api*)
                  '{:find [x y]
                    :where [(path x y)]
                    :rules [[(path x y)
                             [x :edge y]]
                            [(path x y)
                             [x :edge z]
                             (path z y)]]}))))

(t/deftest test-racket-datalog-revpath
  ;; edge(a, b). edge(b, c). edge(c, d). edge(d, a).
  (fix/transact! *api* [{:crux.db/id :a :edge :b}
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
           (api/q (api/db *api*)
                  '{:find [x y]
                    :where [(path x y)]
                    :rules [[(path x y)
                             [x :edge y]]
                            [(path x y)
                             (path x z)
                             [z :edge y]]]}))))

(t/deftest test-racket-datalog-bidipath
  ;; edge(a, b). edge(b, c). edge(c, d). edge(d, a).
  (fix/transact! *api* [{:crux.db/id :a :edge :b}
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
           (api/q (api/db *api*)
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
  (fix/transact! *api* [{:crux.db/id :a}
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
           (api/q (api/db *api*)
                  '{:find [x y]
                    :where [(perm x y)]
                    :rules [[(perm x y)
                             [x :crux.db/id]
                             [y :crux.db/id]
                             [(!= x y)]]]}))))

(t/deftest test-failing-predicates-at-top-level-bug
  (t/testing "predicate order shouldn't matter"
    (t/is (= #{}
             (api/q (api/db *api*)
                    '{:find [f]
                      :where [[(identity 4) f]
                              [(identity false)]]})))

    (t/is (= #{}
             (api/q (api/db *api*)
                    '{:find [f]
                      :where [[(identity false)]
                              [(identity 4) f]]})))))

(t/deftest test-literal-rule-arguments-bug-507
  (t/testing "range clause in rule"
    ;; supplying 4 as the rule arg here is a necessary condition
    (t/is (= #{}
             (api/q (api/db *api*)
                    '{:find [f]
                      :where [(foo 4 f)]
                      :rules [[(foo n f)
                               [(<= 6 n)]
                               [(identity n) f]]]})))

    (t/testing "predicates work for non-numeric comparables too"
      (t/is (= #{}
               (api/q (api/db *api*)
                      '{:find [f]
                        :where [(foo #inst "2019" f)]
                        :rules [[(foo n f)
                                 [(<= #inst "2020" n)]
                                 [(identity n) f]]]}))))))

;; Test from Racket Datalog documentation:
;; https://docs.racket-lang.org/datalog/datalog.html
(t/deftest test-racket-datalog-fib
  ;; fib(0, 0).
  ;; fib(1, 1).

  ;;  fib(N, F) :- N != 1,
  ;;               N != 0,
  ;;               N1 :- -(N, 1),
  ;;               N2 :- -(N, 2),
  ;;               fib(N1, F1),
  ;;               fib(N2, F2),
  ;;               F :- +(F1, F2).

  ;;  fib(10, F)?
  (let [fib-rules '[[(fib n f)
                     [(<= n 1)]
                     [(identity n) f]]
                    [(fib n f)
                     [(> n 1)]
                     [(- n 1) n1]
                     [(- n 2) n2]
                     (fib n1 f1)
                     (fib n2 f2)
                     [(+ f1 f2) f]]]]
    (t/is (= #{[55]}
             (api/q (api/db *api*)
                    {:find '[f]
                     :where '[(fib 10 f)]
                     :rules fib-rules})))

    (t/is (= #{[55]}
             (api/q (api/db *api*)
                    {:find '[f]
                     :where '[(fib n f)]
                     :args '[{n 10}]
                     :rules fib-rules})))))

;; Tests from
;; https://pdfs.semanticscholar.org/9374/f0da312f3ba77fa840071d68935a28cba364.pdf

(t/deftest test-datalog-paper-sgc
  (fix/transact! *api* [{:crux.db/id :ann :parent #{:dorothy :hilary}}
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
           (api/q (api/db *api*)
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
  (fix/transact! *api* [{:crux.db/id :a :d :b}
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
               (api/q (api/db *api*)
                      {:find '[x y]
                       :where '[(r x y)]
                       :rules rules}))))

    (t/testing "stratum 2"
      (t/is (= #{[:a :b]
                 [:b :c]
                 [:a :c]
                 [:b :a]}
               (api/q (api/db *api*)
                      {:find '[x y]
                       :where '[(or (q x y)
                                    (s x y))]
                       :rules rules}))))

    (t/testing "stratum 3"
      (t/is (= #{[:b :a]}
               (api/q (api/db *api*)
                      {:find '[x y]
                       :where '[(p x y)]
                       :rules rules}))))))

(t/deftest test-query-against-empty-database-376
  (let [db (api/db *api*)
        _ (t/is (not (api/entity db :a)))
        _ (fix/transact! *api* [{:crux.db/id :a
                                 :arbitrary-key ["an untyped value" 123]
                                 :nested-map {"and values" :can-be-arbitrarily-nested}}])]
    (t/is (not (api/entity db :a)))
    (t/is (api/entity (api/db *api*) :a))

    (with-open [res (api/open-q db '{:find [x] :where [[x :arbitrary-key _]]})]
      (t/is (empty? (iterator-seq res))))

    (let [db (api/db *api*)]
      (with-open [res (api/open-q db '{:find [x] :where [[x :crux.db/id _]]})]
        (t/is (first (iterator-seq res)))))))

(t/deftest test-can-use-cons-in-query-377
  (fix/transact! *api* [{:crux.db/id :issue-377-test :name "TestName"}])
  (t/is (= #{[:issue-377-test]}
           (api/q (api/db *api*)
                  {:find ['e]
                   :where [['e :name 'n]
                           [(cons '= '(n "TestName"))]]}))))

(t/deftest test-query-keyword-to-entity-tx-351
  (fix/transact! *api* [{:crux.db/id :se.id/ASE,
                         :se/currency :currency/usd}
                        {:crux.db/id :ids/ticker-1000 ;;ids/ticker
                         :ticker/price 67
                         :ticker/market :se.id/ASE
                         :ticker/foo :bar}])

  (t/is (seq (api/q (api/db *api*) '{:find [p]
                                     :where
                                     [[e :crux.db/id someid]
                                      [e :ticker/price p]
                                      [(= p 67)]
                                      [e :ticker/market m2]
                                      [m2 :se/currency :currency/usd]]}))))

(t/deftest test-order-by-when-not-specified-in-return-418
  (fix/transact! *api* [{:crux.db/id :one
                         :val 1}
                        {:crux.db/id :two
                         :val 2}
                        {:crux.db/id :three
                         :val 3}])

  (t/is (= [:three :two :one]
           (mapv first (api/q (api/db *api*) '{:find [e v]
                                               :where [[x :crux.db/id e]
                                                       [x :val v]]
                                               :order-by [[v :desc]]}))))

  (t/is (= [:one :two :three]
           (mapv first (api/q (api/db *api*) '{:find [e v]
                                               :where [[x :crux.db/id e]
                                                       [x :val v]]
                                               :order-by [[v :asc]]}))))
  (t/is (thrown-with-msg? IllegalArgumentException
                          #"Order by requires a var from :find\. unreturned var:"
                          (api/q (api/db *api*) '{:find [e]
                                                  :where [[x :crux.db/id e]
                                                          [x :val v]]
                                                  :order-by [[v :asc]]})))
  (t/is (thrown-with-msg? IllegalArgumentException
                          #"Order by requires a var from :find\. unreturned var:"
                          (api/q (api/db *api*) '{:find [e]
                                                  :where [[x :crux.db/id e]
                                                          [x :val v]]
                                                  :order-by [[v :desc]]}))))

(t/deftest test-query-with-timeout-419
  (fix/transact! *api* [{:crux.db/id :ivan :name "Ivan" :last-name "Ivanov"}
                        {:crux.db/id :petr :name "Petr" :last-name "Petrov"}])

  (t/is (= #{[:ivan] [:petr]}
           (api/q (api/db *api*) '{:find [e]
                                   :where [[e :crux.db/id _]]
                                   :timeout 100}))))

(t/deftest test-nil-query-attribute-453
  (fix/transact! *api* [{:crux.db/id :id :this :that :these :those}])
  (t/is (thrown-with-msg?
          RuntimeException
          #"Spec assertion failed"
          (= #{[:id]} (api/q (api/db *api*) {:find ['e] :where [['_ nil 'e]]})))))

;; TODO: Unsure why this test has started failing, or how much it
;; matters?
#_(t/deftest test-entity-snapshot-520
    (let [ivan {:crux.db/id :ivan}
          _ (fix/transact! *api* [ivan])
          db (api/db *api*)]
      (with-open [shared-db (api/open-db *api*)]
        (t/is (= ivan (api/entity shared-db :ivan)))
        (let [n 1000
              ;; TODO: was 1.4, introduced a bug?
              acceptable-snapshot-speedup 1.1
              factors (->> #(let [db-hit-ns-start (System/nanoTime)
                                  _ (api/entity db :ivan)
                                  db-hit-ns (- (System/nanoTime) db-hit-ns-start)

                                  snapshot-hit-ns-start (System/nanoTime)
                                  _ (api/entity shared-db :ivan)
                                  snapshot-hit-ns (- (System/nanoTime) snapshot-hit-ns-start)]

                              (double (/ db-hit-ns snapshot-hit-ns)))

                           (repeatedly n))]
          (t/is (>= (/ (reduce + factors) n) acceptable-snapshot-speedup))))))

(t/deftest test-greater-than-range-predicate-545
  (t/is (empty?
         (api/q (api/db *api*)
                '{:find [offset]
                  :where [[e :offset offset]
                          [(> offset -9223372036854775808)]] ;; Long/MIN_VALUE
                  :limit 1})))

  (t/testing "checking that entity ranges don't have same bug"
    (t/is (empty?
           (api/q (api/db *api*)
                  '{:find [offset]
                    :where [[e :offset offset]
                            [(= e :foo)]]
                    :limit 1})))))

(t/deftest test-queries-return-distinct-results-631
  (fix/transact! *api* [{:crux.db/id :ii :name "Ivan" :last-name "Ivanov", :age 20}
                        {:crux.db/id :pp :name "Petr" :last-name "Petrov", :age 20}
                        {:crux.db/id :ip :name "Ivan" :last-name "Petrov", :age 25}
                        {:crux.db/id :pi :name "Petr" :last-name "Ivanov", :age 30}])

  (let [db (api/db *api*)]
    (t/testing "eager query with order-by returns set of results"
      (t/is (= #{[30] [25] [20]}
               (api/q db '{:find [a] :where [[_ :age a]]}))))

    (t/testing "query with order-by returns distinct results"
      (t/is (= [[30] [25] [20]]
               (api/q db '{:find [a] :where [[_ :age a]], :order-by [[a :desc]]}))))

    (t/testing "lazy query returns distinct results"
      (with-open [res (api/open-q db '{:find [a] :where [[_ :age a]]})]
        (t/is (= [[20] [25] [30]]
                 (sort (iterator-seq res))))))))

(t/deftest test-unsorted-args-697
  (fix/transact! *api* [{:crux.db/id :foo-some-bar-nil, :bar nil, :foo true}
                        {:crux.db/id :foo-nil-bar-some, :bar true, :foo nil}
                        {:crux.db/id :foo-some-bar-some, :foo true, :bar true}])

  (t/is (= #{[:foo-some-bar-nil] [:foo-nil-bar-some] [:foo-some-bar-some]}
           (api/q (api/db *api*)
                  '{:find [e]
                    :where [[e :foo f]
                            [e :bar g]]

                    :args [{f true, g true}
                           {f true, g nil}
                           {f nil, g true}]}))))

(t/deftest test-binds-args-before-entities
  (t/is (= ['m 'e]
           (->> (q/query-plan-for {:find '[e]
                                   :where '[[e :foo/type "type"]
                                            [e :foo/id m]]
                                   :args [{'m 1}]}
                                  c/->value-buffer
                                  {})
                :vars-in-join-order
                (filter #{'m 'e})))))

(t/deftest test-binds-against-false-arg-885
  (fix/submit+await-tx [[:crux.tx/put {:crux.db/id :foo, :name "foo", :flag? false}]
                        [:crux.tx/put {:crux.db/id :bar, :name "bar", :flag? true}]
                        [:crux.tx/put {:crux.db/id :baz, :name "baz", :flag? nil}]])

  (t/is (= #{["foo" false]}
           (api/q (api/db *api*)
                  '{:find [?name flag?], :where [[?id :name ?name] [?id :flag? flag?]],
                    :args [{flag? false}]})))

  (t/is (= #{["bar" true]}
           (api/q (api/db *api*)
                  '{:find [?name flag?], :where [[?id :name ?name] [?id :flag? flag?]],
                    :args [{flag? true}]})))

  (t/is (= #{["baz" nil]}
           (api/q (api/db *api*)
                  '{:find [?name flag?], :where [[?id :name ?name] [?id :flag? flag?]],
                    :args [{flag? nil}]}))))

(t/deftest test-unused-args-still-binding-882
  (fix/submit+await-tx [[:crux.tx/put {:crux.db/id :foo, :name "foo"}]])

  (t/is (= #{["foo" false]}
           (api/q (api/db *api*)
                  '{:find [?name foo], :where [[?id :name ?name]],
                    :args [{foo false}]})))

  (t/is (= #{["foo" true]}
           (api/q (api/db *api*)
                  '{:find [?name foo], :where [[?id :name ?name]],
                    :args [{foo true}]})))

  (t/is (= #{["foo" nil]}
           (api/q (api/db *api*)
                  '{:find [?name foo], :where [[?id :name ?name]],
                    :args [{foo nil}]}))))

(t/deftest test-leaf-vars-and-ors
  (fix/submit+await-tx [[:crux.tx/put {:crux.db/id :foo, :field1 1 :field2 2}]])
  (t/is (= #{[:foo]}
           (api/q (api/db *api*)'{:find [?id], :where [[?id :field1 ?field1]
                                                       [?id :field2 ?field2]
                                                       (or (and [(boolean ?field2)]))]
                                  :args []}))))

;; TODO: shows slowdown when scanning several attributes without any
;; filters.
#_(t/deftest test-multiple-fields-sql-performance
    (let [n 10000
          acceptable-slowdown-factor 1.1]
      (fix/transact! *api* (fix/people (repeat n {})))
      (t/is (= n (count (api/q (api/db *api*)
                               '{:find [?e] :where [[?e :crux.db/id]]}))))
      (let [single-field-ns-start (System/nanoTime)]
        (t/is (= n (count (api/q (api/db *api*)
                                 '{:find [?e ?age] :where [[?e :age ?age]]}))))
        (let [single-field-ns (- (System/nanoTime) single-field-ns-start)
              _ (prn :single-field-ns--- single-field-ns)
              multiple-fields-ns-start (System/nanoTime)
              result (api/q (api/db *api*)
                            '{:find [?e ?age ?salary ?sex]
                              :where [[?e :age ?age]
                                      [?e :salary ?salary]
                                      [?e :sex ?sex]]})]
          (t/is (= n (count result)))
          (let [multiple-field-ns (- (System/nanoTime) multiple-fields-ns-start)]
            (prn :multiple-field-ns- multiple-field-ns)
            (t/is (< (double (/ multiple-field-ns single-field-ns)) acceptable-slowdown-factor)
                  (str (/ single-field-ns 1000000.0) " " (/ multiple-field-ns 1000000.0))))

          (let [pred-fn-ns-start (System/nanoTime)]
            (with-open [db (api/open-db *api*)]
              (t/is (= result (api/q db
                                     '{:find [?e ?age ?salary ?sex]
                                       :where [[?e :age ?age]
                                               [(get-attr ?e :salary) ?salary]
                                               [(get-attr ?e :sex) ?sex]]}))))
            (let [pred-fn-ns (- (System/nanoTime) pred-fn-ns-start)]
              (prn :pred-fn-ns-------- pred-fn-ns)
              (t/is (< (double (/ pred-fn-ns single-field-ns)) acceptable-slowdown-factor)
                    (str (/ single-field-ns 1000000.0) " " (/ pred-fn-ns 1000000.0)))))

          (let [doc-fields-ns-start (System/nanoTime)]
            (with-open [db (api/open-db *api*)]
              (let [{:keys [index-store query-engine entity-resolver-fn]} db
                    {:keys [document-store]} query-engine]
                (t/is (= result
                         (set (->> (for [e (db/ae index-store :age nil entity-resolver-fn)
                                         :let [content-hash (c/new-id (entity-resolver-fn e))
                                               doc (-> (db/fetch-docs document-store #{content-hash})
                                                       (get content-hash))
                                               {:keys [crux.db/id age salary sex]} doc
                                               tuple [id age salary sex]]]
                                     (if (not-any? c/multiple-values? tuple)
                                       [tuple]
                                       (for [e (c/vectorize-value id)
                                             age (c/vectorize-value age)
                                             salary (c/vectorize-value salary)
                                             sex (c/vectorize-value sex)]
                                         [e age salary sex])))
                                   (apply concat)))))))
            (let [doc-field-ns (- (System/nanoTime) doc-fields-ns-start)]
              (prn :doc-field-ns------ doc-field-ns)
              (t/is (< (double (/ doc-field-ns single-field-ns)) acceptable-slowdown-factor)
                    (str (/ single-field-ns 1000000.0) " " (/ doc-field-ns 1000000.0)))))

          (let [hardcoded-fields-ns-start (System/nanoTime)]
            (with-open [db (api/open-db *api*)]
              (let [{:keys [index-store query-engine entity-resolver-fn]} db
                    {:keys [document-store]} query-engine]
                (t/is (= result
                         (set (for [e (db/ae index-store :age nil entity-resolver-fn)
                                    :let [e-decoded (db/decode-value index-store e)]
                                    age (db/aev index-store :age e nil entity-resolver-fn)
                                    :let [age-decoded (db/decode-value index-store age)]
                                    salary (db/aev index-store :salary e nil entity-resolver-fn)
                                    :let [salary-decoded (db/decode-value index-store salary)]
                                    sex (db/aev index-store :sex e nil entity-resolver-fn)
                                    :let [sex-decoded (db/decode-value index-store sex)]]
                                [e-decoded
                                 age-decoded
                                 salary-decoded
                                 sex-decoded]))))))
            (let [hardcoded-field-ns (- (System/nanoTime) hardcoded-fields-ns-start)]
              (prn :hardcoded-field-ns hardcoded-field-ns)
              (t/is (< (double (/ hardcoded-field-ns single-field-ns)) acceptable-slowdown-factor)
                    (str (/ single-field-ns 1000000.0) " " (/ hardcoded-field-ns 1000000.0)))))

          (let [full-results-ns-start (System/nanoTime)]
            (with-open [db (api/open-db *api*)]
              (let [{:keys [index-store query-engine entity-resolver-fn]} db
                    {:keys [document-store]} query-engine]
                (t/is (= result
                         (->> (for [[e] (api/q db '{:find [?e] :where [[?e :age]] :full-results? true})
                                    :let [{:keys [crux.db/id age salary sex]} e
                                          tuple [id age salary sex]]]
                                (if (not-any? c/multiple-values? tuple)
                                  [tuple]
                                  (for [e (c/vectorize-value id)
                                        age (c/vectorize-value age)
                                        salary (c/vectorize-value salary)
                                        sex (c/vectorize-value sex)]
                                    [e age salary sex])))
                              (reduce into #{}))))))
            (let [full-results-ns (- (System/nanoTime) full-results-ns-start)]
              (prn :full-results-ns--- full-results-ns)
              (t/is (< (double (/ full-results-ns single-field-ns)) acceptable-slowdown-factor)
                    (str (/ single-field-ns 1000000.0) " " (/ full-results-ns 1000000.0)))))

          (api/q (api/db *api*) '{:find [?e] :where [[?e :age]]})

          (let [projection-ns-start (System/nanoTime)]
            (with-open [db (api/open-db *api*)]
              (let [{:keys [index-store query-engine entity-resolver-fn]} db
                    {:keys [document-store]} query-engine]
                (t/is (= result
                         (->> (for [[e] (api/q db '{:find [?e] :where [[?e :age]]})
                                    :let [content-hash (c/new-id (entity-resolver-fn (db/encode-value index-store e)))
                                          doc (-> (db/fetch-docs document-store #{content-hash})
                                                  (get content-hash))
                                          {:keys [crux.db/id age salary sex]} doc
                                          tuple [id age salary sex]]]
                                (if (not-any? c/multiple-values? tuple)
                                  [tuple]
                                  (for [e (c/vectorize-value id)
                                        age (c/vectorize-value age)
                                        salary (c/vectorize-value salary)
                                        sex (c/vectorize-value sex)]
                                    [e age salary sex])))
                              (reduce into #{}))))))
            (let [projection-ns (- (System/nanoTime) projection-ns-start)]
              (prn :projection-ns----- projection-ns)
              (t/is (< (double (/ projection-ns single-field-ns)) acceptable-slowdown-factor)
                    (str (/ single-field-ns 1000000.0) " " (/ projection-ns 1000000.0)))))))))
