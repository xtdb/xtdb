(ns core2.trip-test
  (:require [core2.trip :as trip]
            [clojure.test :as t]))

(t/deftest test-basic-datalog

  (let [db (->> '[{:db/id :john :parent :douglas}
                  {:db/id :bob :parent :john}
                  {:db/id :ebbon :parent :bob}]
                (trip/transact {})
                :db-after)]
    (t/is (= #{[:ebbon :bob]
               [:bob :john]
               [:john :douglas]
               [:bob :douglas]
               [:ebbon :john]
               [:ebbon :douglas]}

             (trip/q '{:find [?a ?b]
                       :in [$ %]
                       :where [(ancestor ?a ?b)]}
                     db
                     '[[(ancestor ?a ?b)
                        [?a :parent ?b]]
                       [(ancestor ?a ?b)
                        [?a :parent ?c]
                        (ancestor ?c ?b)]]))))

  (let [db (->> '[{:db/id :a :edge :b}
                  {:db/id :b :edge :c}
                  {:db/id :c :edge :d}
                  {:db/id :d :edge :a}]
                (trip/transact {})
                :db-after)]
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

             (trip/q '{:find [?x ?y]
                       :in [$ %]
                       :where [(path ?x ?y)]}
                     db
                     '[[(path ?x ?y)
                        [?x :edge ?y]]
                       [(path ?x ?y)
                        [?x :edge ?z]
                        (path ?z ?y)]]))))

  (t/is (= #{[6 1 3 4 2]}
           (trip/q '[:find (sum ?heads) (min ?heads) (max ?heads) (count ?heads) (count-distinct ?heads)
                     :with ?monster
                     :in [[?monster ?heads]]]
                   [["Cerberus" 3]
                    ["Medusa" 1]
                    ["Cyclops" 1]
                    ["Chimera" 1]])))

  (t/is (= #{[1] [5]}
           (trip/q '[:find ?x
                     :in [[?x ?x]]]
                   [[1 1]
                    [1 2]
                    [5 5]])))

  (t/is (= [3 4 5]
           (trip/q '[:find [?z ...]
                     :in [?x ...]
                     :where
                     [(inc ?y) ?z]
                     [(inc ?x) ?y]]
                   [1 2 3])))

  (t/is (= 55
           (trip/q '[:find ?f .
                     :in $ % ?n
                     :where (fib ?n ?f)]
                   {}
                   '[[(fib [?n] ?f)
                      [(<= ?n 1)]
                      [(identity ?n) ?f]]
                     [(fib [?n] ?f)
                      [(> ?n 1)]
                      [(- ?n 1) ?n1]
                      [(- ?n 2) ?n2]
                      (fib ?n1 ?f1)
                      (fib ?n2 ?f2)
                      [(+ ?f1 ?f2) ?f]]]
                   10))))
