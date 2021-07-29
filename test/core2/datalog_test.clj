(ns core2.datalog-test
  (:require [core2.james-bond :as bond]
            [clojure.test :as t]
            [core2.test-util :as tu]
            [core2.core :as c2]))

(t/use-fixtures :each tu/with-node)

(def ivan+petr
  [[:put {:_id "ivan", :first-name "Ivan", :last-name "Ivanov"}]
   [:put {:_id "petr", :first-name "Petr", :last-name "Petrov"}]])

(t/deftest test-scan
  (let [tx (c2/submit-tx tu/*node* ivan+petr)]
    (t/is (= [{:name "Ivan"}
              {:name "Petr"}]
             (->> (c2/plan-query tu/*node*
                                 '{:find [?name]
                                   :where [[?e :first-name ?name]]}
                                 {:basis-tx tx})
                  (into []))))

    (t/is (= [{:e "ivan", :name "Ivan"}
              {:e "petr", :name "Petr"}]
             (->> (c2/plan-query tu/*node*
                                 '{:find [?e ?name]
                                   :where [[?e :first-name ?name]]}
                                 {:basis-tx tx})
                  (into [])))
          "returning eid")))

(t/deftest test-basic-query
  (let [tx (c2/submit-tx tu/*node* ivan+petr)]
    (t/is (= [{:e "ivan"}]
             (->> (c2/plan-query tu/*node*
                                 '{:find [?e]
                                   :where [[?e :first-name "Ivan"]]}
                                 {:basis-tx tx})
                  (into [])))
          "query by single field")

    (t/is (= [{:first-name "Petr", :last-name "Petrov"}]
             (->> (c2/plan-query tu/*node*
                                 '{:find [?first-name ?last-name]
                                   :where [[?e :first-name "Petr"]
                                           [?e :first-name ?first-name]
                                           [?e :last-name ?last-name]]}
                                 {:basis-tx tx})
                  (into [])))
          "returning the queried field")

    (t/is (= [{:first-name "Petr", :last-name "Petrov"}]
             (->> (c2/plan-query tu/*node*
                                 '{:find [?first-name ?last-name]
                                   :where [["petr" :first-name ?first-name]
                                           ["petr" :last-name ?last-name]]}
                                 {:basis-tx tx})
                  (into [])))
          "literal eid")))

(t/deftest test-order-by
  (let [tx (c2/submit-tx tu/*node* ivan+petr)]
    (t/is (= [{:first-name "Ivan"} {:first-name "Petr"}]
             (->> (c2/plan-query tu/*node*
                                 '{:find [?first-name]
                                   :where [[?e :first-name ?first-name]]
                                   :order-by [?first-name]}
                                 {:basis-tx tx})
                  (into []))))

    (t/is (= [{:first-name "Petr"} {:first-name "Ivan"}]
             (->> (c2/plan-query tu/*node*
                                 '{:find [?first-name]
                                   :where [[?e :first-name ?first-name]]
                                   :order-by [?first-name :desc]}
                                 {:basis-tx tx})
                  (into []))))

    (t/is (= [{:first-name "Ivan"}]
             (->> (c2/plan-query tu/*node*
                                 '{:find [?first-name]
                                   :where [[?e :first-name ?first-name]]
                                   :order-by [?first-name]
                                   :limit 1}
                                 {:basis-tx tx})
                  (into []))))

    (t/is (= [{:first-name "Petr"}]
             (->> (c2/plan-query tu/*node*
                                 '{:find [?first-name]
                                   :where [[?e :first-name ?first-name]]
                                   :order-by [?first-name :desc]
                                   :limit 1}
                                 {:basis-tx tx})
                  (into []))))

    (t/is (= [{:first-name "Petr"}]
             (->> (c2/plan-query tu/*node*
                                 '{:find [?first-name]
                                   :where [[?e :first-name ?first-name]]
                                   :order-by [?first-name]
                                   :limit 1
                                   :offset 1}
                                 {:basis-tx tx})
                  (into []))))))

;; https://github.com/tonsky/datascript/blob/1.1.0/test/datascript/test/query.cljc#L12-L36
(t/deftest datascript-test-joins
  (let [tx (c2/submit-tx tu/*node*
                         [[:put {:_id 1, :name "Ivan", :age 15}]
                          [:put {:_id 2, :name "Petr", :age 37}]
                          [:put {:_id 3, :name "Ivan", :age 37}]
                          [:put {:_id 4, :age 15}]])]

    (t/is (= #{{:e 1} {:e 2} {:e 3}}
             (->> (c2/plan-query tu/*node*
                                 '{:find [?e]
                                   :where [[?e :name]]}
                                 {:basis-tx tx})
                  (into #{})))
          "testing without V")

    (t/is (= #{{:e 1, :v 15} {:e 3, :v 37}}
             (->> (c2/plan-query tu/*node*
                                 '{:find [?e ?v]
                                   :where [[?e :name "Ivan"]
                                           [?e :age ?v]]}
                                 {:basis-tx tx})
                  (into #{}))))

    (t/is (= #{{:e1 1, :e2 1}
               {:e1 2, :e2 2}
               {:e1 3, :e2 3}
               {:e1 1, :e2 3}
               {:e1 3, :e2 1}}
             (->> (c2/plan-query tu/*node*
                                 '{:find [?e1 ?e2]
                                   :where [[?e1 :name ?n]
                                           [?e2 :name ?n]]}
                                 {:basis-tx tx})
                  (into #{}))))

    (t/is (= #{{:e 1, :e2 1, :n "Ivan"}
               {:e 3, :e2 3, :n "Ivan"}
               {:e 3, :e2 2, :n "Petr"}}
             (->> (c2/plan-query tu/*node*
                                 '{:find [?e ?e2 ?n]
                                   :where [[?e :name "Ivan"]
                                           [?e :age ?a]
                                           [?e2 :age ?a]
                                           [?e2 :name ?n]]}
                                 {:basis-tx tx})
                  (into #{}))))

    (t/is (= #{{:e 1, :e2 1, :n "Ivan"}
               {:e 2, :e2 2, :n "Petr"}
               {:e 3, :e2 3, :n "Ivan"}}
             (->> (c2/plan-query tu/*node*
                                 '{:find [?e ?e2 ?n]
                                   :where [[?e :name ?n]
                                           [?e :age ?a]
                                           [?e2 :name ?n]
                                           [?e2 :age ?a]]}
                                 {:basis-tx tx})
                  (into #{})))
          "multi-param join")

    (t/is (= #{{:e1 1, :e2 1, :a1 15, :a2 15}
               {:e1 1, :e2 3, :a1 15, :a2 37}
               {:e1 3, :e2 1, :a1 37, :a2 15}
               {:e1 3, :e2 3, :a1 37, :a2 37}}
             (->> (c2/plan-query tu/*node*
                                 '{:find [?e1 ?e2 ?a1 ?a2]
                                   :where [[?e1 :name "Ivan"]
                                           [?e2 :name "Ivan"]
                                           [?e1 :age ?a1]
                                           [?e2 :age ?a2]]}
                                 {:basis-tx tx})
                  (into #{})))
          "cross join required here")))

(t/deftest test-joins
  (let [tx (c2/submit-tx tu/*node* bond/tx-ops)]
    (t/is (= #{{:film-name "Skyfall", :bond-name "Daniel Craig"}}
             (->> (c2/plan-query tu/*node*
                                 '{:find [?film-name ?bond-name]
                                   :in [?film]
                                   :where [[?film :film/name ?film-name]
                                           [?film :film/bond ?bond]
                                           [?bond :person/name ?bond-name]]}
                                 {:basis-tx tx}
                                 "skyfall")
                  (into #{})))
          "one -> one")

    (t/is (= #{{:film-name "Casino Royale", :bond-name "Daniel Craig"}
               {:film-name "Quantum of Solace", :bond-name "Daniel Craig"}
               {:film-name "Skyfall", :bond-name "Daniel Craig"}
               {:film-name "Spectre", :bond-name "Daniel Craig"}}
             (->> (c2/plan-query tu/*node*
                                 '{:find [?film-name ?bond-name]
                                   :in [?bond]
                                   :where [[?film :film/name ?film-name]
                                           [?film :film/bond ?bond]
                                           [?bond :person/name ?bond-name]]}
                                 {:basis-tx tx}
                                 "daniel-craig")
                  (into #{})))
          "one -> many")))

;; https://github.com/tonsky/datascript/blob/1.1.0/test/datascript/test/query_aggregates.cljc#L14-L39
(t/deftest datascript-test-aggregates
  (let [tx (c2/submit-tx tu/*node*
                         [[:put {:_id "Cerberus", :heads 3}]
                          [:put {:_id "Medusa", :heads 1}]
                          [:put {:_id "Cyclops", :heads 1}]
                          [:put {:_id "Chimera", :heads 1}]])]
    (t/is (= #{{:heads 1, :count-?heads 3} {:heads 3, :count-?heads 1}}
             (->> (c2/plan-query tu/*node*
                                 '{:find [?heads (count ?heads)]
                                   :where [[?monster :heads ?heads]]}
                                 {:basis-tx tx})
                  (into #{})))
          "head frequency")

    (t/is (= #{{:sum-?heads 6, :min-?heads 1, :max-?heads 3, :count-?heads 4}}
             (->> (c2/plan-query tu/*node*
                                 '{:find [(sum ?heads)
                                          (min ?heads)
                                          (max ?heads)
                                          (count ?heads)]
                                   :where [[?monster :heads ?heads]]}
                                 {:basis-tx tx})
                  (into #{})))
          "various aggs")))

(t/deftest test-query-with-in-bindings
  (let [tx (c2/submit-tx tu/*node* ivan+petr)]
    (t/is (= #{{:e "ivan"}}
             (->> (c2/plan-query tu/*node*
                                 '{:find [?e]
                                   :in [?name]
                                   :where [[?e :first-name ?name]]}
                                 {:basis-tx tx}
                                 "Ivan")
                  (into #{})))
          "single arg")

    (t/is (= #{{:e "ivan"}}
             (->> (c2/plan-query tu/*node*
                                 '{:find [?e]
                                   :in [?first-name ?last-name]
                                   :where [[?e :first-name ?first-name]
                                           [?e :last-name ?last-name]]}
                                 {:basis-tx tx}
                                 "Ivan" "Ivanov")
                  (into #{})))
          "multiple args")

    (t/is (= #{{:e "ivan"}}
             (->> (c2/plan-query tu/*node*
                                 '{:find [?e]
                                   :in [[?first-name]]
                                   :where [[?e :first-name ?first-name]]}
                                 {:basis-tx tx}
                                 ["Ivan"])
                  (into #{})))
          "tuple with 1 var")

    (t/is (= #{{:e "ivan"}}
             (->> (c2/plan-query tu/*node*
                                 '{:find [?e]
                                   :in [[?first-name ?last-name]]
                                   :where [[?e :first-name ?first-name]
                                           [?e :last-name ?last-name]]}
                                 {:basis-tx tx}
                                 ["Ivan" "Ivanov"])
                  (into #{})))
          "tuple with 2 vars")

    (t/testing "collection"
      (let [query '{:find [?e]
                    :in [[?first-name ...]]
                    :where [[?e :first-name ?first-name]]}]
        (t/is (= #{{:e "petr"}}
                 (->> (c2/plan-query tu/*node* query {:basis-tx tx} ["Petr"])
                      (into #{}))))

        (t/is (= #{{:e "ivan"} {:e "petr"}}
                 (->> (c2/plan-query tu/*node* query {:basis-tx tx} ["Ivan" "Petr"])
                      (into #{}))))))

    (t/testing "relation"
      (let [query '{:find [?e]
                    :in [[[?first-name ?last-name]]]
                    :where [[?e :first-name ?first-name]
                            [?e :last-name ?last-name]]}]

        (t/is (= #{{:e "ivan"}}
                 (->> (c2/plan-query tu/*node* query {:basis-tx tx}
                                     [{:first-name "Ivan", :last-name "Ivanov"}])
                      (into #{}))))

        (t/is (= #{{:e "ivan"}}
                 (->> (c2/plan-query tu/*node* query {:basis-tx tx}
                                     [["Ivan" "Ivanov"]])
                      (into #{}))))

        (t/is (= #{{:e "ivan"} {:e "petr"}}
                 (->> (c2/plan-query tu/*node* query {:basis-tx tx}
                                     [{:first-name "Ivan", :last-name "Ivanov"}
                                      {:first-name "Petr", :last-name "Petrov"}])
                      (into #{}))))
        (t/is (= #{{:e "ivan"} {:e "petr"}}
                 (->> (c2/plan-query tu/*node* query {:basis-tx tx}
                                     [["Ivan" "Ivanov"]
                                      ["Petr" "Petrov"]])
                      (into #{}))))))))

(t/deftest test-in-arity-exceptions
  (let [tx (c2/submit-tx tu/*node* ivan+petr)]
    (t/is (thrown-with-msg? IllegalArgumentException
                            #":in arity mismatch"
                            (->> (c2/plan-query tu/*node*
                                                '{:find [?e]
                                                  :in [?foo]}
                                                {:basis-tx tx})
                                 (into []))))))

(t/deftest test-known-predicates
  (let [tx (c2/submit-tx tu/*node* ivan+petr)]
    (t/is (= #{{:first-name "Ivan", :last-name "Ivanov"}}
             (->> (c2/plan-query tu/*node*
                                 '{:find [?first-name ?last-name]
                                   :where [[?e :first-name ?first-name]
                                           [?e :last-name ?last-name]
                                           [(< ?first-name "James")]]}
                                 {:basis-tx tx})
                  (into #{}))))

    (t/is (= #{{:first-name "Ivan", :last-name "Ivanov"}}
             (->> (c2/plan-query tu/*node*
                                 '{:find [?first-name ?last-name]
                                   :where [[?e :first-name ?first-name]
                                           [?e :last-name ?last-name]
                                           [(<= ?first-name "Ivan")]]}
                                 {:basis-tx tx})
                  (into #{}))))

    (t/is (empty? (->> (c2/plan-query tu/*node*
                                      '{:find [?first-name ?last-name]
                                        :where [[?e :first-name ?first-name]
                                                [?e :last-name ?last-name]
                                                [(<= ?first-name "Ivan")]
                                                [(> ?last-name "Ivanov")]]}
                                      {:basis-tx tx})
                       (into []))))

    (t/is (empty? (->> (c2/plan-query tu/*node*
                                      '{:find [?first-name ?last-name]
                                        :where [[?e :first-name ?first-name]
                                                [?e :last-name ?last-name]
                                                [(< ?first-name "Ivan")]]}
                                      {:basis-tx tx})
                       (into []))))))
