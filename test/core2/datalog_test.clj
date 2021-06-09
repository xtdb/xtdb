(ns core2.datalog-test
  (:require [core2.james-bond :as bond]
            [clojure.test :as t]
            [core2.test-util :as tu]
            [core2.core :as c2]))

(t/use-fixtures :each tu/with-node)

(defn run-query [query & args]
  (into [] (apply c2/plan-q query args)))

(def ivan+petr
  [{:op :put, :doc {:_id "ivan", :first-name "Ivan", :last-name "Ivanov"}}
   {:op :put, :doc {:_id "petr", :first-name "Petr", :last-name "Petrov"}}])

(t/deftest test-scan
  (c2/with-db [db tu/*node* {:tx (c2/submit-tx tu/*node* ivan+petr)}]
    (t/is (= [{:name "Ivan"}
              {:name "Petr"}]
             (run-query '{:find [?name]
                          :where [[?e :first-name ?name]]}
                        db)))

    (t/is (= [{:e "ivan", :name "Ivan"}
              {:e "petr", :name "Petr"}]
             (run-query '{:find [?e ?name]
                          :where [[?e :first-name ?name]]}
                        db))
          "returning eid")))

(t/deftest test-basic-query
  (c2/with-db [db tu/*node* {:tx (c2/submit-tx tu/*node* ivan+petr)}]
    (t/is (= [{:e "ivan"}]
             (run-query '{:find [?e]
                          :where [[?e :first-name "Ivan"]]}
                        db))
          "query by single field")

    (t/is (= [{:first-name "Petr", :last-name "Petrov"}]
             (run-query '{:find [?first-name ?last-name]
                          :where [[?e :first-name "Petr"]
                                  [?e :first-name ?first-name]
                                  [?e :last-name ?last-name]]}
                        db))
          "returning the queried field")

    (t/is (= [{:first-name "Petr", :last-name "Petrov"}]
             (run-query '{:find [?first-name ?last-name]
                          :where [["petr" :first-name ?first-name]
                                  ["petr" :last-name ?last-name]]}
                        db))
          "literal eid")))

(t/deftest test-order-by
  (c2/with-db [db tu/*node* {:tx (c2/submit-tx tu/*node* ivan+petr)}]
    (t/is (= [{:first-name "Ivan"} {:first-name "Petr"}]
             (run-query '{:find [?first-name]
                          :where [[?e :first-name ?first-name]]
                          :order-by [?first-name]}
                        db)))

    (t/is (= [{:first-name "Petr"} {:first-name "Ivan"}]
             (run-query '{:find [?first-name]
                          :where [[?e :first-name ?first-name]]
                          :order-by [?first-name :desc]}
                        db)))

    (t/is (= [{:first-name "Ivan"}]
             (run-query '{:find [?first-name]
                          :where [[?e :first-name ?first-name]]
                          :order-by [?first-name]
                          :limit 1}
                        db)))

    (t/is (= [{:first-name "Petr"}]
             (run-query '{:find [?first-name]
                          :where [[?e :first-name ?first-name]]
                          :order-by [?first-name :desc]
                          :limit 1}
                        db)))

    (t/is (= [{:first-name "Petr"}]
             (run-query '{:find [?first-name]
                          :where [[?e :first-name ?first-name]]
                          :order-by [?first-name]
                          :limit 1
                          :offset 1}
                        db)))))

;; https://github.com/tonsky/datascript/blob/1.1.0/test/datascript/test/query.cljc#L12-L36
(t/deftest datascript-test-joins
  (c2/with-db [db tu/*node*
               {:tx (c2/submit-tx tu/*node*
                                  [{:op :put, :doc {:_id 1, :name "Ivan", :age 15}}
                                   {:op :put, :doc {:_id 2, :name "Petr", :age 37}}
                                   {:op :put, :doc {:_id 3, :name "Ivan", :age 37}}
                                   {:op :put, :doc {:_id 4, :age 15}}])}]

    (t/is (= #{{:e 1} {:e 2} {:e 3}}
             (set (run-query '{:find [?e]
                               :where [[?e :name]]}
                             db)))
          "testing without V")

    (t/is (= #{{:e 1, :v 15} {:e 3, :v 37}}
             (set (run-query '{:find [?e ?v]
                               :where [[?e :name "Ivan"]
                                       [?e :age ?v]]}
                             db))))

    (t/is (= #{{:e1 1, :e2 1}
               {:e1 2, :e2 2}
               {:e1 3, :e2 3}
               {:e1 1, :e2 3}
               {:e1 3, :e2 1}}
             (set (run-query '{:find [?e1 ?e2]
                               :where [[?e1 :name ?n]
                                       [?e2 :name ?n]]}
                             db))))

    (t/is (= #{{:e 1, :e2 1, :n "Ivan"}
               {:e 3, :e2 3, :n "Ivan"}
               {:e 3, :e2 2, :n "Petr"}}
             (set (run-query '{:find [?e ?e2 ?n]
                               :where [[?e :name "Ivan"]
                                       [?e :age ?a]
                                       [?e2 :age ?a]
                                       [?e2 :name ?n]]}
                             db))))

    (t/is (= #{{:e 1, :e2 1, :n "Ivan"}
               {:e 2, :e2 2, :n "Petr"}
               {:e 3, :e2 3, :n "Ivan"}}
             (set (run-query '{:find [?e ?e2 ?n]
                               :where [[?e :name ?n]
                                       [?e :age ?a]
                                       [?e2 :name ?n]
                                       [?e2 :age ?a]]}
                             db)))
          "multi-param join")

    (t/is (= #{{:e1 1, :e2 1, :a1 15, :a2 15}
               {:e1 1, :e2 3, :a1 15, :a2 37}
               {:e1 3, :e2 1, :a1 37, :a2 15}
               {:e1 3, :e2 3, :a1 37, :a2 37}}
             (set (run-query '{:find [?e1 ?e2 ?a1 ?a2]
                               :where [[?e1 :name "Ivan"]
                                       [?e2 :name "Ivan"]
                                       [?e1 :age ?a1]
                                       [?e2 :age ?a2]]}
                             db)))
          "cross join required here")))

(t/deftest test-joins
  (c2/with-db [db tu/*node*
               {:tx (c2/submit-tx tu/*node* bond/tx-ops)}]
    (t/is (= #{{:film-name "Skyfall", :bond-name "Daniel Craig"}}
             (set (run-query '{:find [?film-name ?bond-name]
                               :in [$ ?film]
                               :where [[?film :film/name ?film-name]
                                       [?film :film/bond ?bond]
                                       [?bond :person/name ?bond-name]]}
                             db
                             "skyfall")))
          "one -> one")

    (t/is (= #{{:film-name "Casino Royale", :bond-name "Daniel Craig"}
               {:film-name "Quantum of Solace", :bond-name "Daniel Craig"}
               {:film-name "Skyfall", :bond-name "Daniel Craig"}
               {:film-name "Spectre", :bond-name "Daniel Craig"}}
             (set (run-query '{:find [?film-name ?bond-name]
                               :in [$ ?bond]
                               :where [[?film :film/name ?film-name]
                                       [?film :film/bond ?bond]
                                       [?bond :person/name ?bond-name]]}
                             db
                             "daniel-craig")))
          "one -> many")))

;; https://github.com/tonsky/datascript/blob/1.1.0/test/datascript/test/query_aggregates.cljc#L14-L39
(t/deftest datascript-test-aggregates
  (c2/with-db [db tu/*node*
               {:tx (c2/submit-tx tu/*node*
                                  [{:op :put, :doc {:_id "Cerberus", :heads 3}}
                                   {:op :put, :doc {:_id "Medusa", :heads 1}}
                                   {:op :put, :doc {:_id "Cyclops", :heads 1}}
                                   {:op :put, :doc {:_id "Chimera", :heads 1}}])}]
    (t/is (= #{{:heads 1, :count-?heads 3} {:heads 3, :count-?heads 1}}
             (set (run-query '{:find [?heads (count ?heads)]
                               :where [[?monster :heads ?heads]]}
                             db)))
          "head frequency")

    (t/is (= #{{:sum-?heads 6, :min-?heads 1, :max-?heads 3, :count-?heads 4}}
             (set (run-query '{:find [(sum ?heads)
                                      (min ?heads)
                                      (max ?heads)
                                      (count ?heads)]
                               :where [[?monster :heads ?heads]]}
                             db)))
          "various aggs")))

(t/deftest test-query-with-in-bindings
  (c2/with-db [db tu/*node* {:tx (c2/submit-tx tu/*node* ivan+petr)}]
    (t/is (= #{{:e "ivan"}}
             (set (run-query '{:find [?e]
                               :in [$ ?name]
                               :where [[?e :first-name ?name]]}
                             db
                             "Ivan")))
          "single arg")

    (t/is (= #{{:e "ivan"}}
             (set (run-query '{:find [?e]
                               :in [$ ?first-name ?last-name]
                               :where [[?e :first-name ?first-name]
                                       [?e :last-name ?last-name]]}
                             db
                             "Ivan" "Ivanov")))
          "multiple args")

    (t/is (= #{{:e "ivan"}}
             (set (run-query '{:find [?e]
                               :in [$ [?first-name]]
                               :where [[?e :first-name ?first-name]]}
                             db
                             ["Ivan"])))
          "tuple with 1 var")

    (t/is (= #{{:e "ivan"}}
             (set (run-query '{:find [?e]
                               :in [$ [?first-name ?last-name]]
                               :where [[?e :first-name ?first-name]
                                       [?e :last-name ?last-name]]}
                             db
                             ["Ivan" "Ivanov"])))
          "tuple with 2 vars")

    (t/testing "collection"
      (let [query '{:find [?e]
                    :in [$ [?first-name ...]]
                    :where [[?e :first-name ?first-name]]}]
        (t/is (= #{{:e "petr"}}
                 (set (run-query query db ["Petr"]))))

        (t/is (= #{{:e "ivan"} {:e "petr"}}
                 (set (run-query query db ["Ivan" "Petr"]))))))

    (t/testing "relation"
      (let [query '{:find [?e]
                    :in [$ [[?first-name ?last-name]]]
                    :where [[?e :first-name ?first-name]
                            [?e :last-name ?last-name]]}]

        (t/is (= #{{:e "ivan"}}
                 (set (run-query query db [{:first-name "Ivan", :last-name "Ivanov"}]))))

        (t/is (= #{{:e "ivan"}}
                 (set (run-query query db [["Ivan" "Ivanov"]]))))

        (t/is (= #{{:e "ivan"} {:e "petr"}}
                 (set (run-query query db [{:first-name "Ivan", :last-name "Ivanov"}
                                           {:first-name "Petr", :last-name "Petrov"}]))))
        (t/is (= #{{:e "ivan"} {:e "petr"}}
                 (set (run-query query db [["Ivan" "Ivanov"]
                                           ["Petr" "Petrov"]]))))))))

(t/deftest test-in-arity-exceptions
  (c2/with-db [db tu/*node* {:tx (c2/submit-tx tu/*node* ivan+petr)}]
    (t/is (thrown-with-msg? IllegalArgumentException
                            #":in arity mismatch"
                            (run-query '{:find [?e]
                                         :in []}
                                       db)))

    (t/is (thrown-with-msg? IllegalArgumentException
                            #":in arity mismatch"
                            (run-query '{:find [?e]
                                         :in [$ ?foo]}
                                       db)))))

(t/deftest test-multiple-sources
  (with-open [node1 (c2/start-node {})
              node2 (c2/start-node {})]
    (c2/with-db [db1 node1
                 {:tx (c2/submit-tx node1 [{:op :put, :doc {:_id "foo", :col1 "foo1"}}
                                           {:op :put, :doc {:_id "bar", :col1 "bar1"}}])}]
      (c2/with-db [db2 node2
                   {:tx (c2/submit-tx node2 [{:op :put, :doc {:_id "foo", :col2 "foo2"}}
                                             {:op :put, :doc {:_id "bar", :col2 "bar2"}}])}]
        (t/is (= [{:col1 "foo1", :col2 "foo2"}
                  {:col1 "bar1", :col2 "bar2"}]
                 (run-query '{:find [?col1 ?col2]
                              :in [$db1 $db2]
                              :where [[$db1 ?e :col1 ?col1]
                                      [$db2 ?e :col2 ?col2]]}
                            db1 db2)))

        (t/is (= [{:e "foo", :col1 "foo1", :col2 "foo2"}
                  {:e "bar", :col1 "bar1", :col2 "bar2"}]
                 (run-query '{:find [?e ?col1 ?col2]
                              :in [$db1 $db2]
                              :where [[$db1 ?e :col1 ?col1]
                                      [$db2 ?e :col2 ?col2]]}
                            db1 db2)))))))

(t/deftest test-no-dbs
  (t/is (= #{{:first-name "Ivan", :last-name "Ivanov"}
             {:first-name "Petr", :last-name "Petrov"}}
           (set (run-query '{:find [?first-name ?last-name]
                             :in [[[?first-name ?last-name]]]}
                           [["Ivan" "Ivanov"]
                            ["Petr" "Petrov"]])))))

(t/deftest test-known-predicates
  (c2/with-db [db tu/*node* {:tx (c2/submit-tx tu/*node* ivan+petr)}]
    (t/is (= #{{:first-name "Ivan", :last-name "Ivanov"}}
             (set (run-query '{:find [?first-name ?last-name]
                               :where [[?e :first-name ?first-name]
                                       [?e :last-name ?last-name]
                                       [(< ?first-name "James")]]}
                             db))))

    (t/is (= #{{:first-name "Ivan", :last-name "Ivanov"}}
             (set (run-query '{:find [?first-name ?last-name]
                               :where [[?e :first-name ?first-name]
                                       [?e :last-name ?last-name]
                                       [(<= ?first-name "Ivan")]]}
                             db))))

    (t/is (empty? (run-query '{:find [?first-name ?last-name]
                               :where [[?e :first-name ?first-name]
                                       [?e :last-name ?last-name]
                                       [(<= ?first-name "Ivan")]
                                       [(> ?last-name "Ivanov")]]}
                             db)))

    (t/is (empty? (run-query '{:find [?first-name ?last-name]
                               :where [[?e :first-name ?first-name]
                                       [?e :last-name ?last-name]
                                       [(< ?first-name "Ivan")]]}
                             db)))))
