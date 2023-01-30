;; THIRD-PARTY SOFTWARE NOTICE
;; This file is derivative of test files found in the DataScript
;; project. The Datascript license is copied verbatim in this
;; directory as `LICENSE`.
;; https://github.com/tonsky/datascript

(ns core2.datalog.datalog-test
  (:require [core2.james-bond :as bond]
            [clojure.test :as t :refer [deftest]]
            [core2.test-util :as tu]
            [core2.api :as c2]))

(t/use-fixtures :each tu/with-node)

(def ivan+petr
  [[:put {:id :ivan, :first-name "Ivan", :last-name "Ivanov"}]
   [:put {:id :petr, :first-name "Petr", :last-name "Petrov"}]])

(deftest test-scan
  (let [tx (c2/submit-tx tu/*node* ivan+petr)]
    (t/is (= [{:name "Ivan"}
              {:name "Petr"}]
             (->> (c2/plan-datalog tu/*node*
                                   (-> '{:find [name]
                                         :where [[e :first-name name]]}
                                       (assoc :basis {:tx tx})))
                  (into []))))

    (t/is (= [{:e :ivan, :name "Ivan"}
              {:e :petr, :name "Petr"}]
             (->> (c2/plan-datalog tu/*node*
                                   (-> '{:find [e name]
                                         :where [[e :first-name name]]}
                                       (assoc :basis {:tx tx})))
                  (into [])))
          "returning eid")))

(deftest test-basic-query
  (let [tx (c2/submit-tx tu/*node* ivan+petr)]
    (t/is (= [{:e :ivan}]
             (->> (c2/plan-datalog tu/*node*
                                   (-> '{:find [e]
                                         :where [[e :first-name "Ivan"]]}
                                       (assoc :basis {:tx tx})))
                  (into [])))
          "query by single field")

    (t/is (= [{:first-name "Petr", :last-name "Petrov"}]
             (->> (c2/plan-datalog tu/*node*
                                   (-> '{:find [first-name last-name]
                                         :where [[e :first-name "Petr"]
                                                 [e :first-name first-name]
                                                 [e :last-name last-name]]}
                                       (assoc :basis {:tx tx})))
                  (into [])))
          "returning the queried field")

    (t/is (= [{:first-name "Petr", :last-name "Petrov"}]
             (->> (c2/plan-datalog tu/*node*
                                   (-> '{:find [first-name last-name]
                                         :where [[:petr :first-name first-name]
                                                 [:petr :last-name last-name]]}
                                       (assoc :basis {:tx tx})))
                  (into [])))
          "literal eid")))

(deftest test-order-by
  (let [tx (c2/submit-tx tu/*node* ivan+petr)]
    (t/is (= [{:first-name "Ivan"} {:first-name "Petr"}]
             (->> (c2/plan-datalog tu/*node*
                                   (-> '{:find [first-name]
                                         :where [[e :first-name first-name]]
                                         :order-by [[first-name]]}
                                       (assoc :basis {:tx tx})))
                  (into []))))

    (t/is (= [{:first-name "Petr"} {:first-name "Ivan"}]
             (->> (c2/plan-datalog tu/*node*
                                   (-> '{:find [first-name]
                                         :where [[e :first-name first-name]]
                                         :order-by [[first-name :desc]]}
                                       (assoc :basis {:tx tx})))
                  (into []))))

    (t/is (= [{:first-name "Ivan"}]
             (->> (c2/plan-datalog tu/*node*
                                   (-> '{:find [first-name]
                                         :where [[e :first-name first-name]]
                                         :order-by [[first-name]]
                                         :limit 1}
                                       (assoc :basis {:tx tx})))
                  (into []))))

    (t/is (= [{:first-name "Petr"}]
             (->> (c2/plan-datalog tu/*node*
                                   (-> '{:find [first-name]
                                         :where [[e :first-name first-name]]
                                         :order-by [[first-name :desc]]
                                         :limit 1}
                                       (assoc :basis {:tx tx})))
                  (into []))))

    (t/is (= [{:first-name "Petr"}]
             (->> (c2/plan-datalog tu/*node*
                                   (-> '{:find [first-name]
                                         :where [[e :first-name first-name]]
                                         :order-by [[first-name]]
                                         :limit 1
                                         :offset 1}
                                       (assoc :basis {:tx tx})))
                  (into []))))))

;; https://github.com/tonsky/datascript/blob/1.1.0/test/datascript/test/query.cljc#L12-L36
(deftest datascript-test-joins
  (let [tx (c2/submit-tx tu/*node*
                         [[:put {:id 1, :name "Ivan", :age 15}]
                          [:put {:id 2, :name "Petr", :age 37}]
                          [:put {:id 3, :name "Ivan", :age 37}]
                          [:put {:id 4, :age 15}]])]

    (t/is (= #{{:e 1} {:e 2} {:e 3}}
             (->> (c2/plan-datalog tu/*node*
                                   (-> '{:find [e]
                                         :where [[e :name]]}
                                       (assoc :basis {:tx tx})))
                  (into #{})))
          "testing without V")

    (t/is (= #{{:e 1, :v 15} {:e 3, :v 37}}
             (->> (c2/plan-datalog tu/*node*
                                   (-> '{:find [e v]
                                         :where [[e :name "Ivan"]
                                                 [e :age v]]}
                                       (assoc :basis {:tx tx})))
                  (into #{}))))

    (t/is (= #{{:e1 1, :e2 1}
               {:e1 2, :e2 2}
               {:e1 3, :e2 3}
               {:e1 1, :e2 3}
               {:e1 3, :e2 1}}
             (->> (c2/plan-datalog tu/*node*
                                   (-> '{:find [e1 e2]
                                         :where [[e1 :name n]
                                                 [e2 :name n]]}
                                       (assoc :basis {:tx tx})))
                  (into #{}))))

    (t/is (= #{{:e 1, :e2 1, :n "Ivan"}
               {:e 3, :e2 3, :n "Ivan"}
               {:e 3, :e2 2, :n "Petr"}}
             (->> (c2/plan-datalog tu/*node*
                                   (-> '{:find [e e2 n]
                                         :where [[e :name "Ivan"]
                                                 [e :age a]
                                                 [e2 :age a]
                                                 [e2 :name n]]}
                                       (assoc :basis {:tx tx})))
                  (into #{}))))

    (t/is (= #{{:e 1, :e2 1, :n "Ivan"}
               {:e 2, :e2 2, :n "Petr"}
               {:e 3, :e2 3, :n "Ivan"}}
             (->> (c2/plan-datalog tu/*node*
                                   (-> '{:find [e e2 n]
                                         :where [[e :name n]
                                                 [e :age a]
                                                 [e2 :name n]
                                                 [e2 :age a]]}
                                       (assoc :basis {:tx tx})))
                  (into #{})))
          "multi-param join")

    (t/is (= #{{:e1 1, :e2 1, :a1 15, :a2 15}
               {:e1 1, :e2 3, :a1 15, :a2 37}
               {:e1 3, :e2 1, :a1 37, :a2 15}
               {:e1 3, :e2 3, :a1 37, :a2 37}}
             (->> (c2/plan-datalog tu/*node*
                                   (-> '{:find [e1 e2 a1 a2]
                                         :where [[e1 :name "Ivan"]
                                                 [e2 :name "Ivan"]
                                                 [e1 :age a1]
                                                 [e2 :age a2]]}
                                       (assoc :basis {:tx tx})))
                  (into #{})))
          "cross join required here")))

(deftest test-joins
  (let [tx (c2/submit-tx tu/*node* bond/tx-ops)]
    (t/is (= #{{:film-name "Skyfall", :bond-name "Daniel Craig"}}
             (->> (c2/plan-datalog tu/*node*
                                   (-> '{:find [film-name bond-name]
                                         :in [film]
                                         :where [[film :film--name film-name]
                                                 [film :film--bond bond]
                                                 [bond :person--name bond-name]]}
                                       (assoc :basis {:tx tx}))
                                   "skyfall")
                  (into #{})))
          "one -> one")

    (t/is (= #{{:film-name "Casino Royale", :bond-name "Daniel Craig"}
               {:film-name "Quantum of Solace", :bond-name "Daniel Craig"}
               {:film-name "Skyfall", :bond-name "Daniel Craig"}
               {:film-name "Spectre", :bond-name "Daniel Craig"}}
             (->> (c2/plan-datalog tu/*node*
                                   (-> '{:find [film-name bond-name]
                                         :in [bond]
                                         :where [[film :film--name film-name]
                                                 [film :film--bond bond]
                                                 [bond :person--name bond-name]]}
                                       (assoc :basis {:tx tx}))
                                   "daniel-craig")
                  (into #{})))
          "one -> many")))

;; https://github.com/tonsky/datascript/blob/1.1.0/test/datascript/test/query_aggregates.cljc#L14-L39
(deftest datascript-test-aggregates
  (let [tx (c2/submit-tx tu/*node*
                         [[:put {:id :cerberus, :heads 3}]
                          [:put {:id :medusa, :heads 1}]
                          [:put {:id :cyclops, :heads 1}]
                          [:put {:id :chimera, :heads 1}]])]
    (t/is (= #{{:heads 1, :count-heads 3} {:heads 3, :count-heads 1}}
             (->> (c2/plan-datalog tu/*node*
                                   (-> '{:find [heads (count heads)]
                                         :where [[monster :heads heads]]}
                                       (assoc :basis {:tx tx})))
                  (into #{})))
          "head frequency")

    (t/is (= #{{:sum-heads 6, :min-heads 1, :max-heads 3, :count-heads 4}}
             (->> (c2/plan-datalog tu/*node*
                                   (-> '{:find [(sum heads)
                                                (min heads)
                                                (max heads)
                                                (count heads)]
                                         :where [[monster :heads heads]]}
                                       (assoc :basis {:tx tx})))
                  (into #{})))
          "various aggs")))

(deftest test-query-with-in-bindings
  (let [tx (c2/submit-tx tu/*node* ivan+petr)]
    (t/is (= #{{:e :ivan}}
             (->> (c2/plan-datalog tu/*node*
                                   (-> '{:find [e]
                                         :in [name]
                                         :where [[e :first-name name]]}
                                       (assoc :basis {:tx tx}))
                                   "Ivan")
                  (into #{})))
          "single arg")

    (t/is (= #{{:e :ivan}}
             (->> (c2/plan-datalog tu/*node*
                                   (-> '{:find [e]
                                         :in [first-name last-name]
                                         :where [[e :first-name first-name]
                                                 [e :last-name last-name]]}
                                       (assoc :basis {:tx tx}))
                                   "Ivan" "Ivanov")
                  (into #{})))
          "multiple args")

    (t/is (= #{{:e :ivan}}
             (->> (c2/plan-datalog tu/*node*
                                   (-> '{:find [e]
                                         :in [[first-name]]
                                         :where [[e :first-name first-name]]}
                                       (assoc :basis {:tx tx}))
                                   ["Ivan"])
                  (into #{})))
          "tuple with 1 var")

    (t/is (= #{{:e :ivan}}
             (->> (c2/plan-datalog tu/*node*
                                   (-> '{:find [e]
                                         :in [[first-name last-name]]
                                         :where [[e :first-name first-name]
                                                 [e :last-name last-name]]}
                                       (assoc :basis {:tx tx}))
                                   ["Ivan" "Ivanov"])
                  (into #{})))
          "tuple with 2 vars")

    (t/testing "collection"
      (let [query (-> '{:find [e]
                        :in [[first-name ...]]
                        :where [[e :first-name first-name]]}
                      (assoc :basis {:tx tx}))]
        (t/is (= #{{:e :petr}}
                 (->> (c2/plan-datalog tu/*node* query ["Petr"])
                      (into #{}))))

        (t/is (= #{{:e :ivan} {:e :petr}}
                 (->> (c2/plan-datalog tu/*node* query ["Ivan" "Petr"])
                      (into #{}))))))

    (t/testing "relation"
      (let [query (-> '{:find [e]
                        :in [[[first-name last-name]]]
                        :where [[e :first-name first-name]
                                [e :last-name last-name]]}
                      (assoc :basis {:tx tx}))]

        (t/is (= #{{:e :ivan}}
                 (->> (c2/plan-datalog tu/*node* query
                                       [{:first-name "Ivan", :last-name "Ivanov"}])
                      (into #{}))))

        (t/is (= #{{:e :ivan}}
                 (->> (c2/plan-datalog tu/*node* query
                                       [["Ivan" "Ivanov"]])
                      (into #{}))))

        (t/is (= #{{:e :ivan} {:e :petr}}
                 (->> (c2/plan-datalog tu/*node* query
                                       [{:first-name "Ivan", :last-name "Ivanov"}
                                        {:first-name "Petr", :last-name "Petrov"}])
                      (into #{}))))

        (t/is (= #{{:e :ivan} {:e :petr}}
                 (->> (c2/plan-datalog tu/*node* query
                                       [["Ivan" "Ivanov"]
                                        ["Petr" "Petrov"]])
                      (into #{}))))))))

(deftest test-in-arity-exceptions
  (let [tx (c2/submit-tx tu/*node* ivan+petr)]
    (t/is (thrown-with-msg? IllegalArgumentException
                            #":in arity mismatch"
                            (->> (c2/plan-datalog tu/*node*
                                                  (-> '{:find [e]
                                                        :in [foo]
                                                        :where [[e :foo foo]]}
                                                      (assoc :basis {:tx tx})))
                                 (into []))))))

(deftest test-basic-predicates
  (let [tx (c2/submit-tx tu/*node* ivan+petr)]
    (t/is (= #{{:first-name "Ivan", :last-name "Ivanov"}}
             (->> (c2/plan-datalog tu/*node*
                                   (-> '{:find [first-name last-name]
                                         :where [[e :first-name first-name]
                                                 [e :last-name last-name]
                                                 [(< first-name "James")]]}
                                       (assoc :basis {:tx tx})))
                  (into #{}))))

    (t/is (= #{{:first-name "Ivan", :last-name "Ivanov"}}
             (->> (c2/plan-datalog tu/*node*
                                   (-> '{:find [first-name last-name]
                                         :where [[e :first-name first-name]
                                                 [e :last-name last-name]
                                                 [(<= first-name "Ivan")]]}
                                       (assoc :basis {:tx tx})))
                  (into #{}))))

    (t/is (empty? (->> (c2/plan-datalog tu/*node*
                                        (-> '{:find [first-name last-name]
                                              :where [[e :first-name first-name]
                                                      [e :last-name last-name]
                                                      [(<= first-name "Ivan")]
                                                      [(> last-name "Ivanov")]]}
                                            (assoc :basis {:tx tx})))
                       (into []))))

    (t/is (empty (->> (c2/plan-datalog tu/*node*
                                       (-> '{:find [first-name last-name]
                                             :where [[e :first-name first-name]
                                                     [e :last-name last-name]
                                                     [(< first-name "Ivan")]]}
                                           (assoc :basis {:tx tx})))
                      (into []))))))

(deftest test-value-unification
  (let [tx (c2/submit-tx tu/*node*
                         (conj ivan+petr
                               [:put {:id :sergei :first-name "Sergei" :last-name "Sergei"}]
                               [:put {:id :jeff :first-name "Sergei" :last-name "but-different"}]))]
    (t/is (= [{:e :sergei, :n "Sergei"}]
             (->> (c2/plan-datalog
                   tu/*node*
                   (-> '{:find [e n]
                         :where [[e :last-name n]
                                 [e :first-name n]]}
                       (assoc :basis {:tx tx})))
                  (into []))))
    (t/is (= [{:e :sergei, :f :sergei, :n "Sergei"} {:e :sergei, :f :jeff, :n "Sergei"}]
             (->> (c2/plan-datalog
                   tu/*node*
                   (-> '{:find [e f n]
                         :where [[e :last-name n]
                                 [e :first-name n]
                                 [f :first-name n]]}
                       (assoc :basis {:tx tx})))
                  (into []))))))

(deftest test-semi-join
  (let [!tx (c2/submit-tx tu/*node*
                          [[:put {:id :ivan, :name "Ivan"}]
                           [:put {:id :petr, :name "Petr", :parent :ivan}]
                           [:put {:id :sergei, :name "Sergei", :parent :petr}]
                           [:put {:id :jeff, :name "Jeff", :parent :petr}]])]

    (t/is (= [{:e :ivan} {:e :petr}]
             (c2/datalog-query tu/*node*
                               (-> '{:find [e]
                                     :where [[e :name name]
                                             (exists? [e]
                                                      [c :parent e])]}
                                   (assoc :basis {:tx !tx}))))

          "find people who have children")

    (t/is (= [{:e :sergei} {:e :jeff}]
             (c2/datalog-query tu/*node*
                               (-> '{:find [e]
                                     :where [[e :name name]
                                             [e :parent p]
                                             (exists? [e p]
                                                      [s :parent p]
                                                      [(<> e s)])]}
                                   (assoc :basis {:tx !tx}))))
          "find people who have siblings")

    (t/is (thrown-with-msg? IllegalArgumentException
                            #":unsatisfied-vars"
                            (c2/datalog-query tu/*node*
                                              (-> '{:find [e n]
                                                    :where [[e :foo n]
                                                            (exists? [e]
                                                                     [e :first-name "Petr"]
                                                                     [(= n 1)])]}
                                                  (assoc :basis {:tx !tx})))))))

(deftest test-anti-join
  (let [tx (c2/submit-tx
            tu/*node*
            [[:put {:id :ivan, :first-name "Ivan", :last-name "Ivanov" :foo 1}]
             [:put {:id :petr, :first-name "Petr", :last-name "Petrov" :foo 1}]
             [:put {:id :sergei :first-name "Sergei" :last-name "Sergei" :foo 1}]])]

    (t/is (= [{:e :ivan} {:e :sergei}]
             (->> (c2/plan-datalog
                   tu/*node*
                   (-> '{:find [e]
                         :where [[e :foo 1]
                                 (not-exists? [e]
                                              [e :first-name "Petr"])]}
                       (assoc :basis {:tx tx})))
                  (into []))))

    (t/is (= [{:e :ivan} {:e :sergei}]
             (->> (c2/plan-datalog
                   tu/*node*
                   (-> '{:find [e]
                         :where [[e :foo n]
                                 (not-exists? [e n]
                                              [e :first-name "Petr"]
                                              [e :foo n])]}
                       (assoc :basis {:tx tx})))
                  (into []))))

    (t/is (= []
             (->> (c2/plan-datalog
                   tu/*node*
                   (-> '{:find [e]
                         :where [[e :foo n]
                                 (not-exists? [e n]
                                              [e :foo n])]}
                       (assoc :basis {:tx tx})))
                  (into []))))

    (t/is (= [{:e :petr} {:e :sergei}]
             (->> (c2/plan-datalog
                   tu/*node*
                   (-> '{:find [e]
                         :where [[e :foo 1]
                                 (not-exists? [e]
                                              [e :last-name "Ivanov"])]}
                       (assoc :basis {:tx tx})))
                  (into []))))

    (t/is (= [{:e :ivan} {:e :petr} {:e :sergei}]
             (->> (c2/plan-datalog
                   tu/*node*
                   (-> '{:find [e]
                         :where [[e :foo 1]
                                 (not-exists? [e]
                                              [e :first-name "Jeff"])]}
                       (assoc :basis {:tx tx})))
                  (into []))))

    (t/is (= [{:e :ivan} {:e :petr}]
             (->> (c2/plan-datalog
                   tu/*node*
                   (-> '{:find [e]
                         :where [[e :foo 1]
                                 (not-exists? [e]
                                              [e :first-name n]
                                              [e :last-name n])]}
                       (assoc :basis {:tx tx})))
                  (into []))))

    (t/is (= [{:e :petr, :first-name "Ivan", :last-name "Ivanov", :a "Petr", :b "Petrov"}
              {:e :sergei, :first-name "Ivan", :last-name "Ivanov", :a "Sergei", :b "Sergei"}
              {:e :ivan, :first-name "Petr", :last-name "Petrov", :a "Ivan", :b "Ivanov"}
              {:e :sergei, :first-name "Petr", :last-name "Petrov", :a "Sergei", :b "Sergei"}]
             (->> (c2/plan-datalog
                   tu/*node*
                   (-> '{:find [e first-name last-name a b]
                         :in [[[first-name last-name]]]
                         :where [[e :foo 1]
                                 [e :first-name a]
                                 [e :last-name b]
                                 (not-exists? [e first-name last-name]
                                              [e :first-name first-name]
                                              [e :last-name last-name])]}
                       (assoc :basis {:tx tx}))
                   [["Ivan" "Ivanov"]
                    ["Petr" "Petrov"]])
                  (into []))))

    (t/testing "apply anti-joins"
      (t/is (= [{:n 1, :e :ivan} {:n 1, :e :petr} {:n 1, :e :sergei}]
               (c2/datalog-query
                tu/*node*
                (-> '{:find [e n]
                      :where [[e :foo n]
                              (not-exists? [e n]
                                           [e :first-name "Petr"]
                                           [(= n 2)])]}
                    (assoc :basis {:tx tx})))))

      (t/is (= [{:n 1, :e :ivan} {:n 1, :e :sergei}]
               (c2/datalog-query
                tu/*node*
                (-> '{:find [e n]
                      :where [[e :foo n]
                              (not-exists? [e n]
                                           [e :first-name "Petr"]
                                           [(= n 1)])]}
                    (assoc :basis {:tx tx})))))

      (t/is (= []
               (c2/datalog-query
                tu/*node*
                (-> '{:find [e n]
                      :where [[e :foo n]
                              (not-exists? [n]
                                           [(= n 1)])]}
                    (assoc :basis {:tx tx})))))

      (t/is (= [{:n "Petr", :e :petr} {:n "Sergei", :e :sergei}]
               (c2/datalog-query
                tu/*node*
                (-> '{:find [e n]
                      :where [[e :first-name n]
                              (not-exists? [n]
                                           [(= "Ivan" n)])]}
                    (assoc :basis {:tx tx})))))


      (t/is (= [{:n "Petr", :e :petr} {:n "Sergei", :e :sergei}]
               (c2/datalog-query
                tu/*node*
                (-> '{:find [e n]
                      :where [[e :first-name n]
                              (not-exists? [n]
                                           [e :first-name n]
                                           [e :first-name "Ivan"])]}
                    (assoc :basis {:tx tx})))))

      (t/is (= [{:n 1, :e :ivan} {:n 1, :e :sergei}]
               (c2/datalog-query
                tu/*node*
                (-> '{:find [e n]
                      :where [[e :foo n]
                              (not-exists? [e n]
                                           [e :first-name "Petr"]
                                           [e :foo n]
                                           [(= n 1)])]}
                    (assoc :basis {:tx tx})))))


      (t/is (thrown-with-msg?
             IllegalArgumentException
             #":unsatisfied-vars"
             (c2/datalog-query
              tu/*node*
              (-> '{:find [e n]
                    :where [[e :foo n]
                            (not-exists? [e]
                                         [e :first-name "Petr"]
                                         [(= n 1)])]}
                  (assoc :basis {:tx tx})))))

      ;; TODO what to do if arg var isn't used, either remove it from the join
      ;; or convert the anti-join to an apply and param all the args
      #_(t/is (= [{:e :ivan} {:e :sergei}]
                 (c2/datalog-query
                  tu/*node*
                  (-> '{:find [e n]
                        :where [[e :foo n]
                                (not-exists? [e n]
                                             [e :first-name "Petr"])]}
                      (assoc :basis {:tx tx}))))))

    (t/testing "Multiple anti-joins"
      (t/is (= [{:n "Petr", :e :petr}]
               (c2/datalog-query
                tu/*node*
                (-> '{:find [e n]
                      :where [[e :first-name n]
                              (not-exists? [n]
                                           [(= n "Ivan")])
                              (not-exists? [e]
                                           [e :first-name "Sergei"])]}
                    (assoc :basis {:tx tx}))))))))

(deftest calling-a-function-580
  (let [!tx (c2/submit-tx tu/*node*
                          [[:put {:id :ivan, :age 15}]
                           [:put {:id :petr, :age 22}]
                           [:put {:id :slava, :age 37}]])]
    (t/is (= [{:e1 :ivan, :e2 :petr, :e3 :slava}
              {:e1 :petr, :e2 :ivan, :e3 :slava}]
             (c2/datalog-query tu/*node*
                               (-> '{:find [e1 e2 e3]
                                     :where [[e1 :age a1]
                                             [e2 :age a2]
                                             [e3 :age a3]
                                             [(+ a1 a2) a12]
                                             [(= a12 a3)]]}
                                   (assoc :basis {:tx !tx})))))

    (t/is (= [{:e1 :ivan, :e2 :petr, :e3 :slava}
              {:e1 :petr, :e2 :ivan, :e3 :slava}]
             (c2/datalog-query tu/*node*
                               (-> '{:find [e1 e2 e3]
                                     :where [[e1 :age a1]
                                             [e2 :age a2]
                                             [e3 :age a3]
                                             [(+ a1 a2) a3]]}
                                   (assoc :basis {:tx !tx})))))))

(deftest test-nested-expressions-581
  (let [!tx (c2/submit-tx tu/*node*
                          [[:put {:id :ivan, :age 15}]
                           [:put {:id :petr, :age 22, :height 240, :parent 1}]
                           [:put {:id :slava, :age 37, :parent 2}]])]

    (t/is (= [{:e1 :ivan, :e2 :petr, :e3 :slava}
              {:e1 :petr, :e2 :ivan, :e3 :slava}]
             (c2/datalog-query tu/*node*
                               (-> '{:find [e1 e2 e3]
                                     :where [[e1 :age a1]
                                             [e2 :age a2]
                                             [e3 :age a3]
                                             [(= (+ a1 a2) a3)]]}
                                   (assoc :basis {:tx !tx})))))

    (t/is (= [{:a1 15, :a2 22, :a3 37, :sum-ages 74, :inc-sum-ages 75}]
             (c2/datalog-query tu/*node*
                               (-> '{:find [a1 a2 a3 sum-ages inc-sum-ages]
                                     :where [[:ivan :age a1]
                                             [:petr :age a2]
                                             [:slava :age a3]
                                             [(+ (+ a1 a2) a3) sum-ages]
                                             [(+ a1 (+ a2 a3 1)) inc-sum-ages]]}
                                   (assoc :basis {:tx !tx})))))

    (t/testing "unifies results of two calls"
      (t/is (= [{:a1 15, :a2 22, :a3 37, :sum-ages 74}]
               (c2/datalog-query tu/*node*
                                 (-> '{:find [a1 a2 a3 sum-ages]
                                       :where [[:ivan :age a1]
                                               [:petr :age a2]
                                               [:slava :age a3]
                                               [(+ (+ a1 a2) a3) sum-ages]
                                               [(+ a1 (+ a2 a3)) sum-ages]]}
                                     (assoc :basis {:tx !tx})))))

      (t/is (= []
               (c2/datalog-query tu/*node*
                                 (-> '{:find [a1 a2 a3 sum-ages]
                                       :where [[:ivan :age a1]
                                               [:petr :age a2]
                                               [:slava :age a3]
                                               [(+ (+ a1 a2) a3) sum-ages]
                                               [(+ a1 (+ a2 a3 1)) sum-ages]]}
                                     (assoc :basis {:tx !tx}))))))))

(deftest test-union-join
  (let [!tx (c2/submit-tx tu/*node* [[:put {:id :ivan, :age 20, :role :developer}]
                                     [:put {:id :oleg, :age 30, :role :manager}]
                                     [:put {:id :petr, :age 35, :role :qa}]
                                     [:put {:id :sergei, :age 35, :role :manager}]])]

    (letfn [(q [query]
              (c2/datalog-query tu/*node*
                                (-> query
                                    (assoc :basis {:tx !tx}))))]
      (t/is (= [{:e :petr} {:e :ivan}]
               (q '{:find [e]
                    :where [(union-join [e]
                                        [e :role :developer]
                                        [e :age 35])
                            (union-join [e]
                                        [e :id :petr]
                                        [e :id :ivan])]})))

      (t/is (= [{:e :petr}, {:e :oleg}]
               (q '{:find [e]
                    :where [[:sergei :age age]
                            [:sergei :role role]
                            (union-join [e age role]
                                        [e :age age]
                                        [e :role role])
                            [(<> e :sergei)]]}))))))

(deftest test-nested-query
  (let [!tx (c2/submit-tx tu/*node* bond/tx-ops)]
    (t/is (= [{:bond-name "Roger Moore", :film-name "A View to a Kill"}
              {:bond-name "Roger Moore", :film-name "For Your Eyes Only"}
              {:bond-name "Roger Moore", :film-name "Live and Let Die"}
              {:bond-name "Roger Moore", :film-name "Moonraker"}
              {:bond-name "Roger Moore", :film-name "Octopussy"}
              {:bond-name "Roger Moore", :film-name "The Man with the Golden Gun"}
              {:bond-name "Roger Moore", :film-name "The Spy Who Loved Me"}]
             (c2/datalog-query tu/*node*
                               (-> '{:find [bond-name film-name]
                                     :where [(q {:find [bond bond-name (count bond)]
                                                 :keys [bond-with-most-films bond-name film-count]
                                                 :where [[_ :film--bond bond]
                                                         [bond :person--name bond-name]]
                                                 :order-by [[(count bond) :desc] [bond-name]]
                                                 :limit 1})

                                             [film :film--bond bond-with-most-films]
                                             [film :film--name film-name]]
                                     :order-by [[film-name]]}
                                   (assoc :basis {:tx !tx}))))
          "films made by the Bond with the most films"))

  (let [!tx (c2/submit-tx tu/*node* [[:put {:id :a1, :a 1}]
                                     [:put {:id :a2, :a 2}]
                                     [:put {:id :b2, :b 2}]
                                     [:put {:id :b3, :b 3}]])]
    (t/is (= [{:aid :a2, :bid :b2}]
             (c2/datalog-query tu/*node*
                               (-> '{:find [aid bid]
                                     :where [[aid :a a]
                                             (q {:find [bid]
                                                 :in [a]
                                                 :where [[bid :b a]]})]}
                                   (assoc :basis {:tx !tx}))))
          "(contrived) correlated sub-query")))
