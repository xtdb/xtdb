;; THIRD-PARTY SOFTWARE NOTICE
;; This file is derivative of test files found in the DataScript
;; project. The Datascript license is copied verbatim in this
;; directory as `LICENSE`.
;; https://github.com/tonsky/datascript

(ns core2.core.datalog-test
  (:require [clojure.test :as t :refer [deftest]]
            [core2.datalog :as c2]
            [core2.james-bond :as bond]
            [core2.node :as node]
            [core2.test-util :as tu]
            [core2.util :as util]))

(t/use-fixtures :each tu/with-mock-clock tu/with-node)

(def ivan+petr
  [[:put {:id :ivan, :first-name "Ivan", :last-name "Ivanov"}]
   [:put {:id :petr, :first-name "Petr", :last-name "Petrov"}]])

(deftest test-scan
  (let [tx (c2/submit-tx tu/*node* ivan+petr)]
    (t/is (= [{:name "Ivan"}
              {:name "Petr"}]
             (c2/q tu/*node*
                   (-> '{:find [name]
                         :from [(xt_docs {:first-name name})]}
                       (assoc :basis {:tx tx})))))

    (t/is (= [{:e :ivan, :name "Ivan"}
              {:e :petr, :name "Petr"}]
             (c2/q tu/*node*
                   (-> '{:find [e name]
                         :from [(xt_docs {:id e, :first-name name})]}
                       (assoc :basis {:tx tx}))))
          "returning eid")))

(deftest test-basic-query
  (let [tx (c2/submit-tx tu/*node* ivan+petr)]
    (t/is (= [{:e :ivan}]
             (c2/q tu/*node*
                   (-> '{:find [e]
                         :where [[e :first-name "Ivan"]]}
                       (assoc :basis {:tx tx}))))
          "query by single field")

    (t/is (= [{:first-name "Petr", :last-name "Petrov"}]
             (c2/q tu/*node*
                   (-> '{:find [first-name last-name]
                         :where [[e :first-name "Petr"]
                                 [e :first-name first-name]
                                 [e :last-name last-name]]}
                       (assoc :basis {:tx tx}))))
          "returning the queried field")

    (t/is (= [{:first-name "Petr", :last-name "Petrov"}]
             (c2/q tu/*node*
                   (-> '{:find [first-name last-name]
                         :where [[:petr :first-name first-name]
                                 [:petr :last-name last-name]]}
                       (assoc :basis {:tx tx}))))
          "literal eid")))

(deftest test-order-by
  (let [tx (c2/submit-tx tu/*node* ivan+petr)]
    (t/is (= [{:first-name "Ivan"} {:first-name "Petr"}]
             (c2/q tu/*node*
                   (-> '{:find [first-name]
                         :where [[e :first-name first-name]]
                         :order-by [[first-name]]}
                       (assoc :basis {:tx tx})))))

    (t/is (= [{:first-name "Petr"} {:first-name "Ivan"}]
             (c2/q tu/*node*
                   (-> '{:find [first-name]
                         :where [[e :first-name first-name]]
                         :order-by [[first-name :desc]]}
                       (assoc :basis {:tx tx})))))

    (t/is (= [{:first-name "Ivan"}]
             (c2/q tu/*node*
                   (-> '{:find [first-name]
                         :where [[e :first-name first-name]]
                         :order-by [[first-name]]
                         :limit 1}
                       (assoc :basis {:tx tx})))))

    (t/is (= [{:first-name "Petr"}]
             (c2/q tu/*node*
                   (-> '{:find [first-name]
                         :where [[e :first-name first-name]]
                         :order-by [[first-name :desc]]
                         :limit 1}
                       (assoc :basis {:tx tx})))))

    (t/is (= [{:first-name "Petr"}]
             (c2/q tu/*node*
                   (-> '{:find [first-name]
                         :where [[e :first-name first-name]]
                         :order-by [[first-name]]
                         :limit 1
                         :offset 1}
                       (assoc :basis {:tx tx})))))))

;; https://github.com/tonsky/datascript/blob/1.1.0/test/datascript/test/query.cljc#L12-L36
(deftest datascript-test-joins
  (let [tx (c2/submit-tx tu/*node*
                         [[:put {:id 1, :name "Ivan", :age 15}]
                          [:put {:id 2, :name "Petr", :age 37}]
                          [:put {:id 3, :name "Ivan", :age 37}]
                          [:put {:id 4, :age 15}]])]

    (t/is (= #{{:e 1} {:e 2} {:e 3}}
             (set (c2/q tu/*node*
                        (-> '{:find [e]
                              :where [[e :name]]}
                            (assoc :basis {:tx tx})))))
          "testing without V")

    (t/is (= #{{:e 1, :v 15} {:e 3, :v 37}}
             (set (c2/q tu/*node*
                        (-> '{:find [e v]
                              :where [[e :name "Ivan"]
                                      [e :age v]]}
                            (assoc :basis {:tx tx}))))))

    (t/is (= #{{:e1 1, :e2 1}
               {:e1 2, :e2 2}
               {:e1 3, :e2 3}
               {:e1 1, :e2 3}
               {:e1 3, :e2 1}}
             (set (c2/q tu/*node*
                        (-> '{:find [e1 e2]
                              :where [[e1 :name n]
                                      [e2 :name n]]}
                            (assoc :basis {:tx tx}))))))

    (t/is (= #{{:e 1, :e2 1, :n "Ivan"}
               {:e 3, :e2 3, :n "Ivan"}
               {:e 3, :e2 2, :n "Petr"}}
             (set (c2/q tu/*node*
                        (-> '{:find [e e2 n]
                              :where [[e :name "Ivan"]
                                      [e :age a]
                                      [e2 :age a]
                                      [e2 :name n]]}
                            (assoc :basis {:tx tx}))))))

    (t/is (= #{{:e 1, :e2 1, :n "Ivan"}
               {:e 2, :e2 2, :n "Petr"}
               {:e 3, :e2 3, :n "Ivan"}}
             (set (c2/q tu/*node*
                        (-> '{:find [e e2 n]
                              :where [[e :name n]
                                      [e :age a]
                                      [e2 :name n]
                                      [e2 :age a]]}
                            (assoc :basis {:tx tx})))))
          "multi-param join")

    (t/is (= #{{:e1 1, :e2 1, :a1 15, :a2 15}
               {:e1 1, :e2 3, :a1 15, :a2 37}
               {:e1 3, :e2 1, :a1 37, :a2 15}
               {:e1 3, :e2 3, :a1 37, :a2 37}}
             (set (c2/q tu/*node*
                        (-> '{:find [e1 e2 a1 a2]
                              :where [[e1 :name "Ivan"]
                                      [e2 :name "Ivan"]
                                      [e1 :age a1]
                                      [e2 :age a2]]}
                            (assoc :basis {:tx tx})))))
          "cross join required here")))

(deftest test-joins
  (let [tx (c2/submit-tx tu/*node* bond/tx-ops)]
    (t/is (= #{{:film-name "Skyfall", :bond-name "Daniel Craig"}}
             (set (c2/q tu/*node*
                        (-> '{:find [film-name bond-name]
                              :in [film]
                              :where [[film :film--name film-name]
                                      [film :film--bond bond]
                                      [bond :person--name bond-name]]}
                            (assoc :basis {:tx tx}))
                        "skyfall")))
          "one -> one")

    (t/is (= #{{:film-name "Casino Royale", :bond-name "Daniel Craig"}
               {:film-name "Quantum of Solace", :bond-name "Daniel Craig"}
               {:film-name "Skyfall", :bond-name "Daniel Craig"}
               {:film-name "Spectre", :bond-name "Daniel Craig"}}
             (set (c2/q tu/*node*
                        (-> '{:find [film-name bond-name]
                              :in [bond]
                              :where [[film :film--name film-name]
                                      [film :film--bond bond]
                                      [bond :person--name bond-name]]}
                            (assoc :basis {:tx tx}))
                        "daniel-craig")))
          "one -> many")))

;; https://github.com/tonsky/datascript/blob/1.1.0/test/datascript/test/query_aggregates.cljc#L14-L39
(t/deftest datascript-test-aggregates
  (let [tx (c2/submit-tx tu/*node*
                         [[:put {:id :cerberus, :heads 3}]
                          [:put {:id :medusa, :heads 1}]
                          [:put {:id :cyclops, :heads 1}]
                          [:put {:id :chimera, :heads 1}]])]
    (t/is (= #{{:heads 1, :count-heads 3} {:heads 3, :count-heads 1}}
             (set (c2/q tu/*node*
                        (-> '{:find [heads (count heads)]
                              :keys [heads count-heads]
                              :where [[monster :heads heads]]}
                            (assoc :basis {:tx tx})))))
          "head frequency")

    (t/is (= #{{:sum-heads 6, :min-heads 1, :max-heads 3, :count-heads 4}}
             (set (c2/q tu/*node*
                        (-> '{:find [(sum heads)
                                     (min heads)
                                     (max heads)
                                     (count heads)]
                              :keys [sum-heads min-heads max-heads count-heads]
                              :where [[monster :heads heads]]}
                            (assoc :basis {:tx tx})))))
          "various aggs")))

(t/deftest test-find-exprs
  (let [tx (c2/submit-tx tu/*node* [[:put {:id :o1, :unit-price 1.49, :quantity 4}]
                                    [:put {:id :o2, :unit-price 5.39, :quantity 1}]
                                    [:put {:id :o3, :unit-price 0.59, :quantity 7}]])]
    (t/is (= [{:oid :o1, :o-value 5.96}
              {:oid :o2, :o-value 5.39}
              {:oid :o3, :o-value 4.13}]
             (c2/q tu/*node*
                   (-> '{:find [oid (* unit-price qty)]
                         :keys [oid o-value]
                         :where [[oid :unit-price unit-price]
                                 [oid :quantity qty]]}
                       (assoc :basis {:tx tx})))))))

(deftest test-aggregate-exprs
  (let [tx (c2/submit-tx tu/*node* [[:put {:id :foo, :category :c0, :v 1}]
                                    [:put {:id :bar, :category :c0, :v 2}]
                                    [:put {:id :baz, :category :c1, :v 4}]])]
    (t/is (= [{:category :c0, :sum-doubles 6}
              {:category :c1, :sum-doubles 8}]
             (c2/q tu/*node*
                   (-> '{:find [category (sum (* 2 v))]
                         :keys [category sum-doubles]
                         :where [[e :category category]
                                 [e :v v]]}
                       (assoc :basis {:tx tx}))))))

  (t/is (= [{:x 0, :sum-y 0, :sum-expr 1}
            {:x 1, :sum-y 1, :sum-expr 5}
            {:x 2, :sum-y 3, :sum-expr 14}
            {:x 3, :sum-y 6, :sum-expr 30}]
           (c2/q tu/*node*
                 '{:find [x (sum y) (sum (+ (* y y) x 1))]
                   :keys [x sum-y sum-expr]
                   :in [[[x y]]]}
                 (for [x (range 4)
                       y (range (inc x))]
                   [x y]))))

  (t/is (= [{:sum-evens 20}]
           (c2/q tu/*node*
                 '{:find [(sum (if (= 0 (mod x 2)) x 0))]
                   :keys [sum-evens]
                   :in [[x ...]]}
                 (range 10)))
        "if")

  ;; TODO aggregates nested within other aggregates/forms
  ;; - doesn't appear in TPC-H but guess we'll want these eventually

  #_
  (t/is (= #{[28.5]}
           (xt/q (xt/db *api*)
                 '{:find [(/ (double (sum (* ?x ?x)))
                             (count ?x))]
                   :in [[?x ...]]}
                 (range 10)))
        "aggregates can be included in exprs")

  #_
  (t/is (thrown-with-msg? IllegalArgumentException
                          #"nested agg"
                          (xt/q (xt/db *api*)
                                '{:find [(sum (sum ?x))]
                                  :in [[?x ...]]}
                                (range 10)))
        "aggregates can't be nested")

  (t/testing "implicitly groups by variables present outside of aggregates"
    (t/is (= [{:x-div-y 1, :sum-z 2} {:x-div-y 2, :sum-z 3} {:x-div-y 2, :sum-z 5}]
             (c2/q tu/*node*
                   '{:find [(/ x y) (sum z)]
                     :keys [x-div-y sum-z]
                     :in [[[x y z]]]}
                   [[1 1 2]
                    [2 1 3]
                    [4 2 5]]))
          "even though (/ x y) yields the same result in the latter two rows, we group by them individually")

    #_
    (t/is (= #{[1 3] [1 7] [4 -1]}
             (c2/q tu/*node*
                   '{:find [x (- (sum z) y)]
                     :in [[[x y z]]]}
                   [[1 1 4]
                    [1 3 2]
                    [1 3 8]
                    [4 6 5]]))
          "groups by x and y in this case")))

(deftest test-query-with-in-bindings
  (let [tx (c2/submit-tx tu/*node* ivan+petr)]
    (t/is (= #{{:e :ivan}}
             (set (c2/q tu/*node*
                        (-> '{:find [e]
                              :in [name]
                              :where [[e :first-name name]]}
                            (assoc :basis {:tx tx}))
                        "Ivan")))
          "single arg")

    (t/is (= #{{:e :ivan}}
             (set (c2/q tu/*node*
                        (-> '{:find [e]
                              :in [first-name last-name]
                              :where [[e :first-name first-name]
                                      [e :last-name last-name]]}
                            (assoc :basis {:tx tx}))
                        "Ivan" "Ivanov")))
          "multiple args")

    (t/is (= #{{:e :ivan}}
             (set (c2/q tu/*node*
                        (-> '{:find [e]
                              :in [[first-name]]
                              :where [[e :first-name first-name]]}
                            (assoc :basis {:tx tx}))
                        ["Ivan"])))
          "tuple with 1 var")

    (t/is (= #{{:e :ivan}}
             (set (c2/q tu/*node*
                        (-> '{:find [e]
                              :in [[first-name last-name]]
                              :where [[e :first-name first-name]
                                      [e :last-name last-name]]}
                            (assoc :basis {:tx tx}))
                        ["Ivan" "Ivanov"])))
          "tuple with 2 vars")

    (t/testing "collection"
      (let [query (-> '{:find [e]
                        :in [[first-name ...]]
                        :where [[e :first-name first-name]]}
                      (assoc :basis {:tx tx}))]
        (t/is (= #{{:e :petr}}
                 (set (c2/q tu/*node* query ["Petr"]))))

        (t/is (= #{{:e :ivan} {:e :petr}}
                 (set (c2/q tu/*node* query ["Ivan" "Petr"]))))))

    (t/testing "relation"
      (let [query (-> '{:find [e]
                        :in [[[first-name last-name]]]
                        :where [[e :first-name first-name]
                                [e :last-name last-name]]}
                      (assoc :basis {:tx tx}))]

        (t/is (= #{{:e :ivan}}
                 (set (c2/q tu/*node* query
                            [{:first-name "Ivan", :last-name "Ivanov"}]))))

        (t/is (= #{{:e :ivan}}
                 (set (c2/q tu/*node* query
                            [["Ivan" "Ivanov"]]))))

        (t/is (= #{{:e :ivan} {:e :petr}}
                 (set (c2/q tu/*node* query
                            [{:first-name "Ivan", :last-name "Ivanov"}
                             {:first-name "Petr", :last-name "Petrov"}]))))

        (t/is (= #{{:e :ivan} {:e :petr}}
                 (set (c2/q tu/*node* query
                            [["Ivan" "Ivanov"]
                             ["Petr" "Petrov"]]))))))))

(deftest test-in-arity-exceptions
  (let [tx (c2/submit-tx tu/*node* ivan+petr)]
    (t/is (thrown-with-msg? IllegalArgumentException
                            #":in arity mismatch"
                            (c2/q tu/*node*
                                  (-> '{:find [e]
                                        :in [foo]
                                        :where [[e :foo foo]]}
                                      (assoc :basis {:tx tx})))))))

(deftest test-basic-predicates
  (let [tx (c2/submit-tx tu/*node* ivan+petr)]
    (t/is (= #{{:first-name "Ivan", :last-name "Ivanov"}}
             (set (c2/q tu/*node*
                        (-> '{:find [first-name last-name]
                              :where [[e :first-name first-name]
                                      [e :last-name last-name]
                                      [(< first-name "James")]]}
                            (assoc :basis {:tx tx}))))))

    (t/is (= #{{:first-name "Ivan", :last-name "Ivanov"}}
             (set (c2/q tu/*node*
                        (-> '{:find [first-name last-name]
                              :where [[e :first-name first-name]
                                      [e :last-name last-name]
                                      [(<= first-name "Ivan")]]}
                            (assoc :basis {:tx tx}))))))

    (t/is (empty? (c2/q tu/*node*
                        (-> '{:find [first-name last-name]
                              :where [[e :first-name first-name]
                                      [e :last-name last-name]
                                      [(<= first-name "Ivan")]
                                      [(> last-name "Ivanov")]]}
                            (assoc :basis {:tx tx})))))

    (t/is (empty (c2/q tu/*node*
                       (-> '{:find [first-name last-name]
                             :where [[e :first-name first-name]
                                     [e :last-name last-name]
                                     [(< first-name "Ivan")]]}
                           (assoc :basis {:tx tx})))))))

(deftest test-value-unification
  (let [tx (c2/submit-tx tu/*node*
                         (conj ivan+petr
                               [:put {:id :sergei :first-name "Sergei" :last-name "Sergei"}]
                               [:put {:id :jeff :first-name "Sergei" :last-name "but-different"}]))]
    (t/is (= [{:e :sergei, :n "Sergei"}]
             (c2/q
              tu/*node*
              (-> '{:find [e n]
                    :where [[e :last-name n]
                            [e :first-name n]]}
                  (assoc :basis {:tx tx})))))
    (t/is (= [{:e :sergei, :f :sergei, :n "Sergei"} {:e :sergei, :f :jeff, :n "Sergei"}]
             (c2/q
              tu/*node*
              (-> '{:find [e f n]
                    :where [[e :last-name n]
                            [e :first-name n]
                            [f :first-name n]]}
                  (assoc :basis {:tx tx})))))))

(deftest test-semi-join
  (let [tx (c2/submit-tx tu/*node*
                         [[:put {:id :ivan, :name "Ivan"}]
                          [:put {:id :petr, :name "Petr", :parent :ivan}]
                          [:put {:id :sergei, :name "Sergei", :parent :petr}]
                          [:put {:id :jeff, :name "Jeff", :parent :petr}]])]

    (t/is (= [{:e :ivan} {:e :petr}]
             (c2/q tu/*node*
                   '{:find [e]
                     :where [[e :name name]
                             (exists? {:find [e]
                                       :where [[c :parent e]]})]}))

          "find people who have children")

    (t/is (= [{:e :sergei} {:e :jeff}]
             (c2/q tu/*node*
                   '{:find [e]
                     :where [[e :name name]
                             [e :parent p]
                             (exists? {:find [p]
                                       :in [e]
                                       :where [[s :parent p]
                                               [(<> e s)]]})]}))
          "find people who have siblings")

    (t/is (thrown-with-msg? IllegalArgumentException
                            #":no-available-clauses"
                            (c2/q tu/*node*
                                  '{:find [e n]
                                    :where [[e :foo n]
                                            (exists? {:find [e]
                                                      :where [[e :first-name "Petr"]
                                                              [(= n 1)]]})]})))))

(deftest test-anti-join
  (let [tx (c2/submit-tx
            tu/*node*
            [[:put {:id :ivan, :first-name "Ivan", :last-name "Ivanov" :foo 1}]
             [:put {:id :petr, :first-name "Petr", :last-name "Petrov" :foo 1}]
             [:put {:id :sergei :first-name "Sergei" :last-name "Sergei" :foo 1}]])]

    (t/is (= [{:e :ivan} {:e :sergei}]
             (c2/q tu/*node*
                   '{:find [e]
                     :where [[e :foo 1]
                             (not-exists? {:find [e]
                                           :where [[e :first-name "Petr"]]})]})))

    (t/is (= [{:e :ivan} {:e :sergei}]
             (c2/q tu/*node*
                   '{:find [e]
                     :where [[e :foo n]
                             (not-exists? {:find [e n]
                                           :where [[e :first-name "Petr"]
                                                   [e :foo n]]})]})))

    (t/is (= []
             (c2/q tu/*node*
                   '{:find [e]
                     :where [[e :foo n]
                             (not-exists? {:find [e n]
                                           :where [[e :foo n]]})]})))

    (t/is (= [{:e :petr} {:e :sergei}]
             (c2/q tu/*node*
                   '{:find [e]
                     :where [[e :foo 1]
                             (not-exists? {:find [e]
                                           :where [[e :last-name "Ivanov"]]})]})))

    (t/is (= [{:e :ivan} {:e :petr} {:e :sergei}]
             (c2/q tu/*node*
                   '{:find [e]
                     :where [[e :foo 1]
                             (not-exists? {:find [e]
                                           :where [[e :first-name "Jeff"]]})]})))

    (t/is (= [{:e :ivan} {:e :petr}]
             (c2/q tu/*node*
                   '{:find [e]
                     :where [[e :foo 1]
                             (not-exists? {:find [e]
                                           :where [[e :first-name n]
                                                   [e :last-name n]]})]})))

    (t/is (= [{:e :ivan, :first-name "Petr", :last-name "Petrov", :a "Ivan", :b "Ivanov"}
              {:e :petr, :first-name "Ivan", :last-name "Ivanov", :a "Petr", :b "Petrov"}
              {:e :sergei, :first-name "Ivan", :last-name "Ivanov", :a "Sergei", :b "Sergei"}
              {:e :sergei, :first-name "Petr", :last-name "Petrov", :a "Sergei", :b "Sergei"}]
             (c2/q tu/*node*
                   '{:find [e first-name last-name a b]
                     :in [[[first-name last-name]]]
                     :where [[e :foo 1]
                             [e :first-name a]
                             [e :last-name b]
                             (not-exists? {:find [e first-name last-name]
                                           :where [[e :first-name first-name]
                                                   [e :last-name last-name]]})]}
                   [["Ivan" "Ivanov"]
                    ["Petr" "Petrov"]])))

    (t/testing "apply anti-joins"
      (t/is (= [{:n 1, :e :ivan} {:n 1, :e :petr} {:n 1, :e :sergei}]
               (c2/q tu/*node*
                     '{:find [e n]
                       :where [[e :foo n]
                               (not-exists? {:find [e]
                                             :in [n]
                                             :where [[e :first-name "Petr"]
                                                     [(= n 2)]]})]})))

      (t/is (= [{:n 1, :e :ivan} {:n 1, :e :sergei}]
               (c2/q tu/*node*
                     '{:find [e n]
                       :where [[e :foo n]
                               (not-exists? {:find [e]
                                             :in [n]
                                             :where [[e :first-name "Petr"]
                                                     [(= n 1)]]})]})))

      (t/is (= []
               (c2/q tu/*node*
                     '{:find [e n]
                       :where [[e :foo n]
                               (not-exists? {:find []
                                             :in [n]
                                             :where [[(= n 1)]]})]})))

      (t/is (= [{:n "Petr", :e :petr} {:n "Sergei", :e :sergei}]
               (c2/q tu/*node*
                     '{:find [e n]
                       :where [[e :first-name n]
                               (not-exists? {:find []
                                             :in [n]
                                             :where [[(= "Ivan" n)]]})]})))


      (t/is (= [{:n "Petr", :e :petr} {:n "Sergei", :e :sergei}]
               (c2/q tu/*node*
                     '{:find [e n]
                       :where [[e :first-name n]
                               (not-exists? {:find [n]
                                             :where [[e :first-name n]
                                                     [e :first-name "Ivan"]]})]})))

      (t/is (= [{:n 1, :e :ivan} {:n 1, :e :sergei}]
               (c2/q tu/*node*
                     '{:find [e n]
                       :where [[e :foo n]
                               (not-exists? {:find [e n]
                                             :where [[e :first-name "Petr"]
                                                     [e :foo n]
                                                     [(= n 1)]]})]})))


      (t/is (thrown-with-msg?
             IllegalArgumentException
             #":no-available-clauses"
             (c2/q tu/*node*
                   (-> '{:find [e n]
                         :where [[e :foo n]
                                 (not-exists? {:find [e]
                                               :where [[e :first-name "Petr"]
                                                       [(= n 1)]]})]}
                       (assoc :basis {:tx tx})))))

      ;; TODO what to do if arg var isn't used, either remove it from the join
      ;; or convert the anti-join to an apply and param all the args
      #_(t/is (= [{:e :ivan} {:e :sergei}]
                 (c2/q tu/*node*
                       '{:find [e n]
                         :where [[e :foo n]
                                 (not-exists? {:find [e]
                                               :in [n]
                                               :where [[e :first-name "Petr"]]})]}))))

    (t/testing "Multiple anti-joins"
      (t/is (= [{:n "Petr", :e :petr}]
               (c2/q tu/*node*
                     '{:find [e n]
                       :where [[e :first-name n]
                               (not-exists? {:find []
                                             :in [n]
                                             :where [[(= n "Ivan")]]})
                               (not-exists? {:find [e]
                                             :where [[e :first-name "Sergei"]]})]}))))))

(deftest calling-a-function-580
  (let [tx (c2/submit-tx tu/*node*
                         [[:put {:id :ivan, :age 15}]
                          [:put {:id :petr, :age 22}]
                          [:put {:id :slava, :age 37}]])]
    (t/is (= #{{:e1 :petr, :e2 :ivan, :e3 :slava}
               {:e1 :ivan, :e2 :petr, :e3 :slava}}
             (set
              (c2/q tu/*node*
                    (-> '{:find [e1 e2 e3]
                          :where [[e1 :age a1]
                                  [e2 :age a2]
                                  [e3 :age a3]
                                  [(+ a1 a2) a12]
                                  [(= a12 a3)]]}
                        (assoc :basis {:tx tx}))))))

    (t/is (= #{{:e1 :petr, :e2 :ivan, :e3 :slava}
               {:e1 :ivan, :e2 :petr, :e3 :slava}}
             (set
              (c2/q tu/*node*
                    (-> '{:find [e1 e2 e3]
                          :where [[e1 :age a1]
                                  [e2 :age a2]
                                  [e3 :age a3]
                                  [(+ a1 a2) a3]]}
                        (assoc :basis {:tx tx}))))))))

(deftest test-nested-expressions-581
  (let [tx (c2/submit-tx tu/*node*
                         [[:put {:id :ivan, :age 15}]
                          [:put {:id :petr, :age 22, :height 240, :parent 1}]
                          [:put {:id :slava, :age 37, :parent 2}]])]

    (t/is (= [{:e1 :ivan, :e2 :petr, :e3 :slava}
              {:e1 :petr, :e2 :ivan, :e3 :slava}]
             (c2/q tu/*node*
                   (-> '{:find [e1 e2 e3]
                         :where [[e1 :age a1]
                                 [e2 :age a2]
                                 [e3 :age a3]
                                 [(= (+ a1 a2) a3)]]}
                       (assoc :basis {:tx tx})))))

    (t/is (= [{:a1 15, :a2 22, :a3 37, :sum-ages 74, :inc-sum-ages 75}]
             (c2/q tu/*node*
                   (-> '{:find [a1 a2 a3 sum-ages inc-sum-ages]
                         :where [[:ivan :age a1]
                                 [:petr :age a2]
                                 [:slava :age a3]
                                 [(+ (+ a1 a2) a3) sum-ages]
                                 [(+ a1 (+ a2 a3 1)) inc-sum-ages]]}
                       (assoc :basis {:tx tx})))))

    (t/testing "unifies results of two calls"
      (t/is (= [{:a1 15, :a2 22, :a3 37, :sum-ages 74}]
               (c2/q tu/*node*
                     (-> '{:find [a1 a2 a3 sum-ages]
                           :where [[:ivan :age a1]
                                   [:petr :age a2]
                                   [:slava :age a3]
                                   [(+ (+ a1 a2) a3) sum-ages]
                                   [(+ a1 (+ a2 a3)) sum-ages]]}
                         (assoc :basis {:tx tx})))))

      (t/is (= []
               (c2/q tu/*node*
                     (-> '{:find [a1 a2 a3 sum-ages]
                           :where [[:ivan :age a1]
                                   [:petr :age a2]
                                   [:slava :age a3]
                                   [(+ (+ a1 a2) a3) sum-ages]
                                   [(+ a1 (+ a2 a3 1)) sum-ages]]}
                         (assoc :basis {:tx tx}))))))))

(deftest test-union-join
  (let [tx (c2/submit-tx tu/*node* [[:put {:id :ivan, :age 20, :role :developer}]
                                    [:put {:id :oleg, :age 30, :role :manager}]
                                    [:put {:id :petr, :age 35, :role :qa}]
                                    [:put {:id :sergei, :age 35, :role :manager}]])]

    (letfn [(q [query]
              (c2/q tu/*node*
                    (-> query
                        (assoc :basis {:tx tx}))))]
      (t/is (= [{:e :ivan}]
               (q '{:find [e]
                    :where [(union-join [e]
                                        [e :role :developer]
                                        [e :age 30])
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
                            [(<> e :sergei)]]})))

      (t/testing "functions within union-join"
        (t/is (= [{:age 35, :older-age 45}]
                 (q '{:find [age older-age]
                      :where [[:sergei :age age]
                              (union-join [age older-age]
                                          [(+ age 10) older-age])]})))))))

(deftest test-union-join-with-subquery-638
  (let [tx (c2/submit-tx tu/*node* [[:put {:id :ivan, :age 20, :role :developer}]
                                    [:put {:id :oleg, :age 30, :role :manager}]
                                    [:put {:id :petr, :age 35, :role :qa}]
                                    [:put {:id :sergei, :age 35, :role :manager}]])]
    (t/is (= [{:e :oleg}]
             (c2/q tu/*node* (-> '{:find [e]
                                   :where [(union-join [e]
                                                       (q {:find [e]
                                                           :where [[e :age 30]]}))]}
                                 (assoc :basis {:tx tx})))))))

(deftest test-nested-query
  (let [tx (c2/submit-tx tu/*node* bond/tx-ops)]
    (t/is (= [{:bond-name "Roger Moore", :film-name "A View to a Kill"}
              {:bond-name "Roger Moore", :film-name "For Your Eyes Only"}
              {:bond-name "Roger Moore", :film-name "Live and Let Die"}
              {:bond-name "Roger Moore", :film-name "Moonraker"}
              {:bond-name "Roger Moore", :film-name "Octopussy"}
              {:bond-name "Roger Moore", :film-name "The Man with the Golden Gun"}
              {:bond-name "Roger Moore", :film-name "The Spy Who Loved Me"}]
             (c2/q tu/*node*
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
                       (assoc :basis {:tx tx}))))
          "films made by the Bond with the most films"))

  (let [tx (c2/submit-tx tu/*node* [[:put {:id :a1, :a 1}]
                                    [:put {:id :a2, :a 2}]
                                    [:put {:id :b2, :b 2}]
                                    [:put {:id :b3, :b 3}]])]
    (t/is (= [{:aid :a2, :bid :b2}]
             (c2/q tu/*node*
                   (-> '{:find [aid bid]
                         :where [[aid :a a]
                                 (q {:find [bid]
                                     :in [a]
                                     :where [[bid :b a]]})]}
                       (assoc :basis {:tx tx}))))
          "(contrived) correlated sub-query")))

(t/deftest test-explicit-unwind-574
  (let [tx (c2/submit-tx tu/*node* bond/tx-ops)]
    (t/is (= [{:brand "Aston Martin", :model "DB10"}
              {:brand "Aston Martin", :model "DB5"}
              {:brand "Jaguar", :model "C-X75"}
              {:brand "Jaguar", :model "XJ8"}
              {:brand "Land Rover", :model "Discovery Sport"}
              {:brand "Land Rover", :model "Land Rover Defender Bigfoot"}
              {:brand "Land Rover", :model "Range Rover Sport"}
              {:brand "Mercedes Benz", :model "S-Class"}
              {:brand "Rolls-Royce", :model "Silver Wraith"}]

             (c2/q tu/*node*
                   (-> '{:find [brand model]
                         :in [film]
                         :where [[film :film--vehicles [vehicle ...]]
                                 [vehicle :vehicle--brand brand]
                                 [vehicle :vehicle--model model]]
                         :order-by [[brand] [model]]}
                       (assoc :basis {:tx tx}))
                   :spectre)))))

(t/deftest bug-non-string-table-names-599
  (with-open [node (node/start-node {:core2/live-chunk {:rows-per-block 10, :rows-per-chunk 1000}})]
    (letfn [(submit-ops! [ids]
              (last (for [tx-ops (->> (for [id ids]
                                        [:put {:id id,
                                               :data (str "data" id)
                                               :_table :t1}])
                                      (partition-all 20))]
                      (c2/submit-tx node tx-ops))))

            (count-table [tx]
              (-> (c2/q node (-> '{:find [(count id)]
                                   :keys [id-count]
                                   :from [(t1 [id])]}
                                 (assoc :basis {:tx tx})))
                  (first)
                  (:id-count)))]

      (let [tx (submit-ops! (range 80))]
        (t/is (= 80 (count-table tx))))

      (let [tx (submit-ops! (range 80 160))]
        (t/is (= 160 (count-table tx)))))))

(t/deftest bug-dont-throw-on-non-existing-column-597
  (with-open [node (node/start-node {:core2/live-chunk {:rows-per-block 10, :rows-per-chunk 1000}})]
    (letfn [(submit-ops! [ids]
              (last (for [tx-ops (->> (for [id ids]
                                        [:put {:id id,
                                               :data (str "data" id)
                                               :_table :t1}])
                                      (partition-all 20))]
                      (c2/submit-tx node tx-ops))))]

      (let [_tx1 (c2/submit-tx node [[:put {:id 0 :foo :bar}]])
            tx2 (submit-ops! (range 1010))]
        (t/is (= 1010 (-> (c2/q node '{:find [(count id)]
                                       :keys [id-count]
                                       :from [(t1 {:id id})]})
                          (first)
                          (:id-count))))

        (t/is (= [] (c2/q node '{:find [id]
                                 :from [(xt_docs [id some-attr])]})))))))

(t/deftest add-better-metadata-support-for-keywords
  (with-open [node (node/start-node {:core2/live-chunk {:rows-per-block 10, :rows-per-chunk 1000}})]
    (letfn [(submit-ops! [ids]
              (last (for [tx-ops (->> (for [id ids]
                                        [:put {:id id,
                                               :data (str "data" id)
                                               :_table :t1}])
                                      (partition-all 20))]
                      (c2/submit-tx node tx-ops))))]
      (let [_tx1 (c2/submit-tx node [[:put {:id :some-doc}]])
            ;; going over the chunk boundary
            tx2 (submit-ops! (range 200))]
        (t/is (= [{:id :some-doc}]
                 (c2/q node (-> '{:find [id]
                                  :where [[id :id :some-doc]]}
                                (assoc :basis {:tx tx2})))))))))

(deftest test-subquery-unification
  (let [tx (c2/submit-tx tu/*node* [[:put {:id :a1, :a 2 :b 1 :_table "a"}]
                                    [:put {:id :a2, :a 2 :b 3 :_table "a"}]
                                    [:put {:id :a3, :a 2 :b 0 :_table "a"}]])]

    (t/testing "variables returned from subqueries that must be run as an apply are unified"

      (t/testing "subquery"
        (t/is (= [{:aid :a2 :a 2 :b 3}]
                 (c2/q tu/*node*
                       (-> '{:find [aid a b]
                             :from [(a [{:id aid} a b])]
                             :where [(q {:find [b]
                                         :in [a]
                                         :where [[(+ a 1) b]]})]}
                           (assoc :basis {:tx tx}))))
              "b is unified"))

      (t/testing "union-join"
        (t/is (= [{:aid :a2 :a 2 :b 3}]
                 (c2/q tu/*node*
                       (-> '{:find [aid a b]
                             :from [(a [{:id aid} a b])]
                             :where [(union-join [a b]
                                                 [(+ a 1) b])]}
                           (assoc :basis {:tx tx}))))
              "b is unified")))))

(deftest test-basic-rules
  (c2/submit-tx tu/*node* [[:put {:id :ivan :name "Ivan" :last-name "Ivanov" :age 21}]
                           [:put {:id :petr :name "Petr" :last-name "Petrov" :age 18}]
                           [:put {:id :georgy :name "Georgy" :last-name "George" :age 17}]])
  (letfn [(q [query & args]
            (apply c2/q tu/*node* query args))]

    (t/testing "without rule"
      (t/is (= [{:i :ivan}] (q '{:find [i]
                                 :where [[i :age age]
                                         [(>= age 21)]]}))))

    (t/testing "empty rules"
      (t/is (= [{:i :ivan}] (q '{:find [i]
                                 :where [[i :age age]
                                         [(>= age 21)]]
                                 :rules []}))))

    (t/testing "rule using required bound args"
      (t/is (= [{:i :ivan}] (q '{:find [i]
                                 :where [[i :age age]
                                         (over-twenty-one? age)]
                                 :rules [[(over-twenty-one? age)
                                          [(>= age 21)]]]}))))

    (t/testing "rule using required bound args (different arg names)"
      (t/is (= [{:i :ivan}] (q '{:find [i]
                                 :where [[i :age age]
                                         (over-twenty-one? age)]
                                 :rules [[(over-twenty-one? age-other)
                                          [(>= age-other 21)]]]}))))

    (t/testing "rules directly on arguments"
      (t/is (= [{:age 21}] (q '{:find [age]
                                :where [(over-twenty-one? age)]
                                :in [age]
                                :rules [[(over-twenty-one? age)
                                         [(>= age 21)]]]}
                              21)))

      (t/is (= [] (q '{:find [age]
                       :where [(over-twenty-one? age)]
                       :in [age]
                       :rules [[(over-twenty-one? age)
                                [(>= age 21)]]]}
                     20))))

    (t/testing "testing rule with multiple args"
      (t/is (= [{:i :petr, :age 18, :u :ivan}
                {:i :georgy, :age 17, :u :ivan}
                {:i :georgy, :age 17, :u :petr}]
               (q '{:find [i age u]
                    :where [(older-users age u)
                            [i :age age]]
                    :rules [[(older-users age u)
                             [u :age age2]
                             [(> age2 age)]]]}))))

    (t/testing "testing rule with multiple args (different arg names in rule)"
      (t/is (= [{:i :petr, :age 18, :u :ivan}
                {:i :georgy, :age 17, :u :ivan}
                {:i :georgy, :age 17, :u :petr}]
               (q '{:find [i age u]
                    :where [(older-users age u)
                            [i :age age]]
                    :rules [[(older-users age-other u-other)
                             [u-other :age age2]
                             [(> age2 age-other)]]]}))))


    (t/testing "nested rules"
      (t/is (= [{:i :ivan}] (q '{:find [i]
                                 :where [[i :age age]
                                         (over-twenty-one? age)]
                                 :rules [[(over-twenty-one? x)
                                          (over-twenty-one-internal? x)]
                                         [(over-twenty-one-internal? y)
                                          [(>= y 21)]]]}))))

    (t/testing "nested rules bound (same arg names)"
      (t/is (= [{:i :ivan}] (q '{:find [i]
                                 :where [[i :age age]
                                         (over-twenty-one? age)]
                                 :rules [[(over-twenty-one? age)
                                          (over-twenty-one-internal? age)]
                                         [(over-twenty-one-internal? age)
                                          [(>= age 21)]]]}))))

    (t/is (= [{:i :ivan}] (q '{:find [i]
                               :where [[i :age age]
                                       (over-twenty-one? age)]
                               :rules [[(over-twenty-one? x)
                                        (over-twenty-one-internal? x)]
                                       [(over-twenty-one-internal? y)
                                        [(>= y 21)]]]})))

    (t/testing "rule using literal arguments"
      (t/is (= [{:i :ivan}] (q '{:find [i]
                                 :where [[i :age age]
                                         (over-age? age 21)]
                                 :rules [[(over-age? age required-age)
                                          [(>= age required-age)]]]}))))

    (t/testing "same arg-name different position test (shadowing test)"
      (t/is (= [{:i :ivan}] (q '{:find [i]
                                 :where [[i :age age]
                                         (over-age? age 21)]
                                 :rules [[(over-age? other-age age)
                                          [(>= other-age age)]]]}))))

    (t/testing "semi-join in rule"
      (t/is (= [{:i :petr, :age 18}
                {:i :georgy, :age 17}]
               (q '{:find [i age]
                    :where [[i :age age]
                            (older? age)]
                    :rules [[(older? age)
                             (exists? {:find []
                                       :in [age]
                                       :where [[i :age age2]
                                               [(> age2 age)]]})]]}))))


    (t/testing "anti-join in rule"
      (t/is (= [{:i :ivan, :age 21}]
               (q  '{:find [i age]
                     :where [[i :age age]
                             (not-older? age)]
                     :rules [[(not-older? age)
                              (not-exists? {:find []
                                            :in [age]
                                            :where [[i :age age2]
                                                    [(> age2 age)]]})]]}))))

    (t/testing "subquery in rule"
      (t/is (= [{:i :petr, :other-age 21}
                {:i :georgy, :other-age 21}
                {:i :georgy, :other-age 18}]
               (q '{:find [i other-age]
                    :where [[i :age age]
                            (older-ages age other-age)]
                    :rules [[(older-ages age other-age)
                             (q {:find [other-age]
                                 :in [age]
                                 :where [[i :age other-age]
                                         [(> other-age age)]]})]]}))))

    (t/testing "subquery in rule with aggregates, expressions and order-by"
      (t/is [{:i :ivan, :max-older-age nil, :max-older-age-times2 nil}
             {:i :petr, :max-older-age 21, :max-older-age-times2 42}
             {:i :georgy, :max-older-age 21, :max-older-age-times2 42}]
            (q '{:find [i max-older-age max-older-age-times2]
                 :where [[i :age age]
                         (older-ages age max-older-age max-older-age-times2)]
                 :rules [[(older-ages age max-older-age max-older-age2)
                          (q {:find [(max older-age) (max (* older-age 2))]
                              :keys [max-older-age max-older-age2]
                              :in [age]
                              :where [[i :age older-age]
                                      [(> older-age age)]]
                              :order-by [[(max older-age) :desc]]})]]})))

    (t/testing "rule using multiple branches"
      (t/is (= [{:i :ivan}] (q '{:find [i]
                                 :where [(is-ivan-or-bob? i)]
                                 :rules [[(is-ivan-or-bob? i)
                                          [i :name "Bob"]]
                                         [(is-ivan-or-bob? i)
                                          [i :name "Ivan"]
                                          [i :last-name "Ivanov"]]]})))

      (t/is (= [{:name "Petr"}] (q '{:find [name]
                                     :where [[i :name name]
                                             (not-exists? {:find [i]
                                                           :where [(is-ivan-or-georgy? i)]})]
                                     :rules [[(is-ivan-or-georgy? i)
                                              [i :name "Ivan"]]
                                             [(is-ivan-or-georgy? i)
                                              [i :name "Georgy"]]]})))

      (t/is (= [{:i :ivan}
                {:i :petr}]
               (q '{:find [i]
                    :where [(is-ivan-or-petr? i)]
                    :rules [[(is-ivan-or-petr? i)
                             [i :name "Ivan"]]
                            [(is-ivan-or-petr? i)
                             [i :name "Petr"]]]}))))

    (t/testing "union-join with rules"
      (t/is (= [{:i :ivan}]
               (q '{:find [i]
                    :where [(union-join [i]
                                        (is-ivan-or-bob? i))]
                    :rules [[(is-ivan-or-bob? i)
                             [i :name "Bob"]]
                            [(is-ivan-or-bob? i)
                             [i :name "Ivan"]
                             [i :last-name "Ivanov"]]]}))))


    (t/testing "subquery with rule"
      (t/is (= [{:i :ivan}]
               (q '{:find [i]
                    :where [(q {:find [i]
                                :where [(is-ivan-or-bob? i)]})]
                    :rules [[(is-ivan-or-bob? i)
                             [i :name "Bob"]]
                            [(is-ivan-or-bob? i)
                             [i :name "Ivan"]
                             [i :last-name "Ivanov"]]]})))))

  (t/is (thrown-with-msg?
         IllegalArgumentException
         #":unknown-rule"
         (c2/q tu/*node* '{:find [i]
                           :where [[i :age age]
                                   (over-twenty-one? age)]})))

  (t/is (thrown-with-msg?
         IllegalArgumentException
         #":rule-wrong-arity"
         (c2/q tu/*node* '{:find [i]
                           :where [[i :age age]
                                   (over-twenty-one? i age)]
                           :rules [[(over-twenty-one? x)
                                    [(>= x 21)]]]})))
  (t/is (thrown-with-msg?
         IllegalArgumentException
         #":rule-definitions-require-unique-arity"
         (c2/q tu/*node* '{:find [i]
                           :where [[i :age age]
                                   (is-ivan-or-petr? i name)]
                           :rules [[(is-ivan-or-petr? i name)
                                    [i :name "Ivan"]]
                                   [(is-ivan-or-petr? i)
                                    [i :name "Petr"]]]}))))


(t/deftest test-temporal-opts
  (letfn [(q [query tx current-time]
            (c2/q tu/*node*
                  (-> query
                      (assoc :basis {:tx tx, :current-time (util/->instant current-time)}))))]

    ;; Matthew 2015+

    ;; tx0
    ;; 2018/2019: Matthew, Mark
    ;; 2021+: Matthew, Luke

    ;; tx1
    ;; 2016-2018: Matthew, John
    ;; 2018-2020: Matthew, Mark, John
    ;; 2020: Matthew
    ;; 2021-2022: Matthew, Luke
    ;; 2023: Matthew, Mark (again)
    ;; 2024+: Matthew

    (let [tx0 (c2/submit-tx tu/*node* [[:put {:id :matthew} {:app-time-start #inst "2015"}]
                                       [:put {:id :mark} {:app-time-start #inst "2018", :app-time-end #inst "2020"}]
                                       [:put {:id :luke} {:app-time-start #inst "2021"}]])

          tx1 (c2/submit-tx tu/*node* [[:delete :luke {:app-time-start #inst "2022"}]
                                       [:put {:id :mark} {:app-time-start #inst "2023", :app-time-end #inst "2024"}]
                                       [:put {:id :john} {:app-time-start #inst "2016", :app-time-end #inst "2020"}]])]

      (t/is (= [{:id :matthew}, {:id :mark}]
               (q '{:find [id], :where [[id :id]]}, tx1, #inst "2023")))

      (t/is (= [{:id :matthew}, {:id :luke}]
               (q '{:find [id], :where [[id :id]]}, tx1, #inst "2021"))
            "back in app-time")

      (t/is (= [{:id :matthew}, {:id :luke}]
               (q '{:find [id], :where [[id :id]]}, tx0, #inst "2023"))
            "back in sys-time")

      (t/is (= [{:id :matthew, :app-start (util/->zdt #inst "2015"), :app-end (util/->zdt util/end-of-time)}
                {:id :mark, :app-start (util/->zdt #inst "2018"), :app-end (util/->zdt #inst "2020")}
                {:id :luke, :app-start (util/->zdt #inst "2021"), :app-end (util/->zdt #inst "2022")}
                {:id :mark, :app-start (util/->zdt #inst "2023"), :app-end (util/->zdt #inst "2024")}
                {:id :john, :app-start (util/->zdt #inst "2016"), :app-end (util/->zdt #inst "2020")}]
               (q '{:find [id app-start app-end]
                    :from [(xt_docs [id {:application_time_start app-start
                                         :application_time_end app-end}]
                                    {:for-app-time :all-time})]}
                  tx1, nil))
            "entity history, all time")

      (t/is (= [{:id :matthew, :app-start (util/->zdt #inst "2015"), :app-end (util/->zdt util/end-of-time)}
                {:id :luke, :app-start (util/->zdt #inst "2021"), :app-end (util/->zdt #inst "2022")}]
               (q '{:find [id app-start app-end]
                    :from [(xt_docs [id {:application_time_start app-start
                                         :application_time_end app-end}]
                                    {:for-app-time [:in #inst "2021", #inst "2023"]})]}
                  tx1, nil))
            "entity history, range")

      (t/is (= [{:id :matthew}, {:id :mark}]
               (q '{:find [id],
                    :from [(xt_docs [id]
                                    {:for-app-time [:at #inst "2018"]})
                           (xt_docs [id]
                                    {:for-app-time [:at #inst "2023"]})]},
                  tx1, nil))
            "cross-time join - who was here in both 2018 and 2023?")

      (t/is (= [{:id :matthew} {:id :mark}]
               (q '{:find [id],
                    :from [(xt_docs [id {:application_time_start app-start
                                         :application_time_end app-end}]
                                    {:for-app-time :all-time})

                           (xt_docs [{:id :john
                                      :application_time_start john-start
                                      :application_time_end john-end}]
                                    {:for-app-time :all-time})]

                    :where [[(<> id :john)]

                            ;; eventually: 'overlaps?'
                            [(< app-start john-end)]
                            [(> app-end john-start)]]},
                  tx1, nil))
            "who worked with John?")

      (t/is (= [{:vt-start (util/->zdt #inst "2021")
                 :vt-end (util/->zdt util/end-of-time)
                 :tt-start (util/->zdt #inst "2020-01-01")
                 :tt-end (util/->zdt #inst "2020-01-02")}
                {:vt-start (util/->zdt #inst "2021")
                 :vt-end (util/->zdt #inst "2022")
                 :tt-start (util/->zdt #inst "2020-01-02")
                 :tt-end (util/->zdt util/end-of-time)}]
               (q '{:find [vt-start vt-end tt-start tt-end]
                    :from [(xt_docs {:id :luke
                                     :application_time_start vt-start
                                     :application_time_end vt-end
                                     :system_time_start tt-start
                                     :system_time_end tt-end}
                                    {:for-app-time :all-time
                                     :for-sys-time :all-time})]}
                  tx1 nil))

            "for all sys time"))))

(t/deftest test-snodgrass-99-tutorial
  (letfn [(q [query tx current-time & in]
            (apply c2/q
                   tu/*node*
                   (-> query
                       (assoc :basis {:tx tx, :current-time (util/->instant current-time)}))
                   in))]

    (let [tx0 (c2/submit-tx tu/*node*
                            [[:put {:id 1 :customer-number 145 :property-number 7797} {:app-time-start #inst "1998-01-10"}]]
                            {:sys-time #inst "1998-01-10"})

          tx1 (c2/submit-tx tu/*node*
                            [[:put {:id 1 :customer-number 827 :property-number 7797} {:app-time-start #inst "1998-01-15"}]]
                            {:sys-time #inst "1998-01-15"})

          tx2 (c2/submit-tx tu/*node*
                            [[:delete 1 {:app-time-start #inst "1998-01-20"}]]
                            {:sys-time #inst "1998-01-20"})

          tx3 (c2/submit-tx tu/*node*
                            [[:put {:id 1 :customer-number 145 :property-number 7797} {:app-time-start #inst "1998-01-03"
                                                                                       :app-time-end #inst "1998-01-10"}]]
                            {:sys-time #inst "1998-01-23"})

          tx4 (c2/submit-tx tu/*node*
                            [[:delete 1 {:app-time-start #inst "1998-01-03" :app-time-end #inst "1998-01-05"}]]
                            {:sys-time #inst "1998-01-26"})

          tx5 (c2/submit-tx tu/*node*
                            [[:put {:id 1 :customer-number 145 :property-number 7797} {:app-time-start #inst "1998-01-05"
                                                                                       :app-time-end #inst "1998-01-12"}]
                             [:put {:id 1 :customer-number 827 :property-number 7797} {:app-time-start #inst "1998-01-12"
                                                                                       :app-time-end #inst "1998-01-20"}]]
                            {:sys-time #inst "1998-01-28"})

          tx6 (c2/submit-tx tu/*node*
                            [[:put {:id :delete-1-week-records,
                                    :fn #c2/clj-form (fn delete-1-weeks-records []
                                                       (->> (q '{:find [id app-start app-end]
                                                                 :from [(xt_docs [id
                                                                                  {:application_time_start app-start
                                                                                   :application_time_end app-end}]
                                                                                 {:for-app-time :all-time})]
                                                                 :where [[(= (- #inst "1970-01-08" #inst "1970-01-01")
                                                                             (- app-end app-start))]]})
                                                            (map (fn [{:keys [id app-start app-end]}]
                                                                   [:delete id {:app-time-start app-start
                                                                                :app-time-end app-end}]))))}]
                             [:call :delete-1-week-records]]
                            {:sys-time #inst "1998-01-30"})

          tx7 (c2/submit-tx tu/*node*
                            [[:put {:id 2 :customer-number 827 :property-number 3621} {:app-time-start #inst "1998-01-15"}]]
                            {:sys-time #inst "1998-01-31"})]

      (t/is (= [{:cust 145 :app-start (util/->zdt #inst "1998-01-10")}]
               (q '{:find [cust app-start]
                    :from [(xt_docs {:customer-number cust, :application_time_start app-start}
                                    {:for-app-time :all-time})]}
                  tx0, nil))
            "as-of 14 Jan")

      (t/is (= [{:cust 145, :app-start (util/->zdt #inst "1998-01-10")}
                {:cust 827, :app-start (util/->zdt #inst "1998-01-15")}]
               (q '{:find [cust app-start]
                    :from [(xt_docs {:customer-number cust, :application_time_start app-start}
                                    {:for-app-time :all-time})]}
                  tx1, nil))
            "as-of 18 Jan")

      (t/is (= [{:cust 145, :app-start (util/->zdt #inst "1998-01-05")}
                {:cust 827, :app-start (util/->zdt #inst "1998-01-12")}]
               (q '{:find [cust app-start]
                    :from [(xt_docs {:customer-number cust, :application_time_start app-start}
                                    {:for-app-time :all-time})]
                    :order-by [[app-start :asc]]}
                  tx5, nil))
            "as-of 29 Jan")

      (t/is (= [{:cust 827, :app-start (util/->zdt #inst "1998-01-12"), :app-end (util/->zdt #inst "1998-01-20")}]
               (q '{:find [cust app-start app-end]
                    :from [(xt_docs {:customer-number cust,
                                     :application_time_start app-start
                                     :application_time_end app-end}
                                    {:for-app-time :all-time})]
                    :order-by [[app-start :asc]]}
                  tx6, nil))
            "'as best known' (as-of 30 Jan)")

      (t/is (= [{:prop 3621, :vt-begin (util/->zdt #inst "1998-01-15"), :vt-end (util/->zdt #inst "1998-01-20")}]
               (q '{:find [prop (greatest app-start app-start2) (least app-end app-end2)]
                    :keys [prop vt-begin vt-end]
                    :in [in-prop]
                    :from [(xt_docs {:property-number in-prop
                                     :customer-number cust
                                     :application_time_start app-start
                                     :application_time_end app-end}
                                    {:for-app-time :all-time})

                           (xt_docs {:property-number prop
                                     :customer-number cust
                                     :application_time_start app-start2
                                     :application_time_end app-end2}
                                    {:for-app-time :all-time})]
                    :where [[(<> prop in-prop)]
                            ;; eventually: 'overlaps?'
                            [(< app-start app-end2)]
                            [(> app-end app-start2)]]
                    :order-by [[app-start :asc]]}
                  tx7, nil, 7797))
            "Case 2: Valid-time sequenced and transaction-time current")

      (t/is (= [{:prop 3621,
                 :vt-begin (util/->zdt #inst "1998-01-15"),
                 :vt-end (util/->zdt #inst "1998-01-20"),
                 :recorded-start (util/->zdt #inst "1998-01-31"),
                 :recorded-stop (util/->zdt util/end-of-time)}]
               (q '{:find [prop (greatest app-start app-start2) (least app-end app-end2) (greatest sys-start sys-start2) (least sys-end sys-end2)]
                    :keys [prop vt-begin vt-end recorded-start recorded-stop]
                    :in [in-prop]
                    :from [(xt_docs {:property-number in-prop
                                     :customer-number cust
                                     :application_time_start app-start
                                     :application_time_end app-end
                                     :system_time_start sys-start
                                     :system_time_end sys-end}
                                    {:for-app-time :all-time
                                     :for-sys-time :all-time})

                           (xt_docs {:customer-number cust
                                     :property-number prop
                                     :application_time_start app-start2
                                     :application_time_end app-end2
                                     :system_time_start sys-start2
                                     :system_time_end sys-end2}

                                    {:for-app-time :all-time
                                     :for-sys-time :all-time})]
                    :where [[(<> prop in-prop)]
                            ;; eventually: 'overlaps?'
                            [(< app-start app-end2)]
                            [(> app-end app-start2)]
                            ;; eventually: 'overlaps?'
                            [(< sys-start sys-end2)]
                            [(> sys-end sys-start2)]]
                    :order-by [[app-start :asc]]}
                  tx7, nil, 7797))
            "Case 5: Application-time sequenced and system-time sequenced")

      (t/is (= [{:prop 3621,
                 :vt-begin (util/->zdt #inst "1998-01-15"),
                 :vt-end (util/->zdt #inst "1998-01-20"),
                 :recorded-start (util/->zdt #inst "1998-01-31")}]
               (q '{:find [prop (greatest app-start app-start2) (least app-end app-end2) sys-start2]
                    :keys [prop vt-begin vt-end recorded-start]
                    :in [in-prop]
                    :from [(xt_docs {:property-number in-prop
                                     :customer-number cust
                                     :application_time_start app-start
                                     :application_time_end app-end
                                     :system_time_start sys-start
                                     :system_time_end sys-end}
                                    {:for-app-time :all-time
                                     :for-sys-time :all-time})

                           (xt_docs {:customer-number cust
                                     :property-number prop
                                     :application_time_start app-start2
                                     :application_time_end app-end2
                                     :system_time_start sys-start2
                                     :system_time_end sys-end2})]
                    :where [[(<> prop in-prop)]
                            ;; eventually: 'overlaps?'
                            [(< app-start app-end2)]
                            [(> app-end app-start2)]
                            ;; eventually: 'contains?' with point
                            [(> sys-start2 sys-start)]
                            [(< sys-start2 sys-end)]]
                    :order-by [[app-start :asc]]}
                  tx7, nil, 7797))
            "Case 8: Application-time sequenced and system-time nonsequenced"))))
