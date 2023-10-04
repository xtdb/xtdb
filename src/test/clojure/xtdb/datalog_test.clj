;; THIRD-PARTY SOFTWARE NOTICE
;; This file is derivative of test files found in the DataScript
;; project. The Datascript license is copied verbatim in this
;; directory as `LICENSE`.
;; https://github.com/tonsky/datascript

(ns xtdb.datalog-test
  (:require [clojure.test :as t :refer [deftest]]
            [xtdb.api :as xt]
            [xtdb.james-bond :as bond]
            [xtdb.node :as node]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import (xtdb.types ClojureForm)))

(t/use-fixtures :each tu/with-mock-clock tu/with-node)

(def ivan+petr
  '[[:put :xt_docs {:xt/id :ivan, :first-name "Ivan", :last-name "Ivanov"}]
    [:put :xt_docs {:xt/id :petr, :first-name "Petr", :last-name "Petrov"}]])

(deftest test-scan
  (xt/submit-tx tu/*node* ivan+petr)

  (t/is (= #{{:name "Ivan"}
             {:name "Petr"}}
           (set (xt/q tu/*node*
                      '{:find [name]
                        :where [(match :xt_docs {:first-name name})]}))))

  (t/is (= #{{:e :ivan, :name "Ivan"}
             {:e :petr, :name "Petr"}}
           (set (xt/q tu/*node*
                      '{:find [e name]
                        :where [(match :xt_docs {:xt/id e, :first-name name})]})))
        "returning eid"))

(deftest test-basic-query
  (let [tx (xt/submit-tx tu/*node* ivan+petr)]
    (t/is (= [{:e :ivan}]
             (xt/q tu/*node*
                   '{:find [e]
                     :where [(match :xt_docs {:xt/id e})
                             [e :first-name "Ivan"]]}))
          "query by single field")

    (t/is (= [{:first-name "Petr", :last-name "Petrov"}]
             (xt/q tu/*node*
                   '{:find [first-name last-name]
                     :where [(match :xt_docs {:xt/id e})
                             [e :first-name "Petr"]
                             [e :first-name first-name]
                             [e :last-name last-name]]}))
          "returning the queried field")

    (t/is (= [{:first-name "Petr", :last-name "Petrov"}]
             (xt/q tu/*node*
                   '{:find [first-name last-name]
                     :where [(match :xt_docs [{:xt/id :petr} first-name last-name])]}))
          "literal eid")))


(deftest test-no-table-specified
  (t/is (thrown-with-msg?
         IllegalArgumentException
         #":unspecified-table"
         (xt/q tu/*node* '{:find [i]
                           :where [[e :foo :bar]]}))))

(deftest test-match-literal-eid
  (let [_tx (xt/submit-tx tu/*node* ivan+petr)]
    (t/is (= [{:first-name "Ivan", :last-name "Ivanov"}]
             (xt/q tu/*node*
                   '{:find [first-name last-name]
                     :where [(match :xt_docs [{:xt/id :ivan}])
                             [:ivan :first-name first-name]
                             [:ivan :last-name last-name]]})))))

(deftest test-dollar-match-syntax
  (let [_tx (xt/submit-tx tu/*node* ivan+petr)]
    (t/is (= [{:first-name "Ivan", :last-name "Ivanov"}]
             (xt/q tu/*node*
                   '{:find [first-name last-name]
                     :where [($ :xt_docs [{:xt/id :ivan} first-name last-name])]})))))


(deftest test-order-by
  (let [_tx (xt/submit-tx tu/*node* ivan+petr)]
    (t/is (= [{:first-name "Ivan"} {:first-name "Petr"}]
             (xt/q tu/*node*
                   '{:find [first-name]
                     :where [(match :xt_docs {:xt/id e})
                             [e :first-name first-name]]
                     :order-by [[first-name]]})))

    (t/is (= [{:first-name "Petr"} {:first-name "Ivan"}]
             (xt/q tu/*node*
                   '{:find [first-name]
                     :where [(match :xt_docs {:xt/id e})
                             [e :first-name first-name]]
                     :order-by [[first-name :desc]]})))

    (t/is (= [{:first-name "Ivan"}]
             (xt/q tu/*node*
                   '{:find [first-name]
                     :where [(match :xt_docs {:xt/id e})
                             [e :first-name first-name]]
                     :order-by [[first-name]]
                     :limit 1})))

    (t/is (= [{:first-name "Petr"}]
             (xt/q tu/*node*
                   '{:find [first-name]
                     :where [(match :xt_docs {:xt/id e})
                             [e :first-name first-name]]
                     :order-by [[first-name :desc]]
                     :limit 1})))

    (t/is (= [{:first-name "Petr"}]
             (xt/q tu/*node*
                   '{:find [first-name]
                     :where [(match :xt_docs {:xt/id e})
                             [e :first-name first-name]]
                     :order-by [[first-name]]
                     :limit 1
                     :offset 1})))))

;; https://github.com/tonsky/datascript/blob/1.1.0/test/datascript/test/query.cljc#L12-L36
(deftest datascript-test-joins
  (let [_tx (xt/submit-tx tu/*node*
                          '[[:put :xt_docs {:xt/id 1, :name "Ivan", :age 15}]
                            [:put :xt_docs {:xt/id 2, :name "Petr", :age 37}]
                            [:put :xt_docs {:xt/id 3, :name "Ivan", :age 37}]
                            [:put :xt_docs {:xt/id 4, :age 15}]])]

    (t/is (= #{{:e 1, :v 15} {:e 3, :v 37}}
             (set (xt/q tu/*node*
                        '{:find [e v]
                          :where [(match :xt_docs {:xt/id e})
                                  [e :name "Ivan"]
                                  [e :age v]]}))))

    (t/is (= #{{:e1 1, :e2 3} {:e1 3, :e2 1}}
             (set (xt/q tu/*node*
                        '{:find [e1 e2]
                          :where [(match :xt_docs {:xt/id e1, :name n})
                                  (match :xt_docs {:xt/id e2, :name n})
                                  [(<> e1 e2)]]}))))

    (t/is (= #{{:e 1, :e2 1, :n "Ivan"}
               {:e 3, :e2 3, :n "Ivan"}
               {:e 3, :e2 2, :n "Petr"}
               {:e 1, :e2 4}}
             (set (xt/q tu/*node*
                        '{:find [e e2 n]
                          :where [(match :xt_docs {:xt/id e, :name "Ivan", :age a})
                                  (match :xt_docs {:xt/id e2, :name n, :age a})]}))))

    (t/is (= #{{:e 1, :e2 1, :n "Ivan"}
               {:e 2, :e2 2, :n "Petr"}
               {:e 3, :e2 3, :n "Ivan"}
               {:e 4, :e2 4}}
             (set (xt/q tu/*node*
                        '{:find [e e2 n]
                          :where [(match :xt_docs {:xt/id e, :name n, :age a})
                                  (match :xt_docs {:xt/id e2, :name n, :age a})]})))
          "multi-param join")

    (t/is (= #{{:e1 1, :e2 1, :a1 15, :a2 15}
               {:e1 1, :e2 3, :a1 15, :a2 37}
               {:e1 3, :e2 1, :a1 37, :a2 15}
               {:e1 3, :e2 3, :a1 37, :a2 37}}
             (set (xt/q tu/*node*
                        '{:find [e1 e2 a1 a2]
                          :where [(match :xt_docs {:xt/id e1})
                                  (match :xt_docs {:xt/id e2})
                                  [e1 :name "Ivan"]
                                  [e2 :name "Ivan"]
                                  [e1 :age a1]
                                  [e2 :age a2]]})))
          "cross join required here")))

(deftest test-namespaced-attributes
  (let [_tx (xt/submit-tx tu/*node* [[:put :xt_docs {:xt/id :foo :foo/bar 1}]
                                     [:put :xt_docs {:xt/id :bar :foo/bar 2}]])]
    (t/is (= [{:i :foo, :n 1} {:i :bar, :n 2}]
             (xt/q tu/*node*
                   '{:find [i n]
                     :where [(match :xt_docs {:xt/id i})
                             [i :foo/bar n]]}))
          "simple query with namespaced attributes")
    (t/is (= [{:i/i :foo, :n/n 1} {:i/i :bar, :n/n 2}]
             (xt/q tu/*node*
                   '{:find [i n]
                     :keys [i/i n/n]
                     :where [(match :xt_docs {:xt/id i})
                             [i :foo/bar n]]}))
          "query with namespaced keys")
    (t/is (= [{:i :foo, :n 1 :foo/bar 1} {:i :bar, :n 2 :foo/bar 2}]
             (xt/q tu/*node*
                   '{:find [i n foo/bar]
                     :where [(match :xt_docs [foo/bar {:xt/id i :foo/bar n}])]}))
          "query with namespaced attributes in match syntax")))

(deftest test-joins
  (xt/submit-tx tu/*node* bond/tx-ops)

  (t/is (= #{{:film-name "Skyfall", :bond-name "Daniel Craig"}}
           (set (xt/q tu/*node*
                      ['{:find [film-name bond-name]
                         :in [film]
                         :where [(match :film {:xt/id film, :film/name film-name, :film/bond bond})
                                 (match :person {:xt/id bond, :person/name bond-name})]}
                       :skyfall])))
        "one -> one")

  (t/is (= #{{:film-name "Casino Royale", :bond-name "Daniel Craig"}
             {:film-name "Quantum of Solace", :bond-name "Daniel Craig"}
             {:film-name "Skyfall", :bond-name "Daniel Craig"}
             {:film-name "Spectre", :bond-name "Daniel Craig"}}
           (set (xt/q tu/*node*
                      ['{:find [film-name bond-name]
                         :in [bond]
                         :where [(match :film {:film/name film-name, :film/bond bond})
                                 (match :person {:xt/id bond, :person/name bond-name})]}
                       :daniel-craig])))
        "one -> many"))

;; https://github.com/tonsky/datascript/blob/1.1.0/test/datascript/test/query_aggregates.cljc#L14-L39
(t/deftest datascript-test-aggregates
  (let [_tx (xt/submit-tx tu/*node*
                          '[[:put :xt_docs {:xt/id :cerberus, :heads 3}]
                            [:put :xt_docs {:xt/id :medusa, :heads 1}]
                            [:put :xt_docs {:xt/id :cyclops, :heads 1}]
                            [:put :xt_docs {:xt/id :chimera, :heads 1}]])]
    (t/is (= #{{:heads 1, :count-heads 3} {:heads 3, :count-heads 1}}
             (set (xt/q tu/*node*
                        '{:find [heads (count heads)]
                          :keys [heads count-heads]
                          :where [(match :xt_docs {:xt/id monster})
                                  [monster :heads heads]]})))
          "head frequency")

    (t/is (= #{{:sum-heads 6, :min-heads 1, :max-heads 3, :count-heads 4}}
             (set (xt/q tu/*node*
                        '{:find [(sum heads)
                                 (min heads)
                                 (max heads)
                                 (count heads)]
                          :keys [sum-heads min-heads max-heads count-heads]
                          :where [(match :xt_docs {:xt/id monster})
                                  [monster :heads heads]]})))
          "various aggs")))

(t/deftest test-find-exprs
  (let [_tx (xt/submit-tx tu/*node* '[[:put :xt_docs {:xt/id :o1, :unit-price 1.49, :quantity 4}]
                                      [:put :xt_docs {:xt/id :o2, :unit-price 5.39, :quantity 1}]
                                      [:put :xt_docs {:xt/id :o3, :unit-price 0.59, :quantity 7}]])]
    (t/is (= #{{:oid :o1, :o-value 5.96}
               {:oid :o2, :o-value 5.39}
               {:oid :o3, :o-value 4.13}}
             (set (xt/q tu/*node*
                        '{:find [oid (* unit-price qty)]
                          :keys [oid o-value]
                          :where [(match :xt_docs {:xt/id oid})
                                  [oid :unit-price unit-price]
                                  [oid :quantity qty]]}))))))

(deftest test-aggregate-exprs
  (let [tx (xt/submit-tx tu/*node* '[[:put :xt_docs {:xt/id :foo, :category :c0, :v 1}]
                                     [:put :xt_docs {:xt/id :bar, :category :c0, :v 2}]
                                     [:put :xt_docs {:xt/id :baz, :category :c1, :v 4}]])]
    (t/is (= [{:category :c0, :sum-doubles 6}
              {:category :c1, :sum-doubles 8}]
             (xt/q tu/*node*
                   '{:find [category (sum (* 2 v))]
                     :keys [category sum-doubles]
                     :where [(match :xt_docs {:xt/id e})
                             [e :category category]
                             [e :v v]]}))))

  (t/is (= [{:x 0, :sum-y 0, :sum-expr 1}
            {:x 1, :sum-y 1, :sum-expr 5}
            {:x 2, :sum-y 3, :sum-expr 14}
            {:x 3, :sum-y 6, :sum-expr 30}]
           (xt/q tu/*node*
                 ['{:find [x (sum y) (sum (+ (* y y) x 1))]
                    :keys [x sum-y sum-expr]
                    :in [[[x y]]]}
                  (for [x (range 4)
                        y (range (inc x))]
                    [x y])])))

  (t/is (= [{:sum-evens 20}]
           (xt/q tu/*node*
                 ['{:find [(sum (if (= 0 (mod x 2)) x 0))]
                    :keys [sum-evens]
                    :in [[x ...]]}
                  (range 10)]))
        "if")

  ;; TODO aggregates nested within other aggregates/forms
  ;; - doesn't appear in TPC-H but guess we'll want these eventually

  #_
  (t/is (= #{[28.5]}
           (xt/q (xt/db *api*)
                 ['{:find [(/ (double (sum (* ?x ?x)))
                              (count ?x))]
                    :in [[?x ...]]}
                  (range 10)]))
        "aggregates can be included in exprs")

  #_
  (t/is (thrown-with-msg? IllegalArgumentException
                          #"nested agg"
                          (xt/q (xt/db *api*)
                                ['{:find [(sum (sum ?x))]
                                   :in [[?x ...]]}
                                 (range 10)]))
        "aggregates can't be nested")

  (t/testing "implicitly groups by variables present outside of aggregates"
    (t/is (= [{:x-div-y 1, :sum-z 2} {:x-div-y 2, :sum-z 3} {:x-div-y 2, :sum-z 5}]
             (xt/q tu/*node*
                   ['{:find [(/ x y) (sum z)]
                      :keys [x-div-y sum-z]
                      :in [[[x y z]]]}
                    [[1 1 2]
                     [2 1 3]
                     [4 2 5]]]))
          "even though (/ x y) yields the same result in the latter two rows, we group by them individually")

    #_
    (t/is (= #{[1 3] [1 7] [4 -1]}
             (xt/q tu/*node*
                   ['{:find [x (- (sum z) y)]
                      :in [[[x y z]]]}
                    [[1 1 4]
                     [1 3 2]
                     [1 3 8]
                     [4 6 5]]]))
          "groups by x and y in this case"))

  (t/testing "stddev aggregate"
    (t/is (= [{:y 23.53720459187964}]
             (xt/q tu/*node*
                   ['{:find [(stddev-pop x)]
                      :keys [y]
                      :in [[x ...]]}
                    [10 15 20 35 75]])))))

(deftest test-query-with-in-bindings
  (let [_tx (xt/submit-tx tu/*node* ivan+petr)]
    (t/is (= #{{:e :ivan}}
             (set (xt/q tu/*node*
                        ['{:find [e]
                           :in [name]
                           :where [(match :xt_docs {:xt/id e})
                                   [e :first-name name]]}
                         "Ivan"])))
          "single arg")

    (t/is (= #{{:e :ivan}}
             (set (xt/q tu/*node*
                        ['{:find [e]
                           :in [first-name last-name]
                           :where [(match :xt_docs {:xt/id e})
                                   [e :first-name first-name]
                                   [e :last-name last-name]]}
                         "Ivan" "Ivanov"])))
          "multiple args")

    (t/is (= #{{:e :ivan}}
             (set (xt/q tu/*node*
                        ['{:find [e]
                           :in [[first-name]]
                           :where [(match :xt_docs {:xt/id e})
                                   [e :first-name first-name]]}
                         ["Ivan"]])))
          "tuple with 1 var")

    (t/is (= #{{:e :ivan}}
             (set (xt/q tu/*node*
                        ['{:find [e]
                           :in [[first-name last-name]]
                           :where [(match :xt_docs {:xt/id e})
                                   [e :first-name first-name]
                                   [e :last-name last-name]]}
                         ["Ivan" "Ivanov"]])))
          "tuple with 2 vars")

    (t/testing "collection"
      (let [query '{:find [e]
                    :in [[first-name ...]]
                    :where [(match :xt_docs {:xt/id e})
                            [e :first-name first-name]]}]
        (t/is (= #{{:e :petr}}
                 (set (xt/q tu/*node* [query ["Petr"]]))))

        (t/is (= #{{:e :ivan} {:e :petr}}
                 (set (xt/q tu/*node* [query ["Ivan" "Petr"]]))))))

    (t/testing "relation"
      (let [query '{:find [e]
                    :in [[[first-name last-name]]]
                    :where [(match :xt_docs {:xt/id e})
                            [e :first-name first-name]
                            [e :last-name last-name]]}]

        (t/is (= #{{:e :ivan}}
                 (set (xt/q tu/*node*
                            [query [{:first-name "Ivan", :last-name "Ivanov"}]]))))

        (t/is (= #{{:e :ivan}}
                 (set (xt/q tu/*node*
                            [query [["Ivan" "Ivanov"]]]))))

        (t/is (= #{{:e :ivan} {:e :petr}}
                 (set (xt/q tu/*node*
                            [query
                             [{:first-name "Ivan", :last-name "Ivanov"}
                              {:first-name "Petr", :last-name "Petrov"}]]))))

        (t/is (= #{{:e :ivan} {:e :petr}}
                 (set (xt/q tu/*node*
                            [query
                             [["Ivan" "Ivanov"]
                              ["Petr" "Petrov"]]]))))))))

(deftest test-in-arity-exceptions
  (let [_tx (xt/submit-tx tu/*node* ivan+petr)]
    (t/is (thrown-with-msg? IllegalArgumentException
                            #":in arity mismatch"
                            (xt/q tu/*node*
                                  '{:find [e]
                                    :in [foo]
                                    :where [(match :xt_docs {:xt/id e})
                                            [e :foo foo]]})))))

(deftest test-basic-predicates
  (let [_tx (xt/submit-tx tu/*node* ivan+petr)]
    (t/is (= #{{:first-name "Ivan", :last-name "Ivanov"}}
             (set (xt/q tu/*node*
                        '{:find [first-name last-name]
                          :where [(match :xt_docs {:xt/id e})
                                  [e :first-name first-name]
                                  [e :last-name last-name]
                                  [(< first-name "James")]]}))))

    (t/is (= #{{:first-name "Ivan", :last-name "Ivanov"}}
             (set (xt/q tu/*node*
                        '{:find [first-name last-name]
                          :where [(match :xt_docs {:xt/id e})
                                  [e :first-name first-name]
                                  [e :last-name last-name]
                                  [(<= first-name "Ivan")]]}))))

    (t/is (empty? (xt/q tu/*node*
                        '{:find [first-name last-name]
                          :where [(match :xt_docs {:xt/id e})
                                  [e :first-name first-name]
                                  [e :last-name last-name]
                                  [(<= first-name "Ivan")]
                                  [(> last-name "Ivanov")]]})))

    (t/is (empty (xt/q tu/*node*
                       '{:find [first-name last-name]
                         :where [(match :xt_docs {:xt/id e})
                                 [e :first-name first-name]
                                 [e :last-name last-name]
                                 [(< first-name "Ivan")]]})))))

(deftest test-value-unification
  (let [_tx (xt/submit-tx tu/*node*
                          (conj ivan+petr
                                '[:put :xt_docs {:xt/id :sergei :first-name "Sergei" :last-name "Sergei"}]
                                '[:put :xt_docs {:xt/id :jeff :first-name "Sergei" :last-name "but-different"}]))]
    (t/is (= [{:e :sergei, :n "Sergei"}]
             (xt/q
              tu/*node*
              '{:find [e n]
                :where [(match :xt_docs {:xt/id e})
                        [e :last-name n]
                        [e :first-name n]]})))
    (t/is (= #{{:e :sergei, :f :sergei, :n "Sergei"} {:e :sergei, :f :jeff, :n "Sergei"}}
             (set (xt/q
                   tu/*node*
                   '{:find [e f n]
                     :where [(match :xt_docs {:xt/id e})
                             (match :xt_docs {:xt/id f})
                             [e :last-name n]
                             [e :first-name n]
                             [f :first-name n]]}))))))

(deftest test-implicit-match-unification
  (xt/submit-tx tu/*node* '[[:put :foo {:xt/id :ivan, :name "Ivan"}]
                            [:put :foo {:xt/id :petr, :name "Petr"}]
                            [:put :bar {:xt/id :sergei, :name "Sergei"}]
                            [:put :bar {:xt/id :ivan,:name "Ivan"}]
                            [:put :toto {:xt/id :jon, :name "John"}]
                            [:put :toto {:xt/id :mat,:name "Matt"}]])
  (t/is (= [{:e :ivan, :name "Ivan"}]
           (xt/q tu/*node*
                 '{:find [e name]
                   :where [(match :foo {:xt/id e})
                           (match :bar {:xt/id e})
                           [e :name name]]})))
  (t/is (= []
           (xt/q tu/*node*
                 '{:find [e]
                   :where [(match :foo {:xt/id e})
                           (match :toto {:xt/id e})
                           [e :name]]}))))

(deftest exists-with-no-outer-unification-692
  (let [_tx (xt/submit-tx tu/*node*
                          '[[:put :xt_docs {:xt/id :ivan, :name "Ivan"}]
                            [:put :xt_docs {:xt/id :petr, :name "Petr"}]
                            [:put :xt_docs {:xt/id :ivan2, :name "Ivan"}]])]

    (t/is (= [{:e :ivan} {:e :ivan2}]
             (xt/q tu/*node*
                   '{:find [e]
                     :where [(match :xt_docs {:xt/id e})
                             (exists? {:find [e2]
                                       :in [e]
                                       :where [(match :xt_docs {:xt/id e})
                                               (match :xt_docs {:xt/id e2})
                                               [e :name name]
                                               [e2 :name name]
                                               [(<> e e2)]]})]}))
          "with in variables")
    (t/is (= [{:e :ivan} {:e :ivan2}]
             (xt/q tu/*node*
                   '{:find [e]
                     :where [(match :xt_docs {:xt/id e})
                             (match :xt_docs {:xt/id e2})
                             (exists? {:find [e e2]
                                       :where [(match :xt_docs {:xt/id e})
                                               (match :xt_docs {:xt/id e2})
                                               [e :name name]
                                               [e2 :name name]
                                               [(<> e e2)]]})]}))
          "without in variables")))

(deftest exists-with-keys-699
  (let [_tx (xt/submit-tx tu/*node*
                          '[[:put :xt_docs {:xt/id 1, :name "one"}]
                            [:put :xt_docs {:xt/id 2, :name "two"}]
                            [:put :xt_docs {:xt/id 3, :name "three"}]])]

    (t/is (= [{:e 2} {:e 3}]
             (xt/q tu/*node*
                   '{:find [e]
                     :where [(match :xt_docs {:xt/id e})
                             (match :xt_docs {:xt/id e3})
                             (exists? {:find [(+ e2 2)]
                                       :keys [e3]
                                       :in [e]
                                       :where [(match :xt_docs {:xt/id e2})
                                               [(<> e e2)]]})]})))))

(deftest test-left-join
  (xt/submit-tx tu/*node*
                '[[:put :xt_docs {:xt/id :ivan, :name "Ivan"}]
                  [:put :xt_docs {:xt/id :petr, :name "Petr", :parent :ivan}]
                  [:put :xt_docs {:xt/id :sergei, :name "Sergei", :parent :petr}]
                  [:put :xt_docs {:xt/id :jeff, :name "Jeff", :parent :petr}]])

  (t/is (= #{{:e :ivan, :c :petr}
             {:e :petr, :c :sergei}
             {:e :petr, :c :jeff}
             {:e :sergei, :c nil}
             {:e :jeff, :c nil}}
           (set (xt/q tu/*node*
                      '{:find [e c]
                        :where [(match :xt_docs {:xt/id e, :name name})
                                (left-join {:find [e c]
                                            :where [(match :xt_docs {:xt/id c})
                                                    [c :parent e]]})]})))

        "find people who have children")

  (t/is (= #{{:e :ivan, :s nil}
             {:e :petr, :s nil}
             {:e :sergei, :s :jeff}
             {:e :jeff, :s :sergei}}
           (set (xt/q tu/*node*
                      '{:find [e s]
                        :where [(match :xt_docs {:xt/id e, :name name, :parent p})
                                (left-join {:find [s p]
                                            :in [e]
                                            :where [(match :xt_docs {:xt/id s, :parent p})
                                                    [(<> e s)]]})]})))
        "find people who have siblings")

  (t/is (thrown-with-msg? IllegalArgumentException
                          #":no-available-clauses"
                          (xt/q tu/*node*
                                '{:find [e n]
                                  :where [(match :xt_docs {:xt/id e})
                                          [e :foo n]
                                          (left-join {:find [e]
                                                      :where [(match :xt_docs {:xt/id e})
                                                              [e :first-name "Petr"]
                                                              [(= n 1)]]})]}))))

(deftest test-semi-join
  (let [_tx (xt/submit-tx tu/*node*
                          '[[:put :xt_docs {:xt/id :ivan, :name "Ivan"}]
                            [:put :xt_docs {:xt/id :petr, :name "Petr", :parent :ivan}]
                            [:put :xt_docs {:xt/id :sergei, :name "Sergei", :parent :petr}]
                            [:put :xt_docs {:xt/id :jeff, :name "Jeff", :parent :petr}]])]

    (t/is (= #{{:e :ivan} {:e :petr}}
             (set (xt/q tu/*node*
                        '{:find [e]
                          :where [(match :xt_docs {:xt/id e, :name name})
                                  (exists? {:find [e]
                                            :where [(match :xt_docs {:xt/id c})
                                                    [c :parent e]]})]})))

          "find people who have children")

    (t/is (= #{{:e :sergei} {:e :jeff}}
             (set (xt/q tu/*node*
                        '{:find [e]
                          :where [(match :xt_docs {:xt/id e, :name name, :parent p})
                                  (exists? {:find [p]
                                            :in [e]
                                            :where [(match :xt_docs {:xt/id s, :parent p})
                                                    [(<> e s)]]})]})))
          "find people who have siblings")

    (t/is (thrown-with-msg? IllegalArgumentException
                            #":no-available-clauses"
                            (xt/q tu/*node*
                                  '{:find [e n]
                                    :where [(match :xt_docs {:xt/id e})
                                            [e :foo n]
                                            (exists? {:find [e]
                                                      :where [(match :xt_docs {:xt/id e})
                                                              [e :first-name "Petr"]
                                                              [(= n 1)]]})]})))))

(deftest test-anti-join
  (let [_tx (xt/submit-tx
             tu/*node*
             '[[:put :xt_docs {:xt/id :ivan, :first-name "Ivan", :last-name "Ivanov" :foo 1}]
               [:put :xt_docs {:xt/id :petr, :first-name "Petr", :last-name "Petrov" :foo 1}]
               [:put :xt_docs {:xt/id :sergei :first-name "Sergei" :last-name "Sergei" :foo 1}]])]

    (t/is (= #{{:e :ivan} {:e :sergei}}
             (set (xt/q tu/*node*
                        '{:find [e]
                          :where [(match :xt_docs {:xt/id e})
                                  [e :foo 1]
                                  (not-exists? {:find [e]
                                                :where [(match :xt_docs {:xt/id e})
                                                        [e :first-name "Petr"]]})]}))))

    (t/is (= #{{:e :ivan} {:e :sergei}}
             (set (xt/q tu/*node*
                        '{:find [e]
                          :where [(match :xt_docs {:xt/id e})
                                  [e :foo n]
                                  (not-exists? {:find [e n]
                                                :where [(match :xt_docs {:xt/id e})
                                                        [e :first-name "Petr"]
                                                        [e :foo n]]})]}))))

    (t/is (= []
             (xt/q tu/*node*
                   '{:find [e]
                     :where [(match :xt_docs {:xt/id e})
                             [e :foo n]
                             (not-exists? {:find [e n]
                                           :where [(match :xt_docs {:xt/id e})
                                                   [e :foo n]]})]})))

    (t/is (= #{{:e :petr} {:e :sergei}}
             (set (xt/q tu/*node*
                        '{:find [e]
                          :where [(match :xt_docs {:xt/id e})
                                  [e :foo 1]
                                  (not-exists? {:find [e]
                                                :where [(match :xt_docs {:xt/id e})
                                                        [e :last-name "Ivanov"]]})]}))))

    (t/is (= #{{:e :ivan} {:e :petr} {:e :sergei}}
             (set (xt/q tu/*node*
                        '{:find [e]
                          :where [(match :xt_docs {:xt/id e})
                                  [e :foo 1]
                                  (not-exists? {:find [e]
                                                :where [(match :xt_docs {:xt/id e})
                                                        [e :first-name "Jeff"]]})]}))))

    (t/is (= #{{:e :ivan} {:e :petr}}
             (set (xt/q tu/*node*
                        '{:find [e]
                          :where [(match :xt_docs {:xt/id e})
                                  [e :foo 1]
                                  (not-exists? {:find [e]
                                                :where [(match :xt_docs {:xt/id e})
                                                        [e :first-name n]
                                                        [e :last-name n]]})]}))))

    (t/is (= #{{:e :ivan, :first-name "Petr", :last-name "Petrov", :a "Ivan", :b "Ivanov"}
               {:e :petr, :first-name "Ivan", :last-name "Ivanov", :a "Petr", :b "Petrov"}
               {:e :sergei, :first-name "Ivan", :last-name "Ivanov", :a "Sergei", :b "Sergei"}
               {:e :sergei, :first-name "Petr", :last-name "Petrov", :a "Sergei", :b "Sergei"}}
             (set (xt/q tu/*node*
                        ['{:find [e first-name last-name a b]
                           :in [[[first-name last-name]]]
                           :where [(match :xt_docs {:xt/id e})
                                   [e :foo 1]
                                   [e :first-name a]
                                   [e :last-name b]
                                   (not-exists? {:find [e first-name last-name]
                                                 :where [(match :xt_docs {:xt/id e})
                                                         [e :first-name first-name]
                                                         [e :last-name last-name]]})]}
                         [["Ivan" "Ivanov"]
                          ["Petr" "Petrov"]]]))))

    (t/testing "apply anti-joins"
      (t/is (= #{{:n 1, :e :ivan} {:n 1, :e :petr} {:n 1, :e :sergei}}
               (set (xt/q tu/*node*
                          '{:find [e n]
                            :where [(match :xt_docs {:xt/id e})
                                    [e :foo n]
                                    (not-exists? {:find [e]
                                                  :in [n]
                                                  :where [(match :xt_docs {:xt/id e})
                                                          [e :first-name "Petr"]
                                                          [(= n 2)]]})]}))))

      (t/is (= #{{:n 1, :e :ivan} {:n 1, :e :sergei}}
               (set (xt/q tu/*node*
                          '{:find [e n]
                            :where [(match :xt_docs {:xt/id e})
                                    [e :foo n]
                                    (not-exists? {:find [e]
                                                  :in [n]
                                                  :where [(match :xt_docs {:xt/id e})
                                                          [e :first-name "Petr"]
                                                          [(= n 1)]]})]}))))

      (t/is (= []
               (xt/q tu/*node*
                     '{:find [e n]
                       :where [(match :xt_docs {:xt/id e})
                               [e :foo n]
                               (not-exists? {:find []
                                             :in [n]
                                             :where [[(= n 1)]]})]})))

      (t/is (= #{{:n "Petr", :e :petr} {:n "Sergei", :e :sergei}}
               (set (xt/q tu/*node*
                          '{:find [e n]
                            :where [(match :xt_docs {:xt/id e})
                                    [e :first-name n]
                                    (not-exists? {:find []
                                                  :in [n]
                                                  :where [[(= "Ivan" n)]]})]}))))


      (t/is (= #{{:n "Petr", :e :petr} {:n "Sergei", :e :sergei}}
               (set (xt/q tu/*node*
                          '{:find [e n]
                            :where [(match :xt_docs {:xt/id e})
                                    [e :first-name n]
                                    (not-exists? {:find [n]
                                                  :where [(match :xt_docs {:xt/id e})
                                                          [e :first-name n]
                                                          [e :first-name "Ivan"]]})]}))))

      (t/is (= #{{:n 1, :e :ivan} {:n 1, :e :sergei}}
               (set (xt/q tu/*node*
                          '{:find [e n]
                            :where [(match :xt_docs {:xt/id e})
                                    [e :foo n]
                                    (not-exists? {:find [e n]
                                                  :where [(match :xt_docs {:xt/id e})
                                                          [e :first-name "Petr"]
                                                          [e :foo n]
                                                          [(= n 1)]]})]}))))


      (t/is (thrown-with-msg?
             IllegalArgumentException
             #":no-available-clauses"
             (xt/q tu/*node*
                   '{:find [e n]
                     :where [(match :xt_docs {:xt/id e})
                             [e :foo n]
                             (not-exists? {:find [e]
                                           :where [(match :xt_docs {:xt/id e})
                                                   [e :first-name "Petr"]
                                                   [(= n 1)]]})]})))

      ;; TODO what to do if arg var isn't used, either remove it from the join
      ;; or convert the anti-join to an apply and param all the args
      #_(t/is (= [{:e :ivan} {:e :sergei}]
                 (xt/q tu/*node*
                       '{:find [e n]
                         :where [[e :foo n]
                                 (not-exists? {:find [e]
                                               :in [n]
                                               :where [[e :first-name "Petr"]]})]}))))

    (t/testing "Multiple anti-joins"
      (t/is (= [{:n "Petr", :e :petr}]
               (xt/q tu/*node*
                     '{:find [e n]
                       :where [(match :xt_docs {:xt/id e})
                               [e :first-name n]
                               (not-exists? {:find []
                                             :in [n]
                                             :where [[(= n "Ivan")]]})
                               (not-exists? {:find [e]
                                             :where [(match :xt_docs {:xt/id e})
                                                     [e :first-name "Sergei"]]})]}))))))

(deftest test-simple-literals-in-find
  (xt/submit-tx tu/*node* '[[:put :xt_docs {:xt/id :ivan, :age 15}]
                            [:put :xt_docs {:xt/id :petr, :age 22}]
                            [:put :xt_docs {:xt/id :slava, :age 37}]])


  (t/is (= #{{:_column_0 1, :_column_1 "foo", :xt/id :ivan}
             {:_column_0 1, :_column_1 "foo", :xt/id :petr}
             {:_column_0 1, :_column_1 "foo", :xt/id :slava}}
           (set (xt/q tu/*node*
                      '{:find [1 "foo" xt/id]
                        :where [(match :xt_docs [xt/id])]})))))

(deftest calling-a-function-580
  (let [_tx (xt/submit-tx tu/*node*
                          '[[:put :xt_docs {:xt/id :ivan, :age 15}]
                            [:put :xt_docs {:xt/id :petr, :age 22}]
                            [:put :xt_docs {:xt/id :slava, :age 37}]])]

    (t/is (= #{{:e1 :petr, :e2 :ivan, :e3 :slava}
               {:e1 :ivan, :e2 :petr, :e3 :slava}}
             (set
              (xt/q tu/*node*
                    '{:find [e1 e2 e3]
                      :where [
                              (match :xt_docs {:xt/id e1})
                              (match :xt_docs {:xt/id e2})
                              (match :xt_docs {:xt/id e3})
                              [e1 :age a1]
                              [e2 :age a2]
                              [e3 :age a3]
                              [(+ a1 a2) a12]
                              [(= a12 a3)]]}))))

    (t/is (= #{{:e1 :petr, :e2 :ivan, :e3 :slava}
               {:e1 :ivan, :e2 :petr, :e3 :slava}}
             (set
              (xt/q tu/*node*
                    '{:find [e1 e2 e3]
                      :where [(match :xt_docs {:xt/id e1})
                              (match :xt_docs {:xt/id e2})
                              (match :xt_docs {:xt/id e3})
                              [e1 :age a1]
                              [e2 :age a2]
                              [e3 :age a3]
                              [(+ a1 a2) a3]]}))))))

(deftest test-namespaced-columns-within-match
  (xt/submit-tx tu/*node* [[:put :xt_docs {:xt/id :ivan :age 10}]])
  (t/is (= [{:xt/id :ivan, :age 10}]
           (xt/q tu/*node* '{:find [xt/id age]
                             :where [(match :xt_docs [xt/id])
                                     [xt/id :age age]]})))
  (t/is (= [{:id :ivan, :age 10}]
           (xt/q tu/*node* '{:find [id age]
                             :where [(match :xt_docs {:xt/id id})
                                     [id :age age]]})))
  (t/is (= [{:id :ivan, :age 10}]
           (xt/q tu/*node* '{:find [id age]
                             :where [(match :xt_docs [{:xt/id id}])
                                     [id :age age]]}))))

(deftest test-nested-expressions-581
  (let [_tx (xt/submit-tx tu/*node*
                          '[[:put :xt_docs {:xt/id :ivan, :age 15}]
                            [:put :xt_docs {:xt/id :petr, :age 22, :height 240, :parent 1}]
                            [:put :xt_docs {:xt/id :slava, :age 37, :parent 2}]])]

    (t/is (= #{{:e1 :ivan, :e2 :petr, :e3 :slava}
               {:e1 :petr, :e2 :ivan, :e3 :slava}}
             (set (xt/q tu/*node*
                        '{:find [e1 e2 e3]
                          :where [(match :xt_docs {:xt/id e1})
                                  (match :xt_docs {:xt/id e2})
                                  (match :xt_docs {:xt/id e3})
                                  [e1 :age a1]
                                  [e2 :age a2]
                                  [e3 :age a3]
                                  [(= (+ a1 a2) a3)]]}))))

    (t/is (= [{:a1 15, :a2 22, :a3 37, :sum-ages 74, :inc-sum-ages 75}]
             (xt/q tu/*node*
                   '{:find [a1 a2 a3 sum-ages inc-sum-ages]
                     :where [(match :xt_docs {:xt/id :ivan})
                             (match :xt_docs {:xt/id :petr})
                             (match :xt_docs {:xt/id :slava})
                             [:ivan :age a1]
                             [:petr :age a2]
                             [:slava :age a3]
                             [(+ (+ a1 a2) a3) sum-ages]
                             [(+ a1 (+ a2 a3 1)) inc-sum-ages]]})))

    (t/testing "unifies results of two calls"
      (t/is (= [{:a1 15, :a2 22, :a3 37, :sum-ages 74}]
               (xt/q tu/*node*
                     '{:find [a1 a2 a3 sum-ages]
                       :where [(match :xt_docs {:xt/id :ivan})
                               (match :xt_docs {:xt/id :petr})
                               (match :xt_docs {:xt/id :slava})
                               [:ivan :age a1]
                               [:petr :age a2]
                               [:slava :age a3]
                               [(+ (+ a1 a2) a3) sum-ages]
                               [(+ a1 (+ a2 a3)) sum-ages]]})))

      (t/is (= []
               (xt/q tu/*node*
                     '{:find [a1 a2 a3 sum-ages]
                       :where [(match :xt_docs {:xt/id :ivan})
                               (match :xt_docs {:xt/id :petr})
                               (match :xt_docs {:xt/id :slava})
                               [:ivan :age a1]
                               [:petr :age a2]
                               [:slava :age a3]
                               [(+ (+ a1 a2) a3) sum-ages]
                               [(+ a1 (+ a2 a3 1)) sum-ages]]}))))))

(deftest test-union-join
  (let [_tx (xt/submit-tx tu/*node* '[[:put :xt_docs {:xt/id :ivan, :age 20, :role :developer}]
                                      [:put :xt_docs {:xt/id :oleg, :age 30, :role :manager}]
                                      [:put :xt_docs {:xt/id :petr, :age 35, :role :qa}]
                                      [:put :xt_docs {:xt/id :sergei, :age 35, :role :manager}]])]

    (letfn [(q [query]
              (xt/q tu/*node* query))]
      (t/is (= [{:e :ivan}]
               (q '{:find [e]
                    :where [(match :xt_docs {:xt/id e})
                            (union-join [e]
                                        (and (match :xt_docs {:xt/id e})
                                             [e :role :developer])
                                        (and (match :xt_docs {:xt/id e})
                                             [e :age 30]))
                            (union-join [e]
                                        (and (match :xt_docs {:xt/id e})
                                             [e :xt/id :petr])
                                        (and (match :xt_docs {:xt/id e})
                                             [e :xt/id :ivan]))]})))

      (t/is (= [{:e :petr}, {:e :oleg}]
               (q '{:find [e]
                    :where [(match :xt_docs {:xt/id :sergei})
                            [:sergei :age age]
                            [:sergei :role role]
                            (union-join [e age role]
                                        (and (match :xt_docs {:xt/id e})
                                             [e :age age])
                                        (and (match :xt_docs {:xt/id e})
                                             [e :role role]))
                            [(<> e :sergei)]]})))

      (t/testing "functions within union-join"
        (t/is (= [{:age 35, :older-age 45}]
                 (q '{:find [age older-age]
                      :where [(match :xt_docs {:xt/id :sergei})
                              [:sergei :age age]
                              (union-join [age older-age]
                                          [(+ age 10) older-age])]})))))))

(deftest test-union-join-with-match-syntax-693
  (let [_tx (xt/submit-tx tu/*node* '[[:put :xt_docs {:xt/id :ivan, :age 20, :role :developer}]
                                      [:put :xt_docs {:xt/id :oleg, :age 30, :role :manager}]
                                      [:put :xt_docs {:xt/id :petr, :age 35, :role :qa}]
                                      [:put :xt_docs {:xt/id :sergei, :age 35, :role :manager}]])]
    (t/is (= [{:e :ivan}]
             (xt/q tu/*node* '{:find [e]
                               :where [(union-join [e]
                                                   (and (match :xt_docs {:xt/id e})
                                                        [e :role :developer])
                                                   (and (match :xt_docs {:xt/id e})
                                                        [e :age 30]))
                                       (union-join [e]
                                                   (and (match :xt_docs {:xt/id e})
                                                        [e :xt/id :petr])
                                                   (and (match :xt_docs {:xt/id e})
                                                        [e :xt/id :ivan]))]})))))

(deftest test-union-join-with-subquery-638
  (xt/submit-tx tu/*node* '[[:put :xt_docs {:xt/id :ivan, :age 20, :role :developer}]
                            [:put :xt_docs {:xt/id :oleg, :age 30, :role :manager}]
                            [:put :xt_docs {:xt/id :petr, :age 35, :role :qa}]
                            [:put :xt_docs {:xt/id :sergei, :age 35, :role :manager}]])
  (t/is (= [{:e :oleg}]
           (xt/q tu/*node* '{:find [e]
                             :where [(union-join [e]
                                                 (q {:find [e]
                                                     :where [(match :xt_docs {:xt/id e})
                                                             [e :age 30]]}))]}))))

(deftest test-nested-query
  (xt/submit-tx tu/*node* bond/tx-ops)

  (t/is (= [{:bond-name "Roger Moore", :film-name "A View to a Kill"}
            {:bond-name "Roger Moore", :film-name "For Your Eyes Only"}
            {:bond-name "Roger Moore", :film-name "Live and Let Die"}
            {:bond-name "Roger Moore", :film-name "Moonraker"}
            {:bond-name "Roger Moore", :film-name "Octopussy"}
            {:bond-name "Roger Moore", :film-name "The Man with the Golden Gun"}
            {:bond-name "Roger Moore", :film-name "The Spy Who Loved Me"}]
           (xt/q tu/*node*
                 '{:find [bond-name film-name]
                   :where [(q {:find [bond bond-name (count bond)]
                               :keys [bond-with-most-films bond-name film-count]
                               :where [(match :film {:film/bond bond})
                                       (match :person {:xt/id bond, :person/name bond-name})]
                               :order-by [[(count bond) :desc] [bond-name]]
                               :limit 1})

                           (match :film {:film/bond bond-with-most-films
                                         :film/name film-name})]
                   :order-by [[film-name]]}))
        "films made by the Bond with the most films")

  (t/testing "(contrived) correlated sub-query"
    (xt/submit-tx tu/*node* '[[:put :a {:xt/id :a1, :a 1}]
                              [:put :a {:xt/id :a2, :a 2}]
                              [:put :b {:xt/id :b2, :b 2}]
                              [:put :b {:xt/id :b3, :b 3}]])

    (t/is (= [{:aid :a2, :bid :b2}]
             (xt/q tu/*node*
                   '{:find [aid bid]
                     :where [(match :a {:xt/id aid, :a a})
                             (q {:find [bid]
                                 :in [a]
                                 :where [(match :b {:xt/id bid, :b a})]})]})))))

(t/deftest test-explicit-unwind-574
  (xt/submit-tx tu/*node* bond/tx-ops)

  (t/is (= [{:brand "Aston Martin", :model "DB10"}
            {:brand "Aston Martin", :model "DB5"}
            {:brand "Jaguar", :model "C-X75"}
            {:brand "Jaguar", :model "XJ8"}
            {:brand "Land Rover", :model "Discovery Sport"}
            {:brand "Land Rover", :model "Land Rover Defender Bigfoot"}
            {:brand "Land Rover", :model "Range Rover Sport"}
            {:brand "Mercedes Benz", :model "S-Class"}
            {:brand "Rolls-Royce", :model "Silver Wraith"}]

           (xt/q tu/*node*
                 ['{:find [brand model]
                    :in [film]
                    :where [(match :film {:xt/id film, :film/vehicles [vehicle ...]})
                            (match :vehicle {:xt/id vehicle, :vehicle/brand brand, :vehicle/model model})]
                    :order-by [[brand] [model]]}
                  :spectre]))))

(t/deftest bug-non-string-table-names-599
  (with-open [node (node/start-node {:xtdb/indexer {:rows-per-chunk 1000}})]
    (letfn [(submit-ops! [ids]
              (last (for [tx-ops (->> (for [id ids]
                                        [:put :t1 {:xt/id id,
                                                   :data (str "data" id)
                                                   }])
                                      (partition-all 20))]
                      (xt/submit-tx node tx-ops))))

            (count-table [_tx]
              (-> (xt/q node '{:find [(count id)]
                               :keys [id-count]
                               :where [(match :t1 {:xt/id id})]})
                  (first)
                  (:id-count)))]

      (let [tx (submit-ops! (range 80))]
        (t/is (= 80 (count-table tx))))

      (let [tx (submit-ops! (range 80 160))]
        (t/is (= 160 (count-table tx)))))))

(t/deftest bug-dont-throw-on-non-existing-column-597
  (with-open [node (node/start-node {:xtdb/indexer {:rows-per-chunk 1000}})]
    (letfn [(submit-ops! [ids]
              (last (for [tx-ops (->> (for [id ids]
                                        [:put :t1 {:xt/id id,
                                                   :data (str "data" id)}])
                                      (partition-all 20))]
                      (xt/submit-tx node tx-ops))))]

      (xt/submit-tx node '[[:put :xt_docs {:xt/id 0 :foo :bar}]])
      (submit-ops! (range 1010))

      (t/is (= 1010 (-> (xt/q node '{:find [(count id)]
                                     :keys [id-count]
                                     :where [(match :t1 {:xt/id id})]})
                        (first)
                        (:id-count))))

      (t/is (= [{:xt/id 0}]
               (xt/q node '{:find [xt/id]
                            :where [(match :xt_docs [xt/id some-attr])]}))))))

(t/deftest add-better-metadata-support-for-keywords
  (with-open [node (node/start-node {:xtdb/indexer {:rows-per-chunk 1000}})]
    (letfn [(submit-ops! [ids]
              (last (for [tx-ops (->> (for [id ids]
                                        [:put :t1 {:xt/id id,
                                                   :data (str "data" id)}])
                                      (partition-all 20))]
                      (xt/submit-tx node tx-ops))))]
      (let [_tx1 (xt/submit-tx node '[[:put :xt_docs {:xt/id :some-doc}]])
            ;; going over the chunk boundary
            tx2 (submit-ops! (range 200))]
        (t/is (= [{:xt/id :some-doc}]
                 (xt/q node '{:find [xt/id]
                              :where [(match :xt_docs [xt/id])
                                      [xt/id :xt/id :some-doc]]})))))))

(deftest test-subquery-unification
  (let [tx (xt/submit-tx tu/*node* '[[:put :a {:xt/id :a1, :a 2 :b 1}]
                                     [:put :a {:xt/id :a2, :a 2 :b 3}]
                                     [:put :a {:xt/id :a3, :a 2 :b 0}]])]

    (t/testing "variables returned from subqueries that must be run as an apply are unified"

      (t/testing "subquery"
        (t/is (= [{:aid :a2 :a 2 :b 3}]
                 (xt/q tu/*node*
                       '{:find [aid a b]
                         :where [(match :a [{:xt/id aid} a b])
                                 (q {:find [b]
                                     :in [a]
                                     :where [[(+ a 1) b]]})]}))
              "b is unified"))

      (t/testing "union-join"
        (t/is (= [{:aid :a2 :a 2 :b 3}]
                 (xt/q tu/*node*
                       '{:find [aid a b]
                         :where [(match :a [{:xt/id aid} a b])
                                 (union-join [a b]
                                             [(+ a 1) b])]}))
              "b is unified")))))

(deftest test-basic-rules
  (xt/submit-tx tu/*node* '[[:put :xt_docs {:xt/id :ivan :name "Ivan" :last-name "Ivanov" :age 21}]
                            [:put :xt_docs {:xt/id :petr :name "Petr" :last-name "Petrov" :age 18}]
                            [:put :xt_docs {:xt/id :georgy :name "Georgy" :last-name "George" :age 17}]])
  (letfn [(q [query & args]
            (apply xt/q tu/*node* query args))]

    (t/testing "without rule"
      (t/is (= [{:i :ivan}] (q '{:find [i]
                                 :where [(match :xt_docs {:xt/id i})
                                         [i :age age]
                                         [(>= age 21)]]}))))

    (t/testing "empty rules"
      (t/is (= [{:i :ivan}] (q '{:find [i]
                                 :where [(match :xt_docs {:xt/id i})
                                         [i :age age]
                                         [(>= age 21)]]
                                 :rules []}))))

    (t/testing "rule using required bound args"
      (t/is (= [{:i :ivan}] (q '{:find [i]
                                 :where [(match :xt_docs {:xt/id i})
                                         [i :age age]
                                         (over-twenty-one? age)]
                                 :rules [[(over-twenty-one? age)
                                          [(>= age 21)]]]}))))

    (t/testing "rule using required bound args (different arg names)"
      (t/is (= [{:i :ivan}] (q '{:find [i]
                                 :where [(match :xt_docs {:xt/id i})
                                         [i :age age]
                                         (over-twenty-one? age)]
                                 :rules [[(over-twenty-one? age-other)
                                          [(>= age-other 21)]]]}))))

    (t/testing "rules directly on arguments"
      (t/is (= [{:age 21}] (q ['{:find [age]
                                 :where [(over-twenty-one? age)]
                                 :in [age]
                                 :rules [[(over-twenty-one? age)
                                          [(>= age 21)]]]}
                               21])))

      (t/is (= [] (q ['{:find [age]
                        :where [(over-twenty-one? age)]
                        :in [age]
                        :rules [[(over-twenty-one? age)
                                 [(>= age 21)]]]}
                      20]))))

    (t/testing "testing rule with multiple args"
      (t/is (= #{{:i :petr, :age 18, :u :ivan}
                 {:i :georgy, :age 17, :u :ivan}
                 {:i :georgy, :age 17, :u :petr}}
               (set (q '{:find [i age u]
                         :where [(older-users age u)
                                 (match :xt_docs {:xt/id i})
                                 [i :age age]]
                         :rules [[(older-users age u)
                                  (match :xt_docs {:xt/id u})
                                  [u :age age2]
                                  [(> age2 age)]]]})))))

    (t/testing "testing rule with multiple args (different arg names in rule)"
      (t/is (= #{{:i :petr, :age 18, :u :ivan}
                 {:i :georgy, :age 17, :u :ivan}
                 {:i :georgy, :age 17, :u :petr}}
               (set (q '{:find [i age u]
                         :where [(older-users age u)
                                 (match :xt_docs {:xt/id i})
                                 [i :age age]]
                         :rules [[(older-users age-other u-other)
                                  (match :xt_docs {:xt/id u-other})
                                  [u-other :age age2]
                                  [(> age2 age-other)]]]})))))


    (t/testing "nested rules"
      (t/is (= [{:i :ivan}] (q '{:find [i]
                                 :where [(match :xt_docs {:xt/id i})
                                         [i :age age]
                                         (over-twenty-one? age)]
                                 :rules [[(over-twenty-one? x)
                                          (over-twenty-one-internal? x)]
                                         [(over-twenty-one-internal? y)
                                          [(>= y 21)]]]}))))

    (t/testing "nested rules bound (same arg names)"
      (t/is (= [{:i :ivan}] (q '{:find [i]
                                 :where [(match :xt_docs {:xt/id i})
                                         [i :age age]
                                         (over-twenty-one? age)]
                                 :rules [[(over-twenty-one? age)
                                          (over-twenty-one-internal? age)]
                                         [(over-twenty-one-internal? age)
                                          [(>= age 21)]]]}))))

    (t/is (= [{:i :ivan}] (q '{:find [i]
                               :where [(match :xt_docs {:xt/id i})
                                       [i :age age]
                                       (over-twenty-one? age)]
                               :rules [[(over-twenty-one? x)
                                        (over-twenty-one-internal? x)]
                                       [(over-twenty-one-internal? y)
                                        [(>= y 21)]]]})))

    (t/testing "rule using literal arguments"
      (t/is (= [{:i :ivan}] (q '{:find [i]
                                 :where [(match :xt_docs {:xt/id i})
                                         [i :age age]
                                         (over-age? age 21)]
                                 :rules [[(over-age? age required-age)
                                          [(>= age required-age)]]]}))))

    (t/testing "same arg-name different position test (shadowing test)"
      (t/is (= [{:i :ivan}] (q '{:find [i]
                                 :where [(match :xt_docs {:xt/id i})
                                         [i :age age]
                                         (over-age? age 21)]
                                 :rules [[(over-age? other-age age)
                                          [(>= other-age age)]]]}))))

    (t/testing "semi-join in rule"
      (t/is (= [{:i :petr, :age 18}
                {:i :georgy, :age 17}]
               (q '{:find [i age]
                    :where [(match :xt_docs {:xt/id i})
                            [i :age age]
                            (older? age)]
                    :rules [[(older? age)
                             (exists? {:find []
                                       :in [age]
                                       :where [(match :xt_docs {:xt/id i})
                                               [i :age age2]
                                               [(> age2 age)]]})]]}))))


    (t/testing "anti-join in rule"
      (t/is (= [{:i :ivan, :age 21}]
               (q  '{:find [i age]
                     :where [(match :xt_docs {:xt/id i})
                             [i :age age]
                             (not-older? age)]
                     :rules [[(not-older? age)
                              (not-exists? {:find []
                                            :in [age]
                                            :where [(match :xt_docs {:xt/id i})
                                                    [i :age age2]
                                                    [(> age2 age)]]})]]}))))

    (t/testing "subquery in rule"
      (t/is (= #{{:i :petr, :other-age 21}
                 {:i :georgy, :other-age 21}
                 {:i :georgy, :other-age 18}}
               (set (q '{:find [i other-age]
                         :where [(match :xt_docs {:xt/id i})
                                 [i :age age]
                                 (older-ages age other-age)]
                         :rules [[(older-ages age other-age)
                                  (q {:find [other-age]
                                      :in [age]
                                      :where [(match :xt_docs {:xt/id i})
                                              [i :age other-age]
                                              [(> other-age age)]]})]]})))))

    (t/testing "subquery in rule with aggregates, expressions and order-by"
      (t/is [{:i :ivan, :max-older-age nil, :max-older-age-times2 nil}
             {:i :petr, :max-older-age 21, :max-older-age-times2 42}
             {:i :georgy, :max-older-age 21, :max-older-age-times2 42}]
            (q '{:find [i max-older-age max-older-age-times2]
                 :where [(match :xt_docs {:xt/id i})
                         [i :age age]
                         (older-ages age max-older-age max-older-age-times2)]
                 :rules [[(older-ages age max-older-age max-older-age2)
                          (q {:find [(max older-age) (max (* older-age 2))]
                              :keys [max-older-age max-older-age2]
                              :in [age]
                              :where [(match :xt_docs {:xt/id i})
                                      [i :age older-age]
                                      [(> older-age age)]]
                              :order-by [[(max older-age) :desc]]})]]})))

    (t/testing "rule using multiple branches"
      (t/is (= [{:i :ivan}] (q '{:find [i]
                                 :where [(is-ivan-or-bob? i)]
                                 :rules [[(is-ivan-or-bob? i)
                                          (match :xt_docs {:xt/id i})
                                          [i :name "Bob"]]
                                         [(is-ivan-or-bob? i)
                                          (match :xt_docs {:xt/id i})
                                          [i :name "Ivan"]
                                          [i :last-name "Ivanov"]]]})))

      (t/is (= [{:name "Petr"}] (q '{:find [name]
                                     :where [(match :xt_docs {:xt/id i})
                                             [i :name name]
                                             (not-exists? {:find [i]
                                                           :where [(is-ivan-or-georgy? i)]})]
                                     :rules [[(is-ivan-or-georgy? i)
                                              (match :xt_docs {:xt/id i})
                                              [i :name "Ivan"]]
                                             [(is-ivan-or-georgy? i)
                                              (match :xt_docs {:xt/id i})
                                              [i :name "Georgy"]]]})))

      (t/is (= [{:i :ivan}
                {:i :petr}]
               (q '{:find [i]
                    :where [(is-ivan-or-petr? i)]
                    :rules [[(is-ivan-or-petr? i)
                             (match :xt_docs {:xt/id i})
                             [i :name "Ivan"]]
                            [(is-ivan-or-petr? i)
                             (match :xt_docs {:xt/id i})
                             [i :name "Petr"]]]}))))

    (t/testing "union-join with rules"
      (t/is (= [{:i :ivan}]
               (q '{:find [i]
                    :where [(union-join [i]
                                        (is-ivan-or-bob? i))]
                    :rules [[(is-ivan-or-bob? i)
                             (match :xt_docs {:xt/id i})
                             [i :name "Bob"]]
                            [(is-ivan-or-bob? i)
                             (match :xt_docs {:xt/id i})
                             [i :name "Ivan"]
                             [i :last-name "Ivanov"]]]}))))


    (t/testing "subquery with rule"
      (t/is (= [{:i :ivan}]
               (q '{:find [i]
                    :where [(q {:find [i]
                                :where [(is-ivan-or-bob? i)]})]
                    :rules [[(is-ivan-or-bob? i)
                             (match :xt_docs {:xt/id i})
                             [i :name "Bob"]]
                            [(is-ivan-or-bob? i)
                             (match :xt_docs {:xt/id i})
                             [i :name "Ivan"]
                             [i :last-name "Ivanov"]]]})))))

  (t/is (thrown-with-msg?
         IllegalArgumentException
         #":unknown-rule"
         (xt/q tu/*node* '{:find [i]
                           :where [(match :xt_docs {:xt/id i})
                                   [i :age age]
                                   (over-twenty-one? age)]})))

  (t/is (thrown-with-msg?
         IllegalArgumentException
         #":rule-wrong-arity"
         (xt/q tu/*node* '{:find [i]
                           :where [(match :xt_docs {:xt/id i})
                                   [i :age age]
                                   (over-twenty-one? i age)]
                           :rules [[(over-twenty-one? x)
                                    [(>= x 21)]]]})))
  (t/is (thrown-with-msg?
         IllegalArgumentException
         #":rule-definitions-require-unique-arity"
         (xt/q tu/*node* '{:find [i]
                           :where [(match :xt_docs {:xt/id i})
                                   [i :age age]
                                   (is-ivan-or-petr? i name)]
                           :rules [[(is-ivan-or-petr? i name)
                                    (match :xt_docs {:xt/id i})
                                    [i :name "Ivan"]]
                                   [(is-ivan-or-petr? i)
                                    (match :xt_docs {:xt/id i})
                                    [i :name "Petr"]]]}))))


(t/deftest test-temporal-opts
  (letfn [(q [query tx current-time]
            (xt/q tu/*node*
                  query
                  {:basis {:tx tx, :current-time (util/->instant current-time)}}))]

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

    (let [tx0 (xt/submit-tx tu/*node* '[[:put :xt_docs {:xt/id :matthew} {:for-valid-time [:in #inst "2015"]}]
                                        [:put :xt_docs {:xt/id :mark} {:for-valid-time [:in  #inst "2018"  #inst "2020"]}]
                                        [:put :xt_docs {:xt/id :luke} {:for-valid-time [:in #inst "2021"]}]])

          tx1 (xt/submit-tx tu/*node* '[[:delete :xt_docs :luke {:for-valid-time [:in #inst "2022"]}]
                                        [:put :xt_docs {:xt/id :mark} {:for-valid-time [:in #inst "2023" #inst "2024"]}]
                                        [:put :xt_docs {:xt/id :john} {:for-valid-time [:in #inst "2016" #inst "2020"]}]])]

      (t/is (= #{{:id :matthew}, {:id :mark}}
               (set (q '{:find [id], :where [(match :xt_docs [{:xt/id id}])]}, tx1, #inst "2023"))))

      (t/is (= #{{:id :matthew}, {:id :luke}}
               (set (q '{:find [id], :where [(match :xt_docs [{:xt/id id}])]}, tx1, #inst "2021")))
            "back in app-time")

      (t/is (= #{{:id :matthew}, {:id :luke}}
               (set (q '{:find [id], :where [(match :xt_docs [{:xt/id id}])]}, tx0, #inst "2023")))
            "back in system-time")

      (t/is (= #{{:id :matthew, :app-from (util/->zdt #inst "2015"), :app-to (util/->zdt util/end-of-time)}
                 {:id :mark, :app-from (util/->zdt #inst "2018"), :app-to (util/->zdt #inst "2020")}
                 {:id :luke, :app-from (util/->zdt #inst "2021"), :app-to (util/->zdt #inst "2022")}
                 {:id :mark, :app-from (util/->zdt #inst "2023"), :app-to (util/->zdt #inst "2024")}
                 {:id :john, :app-from (util/->zdt #inst "2016"), :app-to (util/->zdt #inst "2020")}}
               (set (q '{:find [id app-from app-to]
                         :where [(match :xt_docs [{:xt/id id} {:xt/valid-from app-from
                                                               :xt/valid-to app-to}]
                                        {:for-valid-time :all-time})]}
                       tx1, nil)))
            "entity history, all time")

      (t/is (= #{{:id :matthew, :app-from (util/->zdt #inst "2015"), :app-to (util/->zdt util/end-of-time)}
                 {:id :luke, :app-from (util/->zdt #inst "2021"), :app-to (util/->zdt #inst "2022")}}
               (set (q '{:find [id app-from app-to]
                         :where [(match :xt_docs [{:xt/id id} {:xt/valid-from app-from
                                                               :xt/valid-to app-to}]
                                        {:for-valid-time [:in #inst "2021", #inst "2023"]})]}
                       tx1, nil)))
            "entity history, range")

      (t/is (= #{{:id :matthew}, {:id :mark}}
               (set (q '{:find [id],
                         :where [(match :xt_docs {:xt/id id}
                                        {:for-valid-time [:at #inst "2018"]})
                                 (match :xt_docs {:xt/id id}
                                        {:for-valid-time [:at #inst "2023"]})]},
                       tx1, nil)))
            "cross-time join - who was here in both 2018 and 2023?")

      (t/is (= #{{:vt-from (util/->zdt #inst "2021")
                  :vt-to (util/->zdt #inst "2022")
                  :tt-from (util/->zdt #inst "2020")
                  :tt-to (util/->zdt util/end-of-time)}
                 {:vt-from (util/->zdt #inst "2022")
                  :vt-to (util/->zdt util/end-of-time)
                  :tt-from (util/->zdt #inst "2020")
                  :tt-to (util/->zdt  #inst "2020-01-02")}}
               (set (q '{:find [vt-from vt-to tt-from tt-to]
                         :where [(match :xt_docs {:xt/id :luke
                                                  :xt/valid-from vt-from
                                                  :xt/valid-to vt-to
                                                  :xt/system-from tt-from
                                                  :xt/system-to tt-to}
                                        {:for-valid-time :all-time
                                         :for-system-time :all-time})]}
                       tx1 nil)))

            "for all sys time"))))

(t/deftest test-for-valid-time-with-current-time-2493
  (xt/submit-tx tu/*node* '[[:put :xt_docs {:xt/id :matthew} {:for-valid-time [:in nil #inst "2040"]}]])
  (xt/submit-tx tu/*node* '[[:put :xt_docs {:xt/id :matthew} {:for-valid-time [:in #inst "2022" #inst "2030"]}]])
  (t/is (= #{{:id :matthew,
              :vt-from #time/zoned-date-time "2030-01-01T00:00Z[UTC]",
              :vt-to #time/zoned-date-time "2040-01-01T00:00Z[UTC]"}
             {:id :matthew,
              :vt-from #time/zoned-date-time "2022-01-01T00:00Z[UTC]",
              :vt-to #time/zoned-date-time "2030-01-01T00:00Z[UTC]"}}
           (set (xt/q tu/*node*
                      '{:find [id vt-from vt-to], :where [(match :xt_docs {:xt/id id
                                                                             :xt/valid-from vt-from
                                                                             :xt/valid-to vt-to}
                                                                   {:for-valid-time [:in nil #inst "2040"]})]}
                      {:basis {:current-time (util/->instant #inst "2023")}})))))

(t/deftest test-temporal-opts-from-and-to
  (letfn [(q [query tx current-time]
            (xt/q tu/*node* query
                  {:basis {:tx tx, :current-time (util/->instant current-time)}}))]

    ;; tx0
    ;; 2015 - eof : Matthew
    ;; now - 2050 : Mark

    ;; tx1
    ;; now - 2040 : Matthew

    (let [tx0 (xt/submit-tx tu/*node* '[[:put :xt_docs {:xt/id :matthew} {:for-valid-time [:from #inst "2015"]}]
                                        [:put :xt_docs {:xt/id :mark} {:for-valid-time [:to #inst "2050"]}]])
          tx1 (xt/submit-tx tu/*node* '[[:put :xt_docs {:xt/id :matthew} {:for-valid-time [:to #inst "2040"]}]])]
      (t/is (= #{{:id :matthew,
                  :vt-from #time/zoned-date-time "2015-01-01T00:00Z[UTC]",
                  :vt-to #time/zoned-date-time "9999-12-31T23:59:59.999999Z[UTC]"}
                 {:id :mark,
                  :vt-from #time/zoned-date-time "2020-01-01T00:00Z[UTC]",
                  :vt-to #time/zoned-date-time "2050-01-01T00:00Z[UTC]"}}
               (set (q '{:find [id vt-from vt-to],
                         :where [(match :xt_docs {:xt/id id
                                                  :xt/valid-from vt-from
                                                  :xt/valid-to vt-to})]},
                       tx0, #inst "2023"))))

      (t/is (= [{:id :matthew,
                 :vt-from #time/zoned-date-time "2015-01-01T00:00Z[UTC]",
                 :vt-to #time/zoned-date-time "9999-12-31T23:59:59.999999Z[UTC]"}]
               (q '{:find [id vt-from vt-to], :where [(match :xt_docs {:xt/id id
                                                                         :xt/valid-from vt-from
                                                                         :xt/valid-to vt-to}
                                                               {:for-valid-time [:from #inst "2051"]})]},
                  tx0, #inst "2023")))

      (t/is (= [{:id :mark,
                 :vt-from #time/zoned-date-time "2020-01-01T00:00Z[UTC]",
                 :vt-to #time/zoned-date-time "2050-01-01T00:00Z[UTC]"}
                {:id :matthew,
                 :vt-from #time/zoned-date-time "2020-01-02T00:00Z[UTC]",
                 :vt-to #time/zoned-date-time "2040-01-01T00:00Z[UTC]"}]
               (q '{:find [id vt-from vt-to], :where [(match :xt_docs {:xt/id id
                                                                         :xt/valid-from vt-from
                                                                         :xt/valid-to vt-to}
                                                               {:for-valid-time [:to #inst "2040"]})]},
                  tx1, #inst "2023"))))))

(deftest test-snodgrass-99-tutorial
  (letfn [(q [q+args tx current-time]
            (xt/q tu/*node* q+args
                  {:basis {:tx tx, :current-time (util/->instant current-time)}}))]

    (let [tx0 (xt/submit-tx tu/*node*
                            '[[:put :docs {:xt/id 1 :customer-number 145 :property-number 7797}
                               {:for-valid-time [:in #inst "1998-01-10"]}]]
                            {:system-time #inst "1998-01-10"})

          tx1 (xt/submit-tx tu/*node*
                            '[[:put :docs {:xt/id 1 :customer-number 827 :property-number 7797}
                               {:for-valid-time [:in  #inst "1998-01-15"] }]]
                            {:system-time #inst "1998-01-15"})

          _tx2 (xt/submit-tx tu/*node*
                             '[[:delete :docs 1 {:for-valid-time [:in #inst "1998-01-20"]}]]
                             {:system-time #inst "1998-01-20"})

          _tx3 (xt/submit-tx tu/*node*
                             '[[:put :docs {:xt/id 1 :customer-number 145 :property-number 7797}
                                {:for-valid-time [:in #inst "1998-01-03" #inst "1998-01-10"]}]]
                             {:system-time #inst "1998-01-23"})

          _tx4 (xt/submit-tx tu/*node*
                             '[[:delete :docs 1 {:for-valid-time [:in #inst "1998-01-03" #inst "1998-01-05"]}]]
                             {:system-time #inst "1998-01-26"})

          tx5 (xt/submit-tx tu/*node*
                            '[[:put :docs {:xt/id 1 :customer-number 145 :property-number 7797}
                               {:for-valid-time [:in #inst "1998-01-05" #inst "1998-01-12"]}]
                              [:put :docs {:xt/id 1 :customer-number 827 :property-number 7797}
                               {:for-valid-time [:in #inst "1998-01-12" #inst "1998-01-20"]}]]
                            {:system-time #inst "1998-01-28"})

          tx6 (xt/submit-tx tu/*node*
                            [[:put-fn :delete-1-week-records,
                              '(fn delete-1-weeks-records []
                                 (->> (q '{:find [id app-from app-to]
                                           :where [(match :docs {:xt/id id
                                                                 :xt/valid-from app-from
                                                                 :xt/valid-to app-to}
                                                          {:for-valid-time :all-time})
                                                   [(= (- #inst "1970-01-08" #inst "1970-01-01")
                                                       (- app-to app-from))]]})
                                      (map (fn [{:keys [id app-from app-to]}]
                                             [:delete :docs id {:for-valid-time [:in app-from app-to]}]))))]
                             [:call :delete-1-week-records]]
                            {:system-time #inst "1998-01-30"})

          tx7 (xt/submit-tx tu/*node*
                            '[[:put :docs {:xt/id 2 :customer-number 827 :property-number 3621}
                               {:for-valid-time [:in #inst "1998-01-15"]}]]
                            {:system-time #inst "1998-01-31"})]

      (t/is (= [{:cust 145 :app-from (util/->zdt #inst "1998-01-10")}]
               (q '{:find [cust app-from]
                    :where [(match :docs {:customer-number cust, :xt/valid-from app-from}
                                   {:for-valid-time :all-time})]}
                  tx0, nil))
            "as-of 14 Jan")

      (t/is (= #{{:cust 145, :app-from (util/->zdt #inst "1998-01-10")}
                 {:cust 827, :app-from (util/->zdt #inst "1998-01-15")}}
               (set (q '{:find [cust app-from]
                         :where [(match :docs {:customer-number cust, :xt/valid-from app-from}
                                        {:for-valid-time :all-time})]}
                       tx1, nil)))
            "as-of 18 Jan")

      (t/is (= #{{:cust 145, :app-from (util/->zdt #inst "1998-01-05")}
                 {:cust 827, :app-from (util/->zdt #inst "1998-01-12")}}
               (set (q '{:find [cust app-from]
                         :where [(match :docs {:customer-number cust,
                                               :xt/valid-from app-from}
                                        {:for-valid-time :all-time})]
                         :order-by [[app-from :asc]]}
                       tx5, nil)))
            "as-of 29 Jan")

      (t/is (= [{:cust 827, :app-from (util/->zdt #inst "1998-01-12"), :app-to (util/->zdt #inst "1998-01-20")}]
               (q '{:find [cust app-from app-to]
                    :where [(match :docs {:customer-number cust,
                                          :xt/valid-from app-from
                                          :xt/valid-to app-to}
                                   {:for-valid-time :all-time})]
                    :order-by [[app-from :asc]]}
                  tx6, nil))
            "'as best known' (as-of 30 Jan)")

      (t/is (= [{:prop 3621, :vt-begin (util/->zdt #inst "1998-01-15"), :vt-to (util/->zdt #inst "1998-01-20")}]
               (q ['{:find [prop (greatest app-from app-from2) (least app-to app-to2)]
                     :keys [prop vt-begin vt-to]
                     :in [in-prop]
                     :where [(match :docs {:property-number in-prop
                                           :customer-number cust
                                           :xt/valid-time app-time
                                           :xt/valid-from app-from
                                           :xt/valid-to app-to}
                                    {:for-valid-time :all-time})

                             (match :docs {:property-number prop
                                           :customer-number cust
                                           :xt/valid-time app-time-2
                                           :xt/valid-from app-from2
                                           :xt/valid-to app-to2}
                                    {:for-valid-time :all-time})

                             [(<> prop in-prop)]
                             [(overlaps? app-time app-time-2)]]
                     :order-by [[app-from :asc]]}
                   7797]
                  tx7, nil))
            "Case 2: Valid-time sequenced and transaction-time current")

      (t/is (= [{:prop 3621,
                 :vt-begin (util/->zdt #inst "1998-01-15"),
                 :vt-to (util/->zdt #inst "1998-01-20"),
                 :recorded-from (util/->zdt #inst "1998-01-31"),
                 :recorded-stop (util/->zdt util/end-of-time)}]
               (q ['{:find [prop (greatest app-from app-from2) (least app-to app-to2) (greatest sys-from sys-from2) (least sys-to sys-to2)]
                     :keys [prop vt-begin vt-to recorded-from recorded-stop]
                     :in [in-prop]
                     :where [(match :docs {:property-number in-prop
                                           :customer-number cust
                                           :xt/valid-time app-time
                                           :xt/system-time system-time
                                           :xt/valid-from app-from
                                           :xt/valid-to app-to
                                           :xt/system-from sys-from
                                           :xt/system-to sys-to}
                                    {:for-valid-time :all-time
                                     :for-system-time :all-time})

                             (match :docs {:customer-number cust
                                           :property-number prop
                                           :xt/valid-time app-time-2
                                           :xt/system-time system-time-2
                                           :xt/valid-from app-from2
                                           :xt/valid-to app-to2
                                           :xt/system-from sys-from2
                                           :xt/system-to sys-to2}

                                    {:for-valid-time :all-time
                                     :for-system-time :all-time})

                             [(<> prop in-prop)]
                             [(overlaps? app-time app-time-2)]
                             [(overlaps? system-time system-time-2)]]
                     :order-by [[app-from :asc]]}
                   7797]
                  tx7, nil))
            "Case 5: Application-time sequenced and system-time sequenced")

      (t/is (= [{:prop 3621,
                 :vt-begin (util/->zdt #inst "1998-01-15"),
                 :vt-to (util/->zdt #inst "1998-01-20"),
                 :recorded-from (util/->zdt #inst "1998-01-31")}]
               (q ['{:find [prop (greatest app-from app-from2) (least app-to app-to2) sys-from2]
                     :keys [prop vt-begin vt-to recorded-from]
                     :in [in-prop]
                     :where [(match :docs {:property-number in-prop
                                           :customer-number cust
                                           :xt/valid-time app-time
                                           :xt/system-time system-time
                                           :xt/valid-from app-from
                                           :xt/valid-to app-to
                                           :xt/system-from sys-from
                                           :xt/system-to sys-to}
                                    {:for-valid-time :all-time
                                     :for-system-time :all-time})

                             (match :docs {:customer-number cust
                                           :property-number prop
                                           :xt/valid-time app-time-2
                                           :xt/system-time system-time-2
                                           :xt/valid-from app-from2
                                           :xt/valid-to app-to2
                                           :xt/system-from sys-from2
                                           :xt/system-to sys-to2})

                             [(<> prop in-prop)]
                             [(overlaps? app-time app-time-2)]
                             [(contains? system-time sys-from2)]]
                     :order-by [[app-from :asc]]}
                   7797]
                  tx7, nil))
            "Case 8: Application-time sequenced and system-time nonsequenced"))))

(deftest scalar-sub-queries-test
  (xt/submit-tx tu/*node* [[:put :customer {:xt/id 0, :firstname "bob", :lastname "smith"}]
                           [:put :customer {:xt/id 1, :firstname "alice" :lastname "carrol"}]
                           [:put :order {:xt/id 0, :customer 0, :items [{:sku "eggs", :qty 1}]}]
                           [:put :order {:xt/id 1, :customer 0, :items [{:sku "cheese", :qty 3}]}]
                           [:put :order {:xt/id 2, :customer 1, :items [{:sku "bread", :qty 1} {:sku "eggs", :qty 2}]}]])

  (t/are [q result] (= (into #{} result) (set (xt/q tu/*node* q)))
    '{:find [n-customers]
      :where [[(q {:find [(count id)]
                   :where [(match :customer {:xt/id id})]})
               n-customers]]}
    [{:n-customers 2}]

    '{:find [customer, n-orders]
      :where [(match :customer {:xt/id customer})
              [(q {:find [(count order)]
                   :in [customer]
                   :where [(match :order {:customer customer, :xt/id order})]})
               n-orders]]}
    [{:customer 0, :n-orders 2}
     {:customer 1, :n-orders 1}]

    '{:find [customer, n-orders, n-qty]
      :where [(match :customer {:xt/id customer})
              [(q {:find [(count order)]
                   :in [customer]
                   :where [(match :order {:customer customer, :xt/id order})]})
               n-orders]
              [(q {:find [(sum qty2)]
                   :in [customer]
                   :where [(match :order {:customer customer, :xt/id order, :items [item ...]})
                           [(. item :qty) qty2]]})
               n-qty]]}
    [{:customer 0, :n-orders 2, :n-qty 4}
     {:customer 1, :n-orders 1, :n-qty 3}]

    '{:find [n-orders, n-qty]
      :where [[(q {:find [(count order)]
                   :where [(match :order {:xt/id order})]})
               n-orders]
              [(q {:find [(sum qty2)]
                   :where [(match :order {:xt/id order, :items [item ...]})
                           [(. item :qty) qty2]]})
               n-qty]]}
    [{:n-orders 3, :n-qty 7}]

    '{:find [order firstname]
      :where [(match :order {:xt/id order, :customer customer})
              [(q {:find [firstname]
                   :in [customer]
                   :where [(match :customer {:xt/id customer, :firstname firstname})]})
               firstname]]}
    [{:order 0, :firstname "bob"}
     {:order 1, :firstname "bob"}
     {:order 2, :firstname "alice"}]

    '{:find [order firstname]
      :where [(match :order {:xt/id order, :customer customer})
              [(q {:find [firstname2]
                   :in [customer]
                   :where [(match :customer {:xt/id customer, :firstname firstname2})]})
               firstname]]}
    [{:order 0, :firstname "bob"}
     {:order 1, :firstname "bob"}
     {:order 2, :firstname "alice"}]

    '{:find [order fullname]
      :where [(match :order {:xt/id order, :customer customer})
              [(concat
                 (q {:find [fn]
                     :in [customer]
                     :where [(match :customer {:xt/id customer, :firstname fn})]})
                 " "
                 (q {:find [ln]
                     :in [customer]
                     :where [(match :customer {:xt/id customer, :lastname ln})]}))
               fullname]]}
    [{:order 0, :fullname "bob smith"}
     {:order 1, :fullname "bob smith"}
     {:order 2, :fullname "alice carrol"}]

    '{:find [order]
      :where [(match :order {:xt/id order, :customer customer})
              [(= (q {:find [fn]
                      :in [customer]
                      :where [(match :customer {:xt/id customer, :firstname fn})]})
                  "bob")]]}
    [{:order 0}
     {:order 1}]

    '{:find [(q {:find [(count c)] :where [(match :customer {:xt/id c})]})]
      :keys [n]}
    [{:n 2}]


    '{:find [(q {:find [(count c)]
                 :where [(match :customer {:xt/id c})]})
             (q {:find [(count o)]
                 :where [(match :order {:xt/id o})]})]
      :keys [c, o]}
    [{:c 2, :o 3}]

    '{:find [(q {:find [(count c)]
                 :in [c]
                 :where [(match :customer {:xt/id c})]})
             (q {:find [(count o)]
                 :in [c]
                 :where [(match :order {:xt/id o, :customer c})]})]
      :keys [c, o]
      :where [(match :customer [{:xt/id c, :firstname "bob"}])]}
    [{:c 1, :o 2}])

(t/testing "cardinality violation error"
    (t/is (thrown-with-msg? xtdb.RuntimeException #"cardinality violation"
                            (->> '{:find [firstname]
                                   :where [[(q {:find [firstname],
                                                :where [(match :customer {:firstname firstname})]})
                                            firstname]]}
                                 (xt/q tu/*node*)))))

  (t/testing "multiple column error"
    (t/is (thrown-with-msg? xtdb.IllegalArgumentException #"scalar sub query requires exactly one column"
                            (->> '{:find [n-customers]
                                   :where [[(q {:find [firstname (count customer)],
                                                :where [(match :customer {:customer customer, :firstname firstname})]})
                                            n-customers]]}
                                 (xt/q tu/*node*))))))

(deftest test-period-predicates

  (xt/submit-tx tu/*node* '[[:put :xt_docs {:xt/id 1} {:for-valid-time [:in #inst "2015" #inst "2020"]}]
                            [:put :xt_cats {:xt/id 2} {:for-valid-time [:in #inst "2016" #inst "2018"]}]])

  (t/is (= [{:xt/id 1, :id2 2,
             :xt_docs_app_time {:from #time/zoned-date-time "2015-01-01T00:00Z[UTC]",
                                :to #time/zoned-date-time "2020-01-01T00:00Z[UTC]"},
             :xt_cats_app_time {:from #time/zoned-date-time "2016-01-01T00:00Z[UTC]",
                                :to #time/zoned-date-time "2018-01-01T00:00Z[UTC]"}}]
           (xt/q
            tu/*node*
            '{:find [xt/id id2 xt_docs_app_time xt_cats_app_time]
              :where [(match :xt_docs [xt/id {:xt/valid-time xt_docs_app_time}]
                             {:for-valid-time :all-time})
                      (match :xt_cats [{:xt/valid-time xt_cats_app_time :xt/id id2}]
                             {:for-valid-time :all-time})
                      [(contains? xt_docs_app_time xt_cats_app_time)]]})))

  (t/is (= [{:xt/id 1, :id2 2,
             :xt_docs_sys_time {:from #time/zoned-date-time "2020-01-01T00:00Z[UTC]",
                                :to #time/zoned-date-time "9999-12-31T23:59:59.999999Z[UTC]"},
             :xt_cats_sys_time {:from #time/zoned-date-time "2020-01-01T00:00Z[UTC]",
                                :to #time/zoned-date-time "9999-12-31T23:59:59.999999Z[UTC]"}}]
           (xt/q
            tu/*node*
            '{:find [xt/id id2 xt_docs_sys_time xt_cats_sys_time]
              :where [(match :xt_docs [xt/id {:xt/system-time xt_docs_sys_time}]
                             {:for-valid-time :all-time
                              :for-system-time :all-time})
                      (match :xt_cats [{:xt/system-time xt_cats_sys_time :xt/id id2}]
                             {:for-valid-time :all-time
                              :for-system-time :all-time})
                      [(equals? xt_docs_sys_time xt_cats_sys_time)]]}))))

(deftest test-period-constructor
  (t/is (= [{:p1 {:from #time/zoned-date-time "2018-01-01T00:00Z[UTC]",
                  :to #time/zoned-date-time "2022-01-01T00:00Z[UTC]"}}]
           (xt/q
             tu/*node*
             '{:find [p1],
               :where [[(period #inst "2018" #inst "2022") p1]]})))

  (t/is (thrown-with-msg?
          RuntimeException
          #"From cannot be greater than to when constructing a period"
          (xt/q
            tu/*node*
            '{:find [p1],
              :where [[(period #inst "2022" #inst "2020") p1]]}))))

(deftest test-period-and-temporal-col-projection
  (xt/submit-tx tu/*node* '[[:put :xt_docs {:xt/id 1} {:for-valid-time [:in #inst "2015" #inst "2050"]}]])


  (t/is (= [{:xt/id 1,
             :app_time {:from #time/zoned-date-time "2015-01-01T00:00Z[UTC]",
                        :to #time/zoned-date-time "2050-01-01T00:00Z[UTC]"},
             :xt/valid-from #time/zoned-date-time "2015-01-01T00:00Z[UTC]",
             :app-time-to #time/zoned-date-time "2050-01-01T00:00Z[UTC]"}]
           (xt/q
            tu/*node*
            '{:find [xt/id app_time xt/valid-from app-time-to]
              :where [(match :xt_docs
                        [xt/id xt/valid-from
                         {:xt/valid-time app_time
                          :xt/valid-to app-time-to}]
                        {:for-valid-time :all-time})]}))
        "projecting both period and underlying cols")

  (t/is (= [{:xt/id 1,
             :app_time {:from #time/zoned-date-time "2015-01-01T00:00Z[UTC]",
                        :to #time/zoned-date-time "2050-01-01T00:00Z[UTC]"},
             :sys_time {:from #time/zoned-date-time "2020-01-01T00:00Z[UTC]",
                        :to #time/zoned-date-time "9999-12-31T23:59:59.999999Z[UTC]"}}]
           (xt/q
            tu/*node*
            '{:find [xt/id app_time sys_time]
              :where [(match :xt_docs [xt/id {:xt/valid-time app_time
                                              :xt/system-time sys_time}]
                             {:for-valid-time :all-time
                              :for-system-time :all-time})]}))
        "projecting both app and system-time periods")

  (t/is (= [#:xt{:valid-time
                 {:from #time/zoned-date-time "2015-01-01T00:00Z[UTC]",
                  :to #time/zoned-date-time "2050-01-01T00:00Z[UTC]"}}]
           (xt/q
            tu/*node*
            '{:find [xt/valid-time]
              :where [(match :xt_docs
                        [id xt/valid-time]
                        {:for-valid-time :all-time})]}))
        "protecting temporal period in vector syntax")

  (t/is (= [{:xt/id 1
             :id2 1,
             :app_time {:from #time/zoned-date-time "2015-01-01T00:00Z[UTC]",
                        :to #time/zoned-date-time "2050-01-01T00:00Z[UTC]"},
             :sys_time {:from #time/zoned-date-time "2020-01-01T00:00Z[UTC]",
                        :to #time/zoned-date-time "9999-12-31T23:59:59.999999Z[UTC]"}}]
           (xt/q
            tu/*node*
            '{:find [xt/id id2 app_time sys_time]
              :where [(match :xt_docs [xt/id {:xt/valid-time app_time
                                              :xt/system-time sys_time}]
                             {:for-valid-time :all-time
                              :for-system-time :all-time})
                      (match :xt_docs [{:xt/valid-time app_time
                                        :xt/system-time sys_time
                                        :xt/id id2}]
                             {:for-valid-time :all-time
                              :for-system-time :all-time})]}))
        "period unification")

  (xt/submit-tx tu/*node* '[[:put :xt_docs {:xt/id 2}]])

  (t/is (= [{:xt/id 2,
             :time {:from #time/zoned-date-time "2020-01-02T00:00Z[UTC]",
                    :to #time/zoned-date-time "9999-12-31T23:59:59.999999Z[UTC]"}}]
           (xt/q
            tu/*node*
            '{:find [xt/id time]
              :where [(match :xt_docs [xt/id {:xt/valid-time time
                                              :xt/system-time time}]
                             {:for-valid-time :all-time
                              :for-system-time :all-time})]}))
        "period unification within match")

  (xt/submit-tx tu/*node* '[[:put :xt_docs
                             {:xt/id 3 :c {:from #inst "2015" :to #inst "2050"}}
                             {:for-valid-time [:in #inst "2015" #inst "2050"]}]])

  (t/is (= [{:xt/id 3,
             :time
             {:from #time/zoned-date-time "2015-01-01T00:00Z[UTC]",
              :to #time/zoned-date-time "2050-01-01T00:00Z[UTC]"}}]
           (xt/q
            tu/*node*
            '{:find [xt/id time]

              :where [(match :xt_docs [xt/id {:xt/valid-time time
                                              :c time}]
                             {:for-valid-time :all-time
                              :for-system-time :all-time})]}))
        "period unification within match with user period column"))

(deftest test-period-literal-match
  (xt/submit-tx tu/*node* '[[:put :xt_docs {:xt/id 1} {:for-valid-time [:in #inst "2015" #inst "2050"]}]])

  (t/is (thrown-with-msg?
         IllegalArgumentException
         #"Temporal period must be bound to logic var"
         (xt/q
          tu/*node*
          '{:find [id]
            :where [(match :xt_docs
                      [id {:xt/valid-time "111"}]
                      {:for-valid-time :all-time})]}))))

(t/deftest test-explain-plan-654
  (t/is (= '[{:plan [:project [name age]
                     [:project [{age _r0_age} {name _r0_name} {pid _r0_pid}]
                      [:rename {age _r0_age, name _r0_name, pid _r0_pid}
                       [:project [{pid xt/id} name age]
                        [:scan {:table people, :for-valid-time nil, :for-system-time nil}
                         [age name {xt/id (= xt/id ?pid)}]]]]]]}]

           (xt/q tu/*node*
                 '{:find [name age]
                   :in [pid]
                   :where [($ :people [{:xt/id pid} name age])]}
                 {:explain? true}))))

(deftest test-unbound-vars
  (t/is
   (thrown-with-msg?
    IllegalArgumentException
    #"Logic variables in find clause must be bound in where: foo"
    (xt/q
     tu/*node*
     '{:find [foo]
       :where [(match :xt_docs {:first-name name})]}))
   "plain logic var in find")

  (t/is
   (thrown-with-msg?
    IllegalArgumentException
    #"Logic variables in find clause must be bound in where: foo"
    (xt/q
     tu/*node*
     (->
      '{:find [(+ foo 1)]
        :where [(match :xt_docs {:first-name name})]})))
   "logic var within expr in find")

  (t/is
   (thrown-with-msg?
    IllegalArgumentException
    #"Logic variables in find clause must be bound in where: foo"
    (xt/q
     tu/*node*
     (->
      '{:find [(sum foo)]
        :where [(match :xt_docs {:first-name name})]})))
   "logic var within aggr in find")

  (t/is
   (thrown-with-msg?
    IllegalArgumentException
    #"Logic variables in find clause must be bound in where: foo"
    (xt/q
     tu/*node*
     (->
      '{:find [(sum (- 64 (+ 20 4 foo)))]
        :where [(match :xt_docs {:first-name name})]})))
   "deeply nested logic var in find")

  (t/is
   (thrown-with-msg?
    IllegalArgumentException
    #"Logic variables in find clause must be bound in where: baz, bar, foo"
    (xt/q
     tu/*node*
     (->
      '{:find [foo (+ 1 bar) (sum (+ 1 (- 1 baz)))]
        :where [(match :xt_docs {:first-name name})]})))
   "multiple unbound vars in find")

  (t/is
   (thrown-with-msg?
    IllegalArgumentException
    #"Logic variables in find clause must be bound in where: baz, bar, foo"
    (xt/q
     tu/*node*
     (->
      '{:find [foo (+ 1 bar) (sum (+ 1 (- 1 baz)))]
        :where [(match :xt_docs {:first-name name})]})))
   "multiple unbound vars in find")

  (t/is
   (thrown-with-msg?
    IllegalArgumentException
    #"Logic variables in order-by clause must be bound in where: baz"
    (xt/q
     tu/*node*
     (->
      '{:find [name]
        :where [(match :xt_docs {:first-name name})]
        :order-by [[baz :asc]]})))
   "simple order-by var")

  (t/is
   (thrown-with-msg?
    IllegalArgumentException
    #"Logic variables in order-by clause must be bound in where: biff, baz, bing"
    (xt/q
     tu/*node*
     (->
      '{:find [name]
        :where [(match :xt_docs {:first-name name})]
        :order-by [[(count biff) :desc] [baz :asc] [bing]]})))
   "multiple unbound vars in order-by")

  (t/is
   (thrown-with-msg?
    IllegalArgumentException
    #"Logic variables in find clause must be bound in where: age, min_age"
    (xt/q tu/*node*
          '{:find [min_age age]
            :where [(q {:find [(min age)]
                        :where [($ :docs {:age age})]})]}))
   "variables not exposed by subquery"))

(t/deftest test-default-valid-time
  (xt/submit-tx tu/*node* [[:put :xt_docs {:xt/id 1 :foo "2000-4000"} {:for-valid-time [:in #inst "2000" #inst "4000"]}]
                           [:put :xt_docs {:xt/id 1 :foo "3000-"} {:for-valid-time [:from #inst "3000"]}]])

  (t/is (= #{{:xt/id 1, :foo "2000-4000"} {:xt/id 1, :foo "3000-"}}
           (set (xt/q tu/*node*
                      '{:find [xt/id foo]
                        :where [(match :xt_docs [xt/id foo])]}
                      {:default-all-valid-time? true})))))

(t/deftest test-sql-insert
  (xt/submit-tx tu/*node* [[:sql "INSERT INTO foo (xt$id) VALUES (0)"]])
  (t/is (= [{:xt/id 0}]
           (xt/q tu/*node*
                 '{:find [xt/id]
                   :where [(match :foo [xt/id])]}))))


(t/deftest test-dml-insert
  (xt/submit-tx tu/*node* [[:put :foo {:xt/id 0}]])
  (t/is (= [{:id 0}]
           (xt/q tu/*node*
                 '{:find [id]
                   :where [(match :foo {:xt/id id})]}))))

(t/deftest test-metadata-filtering-for-time-data-607
  (with-open [node (node/start-node {:xtdb/indexer {:rows-per-chunk 1}})]
    (xt/submit-tx node [[:put :xt_docs {:xt/id 1 :from-date #time/date "2000-01-01"}]
                        [:put :xt_docs {:xt/id 2 :from-date #time/date "3000-01-01"}]])
    (t/is (= [{:id 1}]
             (xt/q node
                   '{:find [id]
                     :where [(match :xt_docs [{:xt/id id} from-date])
                             [(>= from-date #inst "1500")]
                             [(< from-date #inst "2500")]]})))
    (xt/submit-tx node [[:put :xt_docs2 {:xt/id 1 :from-date #inst "2000-01-01"}]
                        [:put :xt_docs2 {:xt/id 2 :from-date #inst "3000-01-01"}]])
    (t/is (= [{:id 1}]
             (xt/q node
                   '{:find [id]
                     :where [(match :xt_docs2 [{:xt/id id} from-date])
                             [(< from-date #time/date "2500-01-01")]
                             [(< from-date #time/date "2500-01-01")]]})))))

(t/deftest bug-non-namespaced-nested-keys-747
  (xt/submit-tx tu/*node* [[:put :bar {:xt/id 1 :foo {:a/b "foo"}}]])
  (t/is (= [{:foo {:a/b "foo"}}]
           (xt/q tu/*node*
                 '{:find [foo]
                   :where [(match :bar [foo])]}))))

(t/deftest test-row-alias
  (let [docs [{:xt/id 42, :firstname "bob"}
              {:xt/id 43, :firstname "alice", :lastname "carrol"}
              {:xt/id 44, :firstname "jim", :orders [{:sku "eggs", :qty 2}, {:sku "cheese", :qty 1}]}]]
    (xt/submit-tx tu/*node* (map (partial vector :put :customer) docs))
    (t/is (= (set (mapv (fn [doc] {:c doc}) docs))
             (set (xt/q tu/*node* '{:find [c] :where [($ :customer {:xt/* c})]}))))))

(t/deftest test-row-alias-system-time-key-set
  (let [inputs
        [[{:xt/id 0, :a 0} #inst "2023-01-17T00:00:00"]
         [{:xt/id 0, :b 0} #inst "2023-01-18T00:00:00"]
         [{:xt/id 0, :c 0, :a 0} #inst "2023-01-19T00:00:00"]]

        _
        (doseq [[doc system-time] inputs]
          (xt/submit-tx tu/*node* [[:put :x doc]] {:system-time system-time}))

        q (partial xt/q tu/*node*)]

    (t/is (= [{:x {:xt/id 0, :a 0, :c 0}}]
             (q '{:find [x]
                  :where [($ :x {:xt/* x})]})))

    (t/is (= [{:x {:xt/id 0, :b 0}}]
             (q '{:find [x]
                  :where [($ :x {:xt/* x})],}
                {:basis {:tx #xt/tx-key {:tx-id 1, :system-time #time/instant "2023-01-18T00:00:00Z"}}})))

    (t/is (= [{:x {:xt/id 0, :a 0}}]
             (q '{:find [x]
                  :where [($ :x {:xt/* x})],}
                {:basis {:tx #xt/tx-key {:tx-id 0, :system-time #time/instant "2023-01-17T00:00:00Z"}}})))))

(t/deftest test-row-alias-app-time-key-set
  (let [inputs
        [[{:xt/id 0, :a 0} #inst "2023-01-17T00:00:00"]
         [{:xt/id 0, :b 0} #inst "2023-01-18T00:00:00"]
         [{:xt/id 0, :c 0, :a 0} #inst "2023-01-19T00:00:00"]]

        _
        (doseq [[doc app-time] inputs]
          (xt/submit-tx tu/*node* [[:put :x doc {:for-valid-time [:in app-time]}]]))

        q (partial xt/q tu/*node*)]

    (t/is (= [{:x {:xt/id 0, :a 0, :c 0}}]
             (q '{:find [x]
                  :where [($ :x {:xt/* x})]})))

    (t/is (= [{:x {:xt/id 0, :b 0}}]
             (q '{:find [x]
                  :where [($ :x {:xt/* x} {:for-valid-time [:at #time/instant "2023-01-18T00:00:00Z"]})],})))

    (t/is (= [{:x {:xt/id 0, :a 0}}]
             (q '{:find [x]
                  :where [($ :x {:xt/* x} {:for-valid-time [:at #time/instant "2023-01-17T00:00:00Z"]})]})))))

(t/deftest test-normalisation
  (xt/submit-tx tu/*node* [[:put :xt-docs {:xt/id "doc" :Foo/Bar 1 :Bar.Foo/hELLo-wORLd 2}]])
  (t/is (= [{:Foo/Bar 1, :Bar.Foo/Hello-World 2}]
           (xt/q tu/*node* '{:find [Foo/Bar Bar.Foo/Hello-World]
                             :where [(match :xt-docs [Foo/Bar Bar.Foo/Hello-World])]})))
  (t/is (= [{:bar 1, :foo 2}]
           (xt/q tu/*node* '{:find [bar foo]
                             :where [(match :xt-docs {:foo/bar bar :bar.foo/hello-world foo})]}))))

(t/deftest test-table-normalisation
  (let [doc {:xt/id "doc" :foo "bar"}]
    (xt/submit-tx tu/*node* [[:put :xt/the-docs doc]])
    (t/is (= [{:id "doc"}]
             (xt/q tu/*node* '{:find [id]
                               :where [(match :xt/the-docs {:xt/id id})]})))
    (t/is (= [{:doc doc}]
             (xt/q tu/*node* '{:find [doc]
                               :where [(match :xt/the-docs {:xt/* doc})]})))))

(t/deftest test-inconsistent-valid-time-range-2494
  (xt/submit-tx tu/*node* '[[:put :xt-docs {:xt/id 1} {:for-valid-time [:in nil #inst "2011"]}]])
  (t/is (= [{:tx-id 0, :committed? false}]
           (xt/q tu/*node* '{:find [tx-id committed?]
                             :where [($ :xt/txs {:xt/id tx-id,
                                                 :xt/committed? committed?})]})))
  (xt/submit-tx tu/*node* '[[:put :xt-docs {:xt/id 2}]])
  (xt/submit-tx tu/*node* '[[:delete :xt-docs 2 {:for-valid-time [:in nil #inst "2011"]}]])
  (t/is (= #{{:tx-id 0, :committed? false}
             {:tx-id 1, :committed? true}
             {:tx-id 2, :committed? false}}
           (set (xt/q tu/*node* '{:find [tx-id committed?]
                                  :where [($ :xt/txs {:xt/id tx-id,
                                                      :xt/committed? committed?})]})))))

(deftest test-date-and-time-literals
  (t/is (= [{:a true, :b false, :c true, :d true}]
           (xt/q tu/*node*
                 '{:find [a b c d]
                   :where [[(= #time/date "2020-01-01" #time/date "2020-01-01") a]
                           [(= #time/zoned-date-time "3000-01-01T08:12:13.366Z"
                               #time/zoned-date-time "2020-01-01T08:12:13.366Z") b]
                           [(= #time/date-time "2020-01-01T08:12:13.366"
                               #time/date-time "2020-01-01T08:12:13.366") c]
                           [(= #time/time "08:12:13.366" #time/time "08:12:13.366") d]]}))))

(t/deftest bug-temporal-queries-wrong-at-boundary-2531
  (with-open [node (node/start-node {:xtdb/indexer {:rows-per-chunk 10}
                                     :xtdb.tx-producer/tx-producer {:instant-src (tu/->mock-clock)}
                                     :xtdb.log/memory-log {:instant-src (tu/->mock-clock)}})]
    (doseq [i (range 10)]
      (xt/submit-tx node [[:put :ints {:xt/id 0 :n i}]]))

    (t/is (=
           #{{:n 0,
              :valid-time
              {:from #time/zoned-date-time "2020-01-01T00:00Z[UTC]",
               :to #time/zoned-date-time "2020-01-02T00:00Z[UTC]"}}
             {:n 1,
              :valid-time
              {:from #time/zoned-date-time "2020-01-02T00:00Z[UTC]",
               :to #time/zoned-date-time "2020-01-03T00:00Z[UTC]"}}
             {:n 2,
              :valid-time
              {:from #time/zoned-date-time "2020-01-03T00:00Z[UTC]",
               :to #time/zoned-date-time "2020-01-04T00:00Z[UTC]"}}
             {:n 3,
              :valid-time
              {:from #time/zoned-date-time "2020-01-04T00:00Z[UTC]",
               :to #time/zoned-date-time "2020-01-05T00:00Z[UTC]"}}
             {:n 4,
              :valid-time
              {:from #time/zoned-date-time "2020-01-05T00:00Z[UTC]",
               :to #time/zoned-date-time "2020-01-06T00:00Z[UTC]"}}}
           (set (xt/q node '{:find [n valid-time] :where [($ :ints {:n n :xt/id 0 :xt/valid-time valid-time}
                                                             {:for-valid-time [:in #inst "2020-01-01" #inst "2020-01-06"]})]}))))))

(deftest test-no-zero-width-intervals
  (xt/submit-tx tu/*node* [[:put :xt-docs {:xt/id 1 :v 1}]
                           [:put :xt-docs {:xt/id 1 :v 2}]
                           [:put :xt-docs {:xt/id 2 :v 1} {:for-valid-time [:in #inst "2020-01-01" #inst "2020-01-02"]}]])
  (xt/submit-tx tu/*node* [[:put :xt-docs {:xt/id 2 :v 2} {:for-valid-time [:in #inst "2020-01-01" #inst "2020-01-02"]}]])
  (t/is (= [{:v 2}]
           (xt/q tu/*node*
                 '{:find [v]
                   :where [(match :xt-docs [{:xt/id 1} v] {:for-system-time :all-time})]}))
        "no zero width system time intervals")
  (t/is (= [{:v 2}]
           (xt/q tu/*node*
                 '{:find [v]
                   :where [(match :xt-docs [{:xt/id 2} v] {:for-valid-time :all-time})]}))
        "no zero width valid-time intervals"))

(deftest row-alias-on-txs-tables-2809
  (xt/submit-tx tu/*node* [[:put :xt-docs {:xt/id 1 :v 1}]])
  (xt/submit-tx tu/*node* [[:call :non-existing-fn]])

  (let [txs (->> (xt/q tu/*node*
                       '{:find [tx]
                         :where [(match :xt/txs {:xt/* tx})]})
                 (mapv :tx))]

    (t/is (= [{:xt/tx_time #time/zoned-date-time "2020-01-02T00:00Z[UTC]",
               :xt/id 1,
               :xt/committed? false}
              {:xt/tx_time #time/zoned-date-time "2020-01-01T00:00Z[UTC]",
               :xt/id 0,
               :xt/committed? true}]
             (mapv #(dissoc % :xt/error) txs)))

    (t/is (= {:xtdb.error/error-type :runtime-error,
              :xtdb.error/error-key :xtdb.call/no-such-tx-fn,
              :xtdb.error/message "Runtime error: ':xtdb.call/no-such-tx-fn'",
              :fn-id :non-existing-fn}
             (.getData ^xtdb.RuntimeException (.form ^ClojureForm (:xt/error (first txs))))))))
