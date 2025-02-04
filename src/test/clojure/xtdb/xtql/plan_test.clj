;; THIRD-PARTY SOFTWARE NOTICE
;; This file is derivative of test files found in the DataScript project.
;; The Datascript license is copied verbatim in this directory as `LICENSE`.
;; https://github.com/tonsky/datascript

(ns xtdb.xtql.plan-test
  (:require [clojure.test :as t :refer [deftest]]
            [xtdb.api :as xt]
            [xtdb.james-bond :as bond]
            [xtdb.node :as xtn]
            [xtdb.test-util :as tu]
            [xtdb.time :as time]
            [xtdb.xtql :as xtql]
            [xtdb.xtql.plan :as plan]))

(t/use-fixtures :each tu/with-mock-clock tu/with-node)

;;TODO test nested subqueries

(def ivan+petr
  [[:put-docs :docs {:xt/id :ivan, :first-name "Ivan", :last-name "Ivanov"}]
   [:put-docs :docs {:xt/id :petr, :first-name "Petr", :last-name "Petrov"}]])

(deftest test-from
  (xt/submit-tx tu/*node* ivan+petr)

  (t/is (= #{{:first-name "Ivan"}
             {:first-name "Petr"}}
           (set (xt/q tu/*node* '(from :docs [first-name])))))

  (t/is (= #{{:name "Ivan"}
             {:name "Petr"}}
           (set (xt/q tu/*node* '(from :docs [{:first-name name}])))))

  (t/is (= #{{:e :ivan, :name "Ivan"}
             {:e :petr, :name "Petr"}}
           (set (xt/q tu/*node*
                      '(from :docs [{:xt/id e, :first-name name}]))))
        "returning eid")

  (t/is (= #{{:name "Ivan" :also-name "Ivan"}
             {:name "Petr" :also-name "Petr"}}
           (set (xt/q tu/*node* '(from :docs [{:first-name name} {:first-name also-name}]))))
        "projecting col out multiple times to different vars"))

(deftest test-from-star
  (xt/submit-tx tu/*node* ivan+petr)

  (t/is (= #{{:last-name "Petrov", :first-name "Petr", :xt/id :petr}
             {:last-name "Ivanov", :first-name "Ivan", :xt/id :ivan}}
           (set (xt/q tu/*node* '(from :docs [*]))))
        "Extra short-form projections are redundant")

  (t/is (= #{{:last-name "Petrov", :first-name "Petr", :xt/id :petr}
             {:last-name "Ivanov", :first-name "Ivan", :xt/id :ivan}}
           (set (xt/q tu/*node* '(from :docs [first-name *]))))
        "Extra short-form projections are redundant")

  (t/is (= #{{:last-name "Ivanov", :first-name "Ivan", :xt/id :ivan}}
           (set (xt/q tu/*node* '(from :docs [{:first-name "Ivan"} *]))))
        "literal filters still work")

  (xt/submit-tx tu/*node* [[:put-docs :docs {:xt/id :jeff, :first-name "Jeff", :last-name "Jeff"}]])

  (t/is (= #{{:last-name "Jeff", :first-name "Jeff", :xt/id :jeff}}
           (set (xt/q tu/*node* '(from :docs [{:last-name first-name} *]))))
        "Implicitly projected cols unify with explicitly projected cols")

  (t/is (thrown-with-msg? IllegalArgumentException
                          #"\* is not a valid in from when inside a unify context"
                          (xt/q tu/*node*
                                 '(unify (from :docs [*]))))))

(deftest test-from-unification
  (xt/submit-tx tu/*node*
                (conj ivan+petr
                      [:put-docs :docs {:xt/id :jeff, :first-name "Jeff", :last-name "Jeff"}]))

  (t/is (= #{{:name "Jeff"}}
           (set (xt/q tu/*node* '(from :docs [{:first-name name :last-name name}])))))

  (t/is (= #{{:name "Jeff"} {:name "Petr"} {:name "Ivan"}}
           (set (xt/q tu/*node* '(from :docs [{:first-name name} {:first-name name}]))))
        "unifying over the same column, for completeness rather than utility"))

(deftest test-basic-query
  (xt/submit-tx tu/*node* ivan+petr)

  (t/is (= [{:e :ivan}]
           (xt/q tu/*node*
                 '(from :docs [{:xt/id e, :first-name "Ivan"}])))
        "query by single field")

  ;; HACK this scans first-name out twice
  (t/is (= [{:first-name "Petr", :last-name "Petrov"}]
           (xt/q tu/*node*
                 '(from :docs [{:first-name "Petr"} first-name last-name])))
        "returning the queried field")

  (t/is (= [{:first-name "Petr", :last-name "Petrov"}]
           (xt/q tu/*node*
                 '(from :docs [{:xt/id :petr} first-name last-name])))
        "literal eid"))

(deftest test-rel
  (xt/submit-tx tu/*node* ivan+petr)

  (t/is (= [{} {}]
           (xt/q tu/*node* '(rel [{:first-name "Ivan"} {:first-name "Petr"}] []))))

  (t/is (= #{{:first-name "Ivan"} {:first-name "Petr"}}
           (set (xt/q tu/*node* '(rel [{:first-name "Ivan"} {:first-name "Petr"}] [first-name])))))

  (t/is (= [{:first-name "Petr"}]
           (xt/q tu/*node* '(-> (rel [{:first-name "Ivan"} {:first-name "Petr"}] [first-name])
                                (where (= "Petr" first-name))))))

  #_ ;; TODO literal out specs
  (t/is (= [{:first-name "Petr"}]
           (xt/q tu/*node* '(-> (rel [{:first-name "Ivan"} {:first-name "Petr"}] {:bind [{:xt/id :petr} first-name]})
                                (where (= "Petr" first-name))))))

  (t/is (= [{:first-name "Ivan"} {:first-name "Petr"}]
           (xt/q tu/*node*
                 '(rel [{:first-name "Ivan"} {:first-name $petr}] [first-name])
                 {:args {:petr "Petr"}}))
        "simple param")

  (t/is (= [{:foo :bar, :baz {:nested-foo :bar}}]
           (xt/q tu/*node*
                 '(rel [{:foo :bar :baz {:nested-foo $nested-param}}] [foo baz])
                 {:args {:nested-param :bar}}))
        "simple param nested")

  (t/is (= #{{:first-name "Ivan"} {:first-name "Petr"}}
           (set (xt/q tu/*node* '(unify (from :docs [first-name])
                                        (rel [{:first-name "Ivan"} {:first-name "Petr"}] [first-name])))))
        "testing unify")

  (t/is (= [{:first-name "Petr"}]
           (xt/q tu/*node* '(unify (from :docs [{:xt/id :petr} first-name])
                                   (rel [{:first-name "Ivan"} {:first-name "Petr"}] [first-name]))))
        "testing unify with restriction")

  (t/is (= #{{:first-name "Ivan"} {:first-name "Petr"}}
           (set (xt/q tu/*node* '(rel $ivan+petr [first-name])
                      {:args {:ivan+petr [{:first-name "Ivan"} {:first-name "Petr"}]}})))
        "rel arg as parameter")

  (t/is (= #{{:first-name "Petr"}}
           (set (xt/q tu/*node* '(-> (rel $ivan+petr [first-name])
                                     (where (= "Petr" first-name)))
                      {:args {:ivan+petr [{:first-name "Ivan"} {:first-name "Petr"}]}})))
        "rel arg as parameter")

  (t/is (= [{:first-name "Petr"}]
           (xt/q tu/*node* '(unify (from :docs [{:xt/id :petr :first-name first-name}])
                                   (rel $ivan+petr [first-name]))
                 {:args {:ivan+petr [{:first-name "Ivan"} {:first-name "Petr"}]}}))
        "rel arg as paramater in unify"))


(deftest test-order-by
  (let [_tx (xt/submit-tx tu/*node* ivan+petr)]
    (t/is (= [{:first-name "Ivan"} {:first-name "Petr"}]
             (xt/q tu/*node*
                   '(-> (from :docs [first-name])
                        (order-by first-name)))))

    (t/is (= [{:first-name "Petr"} {:first-name "Ivan"}]
             (xt/q tu/*node*
                   '(-> (from :docs [first-name])
                        (order-by {:val first-name :dir :desc})))))


    (t/is (= [{:first-name "Ivan"}]
             (xt/q tu/*node*
                   '(-> (from :docs [first-name])
                        (order-by first-name)
                        (limit 1)))))


    (t/is (= [{:first-name "Petr"}]
             (xt/q tu/*node*
                   '(-> (from :docs [first-name])
                        (order-by {:val first-name :dir :desc})
                        (limit 1)))))

    (t/is (= [{:first-name "Petr"}]
             (xt/q tu/*node*
                   '(-> (from :docs [first-name])
                        (order-by first-name)
                        (offset 1)
                        (limit 1)))))

    (xt/submit-tx tu/*node* [[:put-docs :docs {:xt/id :dave :first-name nil}]
                             [:put-docs :docs {:xt/id :jeff :first-name "Jeff"}]])

    (t/is (= [{}
              {:first-name "Ivan"}
              {:first-name "Jeff"}
              {:first-name "Petr"}]
             (xt/q tu/*node*
                   '(-> (from :docs [first-name])
                        (order-by {:val first-name :dir :asc :nulls :first})))))

    (t/is (= [{:first-name "Ivan"}
              {:first-name "Jeff"}
              {:first-name "Petr"}
              {}]
             (xt/q tu/*node*
                   '(-> (from :docs [first-name])
                        (order-by {:val first-name :dir :asc :nulls :last})))))))

(deftest test-order-by-multiple-cols
  (let [_tx (xt/submit-tx tu/*node*
                          [[:put-docs :docs {:xt/id 2, :n 2}]
                           [:put-docs :docs {:xt/id 3, :n 3}]
                           [:put-docs :docs {:xt/id 1, :n 2}]
                           [:put-docs :docs {:xt/id 4, :n 1}]])]
    (t/is (= [{:i 4, :n 1}
              {:i 1, :n 2}
              {:i 2, :n 2}
              {:i 3, :n 3}]
             (xt/q tu/*node*
                   '(-> (from :docs [{:xt/id i, :n n}])
                        (order-by n i)))))))

;; https://github.com/tonsky/datascript/blob/1.1.0/test/datascript/test/query.cljc#L12-L36
(deftest datascript-test-unify
  (let [_tx (xt/submit-tx tu/*node*
                          [[:put-docs :docs {:xt/id 1, :name "Ivan", :age 15}]
                           [:put-docs :docs {:xt/id 2, :name "Petr", :age 37}]
                           [:put-docs :docs {:xt/id 3, :name "Ivan", :age 37}]
                           [:put-docs :docs {:xt/id 4, :age 15}]])]

    (t/is (= #{{:e 1, :v 15} {:e 3, :v 37}}
             (set (xt/q tu/*node*
                        '(from :docs [{:xt/id e, :name "Ivan", :age v}])))))

    (t/is (= #{{:e1 1, :e2 3} {:e1 3, :e2 1}}
             (set (xt/q tu/*node*
                        '(-> (unify (from :docs [{:xt/id e1, :name n}])
                                    (from :docs [{:xt/id e2, :name n}])
                                    (where (<> e1 e2)))
                             (without :n))))))


    (t/is (= #{{:e 1, :e2 1, :n "Ivan"}
               {:e 3, :e2 3, :n "Ivan"}
               {:e 3, :e2 2, :n "Petr"}
               {:e 1, :e2 4}}
             (set (xt/q tu/*node*
                        '(-> (unify (from :docs [{:xt/id e, :name "Ivan", :age a}])
                                    (from :docs [{:xt/id e2, :name n, :age a}]))
                             (without :a))))))

    (t/is (= #{{:e 1, :e2 1, :n "Ivan"}
               {:e 2, :e2 2, :n "Petr"}
               {:e 3, :e2 3, :n "Ivan"}}
             (set (xt/q tu/*node*
                        '(-> (unify (from :docs [{:xt/id e, :name n, :age a}])
                                    (from :docs [{:xt/id e2, :name n, :age a}]))
                             (without :a)))))
          "multi-param join")

    (t/is (= #{{:e1 1, :e2 1, :a1 15, :a2 15}
               {:e1 1, :e2 3, :a1 15, :a2 37}
               {:e1 3, :e2 1, :a1 37, :a2 15}
               {:e1 3, :e2 3, :a1 37, :a2 37}}
             (set (xt/q tu/*node*
                        '(unify (from :docs [{:xt/id e1, :name "Ivan", :age a1}])
                                (from :docs [{:xt/id e2, :name "Ivan", :age a2}])))))

          "cross join required here")))

(deftest test-namespaced-attributes
  (let [_tx (xt/submit-tx tu/*node* [[:put-docs :docs {:xt/id :foo :foo/bar 1}]
                                     [:put-docs :docs {:xt/id :bar :foo/bar 2}]])]
    (t/is (= [{:i :foo, :n 1} {:i :bar, :n 2}]
             (xt/q tu/*node*
                   '(from :docs [{:xt/id i :foo/bar n}])))
          "simple query with namespaced attributes")
    (t/is (= [{:i/i :foo, :n/n 1} {:i/i :bar, :n/n 2}]
             (xt/q tu/*node*
                   '(-> (from :docs [{:xt/id i :foo/bar n}])
                        (return {:i/i i :n/n n}))))
          "query with namespaced keys")
    (t/is (= [{:i :foo, :n 1 :foo/bar 1} {:i :bar, :n 2 :foo/bar 2}]
             (xt/q tu/*node*
                   '(from :docs [{:xt/id i :foo/bar n} foo/bar])))
          "query with namespaced attributes in match syntax")))

(deftest test-unify
  (xt/submit-tx tu/*node* bond/tx-ops)

  (t/is (= [{:bond :daniel-craig}]
           (xt/q tu/*node*
                 '(unify (from :person [{:xt/id bond, :person/name "Daniel Craig"}])))))

  ;;TODO Params rewritten using with clause, make sure params are tested elsewhere
  (t/is (= #{{:film-name "Skyfall", :bond-name "Daniel Craig"}}
           (set (xt/q tu/*node*
                      '(-> (unify (from :film [{:xt/id film, :film/name film-name, :film/bond bond}])
                                  (from :person [{:xt/id bond, :person/name bond-name}])
                                  (with {film :skyfall}))
                           (return film-name bond-name)))))
        "one -> one")


  (t/is (= #{{:film-name "Casino Royale", :bond-name "Daniel Craig"}
             {:film-name "Quantum of Solace", :bond-name "Daniel Craig"}
             {:film-name "Skyfall", :bond-name "Daniel Craig"}
             {:film-name "Spectre", :bond-name "Daniel Craig"}}
           (set (xt/q tu/*node*
                      '(-> (unify (from :film [{:film/name film-name, :film/bond bond}])
                                  (from :person [{:xt/id bond, :person/name bond-name}])
                                  (with {bond :daniel-craig}))
                           (return film-name bond-name)))))
        "one -> many"))

;; https://github.com/tonsky/datascript/blob/1.1.0/test/datascript/test/query_aggregates.cljc#L14-L39

(t/deftest datascript-test-aggregates
  (let [_tx (xt/submit-tx tu/*node*
                          [[:put-docs :docs {:xt/id :cerberus, :heads 3}]
                           [:put-docs :docs {:xt/id :medusa, :heads 1}]
                           [:put-docs :docs {:xt/id :cyclops, :heads 1}]
                           [:put-docs :docs {:xt/id :chimera, :heads 1}]])]
    (t/is (= #{{:heads 1, :count-heads 3} {:heads 3, :count-heads 1}}
             (set (xt/q tu/*node*
                        '(-> (from :docs [heads])
                             (aggregate heads {:count-heads (count heads)})))))
          "head frequency")

    (t/is (= #{{:sum-heads 6, :min-heads 1, :max-heads 3, :count-heads 4}}
             (set (xt/q tu/*node*
                        '(-> (from :docs [heads])
                             (aggregate {:sum-heads (sum heads)
                                         :min-heads (min heads)
                                         :max-heads (max heads)
                                         :count-heads (count heads)})))))
          "various aggs")))

(deftest test-clojure-case-symbols-in-expr
  (xt/submit-tx tu/*node* [[:put-docs :customers {:xt/id 0, :name "bob"}]
                           [:put-docs :customers {:xt/id 1, :name "alice"}]])

  (t/is (= [{:count 2}]
           (xt/q tu/*node*
                 '(-> (from :customers [xt/id])
                      (aggregate {:count (count xt/id)})))))

  (t/is (= [{:count 2}]
           (xt/q tu/*node*
                 '(-> (from :customers [{:xt/id my-funky/xt-id}])
                      (aggregate {:count (count my-funky/xt-id)}))))))


(t/deftest test-with-op
  (let [_tx (xt/submit-tx tu/*node* [[:put-docs :docs {:xt/id :o1, :unit-price 1.49, :quantity 4}]
                                     [:put-docs :docs {:xt/id :o2, :unit-price 5.39, :quantity 1}]
                                     [:put-docs :docs {:xt/id :o3, :unit-price 0.59, :quantity 7}]])]
    (t/is (= #{{:oid :o1, :o-value 5.96, :unit-price 1.49, :qty 4}
               {:oid :o2, :o-value 5.39, :unit-price 5.39, :qty 1}
               {:oid :o3, :o-value 4.13, :unit-price 0.59, :qty 7}}
             (set (xt/q tu/*node*
                        '(-> (from :docs [{:xt/id oid :unit-price unit-price :quantity qty}])
                             (with {:o-value (* unit-price qty)})))))))


  (t/testing "Duplicate vars"
    ;;Undefined behaviour
    (t/is (= [{:b 3}]
             (xt/q tu/*node*
                   '(-> (rel [{}] [])
                        (with {:b 2} {:b 3}))))))

  (t/testing "overwriting existing col"
    (t/is (= [{:a 2}]
             (xt/q tu/*node*
                   '(-> (rel [{:a 1}] [a])
                        (with {:a 2})))))))

(t/deftest test-with-op-errs
  (let [_tx (xt/submit-tx tu/*node* [[:put-docs :docs {:xt/id :foo}]])]
    (t/is (thrown-with-msg? IllegalArgumentException
                            #"Not all variables in expression are in scope"
                            (xt/q tu/*node*
                                  '(-> (from :docs [{:xt/id id}])
                                       (with {:bar (str baz)})))))))

(t/deftest test-with-unify-clause
  (t/testing "Duplicate vars unify"
    (t/is (= [{:a 1}]
             (xt/q tu/*node*
                   '(unify
                     (with {a 1} {a 1})))))
    (t/is (= []
             (xt/q tu/*node*
                   '(unify
                     (with {b 2} {b 3})))))))

(deftest test-aggregate-exprs
  (xt/submit-tx tu/*node* [[:put-docs :docs {:xt/id :foo, :category :c0, :v 1}]
                           [:put-docs :docs {:xt/id :bar, :category :c0, :v 2}]
                           [:put-docs :docs {:xt/id :baz, :category :c1, :v 4}]])

  (t/is (= [{:category :c0, :sum-doubles 6}
            {:category :c1, :sum-doubles 8}]
           (xt/q tu/*node*
                 '(-> (from :docs [{:category category :v v}])
                      (aggregate category {:sum-doubles (sum (* 2 v))})))))

  (t/is (= [{:cat :c0, :sum-doubles 12}
            {:cat :c1, :sum-doubles 16}]
           (xt/q tu/*node*
                 '(-> (from :docs [{:category category :v v}])
                      (aggregate {:cat category} {:sum-doubles (* 2 (sum (* 2 v)))})))))

  (t/is (= [{:category :c0, :sum-doubles 72}
            {:category :c1, :sum-doubles 128}]
           (xt/q tu/*node*
                 '(-> (from :docs [{:category category :v v}])
                      (aggregate category {:sum-doubles (* 2 (*
                                                              (* 2 (sum v))
                                                              (sum (* 2 v))))})))))

  (t/is (= [{:category :c0, :any-2 true}
            {:category :c1, :any-2 false}]
           (xt/q tu/*node*
                 '(-> (from :docs [{:category category :v v}])
                      (aggregate category {:any-2 (bool-or (= v 2))}))))
    "bool agg")

  (t/is (= [{:v 1, :v-double 2} {:v 4, :v-double 8} {:v 2, :v-double 4}]
           (xt/q tu/*node*
                 '(-> (from :docs [{:category category :v v}])
                      (aggregate v {:v-double (* 2 v)}))))
        "non-agg expression over grouping col")

  (t/is (= [{:x 0, :sum-y 0, :sum-expr 1}
            {:x 1, :sum-y 1, :sum-expr 5}
            {:x 2, :sum-y 3, :sum-expr 14}
            {:x 3, :sum-y 6, :sum-expr 30}]
           (xt/q tu/*node*
                  '(-> (rel $in [x y])
                       (aggregate x {:sum-y (sum y)
                                     :sum-expr (sum (+ (* y y) x 1))}))
                  {:args {:in (for [x (range 4)
                                    y (range (inc x))]
                                {:x x :y y})}})))

  (t/is (= [{:sum-evens 20}]

           (xt/q tu/*node*
                 '(-> (rel $in [x])
                      (aggregate {:sum-evens (sum (if (= 0 (mod x 2)) x 0))}))
                 {:args {:in (map (partial hash-map :x) (range 10))}}))
        "if")

  (t/testing "stddev aggregate"
    (t/is (= [{:y 23.53720459187964}]
             (xt/q tu/*node*
                   '(-> (rel [{:x 10} {:x 15} {:x 20} {:x 35} {:x 75}] [x])
                        (aggregate {:y (stddev-pop x)}))))))

  (t/is (= [{:out 28.5}]
           (xt/q tu/*node*
                  '(-> (rel $in [x])
                       (aggregate {:out (/ (double (sum (* x x)))
                                           (count x))}))
                  {:args {:in (map (partial hash-map :x) (range 10))}}))
        "aggregates can be included in exprs")

  ;TODO currently we require explicit groupings, however detecting the error case of
  ;variables referenced outside of agg-exprs is the same logic required to add them implicitly as
  ;grouping cols, so this can be easily done if desired.

  #_(t/testing "implicitly groups by variables present outside of aggregates"
      (t/is (= [{:x-div-y 1, :sum-z 2} {:x-div-y 2, :sum-z 3} {:x-div-y 2, :sum-z 5}]
               (xt/q tu/*node*
                     ['{:find [(/ x y) (sum z)]
                        :keys [x-div-y sum-z]
                        :in [[[x y z]]]}
                      [[1 1 2]
                       [2 1 3]
                       [4 2 5]]]))
            "even though (/ x y) yields the same result in the latter two rows, we group by them individually")

      (t/is (= #{[1 3] [1 7] [4 -1]}
               (xt/q tu/*node*
                     ['{:find [x (- (sum z) y)]
                        :in [[[x y z]]]}
                      [[1 1 4]
                       [1 3 2]
                       [1 3 8]
                       [4 6 5]]]))
            "groups by x and y in this case"))


  (t/testing "Error Cases"

    (t/is (thrown-with-msg? IllegalArgumentException
                            #"Variables outside of aggregate expressions must be grouping columns"
                            (xt/q tu/*node*
                                  '(-> (from :docs [{:category category :v v}])
                                       (aggregate category {:sum-doubles (* v (sum v))})))))

    (t/is (thrown-with-msg? IllegalArgumentException
                            #"Aggregate functions cannot be nested"
                            (xt/q tu/*node*
                                  '(-> (from :docs [v])
                                       (aggregate {:sum-doubles (sum (count v))})))))

    (t/is (thrown-with-msg? IllegalArgumentException
                            #"Not all variables in expression are in scope"
                            (xt/q tu/*node*
                                  '(-> (from :docs [v])
                                       (aggregate {:sum-doubles (sum y)})))))

    (t/is (thrown-with-msg? IllegalArgumentException
                            #"Not all variables in expression are in scope"
                            (xt/q tu/*node*
                                  '(-> (from :docs [v])
                                       (aggregate y {:sum-doubles (+ y (sum v))})))))

    (t/is (thrown-with-msg? IllegalArgumentException
                            #"Not all variables in expression are in scope"
                            (xt/q tu/*node*
                                  '(-> (from :docs [v])
                                       (aggregate y)))))

    (t/is (thrown-with-msg? IllegalArgumentException
                            #"Not all variables in expression are in scope"
                            (xt/q tu/*node*
                                  '(-> (from :docs [v])
                                       (aggregate {:sum-doubles (+ y (sum v))})))))))

(deftest test-composite-value-bindings
  (xt/submit-tx tu/*node* [[:put-docs :docs {:xt/id 1 :map {:foo 1} :set #{1 2 3}}]])

  (t/is (= [{:xt/id 1}]
           (xt/q tu/*node* '(from :docs [xt/id {:map {:foo 1}}]))))

  #_ ; TODO `=` on sets
  (t/is (= [{:xt/id 1}]
           (xt/q tu/*node* '(from :docs [xt/id {:set #{1 2 3}}]))))

  (t/is (= [{:xt/id 1}]
           (xt/q tu/*node* '(from :docs [xt/id {:map {:foo $arg}}])
                 {:args {:arg 1}})))

  #_ ; TODO `=` on sets
  (t/is (= [{:xt/id 1}]
           (xt/q tu/*node* '(from :docs [xt/id {:set $arg}])
                 {:args {:arg #{1 2 3}}}))))

(deftest test-query-args
  (let [_tx (xt/submit-tx tu/*node* ivan+petr)]
    (t/is (= #{{:e :ivan}}
             (set (xt/q tu/*node*
                        '(from :docs [{:xt/id e :first-name $name}])
                        {:args {:name "Ivan"}})))

          "param in from")

    (t/is (= #{{:e :petr :name "Petr"}}
             (set (xt/q tu/*node*
                        '(-> (from :docs [{:xt/id e :first-name name}])
                             (where (= $name name)))
                        {:args {:name "Petr"}})))
          "param in where op")

    (t/is (= #{{:e :petr :name "Petr" :baz "PETR"}
               {:e :ivan :name "Ivan" :baz "PETR"}}
             (set (xt/q tu/*node*
                        '(unify (from :docs [{:xt/id e :first-name name}])
                                (with {baz (upper $name)}))
                        {:args {:name "Petr"}})))
          "param in unify with")))

(deftest test-subquery-args-and-unification
  (let [_tx (xt/submit-tx tu/*node* ivan+petr)]
    (t/is (= #{{:name "Ivan" :last-name "Ivanov"}}
             (set (xt/q tu/*node*
                        '(unify
                          (with {name "Ivan"})
                          (join (from :docs [{:xt/id e
                                              :first-name $name
                                              :last-name last-name}])
                                {:args [name]
                                 :bind [last-name]})))))

          "single subquery")

    (t/is (= #{{:name "Ivan" :last-name "Ivanov"}}
             (set (xt/q tu/*node*
                        '(unify
                          (with {name "Ivan"})
                          (join (from :docs [{:xt/id e
                                              :first-name $name
                                              :last-name last-name}])
                                {:args [name]
                                 :bind [last-name]})

                          (join (from :docs [{:xt/id e
                                              :first-name $f-name
                                              :last-name last-name}])
                                {:args [{:f-name name}]
                                 :bind [last-name]})))))

          "multiple subqueries with unique args but same bind")))

(deftest test-where-op
  (let [_tx (xt/submit-tx tu/*node* ivan+petr)]
    (t/is (= #{{:first-name "Ivan", :last-name "Ivanov"}}
             (set (xt/q tu/*node*
                        '(-> (from :docs [{:first-name first-name :last-name last-name}])
                             (where (< first-name "James")))))))

    (t/is (= #{{:first-name "Ivan", :last-name "Ivanov"}}
             (set (xt/q tu/*node*
                        '(-> (from :docs [{:first-name first-name :last-name last-name}])
                             (where (<= first-name "Ivan")))))))

    (t/is (empty? (xt/q tu/*node*
                        '(-> (from :docs [{:first-name first-name :last-name last-name}])
                             (where (<= first-name "Ivan")
                                    (> last-name "Ivanov"))))))

    (t/is (empty (xt/q tu/*node*
                       '(-> (from :docs [{:first-name first-name :last-name last-name}])
                            (where (< first-name "Ivan"))))))))


(deftest test-left-join
  (xt/submit-tx tu/*node*
                [[:put-docs :docs {:xt/id :ivan, :name "Ivan"}]
                 [:put-docs :docs {:xt/id :petr, :name "Petr", :parent :ivan}]
                 [:put-docs :docs {:xt/id :sergei, :name "Sergei", :parent :petr}]
                 [:put-docs :docs {:xt/id :jeff, :name "Jeff", :parent :petr}]])

  (t/is (= #{{:e :ivan, :c :petr}
             {:e :petr, :c :sergei}
             {:e :petr, :c :jeff}
             {:e :sergei}
             {:e :jeff}}
           (set (xt/q tu/*node*
                      '(unify (from :docs [{:xt/id e}])
                              (left-join (from :docs [{:xt/id c :parent e}])
                                         [c e])))))

        "independent: find people who have children")

  (t/is (= #{{:e :ivan}
             {:e :petr}
             {:e :sergei, :s :jeff}
             {:e :jeff, :s :sergei}}
           (set (xt/q tu/*node*
                      '(-> (unify (from :docs [{:xt/id e, :name name, :parent p}])
                                  (left-join (-> (from :docs [{:xt/id s, :parent p}])
                                                 (where (<> $e s)))
                                             {:args [e]
                                              :bind [s p]}))
                           (return e s)))))
        "dependent: find people who have siblings")

  (t/is (thrown-with-msg? IllegalArgumentException
                          #"Not all variables in expression are in scope"
                          (xt/q tu/*node*
                                '(unify (from :docs [{:xt/id e :foo n}])
                                        (left-join
                                         (-> (from :docs [{:xt/id e :first-name "Petr"}])
                                             (where (= n 1)))
                                         [e]))))))

(deftest test-exists
  (let [_tx (xt/submit-tx tu/*node*
                          [[:put-docs :docs {:xt/id :ivan, :name "Ivan"}]
                           [:put-docs :docs {:xt/id :petr, :name "Petr", :parent :ivan}]
                           [:put-docs :docs {:xt/id :sergei, :name "Sergei", :parent :petr}]
                           [:put-docs :docs {:xt/id :jeff, :name "Jeff", :parent :petr}]])]

    (t/is (= #{{:x true}}
             (set (xt/q tu/*node*

                        '(unify
                          (with {x (exists? (from :docs [{:xt/id :ivan}]))})))))
          "uncorrelated, contrived and rarely useful")

    (t/is (= #{{:e :ivan} {:e :petr}}
             (set (xt/q tu/*node*
                        '(-> (from :docs [{:xt/id e}])
                             (where (exists? (from :docs [{:parent $e}])
                                             {:args [e]}))))))

          "find people who have children")

    (t/is (= #{{:e :sergei} {:e :jeff}}
             (set (xt/q tu/*node*
                        '(-> (from :docs [{:xt/id e} parent])
                             (where (exists? (-> (from :docs [{:xt/id s, :parent $parent}])
                                                 (where (<> $e s)))
                                             {:args [e parent]}))
                             (without :parent)))))
          "find people who have siblings")))


(deftest test-not-exists
  (let [_tx (xt/submit-tx
             tu/*node*
             [[:put-docs :docs {:xt/id :ivan, :first-name "Ivan", :last-name "Ivanov" :foo 1}]
              [:put-docs :docs {:xt/id :petr, :first-name "Petr", :last-name "Petrov" :foo 1}]
              [:put-docs :docs {:xt/id :sergei :first-name "Sergei" :last-name "Sergei" :foo 1}]])]

    (t/is (= #{{:e :ivan} {:e :sergei}}
             (set (xt/q tu/*node*
                        '(-> (from :docs [{:xt/id e :foo 1}])
                             (where (not (exists? (from :docs [{:xt/id $e} {:first-name "Petr"}])
                                                  {:args [e]}))))))))

    (t/is (= []
             (xt/q tu/*node*
                   '(-> (from :docs [{:xt/id e :foo n}])
                        (where (not (exists? (from :docs [{:xt/id $e} {:foo $n}])
                                             {:args [e n]})))))))

    (t/is (= #{{:n 1, :e :ivan} {:n 1, :e :sergei}}
             (set (xt/q tu/*node*
                        '(-> (from :docs [{:xt/id e :foo n}])
                             (where (not (exists? (-> (from :docs [{:xt/id $e :first-name "Petr"}])
                                                      (where (= $n 1)))
                                                  {:args [e n]}))))))))

    (t/is (= #{{:n 1, :e :sergei, :not-exist true}
               {:n 1, :e :petr, :not-exist true}
               {:n 1, :e :ivan, :not-exist true}}
             (set (xt/q tu/*node*
                        '(-> (from :docs [{:xt/id e :foo n}])
                             (with {:not-exist (not (not (exists? (-> (rel [{}] [])
                                                                      (where (= $n 1)))
                                                                  {:args [n]})))}))))))

    (t/is (= #{{:n "Petr", :e :petr} {:n "Sergei", :e :sergei}}
             (set (xt/q tu/*node*
                        '(-> (from :docs [{:xt/id e, :first-name n}])
                             (where (not (exists? (-> (rel [{}] [])
                                                      (where (= "Ivan" $n)))
                                                  {:args [n]}))))))))


    (t/is (= #{{:n "Petr", :e :petr} {:n "Sergei", :e :sergei}}
             (set (xt/q tu/*node*
                        '(unify
                          (from :docs [{:xt/id e :first-name n}])
                          (where (not (exists? (from :docs [{:first-name "Ivan"} {:first-name $n}]) {:args [n]}))))))))

    (t/is (= [{:first-name "Petr", :e :petr}]
             (xt/q tu/*node*
                   '(unify
                     (from :docs [{:xt/id e} first-name])
                     (where (not (exists?
                                  (-> (rel [{}] [])
                                      (where (= $first-name "Ivan")))
                                  {:args [first-name]}))
                            (not (exists?
                                  (from :docs [{:xt/id $e :first-name "Sergei"}])
                                  {:args [e]}))))))
          "Multiple not exists")))

(deftest testing-unify-with
  (let [_tx (xt/submit-tx tu/*node*
                          [[:put-docs :docs {:xt/id :ivan, :age 15}]
                           [:put-docs :docs {:xt/id :petr, :age 22}]
                           [:put-docs :docs {:xt/id :slava, :age 37}]])]

    (t/is (= #{{:e1 :petr, :e2 :ivan, :e3 :slava}
               {:e1 :ivan, :e2 :petr, :e3 :slava}}
             (set
              (xt/q tu/*node*
                    '(-> (unify
                          (from :docs [{:xt/id e1 :age a1}])
                          (from :docs [{:xt/id e2 :age a2}])
                          (from :docs [{:xt/id e3 :age a3}])
                          (with {a4 (+ a1 a2)})
                          (where (= a4 a3)))
                         (return e1 e2 e3))))))

    (t/is (= #{{:e1 :petr, :e2 :ivan, :e3 :slava}
               {:e1 :ivan, :e2 :petr, :e3 :slava}}
             (set
              (xt/q tu/*node*
                    '(-> (unify
                          (from :docs [{:xt/id e1 :age a1}])
                          (from :docs [{:xt/id e2 :age a2}])
                          (from :docs [{:xt/id e3 :age a3}])
                          (with {a3 (+ a1 a2)}))
                         (return e1 e2 e3))))))))


(deftest test-namespaced-columns-in-from
  (xt/submit-tx tu/*node* [[:put-docs :docs {:xt/id :ivan}]])
  (t/is (= [{:xt/id :ivan}]
           (xt/q tu/*node* '(from :docs [xt/id]))))
  (t/is (= [{:id :ivan}]
           (xt/q tu/*node* '(from :docs [{:xt/id id}])))))

#_
(deftest test-nested-expressions-581
  (let [_tx (xt/submit-tx tu/*node*
                          [[:put-docs :docs {:xt/id :ivan, :age 15}]
                           [:put-docs :docs {:xt/id :petr, :age 22, :height 240, :parent 1}]
                           [:put-docs :docs {:xt/id :slava, :age 37, :parent 2}]])]

    (t/is (= #{{:e1 :ivan, :e2 :petr, :e3 :slava}
               {:e1 :petr, :e2 :ivan, :e3 :slava}}
             (set (xt/q tu/*node*
                        '{:find [e1 e2 e3]
                          :where [(match :docs {:xt/id e1})
                                  (match :docs {:xt/id e2})
                                  (match :docs {:xt/id e3})
                                  [e1 :age a1]
                                  [e2 :age a2]
                                  [e3 :age a3]
                                  [(= (+ a1 a2) a3)]]}))))

    (t/is (= [{:a1 15, :a2 22, :a3 37, :sum-ages 74, :inc-sum-ages 75}]
             (xt/q tu/*node*
                   '{:find [a1 a2 a3 sum-ages inc-sum-ages]
                     :where [(match :docs {:xt/id :ivan})
                             (match :docs {:xt/id :petr})
                             (match :docs {:xt/id :slava})
                             [:ivan :age a1]
                             [:petr :age a2]
                             [:slava :age a3]
                             [(+ (+ a1 a2) a3) sum-ages]
                             [(+ a1 (+ a2 a3 1)) inc-sum-ages]]})))

    (t/testing "unifies results of two calls"
      (t/is (= [{:a1 15, :a2 22, :a3 37, :sum-ages 74}]
               (xt/q tu/*node*
                     '{:find [a1 a2 a3 sum-ages]
                       :where [(match :docs {:xt/id :ivan})
                               (match :docs {:xt/id :petr})
                               (match :docs {:xt/id :slava})
                               [:ivan :age a1]
                               [:petr :age a2]
                               [:slava :age a3]
                               [(+ (+ a1 a2) a3) sum-ages]
                               [(+ a1 (+ a2 a3)) sum-ages]]})))

      (t/is (= []
               (xt/q tu/*node*
                     '{:find [a1 a2 a3 sum-ages]
                       :where [(match :docs {:xt/id :ivan})
                               (match :docs {:xt/id :petr})
                               (match :docs {:xt/id :slava})
                               [:ivan :age a1]
                               [:petr :age a2]
                               [:slava :age a3]
                               [(+ (+ a1 a2) a3) sum-ages]
                               [(+ a1 (+ a2 a3 1)) sum-ages]]}))))))

(deftest test-join-clause
  (xt/submit-tx tu/*node* bond/tx-ops)

  (t/is (= [{:bond-name "Roger Moore", :film-name "A View to a Kill"}
            {:bond-name "Roger Moore", :film-name "For Your Eyes Only"}
            {:bond-name "Roger Moore", :film-name "Live and Let Die"}
            {:bond-name "Roger Moore", :film-name "Moonraker"}
            {:bond-name "Roger Moore", :film-name "Octopussy"}
            {:bond-name "Roger Moore", :film-name "The Man with the Golden Gun"}
            {:bond-name "Roger Moore", :film-name "The Spy Who Loved Me"}]
           (xt/q tu/*node*
                 '(-> (unify (join (-> (unify (from :film [{:film/bond bond}])
                                              (from :person [{:xt/id bond, :person/name bond-name}]))
                                       (aggregate bond-name bond {:film-count (count bond)})
                                       (order-by {:val film-count :dir :desc} bond-name)
                                       (limit 1))
                                   [{:bond bond-with-most-films
                                     :bond-name bond-name}])
                             (from :film [{:film/bond bond-with-most-films
                                           :film/name film-name}]))
                      (order-by film-name)
                      (return bond-name film-name))))
        "films made by the Bond with the most films"))

(deftest test-join-clause-unification
  (xt/submit-tx tu/*node* [[:put-docs :a {:xt/id :a1, :a 2 :b 1}]
                           [:put-docs :a {:xt/id :a2, :a 2 :b 3}]
                           [:put-docs :a {:xt/id :a3, :a 2 :b 0}]])
  (t/is (= [{:aid :a2 :a 2 :b 3}]
           (xt/q tu/*node*
                 '(unify (from :a [{:xt/id aid} a b])
                         (join (rel [{:b (+ $a 1)}] [b])
                               {:args [a]
                                :bind [b]}))))
        "b is unified"))

(t/deftest test-explicit-unnest-574
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
                 '(-> (unify (from :vehicle [{:xt/id vehicle, :vehicle/brand brand, :vehicle/model model}])
                             (from :film [{:xt/id $film, :film/vehicles vehicles}])
                             (unnest {vehicle vehicles}))
                      (order-by brand model)
                      (return brand model))
                 {:args {:film :spectre}}))))


(t/deftest bug-non-string-table-names-599
  (with-open [node (xtn/start-node (merge tu/*node-opts* {:indexer {:rows-per-block 1000}}))]
    (letfn [(submit-ops! [ids]
              (last (for [tx-ops (->> (for [id ids]
                                        [:put-docs :t1 {:xt/id id,
                                                        :data (str "data" id)
                                                        }])
                                      (partition-all 20))]
                      (xt/submit-tx node tx-ops))))

            (count-table [_tx]
              (-> (xt/q node '(-> (from :t1 [{:xt/id id}])
                                  (aggregate {:id-count (count id)})))
                  (first)
                  (:id-count)))]

      (let [tx (submit-ops! (range 80))]
        (t/is (= 80 (count-table tx))))

      (let [tx (submit-ops! (range 80 160))]
        (t/is (= 160 (count-table tx)))))))

(t/deftest bug-dont-throw-on-non-existing-column-597
  (with-open [node (xtn/start-node (merge tu/*node-opts* {:indexer {:rows-per-block 1000}}))]
    (letfn [(submit-ops! [ids]
              (last (for [tx-ops (->> (for [id ids]
                                        [:put-docs :t1 {:xt/id id,
                                                        :data (str "data" id)}])
                                      (partition-all 20))]
                      (xt/submit-tx node tx-ops))))]

      (xt/submit-tx node [[:put-docs :docs {:xt/id 0 :foo :bar}]])
      (submit-ops! (range 1010))

      (t/is (= 1010 (-> (xt/q node
                              '(-> (from :t1 [{:xt/id id}])
                                   (aggregate {:id-count (count id)})))
                        (first)
                        (:id-count))))

      (t/is (= [{:xt/id 0}]
               (xt/q node '(from :docs [xt/id some-attr])))))))


(t/deftest add-better-metadata-support-for-keywords
  (with-open [node (xtn/start-node (merge tu/*node-opts* {:indexer {:rows-per-block 1000}}))]
    (letfn [(submit-ops! [ids]
              (last (for [tx-ops (->> (for [id ids]
                                        [:put-docs :t1 {:xt/id id,
                                                        :data (str "data" id)}])
                                      (partition-all 20))]
                      (xt/submit-tx node tx-ops))))]
      (xt/submit-tx node [[:put-docs :docs {:xt/id :some-doc}]])
      ;; going over the block boundary
      (submit-ops! (range 200))

      (t/is (= [{:xt/id :some-doc}]
               (xt/q node '(-> (from :docs [xt/id])
                               (where (= xt/id :some-doc)))))))))

#_
(deftest test-basic-rules
  (xt/submit-tx tu/*node* [[:put-docs :docs {:xt/id :ivan :name "Ivan" :last-name "Ivanov" :age 21}]
                           [:put-docs :docs {:xt/id :petr :name "Petr" :last-name "Petrov" :age 18}]
                           [:put-docs :docs {:xt/id :georgy :name "Georgy" :last-name "George" :age 17}]])
  (letfn [(q [query & args]
            (apply xt/q tu/*node* query args))]

    (t/testing "without rule"
      (t/is (= [{:i :ivan}] (q '{:find [i]
                                 :where [(match :docs {:xt/id i})
                                         [i :age age]
                                         [(>= age 21)]]}))))

    (t/testing "empty rules"
      (t/is (= [{:i :ivan}] (q '{:find [i]
                                 :where [(match :docs {:xt/id i})
                                         [i :age age]
                                         [(>= age 21)]]
                                 :rules []}))))

    (t/testing "rule using required bound args"
      (t/is (= [{:i :ivan}] (q '{:find [i]
                                 :where [(match :docs {:xt/id i})
                                         [i :age age]
                                         (over-twenty-one? age)]
                                 :rules [[(over-twenty-one? age)
                                          [(>= age 21)]]]}))))

    (t/testing "rule using required bound args (different arg names)"
      (t/is (= [{:i :ivan}] (q '{:find [i]
                                 :where [(match :docs {:xt/id i})
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
                                 (match :docs {:xt/id i})
                                 [i :age age]]
                         :rules [[(older-users age u)
                                  (match :docs {:xt/id u})
                                  [u :age age2]
                                  [(> age2 age)]]]})))))

    (t/testing "testing rule with multiple args (different arg names in rule)"
      (t/is (= #{{:i :petr, :age 18, :u :ivan}
                 {:i :georgy, :age 17, :u :ivan}
                 {:i :georgy, :age 17, :u :petr}}
               (set (q '{:find [i age u]
                         :where [(older-users age u)
                                 (match :docs {:xt/id i})
                                 [i :age age]]
                         :rules [[(older-users age-other u-other)
                                  (match :docs {:xt/id u-other})
                                  [u-other :age age2]
                                  [(> age2 age-other)]]]})))))


    (t/testing "nested rules"
      (t/is (= [{:i :ivan}] (q '{:find [i]
                                 :where [(match :docs {:xt/id i})
                                         [i :age age]
                                         (over-twenty-one? age)]
                                 :rules [[(over-twenty-one? x)
                                          (over-twenty-one-internal? x)]
                                         [(over-twenty-one-internal? y)
                                          [(>= y 21)]]]}))))

    (t/testing "nested rules bound (same arg names)"
      (t/is (= [{:i :ivan}] (q '{:find [i]
                                 :where [(match :docs {:xt/id i})
                                         [i :age age]
                                         (over-twenty-one? age)]
                                 :rules [[(over-twenty-one? age)
                                          (over-twenty-one-internal? age)]
                                         [(over-twenty-one-internal? age)
                                          [(>= age 21)]]]}))))

    (t/is (= [{:i :ivan}] (q '{:find [i]
                               :where [(match :docs {:xt/id i})
                                       [i :age age]
                                       (over-twenty-one? age)]
                               :rules [[(over-twenty-one? x)
                                        (over-twenty-one-internal? x)]
                                       [(over-twenty-one-internal? y)
                                        [(>= y 21)]]]})))

    (t/testing "rule using literal arguments"
      (t/is (= [{:i :ivan}] (q '{:find [i]
                                 :where [(match :docs {:xt/id i})
                                         [i :age age]
                                         (over-age? age 21)]
                                 :rules [[(over-age? age required-age)
                                          [(>= age required-age)]]]}))))

    (t/testing "same arg-name different position test (shadowing test)"
      (t/is (= [{:i :ivan}] (q '{:find [i]
                                 :where [(match :docs {:xt/id i})
                                         [i :age age]
                                         (over-age? age 21)]
                                 :rules [[(over-age? other-age age)
                                          [(>= other-age age)]]]}))))

    (t/testing "semi-join in rule"
      (t/is (= [{:i :petr, :age 18}
                {:i :georgy, :age 17}]
               (q '{:find [i age]
                    :where [(match :docs {:xt/id i})
                            [i :age age]
                            (older? age)]
                    :rules [[(older? age)
                             (exists? {:find []
                                       :in [age]
                                       :where [(match :docs {:xt/id i})
                                               [i :age age2]
                                               [(> age2 age)]]})]]}))))


    (t/testing "anti-join in rule"
      (t/is (= [{:i :ivan, :age 21}]
               (q  '{:find [i age]
                     :where [(match :docs {:xt/id i})
                             [i :age age]
                             (not-older? age)]
                     :rules [[(not-older? age)
                              (not-exists? {:find []
                                            :in [age]
                                            :where [(match :docs {:xt/id i})
                                                    [i :age age2]
                                                    [(> age2 age)]]})]]}))))

    (t/testing "subquery in rule"
      (t/is (= #{{:i :petr, :other-age 21}
                 {:i :georgy, :other-age 21}
                 {:i :georgy, :other-age 18}}
               (set (q '{:find [i other-age]
                         :where [(match :docs {:xt/id i})
                                 [i :age age]
                                 (older-ages age other-age)]
                         :rules [[(older-ages age other-age)
                                  (q {:find [other-age]
                                      :in [age]
                                      :where [(match :docs {:xt/id i})
                                              [i :age other-age]
                                              [(> other-age age)]]})]]})))))

    (t/testing "subquery in rule with aggregates, expressions and order-by"
      (t/is [{:i :ivan, :max-older-age nil, :max-older-age-times2 nil}
             {:i :petr, :max-older-age 21, :max-older-age-times2 42}
             {:i :georgy, :max-older-age 21, :max-older-age-times2 42}]
            (q '{:find [i max-older-age max-older-age-times2]
                 :where [(match :docs {:xt/id i})
                         [i :age age]
                         (older-ages age max-older-age max-older-age-times2)]
                 :rules [[(older-ages age max-older-age max-older-age2)
                          (q {:find [(max older-age) (max (* older-age 2))]
                              :keys [max-older-age max-older-age2]
                              :in [age]
                              :where [(match :docs {:xt/id i})
                                      [i :age older-age]
                                      [(> older-age age)]]
                              :order-by [[(max older-age) :desc]]})]]})))

    (t/testing "rule using multiple branches"
      (t/is (= [{:i :ivan}] (q '{:find [i]
                                 :where [(is-ivan-or-bob? i)]
                                 :rules [[(is-ivan-or-bob? i)
                                          (match :docs {:xt/id i})
                                          [i :name "Bob"]]
                                         [(is-ivan-or-bob? i)
                                          (match :docs {:xt/id i})
                                          [i :name "Ivan"]
                                          [i :last-name "Ivanov"]]]})))

      (t/is (= [{:name "Petr"}] (q '{:find [name]
                                     :where [(match :docs {:xt/id i})
                                             [i :name name]
                                             (not-exists? {:find [i]
                                                           :where [(is-ivan-or-georgy? i)]})]
                                     :rules [[(is-ivan-or-georgy? i)
                                              (match :docs {:xt/id i})
                                              [i :name "Ivan"]]
                                             [(is-ivan-or-georgy? i)
                                              (match :docs {:xt/id i})
                                              [i :name "Georgy"]]]})))

      (t/is (= [{:i :ivan}
                {:i :petr}]
               (q '{:find [i]
                    :where [(is-ivan-or-petr? i)]
                    :rules [[(is-ivan-or-petr? i)
                             (match :docs {:xt/id i})
                             [i :name "Ivan"]]
                            [(is-ivan-or-petr? i)
                             (match :docs {:xt/id i})
                             [i :name "Petr"]]]}))))

    (t/testing "union-join with rules"
      (t/is (= [{:i :ivan}]
               (q '{:find [i]
                    :where [(union-join [i]
                                        (is-ivan-or-bob? i))]
                    :rules [[(is-ivan-or-bob? i)
                             (match :docs {:xt/id i})
                             [i :name "Bob"]]
                            [(is-ivan-or-bob? i)
                             (match :docs {:xt/id i})
                             [i :name "Ivan"]
                             [i :last-name "Ivanov"]]]}))))


    (t/testing "subquery with rule"
      (t/is (= [{:i :ivan}]
               (q '{:find [i]
                    :where [(q {:find [i]
                                :where [(is-ivan-or-bob? i)]})]
                    :rules [[(is-ivan-or-bob? i)
                             (match :docs {:xt/id i})
                             [i :name "Bob"]]
                            [(is-ivan-or-bob? i)
                             (match :docs {:xt/id i})
                             [i :name "Ivan"]
                             [i :last-name "Ivanov"]]]})))))

  (t/is (thrown-with-msg?
         IllegalArgumentException
         #":unknown-rule"
         (xt/q tu/*node* '{:find [i]
                           :where [(match :docs {:xt/id i})
                                   [i :age age]
                                   (over-twenty-one? age)]})))

  (t/is (thrown-with-msg?
         IllegalArgumentException
         #":rule-wrong-arity"
         (xt/q tu/*node* '{:find [i]
                           :where [(match :docs {:xt/id i})
                                   [i :age age]
                                   (over-twenty-one? i age)]
                           :rules [[(over-twenty-one? x)
                                    [(>= x 21)]]]})))
  (t/is (thrown-with-msg?
         IllegalArgumentException
         #":rule-definitions-require-unique-arity"
         (xt/q tu/*node* '{:find [i]
                           :where [(match :docs {:xt/id i})
                                   [i :age age]
                                   (is-ivan-or-petr? i name)]
                           :rules [[(is-ivan-or-petr? i name)
                                    (match :docs {:xt/id i})
                                    [i :name "Ivan"]]
                                   [(is-ivan-or-petr? i)
                                    (match :docs {:xt/id i})
                                    [i :name "Petr"]]]}))))


(t/deftest test-temporal-opts
  (letfn [(q [query {:keys [system-time]} current-time]
            (xt/q tu/*node* query {:snapshot-time system-time, :current-time current-time}))]

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

    (let [tx0 (xt/execute-tx tu/*node* [[:put-docs {:into :docs, :valid-from #inst "2015"}
                                         {:xt/id :matthew}]
                                        [:put-docs {:into :docs, :valid-from #inst "2018", :valid-to #inst "2020"}
                                         {:xt/id :mark}]
                                        [:put-docs {:into :docs, :valid-from #inst "2021"}
                                         {:xt/id :luke}]])

          tx1 (xt/execute-tx tu/*node* [[:delete-docs {:from :docs, :valid-from #inst "2022"} :luke]
                                        [:put-docs {:into :docs, :valid-from #inst "2023", :valid-to #inst "2024"}
                                         {:xt/id :mark}]
                                        [:put-docs {:into :docs, :valid-from #inst "2016", :valid-to #inst "2020"}
                                         {:xt/id :john}]])]

      (t/is (= #{{:id :matthew}, {:id :mark}}
               (set (q '(from :docs [{:xt/id id}]), tx1, #inst "2023"))))

      (t/is (= #{{:id :matthew}, {:id :luke}}
               (set (q '(from :docs [{:xt/id id}]), tx1, #inst "2021")))
            "back in app-time")

      (t/is (= #{{:id :matthew}, {:id :luke}}
               (set (q '(from :docs [{:xt/id id}]), tx0, #inst "2023")))
            "back in system-time")

      (t/is (= #{{:id :matthew, :app-from (time/->zdt #inst "2015")}
                 {:id :mark, :app-from (time/->zdt #inst "2018"), :app-to (time/->zdt #inst "2020")}
                 {:id :luke, :app-from (time/->zdt #inst "2021"), :app-to (time/->zdt #inst "2022")}
                 {:id :mark, :app-from (time/->zdt #inst "2023"), :app-to (time/->zdt #inst "2024")}
                 {:id :john, :app-from (time/->zdt #inst "2016"), :app-to (time/->zdt #inst "2020")}}
               (set (q '(from :docs {:bind [{:xt/id id} {:xt/valid-from app-from
                                                         :xt/valid-to app-to}]
                                     :for-valid-time :all-time})
                       tx1, nil)))
            "entity history, all time")

      (t/is (= #{{:id :matthew, :app-from (time/->zdt #inst "2015")}
                 {:id :luke, :app-from (time/->zdt #inst "2021"), :app-to (time/->zdt #inst "2022")}}
               (set (q '(from :docs {:bind [{:xt/id id} {:xt/valid-from app-from
                                                         :xt/valid-to app-to}]
                                     :for-valid-time (in #inst "2021", #inst "2023")})
                       tx1, nil)))
            "entity history, range")

      (t/is (= #{{:id :matthew}}
               (set (q '(unify (from :docs {:bind [{:xt/id id}]
                                            :for-valid-time (at #inst "2018")})
                               (from :docs {:bind [{:xt/id id}]
                                            :for-valid-time (at #inst "2024")})),
                       tx1, nil)))
            "cross-time join - who was here in both 2018 and 2023?")

      (t/is (= #{{:vt-from (time/->zdt #inst "2021")
                  :vt-to (time/->zdt #inst "2022")
                  :tt-from (time/->zdt #inst "2020")}
                 {:vt-from (time/->zdt #inst "2022")
                  :tt-from (time/->zdt #inst "2020")
                  :tt-to (time/->zdt  #inst "2020-01-02")}}
               (set (q '(from :docs {:bind [{:xt/id :luke
                                             :xt/valid-from vt-from
                                             :xt/valid-to vt-to
                                             :xt/system-from tt-from
                                             :xt/system-to tt-to}]
                                     :for-valid-time :all-time
                                     :for-system-time :all-time})
                       tx1 nil)))

            "for all sys time")))

  (t/testing "testing temporal fns with params"
    (t/is (= #{{:id :matthew, :app-from (time/->zdt #inst "2015")}
               {:id :luke, :app-from (time/->zdt #inst "2021"), :app-to (time/->zdt #inst "2022")}}
             (set (xt/q tu/*node*
                        '(from :docs {:bind [{:xt/id id} {:xt/valid-from app-from
                                                          :xt/valid-to app-to}]
                                      :for-valid-time (in $arg-from $arg-to)})
                        {:args {:arg-from #inst "2021"
                                :arg-to #inst "2023"}}))))

    (t/is (= #{{:id :matthew}}
             (set (xt/q tu/*node*
                        '(unify (from :docs {:bind [{:xt/id id}]
                                             :for-valid-time (at $arg-t1)})
                                (from :docs {:bind [{:xt/id id}]
                                             :for-valid-time (at $arg-t2)}))
                        {:args {:arg-t1 #inst "2018"
                                :arg-t2 #inst "2024"}}))))

    (t/is (= #{{:id :matthew, :app-from (time/->zdt #inst "2015")}
               {:id :luke, :app-from (time/->zdt #inst "2021"), :app-to (time/->zdt #inst "2022")}}
             (set (xt/q tu/*node*
                        '(from :docs {:bind [{:xt/id id} {:xt/valid-from app-from
                                                          :xt/valid-to app-to}]
                                      :for-valid-time (in $arg-from $arg-to)})
                        {:args {:arg-from #inst "2021"
                                :arg-to #inst "2023"}}))))))

(t/deftest test-for-valid-time-with-current-time-2493
  (xt/submit-tx tu/*node* [[:put-docs {:into :docs, :valid-to #inst "2040"}
                            {:xt/id :matthew}]])
  (xt/submit-tx tu/*node* [[:put-docs {:into :docs, :valid-from #inst "2022", :valid-to #inst "2030"}
                            {:xt/id :matthew}]])
  (t/is (= #{{:id :matthew,
              :vt-from #xt/zoned-date-time "2030-01-01T00:00Z[UTC]",
              :vt-to #xt/zoned-date-time "2040-01-01T00:00Z[UTC]"}
             {:id :matthew,
              :vt-from #xt/zoned-date-time "2022-01-01T00:00Z[UTC]",
              :vt-to #xt/zoned-date-time "2030-01-01T00:00Z[UTC]"}}
           (set (xt/q tu/*node*
                      '(from :docs {:bind [{:xt/id id
                                            :xt/valid-from vt-from
                                            :xt/valid-to vt-to}]
                                    :for-valid-time (in nil #inst "2040")})
                      {:current-time (time/->instant #inst "2023")})))))

(t/deftest test-temporal-opts-from-and-to
  (letfn [(q [query {:keys [system-time]} current-time]
            (xt/q tu/*node* query
                  {:snapshot-time system-time, :current-time current-time}))]

    ;; tx0
    ;; 2015 - eof : Matthew
    ;; now - 2050 : Mark

    ;; tx1
    ;; now - 2040 : Matthew

    (let [tx0 (xt/execute-tx tu/*node* [[:put-docs {:into :docs, :valid-from #inst "2015"}
                                         {:xt/id :matthew}]
                                        [:put-docs {:into :docs, :valid-to #inst "2050"}
                                         {:xt/id :mark}]])
          tx1 (xt/execute-tx tu/*node* [[:put-docs {:into :docs, :valid-to #inst "2040"}
                                         {:xt/id :matthew}]])]
      (t/is (= #{{:id :matthew,
                  :vt-from #xt/zoned-date-time "2015-01-01T00:00Z[UTC]"}
                 {:id :mark,
                  :vt-from #xt/zoned-date-time "2020-01-01T00:00Z[UTC]",
                  :vt-to #xt/zoned-date-time "2050-01-01T00:00Z[UTC]"}}
               (set (q '(from :docs [{:xt/id id
                                      :xt/valid-from vt-from
                                      :xt/valid-to vt-to}]),
                       tx0, #inst "2023"))))

      (t/is (= [{:id :matthew,
                 :vt-from #xt/zoned-date-time "2015-01-01T00:00Z[UTC]"}]
               (q '(from :docs {:bind [{:xt/id id
                                        :xt/valid-from vt-from
                                        :xt/valid-to vt-to}]
                                :for-valid-time (from #inst "2051")}),
                  tx0, #inst "2023")))

      (t/is (= [{:id :mark,
                 :vt-from #xt/zoned-date-time "2020-01-01T00:00Z[UTC]",
                 :vt-to #xt/zoned-date-time "2050-01-01T00:00Z[UTC]"}
                {:id :matthew,
                 :vt-from #xt/zoned-date-time "2020-01-02T00:00Z[UTC]",
                 :vt-to #xt/zoned-date-time "2040-01-01T00:00Z[UTC]"}]
               (q '(from :docs {:bind [{:xt/id id
                                        :xt/valid-from vt-from
                                        :xt/valid-to vt-to}]
                                :for-valid-time (to #inst "2040")}),
                  tx1, #inst "2023"))))))

(deftest test-snodgrass-99-tutorial
  (letfn [(q [q {:keys [system-time]} current-time]
            (xt/q tu/*node* q
                  {:snapshot-time system-time, :current-time current-time}))
          (q-with-args [q args {:keys [system-time]} current-time]
            (xt/q tu/*node* q
                  {:args args, :snapshot-time system-time, :current-time current-time}))]

    (let [tx0 (xt/execute-tx tu/*node*
                             [[:put-docs {:into :docs, :valid-from #inst "1998-01-10"}
                               {:xt/id 1 :customer-number 145 :property-number 7797}]]
                             {:system-time #inst "1998-01-10"})

          tx1 (xt/execute-tx tu/*node*
                             [[:put-docs {:into :docs, :valid-from #inst "1998-01-15"}
                               {:xt/id 1 :customer-number 827 :property-number 7797}]]
                             {:system-time #inst "1998-01-15"})

          _tx2 (xt/execute-tx tu/*node*
                              [[:delete-docs {:from :docs, :valid-from #inst "1998-01-20"} 1]]
                              {:system-time #inst "1998-01-20"})

          _tx3 (xt/execute-tx tu/*node*
                              [[:put-docs {:into :docs, :valid-from #inst "1998-01-03", :valid-to #inst "1998-01-10"}
                                {:xt/id 1 :customer-number 145 :property-number 7797}]]
                              {:system-time #inst "1998-01-23"})

          _tx4 (xt/execute-tx tu/*node*
                              [[:delete-docs {:from :docs, :valid-from #inst "1998-01-03", :valid-to #inst "1998-01-05"} 1]]
                              {:system-time #inst "1998-01-26"})

          tx5 (xt/execute-tx tu/*node*
                             [[:put-docs {:into :docs, :valid-from #inst "1998-01-05", :valid-to #inst "1998-01-12"}
                               {:xt/id 1 :customer-number 145 :property-number 7797}]
                              [:put-docs {:into :docs, :valid-from #inst "1998-01-12", :valid-to #inst "1998-01-20"}
                               {:xt/id 1 :customer-number 827 :property-number 7797}]]
                             {:system-time #inst "1998-01-28"})

          tx6 (xt/execute-tx tu/*node*
                             [[:put-fn :delete-1-week-records,
                               '(fn delete-1-weeks-records []
                                  (->> (q '(-> (from :docs {:bind [{:xt/id id
                                                                    :xt/valid-from app-from
                                                                    :xt/valid-to app-to}]
                                                            :for-valid-time :all-time})
                                               (where (= (- #inst "1970-01-08" #inst "1970-01-01")
                                                         (- app-to app-from)))))
                                       (map (fn [{:keys [id app-from app-to]}]
                                              [:delete-docs {:from :docs, :valid-from app-from, :valid-to app-to}
                                               id]))))]
                              [:call :delete-1-week-records]]
                             {:system-time #inst "1998-01-30"})

          tx7 (xt/execute-tx tu/*node*
                             [[:put-docs {:into :docs, :valid-from #inst "1998-01-15"}
                               {:xt/id 2 :customer-number 827 :property-number 3621}]]
                             {:system-time #inst "1998-01-31"})]

      (t/is (= [{:cust 145 :app-from (time/->zdt #inst "1998-01-10")}]
               (q '(from :docs {:bind [{:customer-number cust, :xt/valid-from app-from}]
                                :for-valid-time :all-time})
                  tx0, nil))
            "as-of 14 Jan")

      (t/is (= #{{:cust 145, :app-from (time/->zdt #inst "1998-01-10")}
                 {:cust 827, :app-from (time/->zdt #inst "1998-01-15")}}
               (set (q '(from :docs {:bind [{:customer-number cust, :xt/valid-from app-from}]
                                     :for-valid-time :all-time})
                       tx1, nil)))
            "as-of 18 Jan")

      (t/is (= #{{:cust 145, :app-from (time/->zdt #inst "1998-01-05")}
                 {:cust 827, :app-from (time/->zdt #inst "1998-01-12")}}
               (set (q '(-> (from :docs {:bind [{:customer-number cust,
                                                 :xt/valid-from app-from}]
                                         :for-valid-time :all-time})
                            (order-by {:val app-from :dir :asc}))
                       tx5, nil)))
            "as-of 29 Jan")

      (t/is (= [{:cust 827, :app-from (time/->zdt #inst "1998-01-12"), :app-to (time/->zdt #inst "1998-01-20")}]
               (q '(-> (from :docs {:bind [{:customer-number cust,
                                            :xt/valid-from app-from
                                            :xt/valid-to app-to}]
                                    :for-valid-time :all-time})
                       (order-by {:val app-from :dir :asc}))
                  tx6, nil))
            "'as best known' (as-of 30 Jan)")

      (t/is (= [{:prop 3621, :vt-begin (time/->zdt #inst "1998-01-15"), :vt-to (time/->zdt #inst "1998-01-20")}]
               (q-with-args
                '(-> (unify (from :docs {:bind [{:property-number $prop
                                                 :customer-number cust
                                                 :xt/valid-time app-time
                                                 :xt/valid-from app-from
                                                 :xt/valid-to app-to}]
                                         :for-valid-time :all-time})

                            (from :docs {:bind [{:property-number prop
                                                 :customer-number cust
                                                 :xt/valid-time app-time-2
                                                 :xt/valid-from app-from2
                                                 :xt/valid-to app-to2}]
                                         :for-valid-time :all-time}))

                     (where (<> prop $prop)
                            (overlaps? app-time app-time-2))
                     (order-by {:val app-from :dir :asc})
                     (return prop
                             {:vt-begin (greatest app-from app-from2)}
                             {:vt-to (nullif (least (coalesce app-to xtdb/end-of-time) (coalesce app-to2 xtdb/end-of-time))
                                             xtdb/end-of-time)}))
                {:prop 7797}
                tx7, nil))
            "Case 2: Valid-time sequenced and transaction-time current")

      (t/is (= [{:prop 3621,
                 :vt-begin (time/->zdt #inst "1998-01-15"),
                 :vt-to (time/->zdt #inst "1998-01-20"),
                 :recorded-from (time/->zdt #inst "1998-01-31"),}]
               (q-with-args
                '(-> (unify (from :docs {:bind [{:property-number $prop
                                                 :customer-number cust
                                                 :xt/valid-time app-time
                                                 :xt/system-time system-time
                                                 :xt/valid-from app-from
                                                 :xt/valid-to app-to
                                                 :xt/system-from sys-from
                                                 :xt/system-to sys-to}]
                                         :for-valid-time :all-time
                                         :for-system-time :all-time})

                            (from :docs {:bind [{:customer-number cust
                                                 :property-number prop
                                                 :xt/valid-time app-time-2
                                                 :xt/system-time system-time-2
                                                 :xt/valid-from app-from2
                                                 :xt/valid-to app-to2
                                                 :xt/system-from sys-from2
                                                 :xt/system-to sys-to2}]

                                         :for-valid-time :all-time
                                         :for-system-time :all-time}))

                     (where (<> prop $prop)
                            (overlaps? app-time app-time-2)
                            (overlaps? system-time system-time-2))
                     (order-by {:val app-from :dir :asc})
                     (return prop {:vt-begin (greatest app-from app-from2)
                                   :vt-to (nullif (least (coalesce app-to xtdb/end-of-time) (coalesce app-to2 xtdb/end-of-time))
                                                  xtdb/end-of-time)
                                   :recorded-from (greatest sys-from sys-from2)
                                   :recorded-stop (nullif (least (coalesce sys-to xtdb/end-of-time) (coalesce sys-to2 xtdb/end-of-time))
                                                          xtdb/end-of-time)}))
                {:prop 7797}
                tx7, nil))
            "Case 5: Application-time sequenced and system-time sequenced")

      (t/is (= [{:prop 3621,
                 :vt-begin (time/->zdt #inst "1998-01-15"),
                 :vt-to (time/->zdt #inst "1998-01-20"),
                 :recorded-from (time/->zdt #inst "1998-01-31")}]
               (q-with-args
                '(-> (unify (from :docs {:bind [{:property-number $prop
                                                 :customer-number cust
                                                 :xt/valid-time app-time
                                                 :xt/system-time system-time
                                                 :xt/valid-from app-from
                                                 :xt/valid-to app-to
                                                 :xt/system-from sys-from
                                                 :xt/system-to sys-to}]
                                         :for-valid-time :all-time
                                         :for-system-time :all-time})

                            (from :docs {:bind [{:customer-number cust
                                                 :property-number prop
                                                 :xt/valid-time app-time-2
                                                 :xt/system-time system-time-2
                                                 :xt/valid-from app-from2
                                                 :xt/valid-to app-to2
                                                 :xt/system-from sys-from2
                                                 :xt/system-to sys-to2}]}))

                     (where (<> prop $prop)
                            (overlaps? app-time app-time-2)
                            (contains? system-time sys-from2))
                     (order-by {:val app-from :dir :asc})
                     (return prop
                             {:vt-begin (greatest app-from app-from2)
                              :vt-to (nullif (least (coalesce app-to xtdb/end-of-time) (coalesce app-to2 xtdb/end-of-time))
                                             xtdb/end-of-time)
                              :recorded-from sys-from2}))
                {:prop 7797}
                tx7, nil))
            "Case 8: Application-time sequenced and system-time nonsequenced"))))

(deftest scalar-sub-queries-test
  (xt/submit-tx tu/*node* [[:put-docs :customer {:xt/id 0, :firstname "bob", :lastname "smith"}]
                           [:put-docs :customer {:xt/id 1, :firstname "alice" :lastname "carrol"}]
                           [:put-docs :order {:xt/id 0, :customer 0, :items [{:sku "eggs", :qty 1}]}]
                           [:put-docs :order {:xt/id 1, :customer 0, :items [{:sku "cheese", :qty 3}]}]
                           [:put-docs :order {:xt/id 2, :customer 1, :items [{:sku "bread", :qty 1} {:sku "eggs", :qty 2}]}]])

  (t/are [q result] (= (into #{} result) (set (xt/q tu/*node* q)))

    '(unify (with {n-customers (q (-> (from :customer {:bind [{:xt/id id}]})
                                      (aggregate {:id-count (count id)})))}))
    [{:n-customers 2}]

    '(unify (with {n-customers (+ 1 (q (-> (from :customer {:bind [{:xt/id id}]})
                                           (aggregate {:id-count (count id)}))))}))
    [{:n-customers 3}]

    '(-> (from :customer [{:xt/id customer}])
         (with {:n-orders
                (q (-> (from :order [{:customer $customer, :xt/id order}])
                       (aggregate {:count (count order)}))
                   {:args [customer]})}))

    [{:customer 0, :n-orders 2}
     {:customer 1, :n-orders 1}]


    '(-> (from :customer [{:xt/id customer}])
         (with {:n-orders (q (-> (from :order [{:xt/id order, :customer $customer}])
                                 (aggregate {:n-orders (row-count)}))
                             {:args [customer]})

                :n-qty (q (-> (from :order [{:xt/id order, :customer $customer} items])
                              ;; TODO unnest relation (through rel or from)
                              (unnest {:item items})
                              (return {:qty (. item qty)})
                              (aggregate {:n-qty (sum qty)}))
                          {:args [customer]})}))
    [{:customer 0, :n-orders 2, :n-qty 4}
     {:customer 1, :n-orders 1, :n-qty 3}]

    ;; TODO subqs in rel
    #_#_
    '(rel [{:n-orders (q (-> (from :order [])
                             (aggregate {:n-orders (row-count)})))
            :n-qty (q (-> (from :order [items])
                          ;; TODO unnest relation (through rel or from)
                          (unnest {:item items})
                          (return {:qty (. item qty)})
                          (aggregate {:n-qty (sum qty)})))}]
          [n-orders n-qty])
    [{:n-orders 3, :n-qty 7}]

    '(-> (from :order {:bind [{:xt/id order :customer customer}]})
         (with {:firstname (q (from :customer {:bind [{:xt/id $customer, :firstname firstname}]})
                              {:args [customer]})})
         (without :customer))

    [{:order 0, :firstname "bob"}
     {:order 1, :firstname "bob"}
     {:order 2, :firstname "alice"}]


    '(-> (from :order {:bind [{:xt/id order :customer customer}]})
         (with {:firstname (q (from :customer {:bind [{:xt/id $customer, :firstname firstname2}]})
                              {:args [customer]})})
         (without :customer))

    [{:order 0, :firstname "bob"}
     {:order 1, :firstname "bob"}
     {:order 2, :firstname "alice"}]

    '(-> (from :order {:bind [{:xt/id order, :customer customer}]})
         (with {:fullname (concat
                           (q (from :customer {:bind [{:xt/id $customer, :firstname fn}]})
                              {:args [customer]})
                           " "
                           (q (from :customer {:bind [{:xt/id $customer, :lastname fn}]})
                              {:args [customer]}))})
         (without :customer))

    [{:order 0, :fullname "bob smith"}
     {:order 1, :fullname "bob smith"}
     {:order 2, :fullname "alice carrol"}]

    '(-> (from :order {:bind [{:xt/id order, :customer customer}]})
         (where (= (q (from :customer {:bind [{:xt/id $customer, :firstname fn}]})
                      {:args [customer]})
                   "bob"))
         (without :customer))

    [{:order 0}
     {:order 1}]

    '(-> (unify (from :order {:bind [{:xt/id order, :customer customer}]})
                (where (= (q (from :customer {:bind [{:xt/id $customer, :firstname fn}]})
                             {:args [customer]})
                          "bob")))
         (without :customer))

    [{:order 0}
     {:order 1}]

    '(unify
      (with {n (q (-> (from :customer {:bind [{:xt/id c}]})
                      (aggregate {:c-count (count c)})))}))
    [{:n 2}]


    '(unify
      (with {c (q (-> (from :customer {:bind [{:xt/id c}]})
                      (aggregate {:count (count c)})))
             o (q (-> (from :order {:bind [{:xt/id o}]})
                      (aggregate {:count (count o)})))}))
    [{:c 2, :o 3}]

    '(-> (from :customer {:bind [{:xt/id cid, :firstname "bob"}]})
         (with {:c (q (-> (from :customer {:bind [{:xt/id $cid} {:xt/id c}]})
                          (aggregate {:count (count c)}))
                      {:args [cid]})}
               {:o (q (-> (from :order {:bind [{:customer $cid} {:xt/id o}]})
                          (aggregate {:count (count o)}))
                      {:args [cid]})})
         (without :cid))

    [{:c 1, :o 2}])

  (t/testing "cardinality violation error"
    (t/is (thrown-with-msg? xtdb.RuntimeException #"cardinality violation"
                            (->> '(unify
                                   (with {first-name (q (from :customer [{:firstname firstname}]))}))
                                 (xt/q tu/*node*)))))

  (t/testing "multiple column error"
    (t/is (thrown-with-msg? xtdb.IllegalArgumentException #"Scalar subquery must only return a single column"
                            (->> '(unify
                                   (with
                                    {n-customers
                                     (q (from :customer [{:customer customer, :firstname firstname}]))}))
                                 (xt/q tu/*node*))))))

(deftest test-period-predicates

  (xt/submit-tx tu/*node* [[:put-docs {:into :docs, :valid-from #inst "2015", :valid-to #inst "2020"}
                            {:xt/id 1}]
                           [:put-docs {:into :xt_cats, :valid-from #inst "2016", :valid-to #inst "2018"}
                            {:xt/id 2}]])

  (t/is (= [{:xt/id 1, :id2 2,
             :docs-app-time (tu/->tstz-range #xt/zoned-date-time "2015-01-01T00:00Z"
                                             #xt/zoned-date-time "2020-01-01T00:00Z")
             :xt-cats-app-time (tu/->tstz-range #xt/zoned-date-time "2016-01-01T00:00Z"
                                                #xt/zoned-date-time "2018-01-01T00:00Z")}]
           (xt/q
            tu/*node*
            '(-> (unify (from :docs {:bind [xt/id {:xt/valid-time docs_app_time}]
                                     :for-valid-time :all-time})
                        (from :xt_cats {:bind [{:xt/valid-time xt_cats_app_time :xt/id id2}]
                                        :for-valid-time :all-time}))
                 (where (contains? docs_app_time xt_cats_app_time))))))

  (t/is (= [{:xt/id 1, :id2 2,
             :docs-sys-time (tu/->tstz-range #xt/zoned-date-time "2020-01-01T00:00Z" nil)
             :xt-cats-sys-time (tu/->tstz-range #xt/zoned-date-time "2020-01-01T00:00Z" nil)}]
           (xt/q
            tu/*node*
            '(unify (from :docs {:bind [xt/id {:xt/system-time docs_sys_time}]
                                 :for-valid-time :all-time
                                 :for-system-time :all-time})
                    (from :xt_cats {:bind [{:xt/system-time xt_cats_sys_time :xt/id id2}]
                                    :for-valid-time :all-time
                                    :for-system-time :all-time})
                    (where (equals? docs_sys_time xt_cats_sys_time)))))))


(deftest test-period-constructor

  (t/is (= [{:p1 (tu/->tstz-range #xt/zoned-date-time "2018-01-01T00:00Z"
                                  #xt/zoned-date-time "2022-01-01T00:00Z")}]
           (xt/q
            tu/*node*
            '(unify (with {p1 (period #inst "2018" #inst "2022")})))))

  (t/is (thrown-with-msg?
         RuntimeException
         #"From cannot be greater than to when constructing a period"
         (xt/q
          tu/*node*
          '(unify (with {p1 (period #inst "2022" #inst "2020")}))))))


(deftest test-period-and-temporal-col-projection
  (xt/execute-tx tu/*node* [[:put-docs {:into :docs, :valid-from #inst "2015", :valid-to #inst "2050"}
                            {:xt/id 1}]])


  (t/is (= [{:xt/id 1,
             :valid-time (tu/->tstz-range #xt/zoned-date-time "2015-01-01T00:00Z",
                                          #xt/zoned-date-time "2050-01-01T00:00Z",)
             :xt/valid-from #xt/zoned-date-time "2015-01-01T00:00Z[UTC]",
             :valid-to #xt/zoned-date-time "2050-01-01T00:00Z[UTC]"}]
           (xt/q
            tu/*node*
            '(from :docs {:bind [xt/id xt/valid-from
                                 {:xt/valid-time valid-time
                                  :xt/valid-to valid-to}]
                          :for-valid-time :all-time})))
        "projecting both period and underlying cols")

  (t/is (= [{:xt/id 1,
             :app-time (tu/->tstz-range #xt/zoned-date-time "2015-01-01T00:00Z"
                                        #xt/zoned-date-time "2050-01-01T00:00Z")
             :sys-time (tu/->tstz-range #xt/zoned-date-time "2020-01-01T00:00Z" nil)}]
           (xt/q
            tu/*node*
            '(from :docs {:bind [xt/id {:xt/valid-time app_time
                                        :xt/system-time sys_time}]
                          :for-valid-time :all-time
                          :for-system-time :all-time})))
        "projecting both app and system-time periods")

  (t/is (= [#:xt{:valid-time
                 (tu/->tstz-range #xt/zoned-date-time "2015-01-01T00:00Z"
                                  #xt/zoned-date-time "2050-01-01T00:00Z")}]
           (xt/q tu/*node* '(from :docs
                                  {:bind [id xt/valid-time]
                                   :for-valid-time :all-time})))
        "protecting temporal period in vector syntax")

  #_ ; FIXME period unification needs to handle nulls. be easier to fix this now we have real period types
  (t/is (= [{:xt/id 1
             :id2 1,
             :app-time (tu/->tstz-range #xt/zoned-date-time "2015-01-01T00:00Z"
                                        #xt/zoned-date-time "2050-01-01T00:00Z")
             :sys-time (tu/->tstz-range #xt/zoned-date-time "2020-01-01T00:00Z" nil)}]
           (xt/q tu/*node* '(unify (from :docs {:bind [xt/id {:xt/valid-time app-time
                                                              :xt/system-time sys-time}]
                                                :for-valid-time :all-time
                                                :for-system-time :all-time})
                                   (from :docs {:bind [{:xt/valid-time app-time
                                                        :xt/system-time sys-time
                                                        :xt/id id2}]
                                                :for-valid-time :all-time
                                                :for-system-time :all-time}))))
        "period unification")

  (xt/submit-tx tu/*node* [[:put-docs :docs {:xt/id 2}]])

  #_ ; FIXME period unification needs to handle nulls. be easier to fix this now we have real period types
  (t/is (= [{:xt/id 2,
             :time (tu/->tstz-range #xt/zoned-date-time "2020-01-02T00:00Z" nil)}]
           (xt/q tu/*node*
                 '(from :docs
                        {:bind [xt/id {:xt/valid-time time
                                       :xt/system-time time}]
                         :for-valid-time :all-time
                         :for-system-time :all-time})))
        "period unification within match")

  (xt/submit-tx tu/*node* [[:put-docs {:into :docs, :valid-from #inst "2015", :valid-to #inst "2050"}
                            {:xt/id 3 :c (tu/->tstz-range #inst "2015", #inst "2050")}]])

  (t/is (= [{:xt/id 3,
             :time (tu/->tstz-range #xt/zoned-date-time "2015-01-01T00:00Z"
                                    #xt/zoned-date-time "2050-01-01T00:00Z")}]
           (xt/q tu/*node*
                 '(from :docs {:bind [xt/id {:xt/valid-time time
                                            :c time}]
                               :for-valid-time :all-time
                               :for-system-time :all-time})))
        "period unification within match with user period column")

  #_ ; FIXME period unification needs to handle nulls. be easier to fix this once we have real period types
  (t/is (= [{:xt/id 1} {:xt/id 3}]
           (xt/q
            tu/*node*
            '(from :docs {:bind [xt/id {:xt/valid-time (period #inst "2015" #inst "2050")}]
                          :for-valid-time :all-time
                          :for-system-time :all-time})))
        "period column matching literal"))

(t/deftest test-sql-insert
  (xt/submit-tx tu/*node* [[:sql "INSERT INTO foo (_id) VALUES (0)"]])
  (t/is (= [{:xt/id 0}]
           (xt/q tu/*node*
                 '(from :foo [xt/id])))))


(t/deftest test-put
  (xt/submit-tx tu/*node* [[:put-docs :foo {:xt/id 0}]])
  (t/is (= [{:xt/id 0}]
           (xt/q tu/*node*
                 '(from :foo [xt/id])))))

(t/deftest test-metadata-filtering-for-time-data-607
  (with-open [node (xtn/start-node (merge tu/*node-opts* {:indexer {:rows-per-block 1}}))]
    (xt/submit-tx node [[:put-docs :docs {:xt/id 1 :from-date #xt/date "2000-01-01"}]
                        [:put-docs :docs {:xt/id 2 :from-date #xt/date "3000-01-01"}]])
    (t/is (= [{:id 1}]

             (xt/q node
                   '(-> (from :docs [{:xt/id id} from-date])
                        (where (>= from-date #inst "1500")
                               (< from-date #inst "2500"))
                        (return id)))))

    (xt/submit-tx node [[:put-docs :docs2 {:xt/id 1 :from-date #inst "2000-01-01"}]
                        [:put-docs :docs2 {:xt/id 2 :from-date #inst "3000-01-01"}]])
    (t/is (= [{:id 1}]
             (xt/q node
                   '(-> (from :docs2 [{:xt/id id} from-date])
                        (where (< from-date #xt/date "2500-01-01")
                               (< from-date #xt/date "2500-01-01"))
                        (return id)))))))

(t/deftest bug-non-namespaced-nested-keys-747
  (xt/submit-tx tu/*node* [[:put-docs :bar {:xt/id 1 :foo {:a/b "foo"}}]])
  (t/is (= [{:foo {:a/b "foo"}}]
           (xt/q tu/*node*
                 '(from :bar [foo])))))

#_ ;;TODO from-star
(t/deftest test-row-alias
  (let [docs [{:xt/id 42, :firstname "bob"}
              {:xt/id 43, :firstname "alice", :lastname "carrol"}
              {:xt/id 44, :firstname "jim", :orders [{:sku "eggs", :qty 2}, {:sku "cheese", :qty 1}]}]]
    (xt/submit-tx tu/*node* (map (partial vector :put-docs :customer) docs))
    (t/is (= (set (mapv (fn [doc] {:c doc}) docs))
             (set (xt/q tu/*node* '{:find [c] :where [($ :customer {:xt/* c})]}))))))

#_ ;;TODO from-star
(t/deftest test-row-alias-system-time-key-set
  (let [inputs
        [[{:xt/id 0, :a 0} #inst "2023-01-17T00:00:00"]
         [{:xt/id 0, :b 0} #inst "2023-01-18T00:00:00"]
         [{:xt/id 0, :c 0, :a 0} #inst "2023-01-19T00:00:00"]]

        _
        (doseq [[doc system-time] inputs]
          (xt/submit-tx tu/*node* [[:put-docs :x doc]] {:system-time system-time}))

        q (partial xt/q tu/*node*)]

    (t/is (= [{:x {:xt/id 0, :a 0, :c 0}}]
             (q '{:find [x]
                  :where [($ :x {:xt/* x})]})))

    (t/is (= [{:x {:xt/id 0, :b 0}}]
             (q '{:find [x]
                  :where [($ :x {:xt/* x})],}
                {:snapshot-time #inst "2023-01-18"})))

    (t/is (= [{:x {:xt/id 0, :a 0}}]
             (q '{:find [x]
                  :where [($ :x {:xt/* x})],}
                {:snapshot-time #inst "2023-01-17"})))))

#_
(t/deftest test-row-alias-app-time-key-set ;TODO from-star
  (let [inputs
        [[{:xt/id 0, :a 0} #inst "2023-01-17T00:00:00"]
         [{:xt/id 0, :b 0} #inst "2023-01-18T00:00:00"]
         [{:xt/id 0, :c 0, :a 0} #inst "2023-01-19T00:00:00"]]

        _
        (doseq [[doc app-time] inputs]
          (xt/submit-tx tu/*node* [[:put-docs {:into :x, :valid-from app-time} doc]]))

        q (partial xt/q tu/*node*)]

    (t/is (= [{:x {:xt/id 0, :a 0, :c 0}}]
             (q '{:find [x]
                  :where [($ :x {:xt/* x})]})))

    (t/is (= [{:x {:xt/id 0, :b 0}}]
             (q '{:find [x]
                  :where [($ :x {:xt/* x} {:for-valid-time [:at #xt/instant "2023-01-18T00:00:00Z"]})],})))

    (t/is (= [{:x {:xt/id 0, :a 0}}]
             (q '{:find [x]
                  :where [($ :x {:xt/* x} {:for-valid-time [:at #xt/instant "2023-01-17T00:00:00Z"]})]})))))

(t/deftest test-normalisation
  (xt/submit-tx tu/*node* [[:put-docs :xt-docs {:xt/id "doc" :fOo/bAr 1 :bar.fOO/hello_World 2}]])

  (t/is (= [{:foo/bar 1, :bar.foo/hello-world 2}]
           (xt/q tu/*node* '(from :xt-docs [Foo/Bar Bar.Foo/hello-world]))))

  (t/is (= [{:bar 1, :foo 2}]
           (xt/q tu/*node* '(from :xt-docs [{:foo/bar bar :bar.foo/hello-world foo}])))))

(t/deftest test-table-normalisation
  (let [doc {:xt/id "doc" :foo "bar"}]
    (xt/submit-tx tu/*node* [[:put-docs :foo/the-docs doc]])
    (t/is (= [{:id "doc"}]
             (xt/q tu/*node* '(from :foo/the-docs [{:xt/id id}]))))
    (xt/submit-tx tu/*node* [[:put-docs :foo.bar/the-docs doc]])
    (t/is (= [{:id "doc"}]
             (xt/q tu/*node* '(from :foo.bar/the-docs [{:xt/id id}])))
          "with dots in namespace")))

(t/deftest test-inconsistent-valid-time-range-2494
  (xt/submit-tx tu/*node* [[:put-docs {:into :xt-docs, :valid-to #inst "2011"}
                            {:xt/id 1}]])
  (t/is (= [{:tx-id 0, :committed? false}]

           (xt/q tu/*node*
                 '(from :xt/txs [{:xt/id tx-id, :committed committed?}]))))
  (xt/submit-tx tu/*node* [[:put-docs :xt-docs {:xt/id 2}]])
  (xt/submit-tx tu/*node* [[:delete-docs {:from :xt-docs, :valid-to #inst "2011"} 2]])

  (t/is (= #{{:tx-id 0, :committed? false}
             {:tx-id 1, :committed? true}
             {:tx-id 2, :committed? false}}
           (set (xt/q tu/*node*
                      '(from :xt/txs [{:xt/id tx-id, :committed committed?}]))))))

(deftest test-date-and-time-literals
  (t/is (= [{:a true, :b false, :c true, :d true}]
           (xt/q tu/*node*
                 '(-> (rel [{}] [])
                      (with {:a (= #xt/date "2020-01-01" #xt/date "2020-01-01")
                             :b (= #xt/zoned-date-time "3000-01-01T08:12:13.366Z"
                                   #xt/zoned-date-time "2020-01-01T08:12:13.366Z")
                             :c (= #xt/date-time "2020-01-01T08:12:13.366"
                                   #xt/date-time "2020-01-01T08:12:13.366")
                             :d (= #xt/time "08:12:13.366" #xt/time "08:12:13.366")}))))))

(t/deftest bug-temporal-queries-wrong-at-boundary-2531
  (with-open [node (xtn/start-node (merge tu/*node-opts* {:indexer {:rows-per-block 10}
                                                          :log [:in-memory {:instant-src (tu/->mock-clock)}]}))]
    (doseq [i (range 10)]
      (xt/submit-tx node [[:put-docs :ints {:xt/id 0 :n i}]]))

    (t/is (=
           #{{:n 0,
              :valid-time (tu/->tstz-range #xt/zoned-date-time "2020-01-01T00:00Z",
                                           #xt/zoned-date-time "2020-01-02T00:00Z")}
             {:n 1,
              :valid-time (tu/->tstz-range #xt/zoned-date-time "2020-01-02T00:00Z",
                                           #xt/zoned-date-time "2020-01-03T00:00Z")}
             {:n 2,
              :valid-time (tu/->tstz-range #xt/zoned-date-time "2020-01-03T00:00Z",
                                           #xt/zoned-date-time "2020-01-04T00:00Z")}
             {:n 3,
              :valid-time (tu/->tstz-range #xt/zoned-date-time "2020-01-04T00:00Z",
                                           #xt/zoned-date-time "2020-01-05T00:00Z")}
             {:n 4,
              :valid-time (tu/->tstz-range #xt/zoned-date-time "2020-01-05T00:00Z",
                                           #xt/zoned-date-time "2020-01-06T00:00Z")}}
           (set (xt/q node '(from :ints {:bind [{:n n :xt/id 0 :xt/valid-time valid-time}]
                                         :for-valid-time (in #inst "2020-01-01" #inst "2020-01-06")})))))))

(deftest test-no-zero-width-intervals
  (xt/submit-tx tu/*node* [[:put-docs :xt-docs {:xt/id 1 :v 1}]
                           [:put-docs :xt-docs {:xt/id 1 :v 2}]
                           [:put-docs {:into :xt-docs, :valid-from #inst "2020-01-01", :valid-to #inst "2020-01-02"}
                            {:xt/id 2 :v 1}]])
  (xt/submit-tx tu/*node* [[:put-docs {:into :xt-docs, :valid-from #inst "2020-01-01", :valid-to #inst "2020-01-02"}
                            {:xt/id 2 :v 2}]])
  (t/is (= [{:v 2}]
           (xt/q tu/*node*
                 '(from :xt-docs {:bind [{:xt/id 1} v] :for-system-time :all-time})))
        "no zero width system time intervals")
  (t/is (= [{:v 2}]
           (xt/q tu/*node*
                 '(from :xt-docs {:bind [{:xt/id 2} v] :for-valid-time :all-time})))
        "no zero width valid-time intervals"))

(deftest test-pull
  (xt/submit-tx tu/*node* [[:put-docs :customers {:xt/id 0, :name "bob"}]
                           [:put-docs :customers {:xt/id 1, :name "alice"}]
                           [:put-docs :orders {:xt/id 0, :customer-id 0, :value 26.20}]
                           [:put-docs :orders {:xt/id 1, :customer-id 0, :value 8.99}]
                           [:put-docs :orders {:xt/id 2, :customer-id 1, :value 12.34}]])


  (t/is (= #{{:customer-id 0, :customer {:name "bob"}, :order-id 0, :value 26.20}
             {:customer-id 0, :customer {:name "bob"}, :order-id 1, :value 8.99}
             {:customer-id 1, :customer {:name "alice"}, :order-id 2, :value 12.34}}

           (set (xt/q tu/*node*
                      '(-> (from :orders [{:xt/id order-id} customer-id value])
                           (with {:customer (pull (from :customers [name {:xt/id $customer-id}])
                                                  {:args [customer-id]})}))))))

  (t/is (= #{{:orders [{:id 1, :value 8.99} {:id 0, :value 26.20}], :name "bob", :id 0}
             {:orders [{:id 2, :value 12.34}], :name "alice", :id 1}}
           (set (xt/q tu/*node*
                      '(-> (from :customers [{:xt/id id} name])
                           (with {:orders (pull* (from :orders [{:customer-id $c-id} {:xt/id id} value])
                                                 {:args [{:c-id id}]})}))))))

  (t/is (= [{}]
           (xt/q tu/*node* '(unify (with {orders (pull (rel [] []))})))))

  (t/is (= [{:orders {}}]
           (xt/q tu/*node* '(unify (with {orders (pull (rel [{}] []))})))))

  (t/is (= [{}]
           (xt/q tu/*node* '(unify (with {orders (pull* (rel [] []))})))))

  (t/is (=
         [{:orders [{}]}]
         (xt/q tu/*node*
               '(unify
                 (with {orders (pull* (rel [{}] []))}))))))

(deftest list-diff-test-2887
  (t/is (= []
           (xt/q tu/*node*
                 '(-> (rel [{:x [1 2 3]}] [x])
                      (where (<> x $foo)))
                 {:args {:foo [1 2 3]}}))))


(deftest test-without
  (t/is (= [{}]
           (xt/q tu/*node*
                 '(-> (rel [{:x 1}] [x])
                      (without :x)))))
  (t/is (= [{:y 2}]
           (xt/q tu/*node*
                 '(-> (rel [{:x 1 :y 2}] [x y])
                      (without :x)))))

  (t/is (= [{:x 2}]
           (xt/q tu/*node*
                 '(-> (rel [{:x 2}] [x])
                      (without :z))))
        "ignores missing columns"))

(deftest test-without-removing-cols-3001
  (xt/submit-tx tu/*node*
                [[:put-docs :users {:xt/id "ivan", :first-name "Ivan", :last-name "Ivanov"}]
                 [:put-docs :users {:xt/id "petr", :first-name "Petr", :last-name "Petrov"}]])

  (t/is (= [{:last-name "Petrov"} {:last-name "Ivanov"}]
           (xt/q tu/*node*
                 '(-> (from :users [first-name last-name])
                      (without :first-name))))))

(deftest test-without-normalisation-2959-2969
  (xt/submit-tx tu/*node*
                [[:put-docs :users {:xt/id 1 :name "Oliver"}]])

  (t/is
   (= [{:name "Oliver"}]
      (xt/q tu/*node*
            '(-> (from :users [{:xt/id user-id} name])
                 (without :user-id))))))

(deftest test-struct-accessors
  (t/is (= [{:r 1}]
           (xt/q tu/*node*
                 '(-> (rel [{:x {:foo 1}}] [x])
                      (return {:r (. x foo)})))))
  (t/is (= [{}]
           (xt/q tu/*node*
                 '(-> (rel [{:x {:foo 1}}] [x])
                      (return {:r (. x bar)})))))
  (t/is (= [{:r {:bar [1 2 3]}}]
           (xt/q tu/*node*
                 '(-> (rel [{:x 1 :y {:foo {:bar [1 2 3]} :baz 1}}] [x y])
                      (return {:r (. y foo)}))))))

(deftest test-unnest
  (t/is (= [{:y 1} {:y 2} {:y 3}]
           (xt/q tu/*node*
                 '(-> (unify (rel [{:x [1 2 3]}] [x])
                             (unnest {y x}))
                      (return y)))))

  (t/is (= [{:y 1} {:y 2} {:y 3}]
           (xt/q tu/*node*
                 '(-> (rel [{:x [1 2 3]}] [x])
                      (unnest {:y x})
                      (return y)))))

  (t/is (= [{:y 1} {:y 3}]
           (xt/q tu/*node*
                 '(-> (unify (rel [{:x [1 2 3]}] [x])
                             (unnest {y x})
                             (rel [{:y 1} {:y 3}] [y]))
                      (return y))))
        "unify unnested column"))

(deftest test-unnest-col
  (t/is (= [{:y 1} {:y 2} {:y 3}]
           (xt/q tu/*node*
                 '(-> (rel [{}] [])
                      (unnest {:y $in}))
                 {:args {:in [1 2 3]}}))
        "param")

  (t/is (= [{:y 1} {:y 2} {:y 3}]
           (xt/q tu/*node*
                 '(-> (rel [{}] [])
                      (unnest {:y [1 2 3]}))))
        "expr")

  (t/is (= [{:y 1} {:y 2} {:y 3}]
           (xt/q tu/*node*
                 '(-> (rel [{}] [])
                      (unnest {:y (q
                                   (rel
                                    [{:x [1 2 3]}]
                                    [x]))}))))
        "subquery")

  (t/is (= [{:y 1} {:y 2} {:y 3}]
           (xt/q tu/*node*
                 '(-> (rel [{}] [])
                      (unnest {:y [1
                                   (q
                                    (rel [{:x 2}] [x]))
                                   3]}))))
        "expr with subquery"))

(deftest test-unnest-var
  (t/is (= [{:y 1} {:y 2} {:y 3}]
           (xt/q tu/*node*
                 '(unify (unnest {y $in}))
                 {:args {:in [1 2 3]}}))
        "param")

  (t/is (= [{:y 1} {:y 2} {:y 3}]
           (xt/q tu/*node*
                 '(unify (unnest {y [1 2 3]}))))
        "expr")

  (t/is (= [{:y 1} {:y 2} {:y 3}]
           (xt/q tu/*node*
                 '(unify
                   (unnest {y (q
                               (rel
                                [{:x [1 2 3]}]
                                [x]))}))))
        "subquery")

  (t/is (= [{:y 1} {:y 2} {:y 3}]
           (xt/q tu/*node*
                 '(unify
                   (unnest {y [1
                               (q
                                (rel [{:x 2}] [x]))
                               3]}))))
        "expr with subquery"))

(deftest unnest-rewrite-issue-2982
  (xt/submit-tx tu/*node* bond/tx-ops)

  (t/is (= [{:bond-girl :halle-berry,
             :bond-girls [:halle-berry :rosamund-pike],
             :person/name "Halle Berry"}
            {:bond-girl :rosamund-pike,
             :bond-girls [:halle-berry :rosamund-pike],
             :person/name "Rosamund Pike"}]
           (xt/q tu/*node* '(unify (from :film [{:film/bond-girls bond-girls} {:film/name "Die Another Day"}])
                                   (unnest {bond-girl bond-girls})
                                   (from :person [{:xt/id bond-girl} person/name]))))))

(deftest test-seqs-in-expression-pos-2993
  (xt/submit-tx tu/*node* ivan+petr)

  (t/is (= [{:full-name "Petr Petrov"} {:full-name "Ivan Ivanov"}]
           (xt/q tu/*node* (list '->
                                 '(from :docs [{:xt/id user-id} first-name last-name])
                                 (list 'return {:full-name (cons 'concat (list 'first-name " " 'last-name))}))))))

(deftest delete-ingester-error-3016
  ;; Query works before insert
  (t/is (empty? (xt/q tu/*node* '(from :comments []))))

  (xt/submit-tx tu/*node*
                [[:put-docs :comments {:xt/id 1}]])

  ;; Query works after insert
  (t/is (not (empty? (xt/q tu/*node* '(from :comments [])))))

  (xt/submit-tx tu/*node* [[:delete '{:from :comments
                                      :bind [{:something logic-var}]
                                      :unify [(from :anything [{:else logic-var}])]}]])

  ;; Ingester error after delete
  (t/is (empty? (xt/q tu/*node* '(from :xt/txs [{:committed false}])))))

(deftest plan-expr-test
  (let [required-vars (comp plan/required-vars xtql/parse-expr)]
    (t/testing "required-vars"
      (t/is (= #{'a}
               (required-vars '#{1 (+ a $b) 2})))
      (t/is (= #{'a}
               (required-vars '[1 #{(+ a $b)}  2])))
      (t/is (= #{'a}
               (required-vars '{"foo" #{(+ a $b)}}))))))

(deftest test-push-selection-down-past-map
  (xt/submit-tx tu/*node* [[:put-docs :users {:xt/id :ben :x "foo" :y "foo"}]
                           [:put-docs :users {:xt/id :jeff :x "bar" :y "baz"}]])

  (t/is (= [{:x "foo", :y "foo", :id :ben}]
           (xt/q tu/*node*
                 '(unify
                   (from :users [{:xt/id id} x y])
                   (with {y x}))))
        "tests that columns in projections are rewritten correctly when pushed
         down past map operators of col to col"))

(deftest test-nested-subqueries
  (xt/submit-tx tu/*node* [[:put-docs :users {:xt/id :ben :friends [:jimmy :bob]}]
                           [:put-docs :users {:xt/id :bob :friends []}]
                           [:put-docs :users {:xt/id :jimmy :friends [:ben]}]])

  (t/is (= [{:id :bob}
            {:friends [{:id :ben, :friends [{:id :bob, :friends []}
                                            {:id :jimmy, :friends [:ben]}]}], :id :jimmy}
            {:friends [{:id :bob}
                       {:id :jimmy, :friends [{:id :ben, :friends [:jimmy :bob]}]}], :id :ben}]
           (xt/q tu/*node* '(-> (from :users [{:xt/id id} friends])
                                (with {:friends
                                       (pull* (-> (unify
                                                   (from :users [{:xt/id id} friends])
                                                   (unnest {id $friends}))
                                                  (with {:friends (pull*
                                                                   (-> (unify
                                                                        (from :users [{:xt/id id} friends])
                                                                        (unnest {id $friends}))
                                                                       (return id friends))
                                                                   {:args [friends]})})
                                                  (return id friends))
                                              {:args [friends]})}))))))

(deftest test-expression-in-join-args
  (xt/submit-tx tu/*node* [[:put-docs :a {:xt/id :a1, :a 2 :b 1}]
                           [:put-docs :a {:xt/id :a2, :a 2 :b 3}]
                           [:put-docs :a {:xt/id :a3, :a 2 :b 0}]])

  (t/is (= [{:aid :a2 :a 2 :b 3 :c 4}]
           (xt/q tu/*node*
                 '(unify (from :a [{:xt/id aid} a b])
                         (join (rel [{:b $a :c $c}] [b c])
                               {:args [{:a (+ a 1) :c (+ 1 3)}]
                                :bind [b c]}))))
        "simple expression in arg (join)")

  (t/is (= [{:aid :a3, :a 2} {:aid :a2, :a 2, :b 3} {:aid :a1, :a 2}]
           (xt/q tu/*node*
                 '(unify (from :a [{:xt/id aid} a b])
                         (left-join (rel [{:b $a}] [b])
                                    {:args [{:a (+ a 1)}]
                                     :bind [b]}))))
        "simple expression in arg (left-join)")

  (t/is (= [{:aid :a2 :a 2 :b 3}]
           (xt/q tu/*node*
                 '(unify (from :a [{:xt/id aid} a b])
                         (join (rel [{:b $a}] [b])
                               {:args [{:a (+ a (q (rel [{:c 1}] [c])))}]
                                :bind [b]}))))
        "expression with subquery (join)")

  (t/is (= [{:aid :a3, :a 2} {:aid :a2, :a 2, :b 3} {:aid :a1, :a 2}]
           (xt/q tu/*node*
                 '(unify (from :a [{:xt/id aid} a b])
                         (left-join (rel [{:b $a}] [b])
                                    {:args [{:a (+ a (q (rel [{:c 1}] [c])))}]
                                     :bind [b]}))))
        "expression with subquery (left-join)"))

(deftest test-nested-subquery-parameters-3181
  (t/is (= [{:foo 1}] (xt/q tu/*node* '(-> (rel [{}] [])
                                           (with {:foo (q (rel [{:inner $inner}] [inner])
                                                          {:args [{:inner 1}]})}))))
        "inner literal parameter")

  (t/is (= [{:foo 1}]
           (xt/q tu/*node* '(-> (rel [{}] [])
                                (with {:foo (q (-> (rel [{}] [])
                                                   (with {:foo $inner}))
                                               {:args [{:inner $outer}]})}))
                 {:args {:outer 1}}))
        "inner outer parameter")

  (xt/submit-tx tu/*node* [[:put-docs :docs {:xt/id 1 :bar 1}]])

  (t/is (= [{:foo 2}]
           (xt/q tu/*node* '(-> (rel [{}] [])
                                (with {:foo (q (-> (rel [{}] [])
                                                   (with {:foo $inner}))
                                               {:args [{:inner (+ 1 (q (rel [{:bar 1}] [bar])))}]})}))))
        "inner paramter with subquery"))
