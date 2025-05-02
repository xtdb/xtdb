(ns xtdb.xtql-test
  (:require [clojure.test :as t :refer [deftest]]
            [xtdb.xtql :as xtql]))

(defn- roundtrip-expr [expr]
  (xtql/unparse (xtql/parse-expr expr {})))

(t/deftest test-parse-expr
  (t/is (= nil (roundtrip-expr nil)))
  (t/is (= 'a (roundtrip-expr 'a)))

  (t/is (= 12 (roundtrip-expr 12)))
  (t/is (= 12.8 (roundtrip-expr 12.8)))
  (t/is (= true (roundtrip-expr true)))
  (t/is (= false (roundtrip-expr false)))
  (t/is (= nil (roundtrip-expr nil)))

  (t/is (= :a (roundtrip-expr :a)))

  (t/is (= #xt/date "2020-01-01" (roundtrip-expr #xt/date "2020-01-01")))

  (t/is (= #xt/date-time "2020-01-01T12:34:56.789"
           (roundtrip-expr #xt/date-time "2020-01-01T12:34:56.789")))

  (t/is (= #xt/zoned-date-time "2020-01-01T12:34:56.789Z"
           (roundtrip-expr #xt/zoned-date-time "2020-01-01T12:34:56.789Z")))

  (t/is (= #xt/duration "PT3H1M35.23S"
           (roundtrip-expr #xt/duration "PT3H1M35.23S")))

  (t/is (= [1 2 3] (roundtrip-expr [1 2 3]))
        "vectors")

  (t/is (= #{1 2 3} (roundtrip-expr #{1 2 3}))
        "sets")

  (t/is (= '{:foo (+ 1 a)} (roundtrip-expr '{:foo (+ 1 a)}))
        "maps")

  (t/testing "calls"
    (t/is (= '(foo) (roundtrip-expr '(foo)))
          "no-args")

    (t/is (= '(foo 12 "hello") (roundtrip-expr '(foo 12 "hello")))
          "args"))

  (t/testing "struct accessors"

    (t/is (= '(. foo bar) (roundtrip-expr '(. foo bar))))

    (t/is
     (thrown-with-msg?
      IllegalArgumentException #"malformed-get"
      (roundtrip-expr '(. foo))))))

(t/deftest test-expr-subquery
  (t/is (= '(exists? (from :foo [a]))
           (roundtrip-expr '(exists? (from :foo [a])))))

  (t/is (= '(exists? [(fn [a] (from :foo [a])) a])
           (roundtrip-expr '(exists? (fn [a] (from :foo [a]))))))

  (t/is (= '(q (from :foo [a]))
           (roundtrip-expr '(q (from :foo [a]))))))

(defn- roundtrip-q
  ([q] (roundtrip-q q {}))
  ([q env] (xtql/unparse-query (xtql/parse-query q env))))

(defn- roundtrip-q-tail
  ([q] (roundtrip-q-tail q {}))
  ([q env] (xtql/unparse-query-tail (xtql/parse-query-tail q env))))

(defn- roundtrip-unify-clause
  ([q] (roundtrip-unify-clause q {}))
  ([q env] (xtql/unparse-unify-clause (xtql/parse-unify-clause q env))))

(t/deftest test-parse-from
  (t/is (= '(from :foo [a])
           (roundtrip-q '(from :foo [a]))))

  (t/is (= '(from :foo {:for-valid-time (at #inst "2020"), :bind [a]})
           (roundtrip-q '(from :foo {:for-valid-time (at #inst "2020"), :bind [a]}))))

  (t/is (= '(from :foo {:for-system-time (at #inst "2020"), :bind [a]})
           (roundtrip-q '(from :foo {:for-system-time (at #inst "2020"), :bind [a]}))))

  (t/is (= '(from :foo [a {:xt/id b} c {:d "fish"}])
           (roundtrip-q '(from :foo [a {:xt/id b} {:c c} {:d "fish"}]))))

  (let [q '(from :foo [{:foo {:baz 1}}])]
    (t/is (= q
             (roundtrip-q q))))

  (t/is (thrown-with-msg?
         IllegalArgumentException #"Invalid keys provided to option map"
         (roundtrip-q '(from :foo {:bar x :baz x}))))

  (t/is (thrown-with-msg?
         IllegalArgumentException #"Attribute in bind spec must be keyword"
         (roundtrip-q '(from :foo [{bar x "fish" y}])))))

(t/deftest test-from-star
  (t/is (= '(from :foo [* a {:xt/id b} {:d "fish"}])
           (roundtrip-q '(from :foo [a {:xt/id b} * {:d "fish"}]))))

  (t/is (= '(from :foo {:bind [* a {:xt/id b} {:d "fish"}]
                        :for-system-time :all-time})
           (roundtrip-q '(from :foo {:bind [a {:xt/id b} * {:d "fish"}]
                                     :for-system-time :all-time})))))

(t/deftest test-parse-unify
  (t/is (= '(unify (from :foo [{:baz b}])
                   (from :bar [{:baz b}]))

           (roundtrip-q '(unify (from :foo [{:baz b}])
                                (from :bar [{:baz b}]))))))

(t/deftest test-parse-where
  (let [q '(where false (= 1 foo))]
    (t/is (= q
             (roundtrip-q-tail q)))))

(t/deftest test-parse-pipeline
  (let [q '(-> (from :foo [a])
               (without :a))]
    (t/is (= q
             (roundtrip-q q)))))

(t/deftest test-parse-with-operator
  (let [q '(with {:bar 1} {:baz (+ 1 1)})]
    (t/is (= q
             (roundtrip-q-tail q))))

  (t/is (thrown-with-msg?
         IllegalArgumentException #"Short form of col spec must be a symbol"
         (roundtrip-q-tail '(with (+ 1 1)))))

  (t/is (thrown-with-msg?
         IllegalArgumentException #"Attribute in col spec must be keyword"
         (roundtrip-q-tail '(with {bar (+ 1 1)})))))

(t/deftest test-parse-with-unify-clause
  (let [q '(with {bar 1} {baz (+ 1 1)})]
    (t/is (= q
             (roundtrip-unify-clause q))))

  (t/is (thrown-with-msg?
         IllegalArgumentException #"Var specs must be pairs of bindings"
         (roundtrip-unify-clause '(with (+ 1 1)))))

  (t/is (thrown-with-msg?
         IllegalArgumentException #"Attribute in var spec must be symbol"
         (roundtrip-unify-clause '(with {:bar (+ 1 1)})))))

(t/deftest test-parse-without
  (let [q '(-> (from :foo [xt/id a])
               (without :a :xt/id :f))]
    (t/is (= q
             (roundtrip-q q)))

    (t/is (thrown-with-msg?
           IllegalArgumentException #"Columns must be keywords in without"
           (roundtrip-q-tail '(without {:bar baz}))))))

(t/deftest test-parse-return
  (let [q '(-> (from :foo [a])
               (return a {:b a}))]
    (t/is (= q
             (roundtrip-q q)))))

(t/deftest test-parse-aggregate
  (let [q '(-> (from :foo [a c])
               (return c {:b (sum a)}))]
    (t/is (= q
             (roundtrip-q q)))))

(t/deftest test-parse-join
  (t/is (= '(join (from :foo [a c])
                  [{:a b} c])
           (roundtrip-unify-clause '(join (from :foo [a c])
                                          [{:a b} c]))))

  (t/is (thrown-with-msg?
         IllegalArgumentException #"malformed-bind"
         (roundtrip-unify-clause '(join (fn [x]
                                          (from :foo [x]))
                                        {:baz x})))))

(t/deftest test-parse-order-by
  (t/is (= '(order-by (+ a b)
                      x
                      {:val y :nulls :last}
                      {:val z :dir :asc}
                      {:val l :dir :desc :nulls :first})
           (roundtrip-q-tail '(order-by (+ a b)
                                        {:val x}
                                        {:val y :nulls :last}
                                        {:val z :dir :asc}
                                        {:val l :dir :desc :nulls :first}))))
  (t/is (thrown-with-msg?
         IllegalArgumentException #"Invalid keys provided to option map"
         (roundtrip-q-tail '(order-by {:foo y :nulls :last}))))

  (t/is (thrown-with-msg?
         IllegalArgumentException #"order-by-val-missing"
         (roundtrip-q-tail '(order-by {:nulls :last}))))

  (t/is (thrown-with-msg?
         IllegalArgumentException #"malformed-order-by-direction"
         (roundtrip-q-tail '(order-by {:val x :dir "d"}))))

  (t/is (thrown-with-msg?
         IllegalArgumentException #"malformed-order-by-nulls"
         (roundtrip-q-tail '(order-by {:val x :nulls :fish})))))

(t/deftest test-parse-union-all
  (t/is (= '(union-all (from :foo [{:baz b}])
                       (from :bar [{:baz b}]))
           (roundtrip-q '(union-all (from :foo [{:baz b}])
                                    (from :bar [{:baz b}]))))))

(t/deftest test-parse-limit-test
  (t/is (= '(limit 10)
           (roundtrip-q-tail '(limit 10)))))

(t/deftest test-parse-offset-test
  (t/is (= '(offset 5)
           (roundtrip-q-tail '(offset 5)))))

(t/deftest test-parse-params
  (let [q '(where (= $bar foo))]
    (t/is (= q
             (roundtrip-unify-clause q)))))

(t/deftest test-parse-rel
  (let [q '(rel [{:a 12 :b "foo"} {:a 1 :c "bar"}] [a b c])]
    (t/is (= q
             (roundtrip-q q))
          "simple static rel"))

  (let [q '(-> (rel [{:first-name "Ivan"} {:first-name "Petr"}] [first-name last-name])
               (where (= "Petr" first-name)))]
    (t/is (= q
             (roundtrip-q q))
          "rel in pipeline"))

  (let [q '(unify (from :docs [first-name])
                  (rel [{:first-name "Ivan"} {:first-name "Petr"}] [first-name]))]
    (t/is (= q
             (roundtrip-q q))
          "rel in unify"))

  (let [q '(rel [{:foo :bar :baz {:nested-foo $nested-param}}] [foo baz])]
    (t/is (= q
             (roundtrip-q q))
          "rel with paramater in documents"))

  (let [q '(rel bar [foo baz])]
    (t/is (= q
             (roundtrip-q q {:params #{'bar}}))
          "rel as a parameter")))

(deftest test-generated-queries
  (let [q '(unify (from :users [{:xt/id user-id} first-name last-name])
                  (from :articles [{:author-id user-id} title content]))]
    (t/is (= q
             (roundtrip-q (roundtrip-q q)))
          "roundtripping twice to test generated queries (i.e. cons ...)")

    (t/is (= q
             (roundtrip-q (lazy-seq (list 'unify
                                          (lazy-seq '(from :users [{:xt/id user-id} first-name last-name]))
                                          (lazy-seq '(from :articles [{:author-id user-id} title content]))) q)))
          "testing parsing lazy-seq")))

(deftest test-unnest
  (let [q '(unnest {y x})]
    (t/is (= q
             (roundtrip-unify-clause q))
          "unnest unify clause"))

  (let [q '(unnest {:y x})]
    (t/is (= q
             (roundtrip-q-tail q))
          "unnest op"))

  (t/is (thrown-with-msg?
         IllegalArgumentException #"Unnest takes only a single binding"
         (roundtrip-q-tail '(unnest {:a b :c d}))))

  (t/is (thrown-with-msg?
         IllegalArgumentException #"Unnest takes only a single binding"
         (roundtrip-q-tail '(unnest {:a b} {:c d}))))

  (t/is (thrown-with-msg?
         IllegalArgumentException #"Unnest takes only a single binding"
         (roundtrip-q-tail '(unnest a)))))
