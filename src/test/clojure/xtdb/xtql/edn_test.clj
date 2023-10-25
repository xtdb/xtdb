(ns xtdb.xtql.edn-test
  (:require [clojure.test :as t]
            [xtdb.xtql.edn :as edn]))

  ;; TODO check errors

(defn- roundtrip-expr [expr]
  (edn/unparse (edn/parse-expr expr)))

(defn- roundtrip-q-opts [opts]
  (edn/unparse (edn/parse-query-opts opts)))

(t/deftest test-parse-expr
  (t/is (= 'a (roundtrip-expr 'a)))

  (t/is (= 12 (roundtrip-expr 12)))
  (t/is (= 12.8 (roundtrip-expr 12.8)))
  (t/is (= true (roundtrip-expr true)))
  (t/is (= false (roundtrip-expr false)))
  (t/is (= nil (roundtrip-expr nil)))

  (t/is (= :a (roundtrip-expr :a)))

  (t/is (= #time/date "2020-01-01" (roundtrip-expr #time/date "2020-01-01")))

  (t/is (= #time/date-time "2020-01-01T12:34:56.789"
           (roundtrip-expr #time/date-time "2020-01-01T12:34:56.789")))

  (t/is (= #time/zoned-date-time "2020-01-01T12:34:56.789Z"
           (roundtrip-expr #time/zoned-date-time "2020-01-01T12:34:56.789Z")))

  (t/is (= #time/duration "PT3H1M35.23S"
           (roundtrip-expr #time/duration "PT3H1M35.23S")))

  (t/is (= [1 2 3] (roundtrip-expr [1 2 3]))
        "vectors")

  (t/is (= #{1 2 3} (roundtrip-expr #{1 2 3}))
        "sets")

  (t/testing "calls"
    (t/is (= '(foo) (roundtrip-expr '(foo)))
          "no-args")

    (t/is (= '(foo 12 "hello") (roundtrip-expr '(foo 12 "hello")))
          "args")))

(t/deftest test-expr-subquery
  (t/is (= '(exists? (from :foo))
           (roundtrip-expr '(exists? (from :foo)))))

  (t/is (= '(exists? (from :foo) {:args [a {:b outer-b}]})
           (roundtrip-expr '(exists? (from :foo) {:args [a {:b outer-b}]}))))

  (t/is (= '(not-exists? (from :foo))
           (roundtrip-expr '(not-exists? (from :foo)))))

  (t/is (= '(q (from :foo))
           (roundtrip-expr '(q (from :foo))))))

(defn- roundtrip-q [q]
  (edn/unparse (edn/parse-query q)))

(defn- roundtrip-q-tail [q]
  (edn/unparse (edn/parse-query-tail q)))

(defn- roundtrip-unify-clause [q]
  (edn/unparse (edn/parse-unify-clause q)))

(t/deftest test-parse-from
  (t/is (= '(from :foo)
           (roundtrip-q '(from :foo))))

  (t/is (= '(from :foo {:for-valid-time (at #inst "2020")})
           (roundtrip-q '(from :foo {:for-valid-time (at #inst "2020")}))))

  (t/is (= '(from :foo {:for-system-time (at #inst "2020")})
           (roundtrip-q '(from :foo {:for-system-time (at #inst "2020")}))))

  (t/is (= '(from :foo {:bind [a {:xt/id b} c]})
           (roundtrip-q '(from :foo {:bind [a {:xt/id b} {:c c}]}))))
  )

(t/deftest test-parse-unify
  (t/is (= '(unify (from :foo {:bind [{:baz b}]})
                   (from :bar {:bind [{:baz b}]}))

           (roundtrip-q '(unify (from :foo {:bind {:baz b}})
                                (from :bar {:bind {:baz b}}))))))

(t/deftest test-parse-where
  (let [q '(where false (= 1 foo))]
    (t/is (= q
             (roundtrip-q-tail q)))))

(t/deftest test-parse-pipeline
  (let [q '(-> (from :foo {:bind [a]})
               (without :a))]
    (t/is (= q
             (roundtrip-q q)))))

(t/deftest test-parse-with-operator
  (let [q '(with {:bar 1} {:baz (+ 1 1)})]
    (t/is (= q
             (roundtrip-q-tail q)))))

(t/deftest test-parse-with-unify-clause
  (let [q '(with {bar 1} {baz (+ 1 1)})]
    (t/is (= q
             (roundtrip-unify-clause q)))))
;TODO with as unify-clause, will fail due to lack of distinct binding types

(t/deftest test-parse-without
  (let [q '(-> (from :foo {:bind [a]})
               (without :a :b :f))]
    (t/is (= q
             (roundtrip-q q)))))

(t/deftest test-parse-return
  (let [q '(-> (from :foo {:bind [a]})
               (return :a {:a b}))]
    (t/is (= q
             (roundtrip-q q)))))

(t/deftest test-parse-aggregate
  (let [q '(-> (from :foo {:bind [a]})
               (return {:b (sum a)}))]
    (t/is (= q
             (roundtrip-q q)))))

(t/deftest test-parse-join
  (t/is (= '(join (from :foo {:bind [a]})
                  {:bind [{:a b}]})
           (roundtrip-unify-clause '(join (from :foo {:bind [a]})
                                          {:bind [{:a b}]})))))

(t/deftest test-parse-order-by
  (t/is (= '(order-by (+ a b) [b {:dir :desc}])
           (roundtrip-q-tail '(order-by (+ a b) [b {:dir :desc}])))))

(t/deftest test-parse-union-all
  (t/is (= '(union-all (from :foo {:bind [{:baz b}]})
                       (from :bar {:bind [{:baz b}]}))
           (roundtrip-q '(union-all (from :foo {:bind {:baz b}})
                                    (from :bar {:bind {:baz b}}))))))

(t/deftest test-parse-limit-test
  (t/is (= '(limit 10)
           (roundtrip-q-tail '(limit 10)))))

(t/deftest test-parse-offset-test
  (t/is (= '(offset 5)
           (roundtrip-q-tail '(offset 5)))))

(t/deftest test-parse-query-opts
  (let [opts {:args [{:a "foo"} {:b "bar"}]}]
    (t/is (= opts
             (roundtrip-q-opts opts)))))

(t/deftest test-parse-params
  (let [q '(where (= $bar foo))]
    (t/is (= q
             (roundtrip-unify-clause q)))))
