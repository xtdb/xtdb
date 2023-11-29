(ns xtdb.xtql.json-test
  (:require [clojure.test :as t :refer [deftest]]
            [clojure.data.json :as json]
            [xtdb.xtql.edn :as edn]
            [xtdb.xtql.json :as xjson]))

(defn- roundtrip-expr [expr]
  (let [parsed (xjson/parse-expr expr)]
    [(edn/unparse parsed) (xjson/unparse parsed)]))

(defn- roundtrip-value [v t]
  (let [[edn {v "@value", t "@type"}] (roundtrip-expr {"@value" v, "@type" (when t (str "xt:" (name t)))})]
    [edn v (some-> t (subs 3) keyword)]))

(t/deftest test-parse-expr
  (t/is (= ['a "a"] (roundtrip-expr "a")))

  (t/is (= [12 12] (roundtrip-expr 12)))
  (t/is (= [12.8 12.8] (roundtrip-expr 12.8)))
  (t/is (= [true true] (roundtrip-expr true)))
  (t/is (= [false false] (roundtrip-expr false)))
  (t/is (= [nil nil] (roundtrip-expr nil)))
  (t/is (= [nil nil nil] (roundtrip-value nil nil)))

  (t/is (= [:a "a" :keyword] (roundtrip-value "a" :keyword)))

  (t/is (= [#time/date "2020-01-01" "2020-01-01" :date]
           (roundtrip-value "2020-01-01" :date)))

  (t/is (= [#time/date-time "2020-01-01T12:34:56.789" "2020-01-01T12:34:56.789" :timestamp]
           (roundtrip-value "2020-01-01T12:34:56.789" :timestamp)))

  (t/is (= [#time/zoned-date-time "2020-01-01T12:34:56.789Z" "2020-01-01T12:34:56.789Z" :timestamptz]
           (roundtrip-value "2020-01-01T12:34:56.789Z" :timestamptz)))

  (t/is (= [#time/duration "PT3H1M35.23S" "PT3H1M35.23S" :duration]
           (roundtrip-value "PT3H1M35.23S" :duration)))

  (t/is (= [[1 2 3] [1 2 3]]
           (roundtrip-expr [1 2 3]))
        "vectors")

  (t/is (= [#{1 2 3} {1 1, 2 1, 3 1} :set]
           (-> (roundtrip-value [1 2 3] :set)
               (update 1 frequencies)))
        "sets")

  (t/testing "calls"
    (t/is (= ['(foo) {"foo" []}]
             (roundtrip-expr {"foo" []}))

          "no-args")

    (t/is (= ['(foo 12 "hello") {"foo" [12 {"@value" "hello"}]}]
             (roundtrip-expr {"foo" [12 {"@value" "hello"}]}))

          "args"))

  ;; TODO check errors
  ;; TODO Implement Expr$Get
  )

(defn- roundtrip-json [data]
  (letfn [(value-wrt-fn [_ v] (xjson/object->json-value v))
          (value-rdr-fn [_ v] (if (map? v) (xjson/json-value->object v) v))]
    (-> (json/write-str data :value-fn value-wrt-fn)
        (json/read-str :value-fn value-rdr-fn))))

(deftest test-json-conversion
  (t/is (= {"foo" "foo"
            "bar" :bar
            "nested-map" {"toto" :toto}
            "set" #{1 2 "foo" :bar}
            "date" #time/date "1998-09-02"
            "duration "#time/duration "PT1H"}

           (roundtrip-json {"foo" "foo"
                            "bar" :bar
                            "nested-map" {"toto" :toto}
                            "set" #{1 2 "foo" :bar}
                            "date" #time/date "1998-09-02"
                            "duration "#time/duration "PT1H"}))))

(t/deftest test-expr-subquery
  (t/is (= ['(exists? (from :foo [a])) {"exists" {"from" "foo", "bind" ["a"]}}]
           (roundtrip-expr {"exists" {"from" "foo", "bind" ["a"]}})))

  (t/is (= ['(exists? (from :foo [a]) {:args [a {:b outer-b}]})
            {"exists" {"from" "foo", "bind" ["a"]}, "args" ["a" {"b" "outer-b"}]}]
           (roundtrip-expr {"exists" {"from" "foo", "bind" ["a"]}, "args" ["a" {"b" "outer-b"}]})))

  (t/is (= ['(q (from :foo [a])) {"q" {"from" "foo", "bind" ["a"]}}]
           (roundtrip-expr {"q" {"from" "foo", "bind" ["a"]}}))))

(defn- roundtrip-q [query]
  (let [parsed (xjson/parse-query query)]
    [(edn/unparse parsed) (xjson/unparse parsed)]))

(defn- roundtrip-q-tail [query]
  (let [parsed (xjson/parse-query-tail query)]
    [(edn/unparse parsed) (xjson/unparse parsed)]))

(t/deftest test-parse-from
  (t/is (= ['(from :foo [a]) {"from" "foo", "bind" ["a"]}]
           (roundtrip-q {"from" "foo", "bind" ["a"]})))

  (let [json-q {"from" "foo", "bind" ["a"], "forValidTime" {"at" {"@value" "2020-01-01", "@type" "xt:date"}}}]
    (t/is (= ['(from :foo {:for-valid-time (at #time/date "2020-01-01"), :bind [a]})
              json-q]
             (roundtrip-q json-q))))

  (let [json-q {"from" "foo", "bind" ["a"],
                "forValidTime" "allTime"
                "forSystemTime" {"in" [{"@value" "2020-01-01", "@type" "xt:date"} nil]}}]
    (t/is (= ['(from :foo
                     {:bind [a]
                      :for-valid-time :all-time
                      :for-system-time (in #time/date "2020-01-01" nil)})
              json-q]
             (roundtrip-q json-q))))

  (t/is (= ['(from :foo [a {:xt/id b} c]) {"from" "foo", "bind" ["a" {"xt/id" "b"} "c"]}]
           (roundtrip-q {"from" "foo", "bind" ["a" {"xt/id" "b"} {"c" "c"}]})))

  ;; TODO check errors
  )

(t/deftest test-parse-table
  (let [json-q {"table" [{"a" 12 "b" {"@value" "foo"}} {"a" 1 "c" {"@value" "bar"}}]
                "bind" ["a" "b" "c"]}]
    (t/is (= ['(table [{:a 12 :b "foo"} {:a 1 :c "bar"}] [a b c])
              json-q]
             (roundtrip-q json-q))
          "simple static table"))

  (let [json-q [{"table" [{"first-name" {"@value" "Ivan"}} {"first-name" {"@value" "Petr"}}]
                 "bind" ["first-name" "last-name"]}
                {"where" [{"=" [{"@value" "Petr"} "first-name"]}]}]]
    (t/is (= ['(-> (table [{:first-name "Ivan"} {:first-name "Petr"}] [first-name last-name])
                   (where (= "Petr" first-name)))
              json-q]
             (roundtrip-q json-q))
          "table in pipeline"))

  (let [json-q {"unify" [{"from" "docs"
                          "bind" ["first-name"]}
                         {"table" [{"first-name" {"@value" "Ivan"}} {"first-name" {"@value" "Petr"}}]
                          "bind" ["first-name"]}]}]
    (t/is (= ['(unify (from :docs [first-name])
                      (table [{:first-name "Ivan"} {:first-name "Petr"}] [first-name]))
              json-q]
             (roundtrip-q json-q))
          "table in unify")))

(t/deftest test-pipe
  (t/is (= ['(-> (from :foo [a])
                 (where (> a 3)))
            [{"from" "foo", "bind" ["a"]}
             {"where" [{">" ["a" 3]}]}]]

           (roundtrip-q [{"from" "foo", "bind" ["a"]}
                         {"where" [{">" ["a" 3]}]}]))))

(t/deftest test-unify
  (t/is (= ['(unify (from :foo [a]) (from :bar [b]) (where (> a b)))
            {"unify"
             [{"from" "foo", "bind" ["a"]}
              {"from" "bar", "bind" ["b"]}
              {"where" [{">" ["a" "b"]}]}]}]

           (roundtrip-q {"unify" [{"from" "foo", "bind" ["a"]}
                                  {"from" "bar", "bind" ["b"]}
                                  {"where" [{">" ["a" "b"]}]}]}))))

(t/deftest test-where
  (t/is (= ['(where (>= foo bar) (< bar baz))
            {"where" [{">=" ["foo" "bar"]}
                      {"<" ["bar" "baz"]}]}]

           (roundtrip-q-tail {"where" [{">=" ["foo" "bar"]}
                                       {"<" ["bar" "baz"]}]}))))

(t/deftest test-with
  (t/is (= ['(with {:c (+ a b)})
            {"with" [{"c" {"+" ["a" "b"]}}]}]

           (roundtrip-q-tail {"with" [{"c" {"+" ["a" "b"]}}]}))))

(t/deftest test-without
  (t/is (= ['(without :a :b :c)
            {"without" ["a" "b" "c"]}]

           (roundtrip-q-tail {"without" ["a" "b" "c"]}))))

(t/deftest test-return
  (t/is (= ['(return a b {:c (+ a b)})
            {"return" ["a" "b" {"c" {"+" ["a" "b"]}}]}]

           (roundtrip-q-tail {"return" ["a" "b" {"c" {"+" ["a" "b"]}}]}))))

(t/deftest test-aggregate
  (t/is (= ['(aggregate a b {:c (sum (+ a b))})
            {"aggregate" ["a" "b" {"c" {"sum" [{"+" ["a" "b"]}]}}]}]

           (roundtrip-q-tail {"aggregate" ["a" "b" {"c" {"sum" [{"+" ["a" "b"]}]}}]}))))

(t/deftest test-union-all
  (t/is (= ['(union-all (from :foo [a])
                        (from :bar [a]))
            {"unionAll" [{"from" "foo", "bind" ["a"]}
                         {"from" "bar", "bind" ["a"]}]}]

           (roundtrip-q {"unionAll" [{"from" "foo", "bind" ["a"]}
                                     {"from" "bar", "bind" ["a"]}]}))))

(t/deftest test-joins
  (t/is (= ['(unify (from :foo [a])
                    (join (from :bar [a b])
                          [a b])
                    (left-join (from :baz [a c])
                               {:args [a], :bind [c]}))

            {"unify" [{"from" "foo", "bind" ["a"]}
                      {"join" {"from" "bar", "bind" ["a" "b"]}
                       "bind" ["a" "b"]}
                      {"leftJoin" {"from" "baz", "bind" ["a" "c"]}
                       "args" ["a"]
                       "bind" ["c"]}]}]

           (roundtrip-q {"unify" [{"from" "foo", "bind" ["a"]}
                                  {"join" {"from" "bar", "bind" ["a" "b"]}
                                   "bind" ["a" "b"]}
                                  {"leftJoin" {"from" "baz", "bind" ["a" "c"]}
                                   "args" ["a"]
                                   "bind" ["c"]}]}))))
(t/deftest test-order-by
  (t/is (= ['(order-by x
                       (+ a b)
                       {:val y :nulls :last}
                       {:val z :dir :asc}
                       {:val l :dir :desc :nulls :first})
            {"orderBy" ["x"
                        {"+" ["a" "b"]}
                        {"val" "y" "nulls" "last"}
                        {"val" "z" "dir" "asc"}
                        {"val" "l" "dir" "desc" "nulls" "first"}]}]
           (roundtrip-q-tail
            {"orderBy" ["x"
                        {"val" {"+" ["a" "b"]}}
                        {"val" "y" "nulls" "last"}
                        {"val" "z" "dir" "asc"}
                        {"val" "l" "dir" "desc" "nulls" "first"}]})))

  (t/is (thrown-with-msg?
         IllegalArgumentException #"Invalid keys provided to option map"
         (roundtrip-q-tail {"orderBy" [{"foo" "y" "nulls" "last"}]})))

  (t/is (thrown-with-msg?
         IllegalArgumentException #"order-by-val-missing"
         (roundtrip-q-tail {"orderBy" [{"nulls" "last"}]})))

  (t/is (thrown-with-msg?
         IllegalArgumentException #"malformed-order-by-direction"
         (roundtrip-q-tail {"orderBy" [{"val" "x" "dir" "d"}]})))

  (t/is (thrown-with-msg?
         IllegalArgumentException #"malformed-order-by-nulls"
         (roundtrip-q-tail {"orderBy" [{"val" "x" "nulls" "fish"}]}))))

(deftest test-limit-2939
  (t/is (= ['(-> (from :users [name])
                 (limit 10))
            [{"from" "users" "bind" ["name"]}
             {"limit" 10}]]
           (roundtrip-q [{"from" "users" "bind" ["name"]}
                         {"limit" 10}]))))

(deftest test-unnest
  (t/is (= ['(unify (table [{:x [1 2 3]}] [x])
                    (unnest {y x}))
            {"unify" [{"table" [{"x" [1 2 3]}]
                       "bind" ["x"]}
                      {"unnest" [{"y" "x"}]}]}]
           (roundtrip-q {"unify" [{"table" [{"x" [1 2 3]}]
                                   "bind" ["x"]}
                                  {"unnest" [{"y" "x"}]}]})))

  (t/is (= ['(-> (table [{:x [1 2 3]}] [x])
                 (unnest {:y x}))
            [{"table" [{"x" [1 2 3]}]
              "bind" ["x"]}
             {"unnest" [{"y" "x"}]}]]
           (roundtrip-q [{"table" [{"x" [1 2 3]}]
                          "bind" ["x"]}
                         {"unnest" [{"y" "x"}]}] ))))
