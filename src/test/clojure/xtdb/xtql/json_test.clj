(ns xtdb.xtql.json-test
  (:require [clojure.test :as t]
            [xtdb.xtql.edn :as edn]
            [xtdb.xtql.json :as json]))

(defn- roundtrip-expr [expr]
  (let [parsed (json/parse-expr expr)]
    [(edn/unparse parsed) (json/unparse parsed)]))

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
  )

(t/deftest test-expr-subquery
  (t/is (= ['(exists? (from :foo)) {"exists" {"from" "foo"}}]
           (roundtrip-expr {"exists" {"from" "foo"}})))

  (t/is (= ['(exists? [(from :foo) a {:b outer-b}])
            {"exists" {"from" "foo"}, "args" ["a" {"b" "outer-b"}]}]
           (roundtrip-expr {"exists" {"from" "foo"}, "args" ["a" {"b" "outer-b"}]})))

  (t/is (= ['(not-exists? (from :foo)) {"notExists" {"from" "foo", "bind" []}}]
           (roundtrip-expr {"notExists" {"from" "foo", "bind" []}})))

  (t/is (= ['(q (from :foo)) {"q" {"from" "foo", "bind" []}}]
           (roundtrip-expr {"q" {"from" "foo", "bind" []}}))))

(defn- roundtrip-q [query]
  (let [parsed (json/parse-query query)]
    [(edn/unparse parsed) (json/unparse parsed)]))

(defn- roundtrip-q-tail [query]
  (let [parsed (json/parse-query-tail query)]
    [(edn/unparse parsed) (json/unparse parsed)]))

(t/deftest test-parse-from
  (t/is (= ['(from :foo) {"from" "foo"}]
           (roundtrip-q {"from" "foo"})))

  (let [json-q {"from" "foo", "forValidTime" {"at" {"@value" "2020-01-01", "@type" "xt:date"}}}]
    (t/is (= ['(from [:foo {:for-valid-time [:at #time/date "2020-01-01"]}])
              json-q]
             (roundtrip-q json-q))))

  (let [json-q {"from" "foo",
                "forValidTime" "allTime"
                "forSystemTime" {"in" [{"@value" "2020-01-01", "@type" "xt:date"} nil]}}]
    (t/is (= ['(from [:foo {:for-valid-time :all-time
                            :for-system-time [:in #time/date "2020-01-01" nil]}])
              json-q]
             (roundtrip-q json-q))))

  (t/is (= ['(from :foo a {:xt/id b} c) {"from" "foo", "bind" ["a" {"xt/id" "b"} "c"]}]
           (roundtrip-q {"from" "foo", "bind" ["a" {"xt/id" "b"} {"c" "c"}]})))

  ;; TODO check errors
  )

(t/deftest test-pipe
  (t/is (= ['(-> (from :foo a)
                 (where (> a 3)))
            {"->" [{"from" "foo", "bind" ["a"]}
                   {"where" [{">" ["a" 3]}]}]}]

           (roundtrip-q {"->" [{"from" "foo", "bind" ["a"]}
                               {"where" [{">" ["a" 3]}]}]}))))

(t/deftest test-unify
  (t/is (= ['(unify (from :foo a) (from :bar b) (where (> a b)))
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
  (t/is (= ['(with :a :b {:c (+ a b)})
            {"with" ["a" "b" {"c" {"+" ["a" "b"]}}]}]

           (roundtrip-q-tail {"with" ["a" "b" {"c" {"+" ["a" "b"]}}]}))))

(t/deftest test-without
  (t/is (= ['(without :a :b :c)
            {"without" ["a" "b" "c"]}]

           (roundtrip-q-tail {"without" ["a" "b" "c"]}))))

(t/deftest test-return
  (t/is (= ['(return :a :b {:c (+ a b)})
            {"return" ["a" "b" {"c" {"+" ["a" "b"]}}]}]

           (roundtrip-q-tail {"return" ["a" "b" {"c" {"+" ["a" "b"]}}]}))))

(t/deftest test-aggregate
  (t/is (= ['(aggregate :a :b {:c (sum (+ a b))})
            {"aggregate" ["a" "b" {"c" {"sum" [{"+" ["a" "b"]}]}}]}]

           (roundtrip-q-tail {"aggregate" ["a" "b" {"c" {"sum" [{"+" ["a" "b"]}]}}]}))))

(t/deftest test-union-all
  (t/is (= ['(union-all (from :foo a)
                        (from :bar a))
            {"unionAll" [{"from" "foo", "bind" ["a"]}
                         {"from" "bar", "bind" ["a"]}]}]

           (roundtrip-q {"unionAll" [{"from" "foo", "bind" ["a"]}
                                     {"from" "bar", "bind" ["a"]}]}))))

(t/deftest test-joins
  (t/is (= ['(unify (from :foo a)
                    (join (from :bar a b) a b)
                    (left-join [(from :baz a c) a] c))

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
  (t/is (= ['(-> (from :foo a b)
                 (order-by (+ a b) [b {:dir :desc}]))

            {"->" [{"from" "foo", "bind" ["a" "b"]}
                   {"orderBy" [{"+" ["a" "b"]}
                               ["b" {"dir" "desc"}]]}]}]

           (roundtrip-q {"->" [{"from" "foo", "bind" ["a" "b"]}
                               {"orderBy" [{"+" ["a" "b"]}
                                           ["b" {"dir" "desc"}]]}]}))))
