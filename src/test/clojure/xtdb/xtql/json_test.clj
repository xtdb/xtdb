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

(defn- roundtrip-q [query]
  (let [parsed (json/parse-query query)]
    [(edn/unparse parsed) (json/unparse parsed)]))

(t/deftest test-parse-from
  (t/is (= ['(from :foo) {"from" ["foo"]}]
           (roundtrip-q {"from" ["foo"]})))

  (let [json-q {"from" [{"table" ["foo" {"forValidTime" {"at" {"@value" "2020-01-01", "@type" "xt:date"}}}]}]}]
    (t/is (= ['(from [:foo {:for-valid-time [:at #time/date "2020-01-01"]}])
              json-q]
             (roundtrip-q json-q))))

  (let [json-q {"from" [{"table" ["foo" {"forValidTime" "allTime"
                                         "forSystemTime" {"in" [{"@value" "2020-01-01", "@type" "xt:date"} nil]}}]}]}]
    (t/is (= ['(from [:foo {:for-valid-time :all-time
                            :for-system-time [:in #time/date "2020-01-01" nil]}])
              json-q]
             (roundtrip-q json-q))))

  (t/is (= ['(from :foo a {:xt/id b} c) {"from" ["foo" "a" {"xt/id" "b"} "c"]}]
           (roundtrip-q {"from" ["foo" "a" {"xt/id" "b"} {"c" "c"}]})))

  ;; TODO check errors
  )

(t/deftest test-pipe
  (t/is (= ['(-> (from :foo a)
                 (where (> a 3)))
            {"->" [{"from" ["foo" "a"]}
                   {"where" [{">" ["a" 3]}]}]}]

           (roundtrip-q {"->" [{"from" ["foo" "a"]}
                               {"where" [{">" ["a" 3]}]}]}))))

(t/deftest test-unify
  (t/is (= ['(unify (from :foo a) (from :bar b) (where (> a b)))
            {"unify"
             [{"from" ["foo" "a"]}
              {"from" ["bar" "b"]}
              {"where" [{">" ["a" "b"]}]}]}]

           (roundtrip-q {"unify" [{"from" ["foo" "a"]}
                                  {"from" ["bar" "b"]}
                                  {"where" [{">" ["a" "b"]}]}]}))))

(t/deftest test-where
  (t/is (= ['(where (>= foo bar) (< bar baz))
            {"where" [{">=" ["foo" "bar"]}
                      {"<" ["bar" "baz"]}]}]

           (roundtrip-q {"where" [{">=" ["foo" "bar"]}
                                  {"<" ["bar" "baz"]}]}))))
