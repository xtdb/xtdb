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

(defn- roundtrip-q [q]
  (edn/unparse (edn/parse-query q)))

