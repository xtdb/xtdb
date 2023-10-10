(ns xtdb.xtql.json-test
  (:require [clojure.test :as t]
            [xtdb.xtql.edn :as edn]
            [xtdb.xtql.json :as json]))

(defn- roundtrip-expr [expr]
  (let [parsed (json/parse-expr expr)
        json (json/unparse parsed)
        edn (edn/unparse parsed)]
    [edn json]))

(defn- roundtrip-value [v t]
  (let [[edn {v "@value", t "@type"}] (roundtrip-expr {"@value" v, "@type" (when t (str "xt:" (name t)))})]
    [edn v (some-> t (subs 3) keyword)]))

(t/deftest test-parse-expr
  (t/is (= ['a "a"] (roundtrip-expr "a")))

  (t/is (= [12 12 nil] (roundtrip-value 12 nil)))
  (t/is (= [12.8 12.8 nil] (roundtrip-value 12.8 nil)))
  (t/is (= [true true nil] (roundtrip-value true nil)))
  (t/is (= [false false nil] (roundtrip-value false nil)))
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

  (t/is (= [[1 2 3] [{"@value" 1} {"@value" 2} {"@value" 3}]]
           (roundtrip-expr [{"@value" 1} {"@value" 2} {"@value" 3}]))
        "vectors")

  (t/is (= [#{1 2 3} {{"@value" 1} 1, {"@value" 2} 1, {"@value" 3} 1} :set]
           (-> (roundtrip-value [{"@value" 1} {"@value" 2} {"@value" 3}] :set)
               (update 1 frequencies)))
        "sets")

  (t/testing "calls"
    (t/is (= ['(foo) {"foo" []}]
             (roundtrip-expr {"foo" []}))

          "no-args")

    (t/is (= ['(foo 12 "hello") {"foo" [{"@value" 12} {"@value" "hello"}]}]
             (roundtrip-expr {"foo" [{"@value" 12} {"@value" "hello"}]}))

          "args"))

  ;; TODO check errors
  )

(defn- roundtrip-q [q]
  (edn/unparse (edn/parse-query q)))

