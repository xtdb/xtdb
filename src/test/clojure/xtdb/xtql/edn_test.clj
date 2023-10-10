(ns xtdb.xtql.edn-test
  (:require [clojure.test :as t]
            [xtdb.xtql.edn :as edn]))

(defn- roundtrip-expr [expr]
  (edn/unparse (edn/parse-expr expr)))

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

(defn- roundtrip-q [q]
  (edn/unparse (edn/parse-query q)))

(t/deftest test-parse-from
  (t/is (= '(from :foo)
           (roundtrip-q '(from :foo))))

  (t/is (= '(from [:foo {:for-valid-time [:at #time/instant "2020-01-01T00:00:00Z"]}])
           (roundtrip-q '(from [:foo {:for-valid-time [:at #inst "2020"]}]))))

  ;; TODO system-time

  (t/is (= '(from :foo a {:xt/id b} c)
           (roundtrip-q '(from :foo a {:xt/id b} {:c c}))))

  ;; TODO check errors
  )
