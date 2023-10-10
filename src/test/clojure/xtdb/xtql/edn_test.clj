(ns xtdb.xtql.edn-test
  (:require [clojure.test :as t]
            [xtdb.xtql.edn :as edn]))

(defn- roundtrip-expr [q]
  (edn/unparse (edn/parse-expr q)))

(t/deftest test-parse-expr
  (t/is (= 'a (roundtrip-expr 'a)))
  (t/is (= :a (roundtrip-expr :a)))

  ;; TODO draw the rest of the exprs

  )

(defn- roundtrip-q [q]
  (edn/unparse (edn/parse-query q)))

(t/deftest test-parse-from
  (t/is (= '(from :foo)
           (roundtrip-q '(from :foo))))

  (t/is (= '(from [:foo {:for-valid-time [:at #=(util/->instant #inst "2020")]}])
           (roundtrip-q '(from [:foo {:for-valid-time [:at #inst "2020"]}]))))

  ;; TODO system-time

  (t/is (= '(from :foo a {:xt/id b} c)
           (roundtrip-q '(from :foo a {:xt/id b} {:c c}))))

  ;; TODO check errors
  )
