(ns xtdb.expression.list-test
  (:require [clojure.test :as t]
            [xtdb.expression :as expr]
            [xtdb.expression.list :as list-expr]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import (xtdb.arrow ListExpression Vector)))

(t/use-fixtures :each tu/with-allocator)

(t/deftest compile-list-expr-test
  (letfn [(run-test [form opts]
            (let [expr (expr/form->expr form opts)
                  {:keys [->list-expr vec-type]} (list-expr/compile-list-expr expr {})
                  ^ListExpression res (->list-expr {} {})]
              {:vec-type vec-type
               :size (some-> res (.getSize))
               :value (when res
                        (util/with-open [out-vec (Vector/open tu/*allocator* "$data$" vec-type)]
                          (.writeTo res out-vec 0 (.getSize res))
                          (vec (.getAsList out-vec))))}))]
    (t/is (= {:vec-type #xt/type :i64
              :size 10
              :value [1 2 3 4 5 6 7 8 9 10]}
             (run-test '(generate_series 1 11 1) {}))
          "happy case with generate_series")

    (t/is (= {:vec-type #xt/type [:? :null]
              :size nil
              :value nil}
             (run-test '(get_field 1) {}))
          "expr that returns nil")

    (t/is (= {:vec-type #xt/type [:? :null]
              :size nil
              :value nil}
             (run-test '(+ 1 1) {}))
          "expr that doesn't return a list")

    (t/is (= {:vec-type #xt/type [:? :i64]
              :size nil
              :value nil}
             (run-test '(if true (+ 1 1) [1 2]) {}))
          "expr that might return a list (and doesn't)")

    (t/is (= {:vec-type #xt/type [:? :i64]
              :size 2
              :value [1 2]}
             (run-test '(if false (+ 1 1) [1 2]) {}))
          "expr that might return a list (and does)")))

(t/deftest testing-write-to
  (let [expr (expr/form->expr '[1 2 3 4 5 6 7 8 9 10] {})
        {:keys [->list-expr vec-type]} (list-expr/compile-list-expr expr {})
        ^ListExpression list-expr (->list-expr {} {})
        run-write-test (fn [start count]
                         (util/with-open [out-vec (Vector/open tu/*allocator* "$data$" vec-type)]
                           (.writeTo list-expr out-vec start count)
                           (vec (.getAsList out-vec))))]

    (t/is (= [1 2 3 4 5 6 7 8 9 10]
             (run-write-test 0 (.getSize list-expr)))
          "writeTo - all values from start")

    (t/is (= [1 2 3 4 5]
             (run-write-test 0 5))
          "writeTo - half values from start")

    (t/is (= [6 7 8 9 10]
             (run-write-test 5 5))
          "writeTo - half values from halfway")

    (t/is (thrown-with-msg?
           IllegalArgumentException
           #"No matching clause: 10"
           (run-write-test 0 15))
          "writeTo - more values than size")))
