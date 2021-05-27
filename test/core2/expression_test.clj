(ns core2.expression-test
  (:require [clojure.test :as t]
            [core2.core :as c2]
            [core2.expression :as expr]
            [core2.expression.temporal :as expr.temp]
            [core2.test-util :as tu]
            [core2.types :as ty])
  (:import org.apache.arrow.vector.types.pojo.Schema
           org.apache.arrow.vector.types.Types$MinorType))

(t/use-fixtures :each tu/with-allocator)

(def a-field (ty/->field "a" (.getType Types$MinorType/FLOAT8) false))
(def b-field (ty/->field "b" (.getType Types$MinorType/FLOAT8) false))
(def d-field (ty/->field "d" (.getType Types$MinorType/BIGINT) false))
(def e-field (ty/->field "e" (.getType Types$MinorType/VARCHAR) false))

(def data
  (for [n (range 1000)]
    {:a (double n), :b (double n), :d n, :e (format "%04d" n)}))

(t/deftest test-simple-projection
  (with-open [in-rel (tu/->relation (Schema. [a-field b-field d-field e-field]) data)]
    (letfn [(project [form]
              (with-open [project-col (.project (expr/->expression-projection-spec "c" form {})
                                                tu/*allocator* in-rel)]
                (tu/<-column project-col)))]

      (t/is (= (mapv (comp double +) (range 1000) (range 1000))
               (project '(+ a b))))

      (t/is (= (mapv (comp double -) (range 1000) (map (partial * 2) (range 1000)))
               (project '(- a (* 2.0 b)))))

      (t/is (= (mapv (comp double +) (range 1000) (range 1000) (repeat 2))
               (project '[:+ a [:+ b 2]]))
            "support keyword and vectors")

      (t/is (= (mapv + (repeat 2) (range 1000))
               (project '(+ 2 d)))
            "mixing types")

      (t/is (= (repeat 1000 true)
               (project '(= a d)))
            "predicate")

      (t/is (= (mapv #(Math/sin ^double %) (range 1000))
               (project '(sin a)))
            "math")

      (t/is (= (repeat 1000 0.0)
               (project '(if false a 0)))
            "if")

      (t/is (thrown? IllegalArgumentException (project '(vec a)))
            "cannot call arbitrary functions"))))

(t/deftest can-compile-simple-expression
  (with-open [in-rel (tu/->relation (Schema. [a-field b-field d-field e-field]) data)]
    (letfn [(select-relation [form params]
              (-> (.select (expr/->expression-relation-selector form params)
                           in-rel)
                  (.getCardinality)))

            (select-column [form ^String col-name params]
              (-> (.select (expr/->expression-column-selector form params)
                           (.readColumn in-rel col-name))
                  (.getCardinality)))]

      (t/testing "selector"
        (t/is (= 500 (select-relation '(>= a 500) {})))
        (t/is (= 500 (select-column '(>= a 500) "a" {})))
        (t/is (= 500 (select-column '(>= e "0500") "e" {}))))

      (t/testing "parameter"
        (t/is (= 500 (select-column '(>= a ?a) "a" {'?a 500})))
        (t/is (= 500 (select-column '(>= e ?e) "e" {'?e "0500"})))))))

(t/deftest can-extract-min-max-range-from-expression
  (t/is (= [[-9223372036854775808, -9223372036854775808, 1546300800000,
             -9223372036854775808, -9223372036854775808, -9223372036854775808]
            [9223372036854775807, 9223372036854775807, 9223372036854775807,
             1546300799999, 9223372036854775807, 9223372036854775807]]
           (map vec (expr.temp/->temporal-min-max-range
                     {"_valid-time-start" '(<= _vt-time-start #inst "2019")
                      "_valid-time-end" '(> _vt-time-end  #inst "2019")}
                     {}))))

  (t/testing "symbol column name"
    (t/is (= [[-9223372036854775808, -9223372036854775808, 1546300800000,
               -9223372036854775808, -9223372036854775808, -9223372036854775808]
              [9223372036854775807, 9223372036854775807, 1546300800000,
               9223372036854775807, 9223372036854775807, 9223372036854775807]]
             (map vec (expr.temp/->temporal-min-max-range
                       {'_valid-time-start '(= _vt-time-start #inst "2019")}
                       {})))))

  (t/testing "conjunction"
    (t/is (= [[-9223372036854775808, -9223372036854775808, 1577836800000
               -9223372036854775808, -9223372036854775808, -9223372036854775808]
              [9223372036854775807, 9223372036854775807, 9223372036854775807,
               9223372036854775807, 9223372036854775807, 9223372036854775807]]
             (map vec (expr.temp/->temporal-min-max-range
                       {"_valid-time-start" '(and (>= #inst "2019" _vt-time-start)
                                                  (>= #inst "2020" _vt-time-start))}
                       {})))))

  (t/testing "disjunction not supported"
    (t/is (= [[-9223372036854775808, -9223372036854775808, -9223372036854775808,
               -9223372036854775808, -9223372036854775808, -9223372036854775808]
              [9223372036854775807, 9223372036854775807, 9223372036854775807,
               9223372036854775807, 9223372036854775807, 9223372036854775807]]
             (map vec (expr.temp/->temporal-min-max-range
                       {"_valid-time-start" '(or (= _vt-time-start #inst "2019")
                                                 (= _vt-time-start #inst "2020"))}
                       {})))))

  (t/testing "parameters"
    (t/is (= [[-9223372036854775808, -9223372036854775808, -9223372036854775808,
               1514764800001, 1546300800000, -9223372036854775808]
              [9223372036854775807, 9223372036854775807, 1514764800000
               9223372036854775807, 9223372036854775807, 1546300799999]]
             (map vec (expr.temp/->temporal-min-max-range
                       {"_tx-time-start" '(>= ?tt _tx-time-start)
                        "_tx-time-end" '(< ?tt _tx-time-end)
                        "_valid-time-start" '(<= ?vt _vt-time-start)
                        "_valid-time-end" '(> ?vt _vt-time-end)}
                       '{?tt #inst "2019" ?vt #inst "2018"}))))))

(t/deftest test-date-trunc
  (with-open [node (c2/start-node {})]
    (c2/await-tx node @(c2/submit-tx node [{:op :put, :doc {:_id "foo", :date #inst "2021-01-21T12:34:56Z"}}]))
    (with-open [db (c2/open-db node)]
      (t/is (= [{:trunc #inst "2021-01-21"}]
               (into [] (c2/plan-q db '[:project [{trunc (date-trunc "DAY" date)}]
                                        [:scan [date]]]))))

      (t/is (= [{:trunc #inst "2021-01-21T12:34"}]
               (into [] (c2/plan-q db '[:project [{trunc (date-trunc "MINUTE" date)}]
                                        [:scan [date]]]))))

      (t/is (= [{:trunc #inst "2021-01-21"}]
               (into [] (c2/plan-q db '[:select (> trunc #inst "2021")
                                        [:project [{trunc (date-trunc "DAY" date)}]
                                         [:scan [date]]]]))))

      (t/is (= [{:trunc #inst "2021-01-21"}]
               (into [] (c2/plan-q db '[:project [{trunc (date-trunc "DAY" trunc)}]
                                        [:project [{trunc (date-trunc "MINUTE" date)}]
                                         [:scan [date]]]])))))))
