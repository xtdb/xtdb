(ns core2.expression-test
  (:require [clojure.test :as t]
            [core2.expression :as expr]
            [core2.expression.temporal :as expr.temp]
            [core2.test-util :as tu]
            [core2.types :as ty]
            [core2.util :as util])
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
  (let [in-rel (tu/->relation (Schema. [a-field b-field d-field e-field]) data)]
    (try
      (letfn [(project [form]
                (let [project-col (.project (expr/->expression-projection-spec "c" (expr/form->expr form))
                                            tu/*allocator* in-rel)]
                  (try
                    (tu/<-column project-col)
                    (finally
                      (util/try-close project-col)))))]

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
              "cannot call arbitrary functions"))
      (finally
        (util/try-close in-rel)))))

(t/deftest can-compile-simple-expression
  (let [in-rel (tu/->relation (Schema. [a-field b-field d-field e-field]) data)]
    (try
      (letfn [(select-relation [form params]
                (-> (.select (expr/->expression-relation-selector (expr/form->expr form) params)
                             in-rel)
                    (.getCardinality)))

              (select-column [form ^String col-name params]
                (-> (.select (expr/->expression-column-selector (expr/form->expr form) params)
                             (.readColumn in-rel col-name))
                    (.getCardinality)))]

        (t/testing "selector"
          (t/is (= 500 (select-relation '(>= a 500) {})))
          (t/is (= 500 (select-column '(>= a 500) "a" {})))
          (t/is (= 500 (select-column '(>= e "0500") "e" {}))))

        (t/testing "parameter"
          (t/is (= 500 (select-column '(>= a ?a) "a" {'?a 500})))
          (t/is (= 500 (select-column '(>= e ?e) "e" {'?e "0500"})))))
      (finally
        (util/try-close in-rel)))))

(t/deftest can-extract-min-max-range-from-expression
  (t/is (= [[-9223372036854775808, -9223372036854775808, 1546300800000,
             -9223372036854775808, -9223372036854775808, -9223372036854775808]
            [9223372036854775807, 9223372036854775807, 9223372036854775807,
             1546300799999, 9223372036854775807, 9223372036854775807]]
           (map vec (expr.temp/->temporal-min-max-range
                     {"_valid-time-start" (expr/form->expr '(<= _vt-time-start #inst "2019"))
                      "_valid-time-end" (expr/form->expr '(> _vt-time-end  #inst "2019"))}
                     {}))))

  (t/testing "symbol column name"
    (t/is (= [[-9223372036854775808, -9223372036854775808, 1546300800000,
               -9223372036854775808, -9223372036854775808, -9223372036854775808]
              [9223372036854775807, 9223372036854775807, 1546300800000,
               9223372036854775807, 9223372036854775807, 9223372036854775807]]
             (map vec (expr.temp/->temporal-min-max-range
                       {'_valid-time-start (expr/form->expr '(= _vt-time-start #inst "2019"))}
                       {})))))

  (t/testing "conjunction"
    (t/is (= [[-9223372036854775808, -9223372036854775808, 1577836800000
               -9223372036854775808, -9223372036854775808, -9223372036854775808]
              [9223372036854775807, 9223372036854775807, 9223372036854775807,
               9223372036854775807, 9223372036854775807, 9223372036854775807]]
             (map vec (expr.temp/->temporal-min-max-range
                       {"_valid-time-start" (expr/form->expr '(and (>= #inst "2019" _vt-time-start)
                                                                   (>= #inst "2020" _vt-time-start)))}
                       {})))))

  (t/testing "disjunction not supported"
    (t/is (= [[-9223372036854775808, -9223372036854775808, -9223372036854775808,
               -9223372036854775808, -9223372036854775808, -9223372036854775808]
              [9223372036854775807, 9223372036854775807, 9223372036854775807,
               9223372036854775807, 9223372036854775807, 9223372036854775807]]
             (map vec (expr.temp/->temporal-min-max-range
                       {"_valid-time-start" (expr/form->expr '(or (= _vt-time-start #inst "2019")
                                                                  (= _vt-time-start #inst "2020")))}
                       {})))))

  (t/testing "parameters"
    (t/is (= [[-9223372036854775808, -9223372036854775808, -9223372036854775808,
               1514764800001, 1546300800000, -9223372036854775808]
              [9223372036854775807, 9223372036854775807, 1514764800000
               9223372036854775807, 9223372036854775807, 1546300799999]]
             (map vec (expr.temp/->temporal-min-max-range
                       {"_tx-time-start" (expr/form->expr '(>= ?tt _tx-time-start))
                        "_tx-time-end" (expr/form->expr '(< ?tt _tx-time-end))
                        "_valid-time-start" (expr/form->expr '(<= ?vt _vt-time-start))
                        "_valid-time-end" (expr/form->expr '(> ?vt _vt-time-end))}
                       '{?tt #inst "2019" ?vt #inst "2018"}))))))
