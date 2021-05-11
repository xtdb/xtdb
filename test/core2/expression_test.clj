(ns core2.expression-test
  (:require [clojure.test :as t]
            [core2.expression :as expr]
            [core2.expression.temporal :as expr.temp]
            [core2.test-util :as test-util]
            [core2.types :as ty]
            [core2.util :as util])
  (:import org.apache.arrow.memory.RootAllocator
           [org.apache.arrow.vector BigIntVector BitVector FieldVector Float8Vector ValueVector VarCharVector VectorSchemaRoot]
           org.apache.arrow.vector.complex.DenseUnionVector))

(t/deftest can-compile-simple-expression
  (with-open [allocator (RootAllocator.)
              a (Float8Vector. "a" allocator)
              b (Float8Vector. "b" allocator)
              d (BigIntVector. "d" allocator)
              e (VarCharVector. "e" allocator)]
    (letfn [(->acc []
              (.createVector (ty/->primitive-dense-union-field "c") allocator))]

      (let [rows 1000]
        (.setValueCount a rows)
        (.setValueCount b rows)
        (.setValueCount d rows)
        (.setValueCount e rows)
        (dotimes [n rows]
          (.set a n (double n))
          (.set b n (double n))
          (.set d n n)
          (ty/set-safe! e n (format "%04d" n))))

      (with-open [in (VectorSchemaRoot/of (into-array FieldVector [a b d]))]
        (let [expr (expr/form->expr '(+ a b))
              expr-projection-spec (expr/->expression-projection-spec "c" expr)]
          (t/is (= '[a b] (expr/variables expr)))
          (with-open [^DenseUnionVector acc (->acc)]
            (.project expr-projection-spec in acc)
            (let [v (util/maybe-single-child-dense-union acc)
                  xs (test-util/->list v)]
              (t/is (instance? Float8Vector v))
              (t/is (= (mapv (comp double +) (range 1000) (range 1000))
                       xs)))))

        (let [expr (expr/form->expr '(- a (* 2.0 b)))
              expr-projection-spec (expr/->expression-projection-spec "c" expr)]
          (t/is (= '[a b] (expr/variables expr)))
          (with-open [^DenseUnionVector acc (->acc)]
            (.project expr-projection-spec in acc)
            (let [xs (test-util/->list (util/maybe-single-child-dense-union acc))]
              (t/is (= (mapv (comp double -) (range 1000) (map (partial * 2) (range 1000)))
                       xs)))))

        (t/testing "support keyword and vectors"
          (let [expr (expr/form->expr '[:+ a [:+ b 2]])
                expr-projection-spec (expr/->expression-projection-spec "c" expr)]
            (t/is (= '[a b] (expr/variables expr)))
            (with-open [^DenseUnionVector acc (->acc)]
              (.project expr-projection-spec in acc)
              (let [xs (test-util/->list (util/maybe-single-child-dense-union acc))]
                (t/is (= (mapv (comp double +) (range 1000) (range 1000) (repeat 2))
                         xs))))))

        (t/testing "mixing types"
          (let [expr (expr/form->expr '(+ 2 d))
                expr-projection-spec (expr/->expression-projection-spec "c" expr)]
            (t/is (= '[d] (expr/variables expr)))
            (with-open [^DenseUnionVector acc (->acc)]
              (.project expr-projection-spec in acc)
              (let [v (util/maybe-single-child-dense-union acc)
                    xs (test-util/->list (util/maybe-single-child-dense-union acc))]
                (t/is (instance? BigIntVector v))
                (t/is (= (mapv + (repeat 2) (range 1000))
                         xs)))))

          (let [expr (expr/form->expr '(+ 2.0 d))
                expr-projection-spec (expr/->expression-projection-spec "c" expr)]
            (t/is (= '[d] (expr/variables expr)))
            (with-open [^ValueVector acc (->acc)]
              (.project expr-projection-spec in acc)
              (let [v (util/maybe-single-child-dense-union acc)
                    xs (test-util/->list (util/maybe-single-child-dense-union acc))]
                (t/is (instance? Float8Vector v))
                (t/is (= (mapv (comp double +) (repeat 2) (range 1000))
                         xs))))))

        (t/testing "predicate"
          (let [expr (expr/form->expr '(= a d))
                expr-projection-spec (expr/->expression-projection-spec "c" expr)]
            (t/is (= '[a d] (expr/variables expr)))
            (with-open [^ValueVector acc (->acc)]
              (.project expr-projection-spec in acc)
              (let [v (util/maybe-single-child-dense-union acc)
                    xs (test-util/->list v)]
                (t/is (instance? BitVector v))
                (t/is (= (repeat 1000 true) xs))))))

        (t/testing "math"
          (let [expr (expr/form->expr '(sin a))
                expr-projection-spec (expr/->expression-projection-spec "c" expr)]
            (t/is (= '[a] (expr/variables expr)))
            (with-open [^ValueVector acc (->acc)]
              (.project expr-projection-spec in acc)
              (let [v (util/maybe-single-child-dense-union acc)
                    xs (test-util/->list v)]
                (t/is (instance? Float8Vector v))
                (t/is (= (mapv #(Math/sin ^double %) (range 1000)) xs))))))

        (t/testing "if"
          (let [expr (expr/form->expr '(if false a 0))
                expr-projection-spec (expr/->expression-projection-spec "c" expr)]
            (t/is (= '[a] (expr/variables expr)))
            (with-open [^ValueVector acc (->acc)]
              (.project expr-projection-spec in acc)
              (let [v (util/maybe-single-child-dense-union acc)
                    xs (test-util/->list v)]
                (t/is (instance? Float8Vector v))
                (t/is (= (repeat 1000 0.0) xs))))))

        (t/testing "cannot call arbitrary functions"
          (let [expr (expr/form->expr '(vec a))
                expr-projection-spec (expr/->expression-projection-spec "c" expr)]
            (with-open [^DenseUnionVector acc (->acc)]
              (t/is (thrown? IllegalArgumentException
                             (.project expr-projection-spec in acc))))))

        (t/testing "selector"
          (let [expr (expr/form->expr '(>= a 500))
                selector (expr/->expression-root-selector expr)]
            (t/is (= '[a] (expr/variables expr)))
            (t/is (= 500 (.getCardinality (.select selector in)))))

          (let [expr (expr/form->expr '(>= a 500))
                selector (expr/->expression-vector-selector expr)]
            (t/is (= '[a] (expr/variables expr)))
            (t/is (= 500 (.getCardinality (.select selector a)))))

          (let [expr (expr/form->expr '(>= e "0500"))
                selector (expr/->expression-vector-selector expr)]
            (t/is (= '[e] (expr/variables expr)))
            (t/is (= 500 (.getCardinality (.select selector e))))))

        (t/testing "parameter"
          (let [expr (expr/form->expr '(>= a ?a))
                selector (expr/->expression-vector-selector expr {'?a 500})]
            (t/is (= '[a] (expr/variables expr)))
            (t/is (= 500 (.getCardinality (.select selector a)))))

          (let [expr (expr/form->expr '(>= e ?e))
                selector (expr/->expression-vector-selector expr {'?e "0500"})]
            (t/is (= 500 (.getCardinality (.select selector e))))))))))

(t/deftest can-extract-min-max-range-from-expression
  (t/is (= [[-9223372036854775808, -9223372036854775808, 1546300800000,
             -9223372036854775808, -9223372036854775808, -9223372036854775808]
            [9223372036854775807, 9223372036854775807, 9223372036854775807,
             1546300799999, 9223372036854775807, 9223372036854775807]]
           (map vec (expr.temp/->temporal-min-max-range
                     {"_valid-time-start" (expr/form->expr '(<= _vt-time-start #inst "2019"))
                      "_valid-time-end" (expr/form->expr '(> _vt-time-end  #inst "2019"))}
                     {}))))

  (t/is (= [[-9223372036854775808, -9223372036854775808, 1546300800000,
             -9223372036854775808, -9223372036854775808, -9223372036854775808]
            [9223372036854775807, 9223372036854775807, 1546300800000,
             9223372036854775807, 9223372036854775807, 9223372036854775807]]
           (map vec (expr.temp/->temporal-min-max-range
                     {"_valid-time-start" (expr/form->expr '(= _vt-time-start #inst "2019"))}
                     {}))))

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
