(ns core2.expression-test
  (:require [clojure.test :as t]
            [core2.expression :as expr]
            [core2.test-util :as test-util]
            [core2.util :as util])
  (:import org.apache.arrow.memory.RootAllocator
           [org.apache.arrow.vector BigIntVector BitVector FieldVector Float8Vector ValueVector VectorSchemaRoot]))

(t/deftest can-compile-simple-expression
  (with-open [allocator (RootAllocator.)
              a (Float8Vector. "a" allocator)
              b (Float8Vector. "b" allocator)
              d (BigIntVector. "d" allocator)]
    (let [rows 1000]
      (.setValueCount a rows)
      (.setValueCount b rows)
      (.setValueCount d rows)
      (dotimes [n rows]
        (.set a n (double n))
        (.set b n (double n))
        (.set d n n)))

    (with-open [in (VectorSchemaRoot/of (into-array FieldVector [a b d]))]
      (let [form '(+ a b)
            expr-projection-spec (expr/->expression-projection-spec "c" form)]
        (t/is (= '[a b] (expr/variables (expr/form->expr form))))
        (with-open [^ValueVector acc (.project expr-projection-spec in allocator)]
          (let [v (util/maybe-single-child-dense-union acc)
                xs (test-util/->list v)]
            (t/is (instance? Float8Vector v))
            (t/is (= (mapv (comp double +) (range 1000) (range 1000))
                     xs)))))

      (let [form '(- a (* 2.0 b))
            expr-projection-spec (expr/->expression-projection-spec "c" form)]
        (t/is (= '[a b] (expr/variables (expr/form->expr form))))
        (with-open [^ValueVector acc (.project expr-projection-spec in allocator)]
          (let [xs (test-util/->list (util/maybe-single-child-dense-union acc))]
            (t/is (= (mapv (comp double -) (range 1000) (map (partial * 2) (range 1000)))
                     xs)))))

      (t/testing "support keyword and vectors"
        (let [form '[:+ a [:+ b 2]]
              expr-projection-spec (expr/->expression-projection-spec "c" form)]
          (t/is (= '[a b] (expr/variables (expr/form->expr form))))
          (with-open [^ValueVector acc (.project expr-projection-spec in allocator)]
            (let [xs (test-util/->list (util/maybe-single-child-dense-union acc))]
              (t/is (= (mapv (comp double +) (range 1000) (range 1000) (repeat 2))
                       xs))))))

      (t/testing "mixing types"
        (let [form '(+ 2 d)
              expr-projection-spec (expr/->expression-projection-spec "c" form)]
          (t/is (= '[d] (expr/variables (expr/form->expr form))))
          (with-open [^ValueVector acc (.project expr-projection-spec in allocator)]
            (let [v (util/maybe-single-child-dense-union acc)
                  xs (test-util/->list (util/maybe-single-child-dense-union acc))]
              (t/is (instance? BigIntVector v))
              (t/is (= (mapv + (repeat 2) (range 1000))
                       xs)))))

        (let [form '(+ 2.0 d)
              expr-projection-spec (expr/->expression-projection-spec "c" form)]
          (t/is (= '[d] (expr/variables (expr/form->expr form))))
          (with-open [^ValueVector acc (.project expr-projection-spec in allocator)]
            (let [v (util/maybe-single-child-dense-union acc)
                  xs (test-util/->list (util/maybe-single-child-dense-union acc))]
              (t/is (instance? Float8Vector v))
              (t/is (= (mapv (comp double +) (repeat 2) (range 1000))
                       xs))))))

      (t/testing "predicate"
        (let [form '(= a d)
              expr-projection-spec (expr/->expression-projection-spec "c" form)]
          (t/is (= '[a d] (expr/variables (expr/form->expr form))))
          (with-open [^ValueVector acc (.project expr-projection-spec in allocator)]
            (let [v (util/maybe-single-child-dense-union acc)
                  xs (test-util/->list v)]
              (t/is (instance? BitVector v))
              (t/is (= (repeat 1000 true) xs))))))

      (t/testing "math"
        (let [form '(sin a)
              expr-projection-spec (expr/->expression-projection-spec "c" form)]
          (t/is (= '[a] (expr/variables (expr/form->expr form))))
          (with-open [^ValueVector acc (.project expr-projection-spec in allocator)]
            (let [v (util/maybe-single-child-dense-union acc)
                  xs (test-util/->list v)]
              (t/is (instance? Float8Vector v))
              (t/is (= (mapv #(Math/sin ^double %) (range 1000)) xs))))))

      (t/testing "if"
        (let [form '(if false a 0)
              expr-projection-spec (expr/->expression-projection-spec "c" form)]
          (t/is (= '[a] (expr/variables (expr/form->expr form))))
          (with-open [^ValueVector acc (.project expr-projection-spec in allocator)]
            (let [v (util/maybe-single-child-dense-union acc)
                  xs (test-util/->list v)]
              (t/is (instance? Float8Vector v))
              (t/is (= (repeat 1000 0.0) xs))))))

      (t/testing "cannot call arbitrary functions"
        (let [form '(vec a)
              expr-projection-spec (expr/->expression-projection-spec "c" form)]
          (t/is (thrown? IllegalArgumentException (.project expr-projection-spec in allocator)))))

      (t/testing "selector"
        (let [form '(>= a 500)
              selector (expr/->expression-root-selector form)]
          (t/is (= '[a] (expr/variables (expr/form->expr form))))
          (t/is (= 500 (.getCardinality (.select selector in)))))

        (let [form '(>= a 500)
              selector (expr/->expression-vector-selector form)]
          (t/is (= '[a] (expr/variables (expr/form->expr form))))
          (t/is (= 500 (.getCardinality (.select selector a)))))))))
