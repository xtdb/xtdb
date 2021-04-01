(ns core2.expression-test
  (:require [clojure.test :as t]
            [core2.expression :as expr]
            [core2.test-util :as test-util]
            [core2.util :as util])
  (:import org.apache.arrow.memory.RootAllocator
           [org.apache.arrow.vector FieldVector Float8Vector ValueVector VectorSchemaRoot]))

(t/deftest can-compile-simple-expression
  (with-open [allocator (RootAllocator.)
              a (Float8Vector. "a" allocator)
              b (Float8Vector. "b" allocator)]
    (let [rows 1000]
      (.setValueCount a rows)
      (.setValueCount b rows)
      (dotimes [n rows]
        (.set a n (double n))
        (.set b n (double n))))

    (with-open [in (VectorSchemaRoot/of (into-array FieldVector [a b]))]
      (let [expr '(+ a b)
            expr-projection-spec (expr/->expression-projection-spec "c" expr)]
        (t/is (= '(a b) (expr/variables expr)))
        (with-open [^ValueVector acc (.project expr-projection-spec in allocator)]
          (let [xs (test-util/->list (util/maybe-single-child-dense-union acc))]
            (t/is (= (mapv (comp double +) (range 1000) (range 1000))
                     xs)))))

      (let [expr '(- a (* 2.0 b))
            expr-projection-spec (expr/->expression-projection-spec "c" expr)]
        (t/is (= '(a b) (expr/variables expr)))
        (with-open [^ValueVector acc (.project expr-projection-spec in allocator)]
          (let [xs (test-util/->list (util/maybe-single-child-dense-union acc))]
            (t/is (= (mapv (comp double -) (range 1000) (map (partial * 2) (range 1000)))
                     xs)))))

      (t/testing "support keyword and vectors"
        (let [expr '[:+ a [:+ b 2]]
            expr-projection-spec (expr/->expression-projection-spec "c" expr)]
        (t/is (= '(a b) (expr/variables expr)))
        (with-open [^ValueVector acc (.project expr-projection-spec in allocator)]
          (let [xs (test-util/->list (util/maybe-single-child-dense-union acc))]
            (t/is (= (mapv (comp double +) (range 1000) (range 1000) (repeat 2))
                     xs)))))))))
