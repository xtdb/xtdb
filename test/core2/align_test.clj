(ns core2.align-test
  (:require [clojure.test :as t]
            [core2.align :as align]
            [core2.expression :as expr]
            [core2.test-util :as tu]
            [core2.vector.indirect :as iv])
  (:import java.util.List
           org.apache.arrow.vector.VectorSchemaRoot))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-align
  (with-open [age-vec (tu/->mono-vec "age" :i64 [12 42 15 83 25])
              age-row-id-vec (tu/->mono-vec "_row-id" :i64 [2 5 9 12 13])

              age-root (let [^List vecs [age-row-id-vec age-vec]]
                         (VectorSchemaRoot. vecs))

              name-vec (tu/->mono-vec "name" :utf8 ["Al" "Dave" "Bob" "Steve"])
              name-row-id-vec (tu/->mono-vec "_row-id" :i64 [1 2 9 13])

              name-root (let [^List vecs [name-row-id-vec name-vec]]
                          (VectorSchemaRoot. vecs))]

    (let [row-ids (doto (align/->row-id-bitmap (.select (expr/->expression-relation-selector '(<= age 30) {:col-types {'age :i64}})
                                                        tu/*allocator*
                                                        (iv/->indirect-rel [(iv/->direct-vec age-vec)])
                                                        {})
                                               age-row-id-vec)
                    (.and (align/->row-id-bitmap (.select (expr/->expression-relation-selector '(<= name "Frank") {:col-types {'name :utf8}})
                                                          tu/*allocator*
                                                          (iv/->indirect-rel [(iv/->direct-vec name-vec)])
                                                          {})
                                                 name-row-id-vec)))
          roots [name-root age-root]]
      (t/is (= [{:name "Dave", :age 12}
                {:name "Bob", :age 15}]
               (iv/rel->rows (align/align-vectors roots row-ids nil)))))))
