(ns xtdb.expression.comparator-test
  (:require [clojure.test :as t]
            [xtdb.expression-test :as et]
            [xtdb.test-util :as tu]
            [xtdb.types :as types]))

(t/use-fixtures :each tu/with-allocator)

(t/deftest absent-handling-2944
  (with-open [rel (tu/open-rel [(-> (types/col-type->field "x" [:struct '{float :f64
                                                                          maybe-float [:union #{:f64 :absent}]
                                                                          maybe-str [:union #{:utf8 :absent}]
                                                                          null-float [:union #{:f64 :null}]}])
                                    (tu/open-vec [{:float 12.0, :null-float nil}]))])]

    (t/is (= {:res [0], :res-type [:union #{:i32 :null}]}
             (et/run-projection rel '(compare (. x maybe-float) (. x maybe-float)))))

    (t/is (= {:res [[false true false true false true]]
              :res-type [:list :bool]}
             (et/run-projection rel '[(<> (. x maybe-float) (. x maybe-float))
                                      (= (. x maybe-float) (. x maybe-float))
                                      (< (. x maybe-float) (. x maybe-float))
                                      (<= (. x maybe-float) (. x maybe-float))
                                      (> (. x maybe-float) (. x maybe-float))
                                      (>= (. x maybe-float) (. x maybe-float))])))

    (t/is (= {:res [[nil nil nil nil nil nil nil nil]]
              :res-type [:list [:union #{:null :bool :i32}]]}
             (et/run-projection rel '[(<> (. x maybe-float) (. x null-float))
                                      (= (. x null-float) (. x maybe-float))
                                      (< (. x null-float) (. x maybe-float))
                                      (<= (. x maybe-float) (. x null-float))
                                      (> (. x maybe-float) (. x null-float))
                                      (>= (. x null-float) (. x maybe-float))
                                      (compare (. x null-float) (. x maybe-float))
                                      (compare (. x maybe-float) (. x null-float))])))

    (t/is (= {:res [[-1 -1 0 0 -1 1, 1 1 0 0 1 -1]], :res-type [:list :i32]}
             (et/run-projection rel '[(compare-nulls-first (. x null-float) (. x maybe-float))
                                      (compare-nulls-first (. x null-float) (. x float))
                                      (compare-nulls-first (. x null-float) (. x null-float))
                                      (compare-nulls-first (. x maybe-float) (. x maybe-float))
                                      (compare-nulls-first (. x maybe-float) (. x float))
                                      (compare-nulls-first (. x maybe-float) (. x null-float))

                                      (compare-nulls-last (. x null-float) (. x maybe-float))
                                      (compare-nulls-last (. x null-float) (. x float))
                                      (compare-nulls-last (. x null-float) (. x null-float))
                                      (compare-nulls-last (. x maybe-float) (. x maybe-float))
                                      (compare-nulls-last (. x maybe-float) (. x float))
                                      (compare-nulls-last (. x maybe-float) (. x null-float))])))

    (t/is (= {:res [[true false true true false true]], :res-type [:list :bool]}
             (et/run-projection rel '[(null-eq (. x null-float) (. x maybe-float))
                                      (null-eq (. x null-float) (. x float))
                                      (null-eq (. x null-float) (. x null-float))
                                      (null-eq (. x maybe-float) (. x maybe-float))
                                      (null-eq (. x maybe-float) (. x float))
                                      (null-eq (. x maybe-float) (. x null-float))])))))
