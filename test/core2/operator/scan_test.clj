(ns core2.operator.scan-test
  (:require [clojure.test :as t]
            [core2.core :as c2]
            [core2.test-util :as tu]
            [core2.expression :as expr]
            [core2.expression.metadata :as expr.meta])
  (:import org.apache.arrow.memory.RootAllocator))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-separate-query-allocator
  (with-open [node (c2/start-node {})]
    (let [tx @(c2/submit-tx node [{:op :put, :doc {:_id "foo"}}])]
      (c2/await-tx node tx)
      (tu/finish-chunk node)
      (with-open [query-allocator (RootAllocator.)
                  db (c2/open-db node)
                  res (.scan db
                             query-allocator
                             ["_id"]
                             (expr.meta/->metadata-selector (expr/form->expr '_id))
                             {} nil nil)]
        (t/is (= 1 (count (into [] (mapcat seq) (tu/<-cursor res)))))))))
