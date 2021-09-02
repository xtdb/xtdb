(ns core2.types.json-test
  (:require [clojure.test :as t]
            [clojure.spec.alpha :as s]
            [core2.types.json :as tj])
  (:import [org.apache.arrow.vector.complex UnionVector]
           [org.apache.arrow.memory RootAllocator]))

(t/deftest can-build-sparse-union-vector
  (with-open [a (RootAllocator.)
              v (UnionVector/empty "" a)
              writer (.getWriter v)]
    (doseq [x [false
               nil
               2
               3.14
               "Hello"
               []
               [2 3.14 [false nil]]
               {}
               {:B 2 :C 1 :F false}
               [1 {:B [2]}]
               ;; NOTE: should contain :F
               {:B 3.14 :D {:E ["hello" -1]} :F nil}]]
      (tj/append-writer a writer nil nil (s/conform :json/value x)))
    (.setValueCount v (.getPosition writer))

    (t/is (= "[false, null, 2, 3.14, Hello, [], [2,3.14,[false,null]], {}, {\"B\":2,\"C\":1,\"F\":false}, [1,{\"B\":[2]}], {\"B\":3.14,\"D\":{\"E\":[\"hello\",-1]}}]"
             (str v)))))
