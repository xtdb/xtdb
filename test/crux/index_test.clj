(ns crux.index-test
  (:require [clojure.test :as t]
            [crux.index :as idx]
            [crux.byte-utils :as bu]))

(t/deftest test-ordering-of-values
  (t/testing "longs"
    (let [values (shuffle [nil -1 0 1])
          value+bytes (for [v values]
                        [v (idx/value->bytes v)])]
      (t/is (= (sort-by first value+bytes)
               (sort-by second bu/bytes-comparator value+bytes)))))

  (t/testing "doubles"
    (let [values (shuffle [nil -1.0 0.0 1.0])
          value+bytes (for [v values]
                        [v (idx/value->bytes v)])]
      (t/is (= (sort-by first value+bytes)
               (sort-by second bu/bytes-comparator value+bytes)))))

  (t/testing "dates"
    (let [values (shuffle [nil #inst "1900" #inst "1970" #inst "2018"])
          value+bytes (for [v values]
                        [v (idx/value->bytes v)])]
      (t/is (= (sort-by first value+bytes)
               (sort-by second bu/bytes-comparator value+bytes)))))

   (t/testing "strings"
    (let [values (shuffle [nil "a" "ad" "c" "delta" "eq" "foo" "" "0" "året" "漢字" "यूनिकोड"])
          value+bytes (for [v values]
                        [v (idx/value->bytes v)])]
      (t/is (= (sort-by first value+bytes)
               (sort-by second bu/bytes-comparator value+bytes))))))
