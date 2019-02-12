(ns crux.codec-test
  (:require [clojure.test :as t]
            [crux.codec :as c]
            [crux.memory :as mem])
  (:import crux.codec.Id))

(t/deftest test-ordering-of-values
  (t/testing "longs"
    (let [values (shuffle [nil -33 -1 0 1 33])
          value+buffer (for [v values]
                         [v (c/->value-buffer v)])]
      (t/is (= (sort-by first value+buffer)
               (sort-by second mem/buffer-comparator value+buffer)))))

  (t/testing "doubles"
    (let [values (shuffle [nil -33.0 -1.0 0.0 1.0 33.0])
          value+buffer (for [v values]
                         [v (c/->value-buffer v)])]
      (t/is (= (sort-by first value+buffer)
               (sort-by second mem/buffer-comparator value+buffer)))))

  (t/testing "dates"
    (let [values (shuffle [nil #inst "1900" #inst "1970" #inst "2018"])
          value+buffer (for [v values]
                         [v (c/->value-buffer v)])]
      (t/is (= (sort-by first value+buffer)
               (sort-by second mem/buffer-comparator value+buffer)))))

   (t/testing "strings"
    (let [values (shuffle [nil "a" "ad" "c" "delta" "eq" "foo" "" "0" "året" "漢字" "यूनिकोड"])
          value+buffer (for [v values]
                         [v (c/->value-buffer v)])]
      (t/is (= (sort-by first value+buffer)
               (sort-by second mem/buffer-comparator value+buffer))))))

(t/deftest test-id-reader
  (t/is (c/new-id "http://google.com") #crux/id "http://google.com")
  (t/is "234988566c9a0a9cf952cec82b143bf9c207ac16"
        (str #crux/id "http://google.com"))
  (t/is (instance? Id (c/new-id #crux/id "http://google.com"))))
