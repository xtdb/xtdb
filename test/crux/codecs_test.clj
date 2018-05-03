(ns crux.codecs-test
  (:require [crux.codecs :refer :all]
            [clojure.test :as t]))

(t/deftest test-codecs-work-as-expected
  (defframe foo :a :int32 :b :int32)
  (defframe md5f :a :int32 :b :md5)

  (t/testing "Can encode/decode vanilla frame"
    (t/is (= {:a 1 :b 2} (decode foo (.array #^bytes (encode foo {:a 1 :b 2}))))))


  (t/testing "Can encode/decode exotic frame"
    (t/is (= 1 (:a (decode foo (.array #^bytes (encode md5f {:a 1 :b 2}))))))))

(t/deftest test-prefix-codecs
  (defframe foo1 :a :int32 :b :int32)
  (defframe foo2 :a :int32 :c :int32)

  (defprefixedframe bob [:a :int32] {1 foo1 2 foo2})

  (t/testing "Can encode/decode prefixed frame"
    (t/is (= {:a 1 :b 2} (decode bob (.array #^bytes (encode bob {:a 1 :b 2})))))
    (t/is (= {:a 2 :c 2} (decode bob (.array #^bytes (encode bob {:a 2 :c 2})))))))
