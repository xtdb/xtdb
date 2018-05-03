(ns crux.codecs-test
  (:require [crux.codecs :refer :all]
            [clojure.test :as t])
  (:import [java.nio ByteBuffer]))

(t/deftest test-codecs-work-as-expected
  (let [f (compile-frame :a :int32 :b :int32)]
    (t/testing "Can encode vanilla frame"
      (encode f {:a 1 :b 2}))

    (t/testing "Can encode/decode vanilla frame"
      (t/is (= {:a 1 :b 2} (decode f (.array ^ByteBuffer (encode f {:a 1 :b 2})))))))

  (t/testing "Can encode/decode exotic frame"
    (let [f (compile-frame :a :int32 :b :md5)]
      (t/is (= 1 (:a (decode f (.array ^ByteBuffer (encode f {:a 1 :b "sad"})))))))))

(t/deftest test-prefix-codecs
  (let [f1 (compile-frame :a :int32 :b :int32)
        f2 (compile-frame :a :int32 :c :int32)
        f (compile-header-frame [:a :int32] {1 f1 2 f2})]

    (encode f {:a 1 :b 2})

    (t/testing "Can encode/decode prefixed frame"
      (t/is (= {:a 1 :b 2} #^bytes (decode f #^bytes (.array ^ByteBuffer (encode f {:a 1 :b 2})))))
      (t/is (= {:a 2 :c 2} #^bytes (decode f #^bytes (.array ^ByteBuffer (encode f {:a 2 :c 2}))))))))

(t/deftest test-enums
  (let [e (compile-enum :foo :tar)
        f (compile-frame :a :int32 :b e)]
     (encode f {:a 1 :b :foo})
    (t/is (= {:a 1, :b :foo} (decode f #^bytes (.array ^ByteBuffer (encode f {:a 1 :b :foo})))))))

(t/deftest test-various-datatypes
  (let [f (compile-frame  :a :string)
        m {:a "hello"}
        encoded (encode f m)]
    (t/is (= {:a "hello"} (decode f #^bytes (.array ^ByteBuffer encoded))))))
