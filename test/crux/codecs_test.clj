(ns crux.codecs-test
  (:require [clojure.test :as t]
            [crux.codecs :refer :all])
  (:import java.nio.ByteBuffer))

(defn- round-trip [f m]
  (decode f #^bytes (.array ^ByteBuffer (encode f m))))

(t/deftest test-codecs-work-as-expected
  (let [f (compile-frame :a :int32 :b :int32)]
    (t/testing "Can encode vanilla frame"
      (encode f {:a 1 :b 2}))

    (t/testing "Can encode/decode vanilla frame"
      (t/is (= {:a 1 :b 2} (round-trip f {:a 1 :b 2})))))

  (t/testing "Can encode/decode exotic frame"
    (let [f (compile-frame :a :int32 :b :md5)]
      (t/is (= 1 (:a (round-trip f {:a 1 :b "sad"})))))))

(t/deftest test-prefix-codecs
  (let [f1 (compile-frame :a :int32 :b :int32)
        f2 (compile-frame :a :int32 :c :int32)
        f (compile-header-frame [:a :int32] {1 f1 2 f2})]

    (t/testing "Can encode/decode prefixed frame"
      (t/is (= {:a 1 :b 2} (round-trip f {:a 1 :b 2})))
      (t/is (= {:a 2 :c 2} (round-trip f {:a 2 :c 2}))))))

(t/deftest test-enums
  (let [e (compile-enum :foo :tar)
        f (compile-frame :a :int32 :b e)]
    (t/is (= {:a 1, :b :foo} (round-trip f {:a 1 :b :foo})))))

(t/deftest test-prefix-uses-enum
  (let [e (compile-enum :foo :tar)
        f1 (compile-frame :a e :b :int32)
        f2 (compile-frame :a e :c :int32)
        f (compile-header-frame [:a e] {:foo f1 :tar f2})]

    (t/testing "Can encode/decode prefixed frame"
      (t/is (= {:a :foo :b 2} (round-trip f {:a :foo :b 2})))
      (t/is (= {:a :tar :c 2} (round-trip f {:a :tar :c 2}))))))

(t/deftest test-various-datatypes
  (t/testing "String datatype"
    (let [f (compile-frame  :a :string)]
      (t/is (= {:a "hello"} (round-trip f {:a "hello"})))))

  (t/testing "Keyword datatype"
    (let [f (compile-frame  :a :keyword)]
      (t/is (= {:a :bob} (round-trip f {:a :bob}))))))
