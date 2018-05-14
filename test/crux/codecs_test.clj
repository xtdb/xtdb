(ns crux.codecs-test
  (:require [clojure.test :as t]
            [crux.byte-utils :as bu]
            [crux.codecs :refer :all])
  (:import java.nio.ByteBuffer))

(defn- round-trip [f m]
  (decode f ^bytes (.array ^ByteBuffer (encode f m))))

(t/deftest test-codecs-work-as-expected
  (let [f (compile-frame :a :long :b :long)]
    (t/testing "Can encode vanilla frame"
      (encode f {:a 1 :b 2}))

    (t/testing "Can encode/decode vanilla frame"
      (t/is (= {:a 1 :b 2} (round-trip f {:a 1 :b 2})))))

  (t/testing "Can encode/decode exotic frame"
    (let [f (compile-frame :a :long :b :md5)]
      (t/is (= 1 (:a (round-trip f {:a 1 :b (bu/md5 (.getBytes "sad"))})))))))

(t/deftest test-prefix-codecs
  (let [f1 (compile-frame :a :long :b :long)
        f2 (compile-frame :a :long :c :long)
        f (compile-header-frame [:a :long] {1 f1 2 f2})]

    (t/testing "Can encode/decode prefixed frame"
      (t/is (= {:a 1 :b 2} (round-trip f {:a 1 :b 2})))
      (t/is (= {:a 2 :c 2} (round-trip f {:a 2 :c 2}))))))

(t/deftest test-enums
  (let [e (compile-enum :foo :tar)
        f (compile-frame :a :long :b e)]
    (t/is (= {:a 1, :b :foo} (round-trip f {:a 1 :b :foo})))))

(t/deftest test-prefix-uses-enum
  (let [e (compile-enum :foo :tar)
        f1 (compile-frame :a e :b :long)
        f2 (compile-frame :a e :c :long)
        f (compile-header-frame [:a e] {:foo f1 :tar f2})]

    (t/testing "Can encode/decode prefixed frame"
      (t/is (= {:a :foo :b 2} (round-trip f {:a :foo :b 2})))
      (t/is (= {:a :tar :c 2} (round-trip f {:a :tar :c 2})))))

  (let [e (compile-enum :foo :tar)
        f1 (compile-frame :a e :b :string)
        f2 (compile-frame :a e :c :string)
        f (compile-header-frame [:a e] {:foo f1 :tar f2})]

    (t/testing "Can encode/decode prefixed frame with string"
      (t/is (= {:a :foo :b "test1"} (round-trip f {:a :foo :b "test1"})))
      (t/is (= {:a :tar :c "test2"} (round-trip f {:a :tar :c "test2"}))))))

(t/deftest test-various-datatypes
  (t/testing "String datatype"
    (let [f (compile-frame  :a :string)]
      (t/is (= {:a "hello"} (round-trip f {:a "hello"})))))

  (t/testing "Keyword datatype"
    (let [f (compile-frame  :a :keyword)]
      (t/is (= {:a :bob} (round-trip f {:a :bob})))))

  (t/testing "Long datatype"
    (let [f (compile-frame  :a :long)]
      (t/is (= {:a 1} (round-trip f {:a 1})))))

  (t/testing "Double datatype"
    (let [f (compile-frame  :a :double)]
      (t/is (= {:a 1.0} (round-trip f {:a 1.0}))))))
