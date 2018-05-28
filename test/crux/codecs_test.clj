(ns crux.codecs-test
  (:require [clojure.test :as t]
            [crux.byte-utils :as bu]
            [crux.codecs :refer :all])
  (:import java.nio.ByteBuffer))

(defn- round-trip [f m]
  (decode f (encode f m)))

(t/deftest test-codecs-work-as-expected
  (let [f (frame :a :long :b :long)]
    (t/testing "Can encode vanilla frame"
      (encode f {:a 1 :b 2}))

    (t/testing "Can encode/decode vanilla frame"
      (t/is (= {:a 1 :b 2} (round-trip f {:a 1 :b 2})))))

  (t/testing "Can encode/decode exotic frame"
    (let [f (frame :a :long :b :md5)]
      (t/is (= 1 (:a (round-trip f {:a 1 :b (bu/md5 (.getBytes "sad"))})))))))

(t/deftest test-enums
  (let [e (enum :foo :tar)
        f (frame :a :long :b e)]
    (t/is (= {:a 1, :b :foo} (round-trip f {:a 1 :b :foo})))))

(t/deftest test-various-datatypes
  (t/testing "String datatype"
    (let [f (frame  :a :string)]
      (t/is (= {:a "hello"} (round-trip f {:a "hello"})))))

  (t/testing "Keyword datatype"
    (let [f (frame  :a :keyword)]
      (t/is (= {:a :bob} (round-trip f {:a :bob})))))

  (t/testing "Long datatype"
    (let [f (frame  :a :long)]
      (t/is (= {:a 1} (round-trip f {:a 1})))))

  (t/testing "Double datatype"
    (let [f (frame  :a :double)]
      (t/is (= {:a 1.0} (round-trip f {:a 1.0}))))))
