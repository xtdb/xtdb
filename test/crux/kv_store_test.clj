(ns crux.kv-store-test
  (:require [clojure.test :as t]
            [crux.byte-utils :as bu]
            [crux.fixtures :as f]
            [crux.kv-store :as ks])
  (:import [java.io Closeable]))

(t/use-fixtures :each f/with-each-kv-store f/start-system)

(t/deftest test-store-and-value []
  (t/testing "store and retrieve and seek value"
    (ks/store f/*kv* (bu/long->bytes 1) (.getBytes "Crux"))
    (t/is (= "Crux" (String. ^bytes (ks/value f/*kv* (bu/long->bytes 1)))))
    (t/is (= [1 "Crux"] (let [[k v] (ks/seek f/*kv* (bu/long->bytes 1))]
                          [(bu/bytes->long k) (String. ^bytes v)]))))

  (t/testing "non existing key"
    (t/is (nil? (ks/value f/*kv* (bu/long->bytes 2))))))

(t/deftest test-merge-adds-ints []
  (let [k (.getBytes (String. "foo"))]
    (t/testing "merging non existing key stores value"
      (ks/merge! f/*kv* k (bu/long->bytes 1))
      (t/is (= 1 (bu/bytes->long (ks/value f/*kv* k)))))

    (t/testing "can merge more than once"
      (ks/merge! f/*kv* k (bu/long->bytes 2))
      (t/is (= 3 (bu/bytes->long (ks/value f/*kv* k)))))

    (t/testing "store after merge resets value"
      (ks/store f/*kv* k (bu/long->bytes 1))
      (t/is (= 1 (bu/bytes->long (ks/value  f/*kv* k)))))))

(t/deftest test-can-put-all []
  (ks/put-all! f/*kv* (map (fn [i]
                             [(bu/long->bytes i) (bu/long->bytes (inc i))])
                           (range 10)))
  (doseq [i (range 10)]
    (t/is (= (inc i) (bu/bytes->long (ks/value f/*kv* (bu/long->bytes i)))))))

;; TODO will migrate :-)
#_(t/deftest test-seek-and-iterate []
  (doseq [[^String k v] {"a" 1 "b" 2 "c" 3 "d" 4}]
    (ks/store f/*kv* (.getBytes k) (bu/long->bytes v)))

  (t/testing "seek range is exclusive"
    (t/is (= [["b" 2] ["c" 3]]
             (for [[^bytes k v] (into [] (ks/seek-and-iterate f/*kv* (.getBytes "b") (.getBytes "d")))]
               [(String. k) (bu/bytes->long v)]))))

  (t/testing "seek range after existing keys returns empty"
    (t/is (= [] (into [] (ks/seek-and-iterate f/*kv* (.getBytes "d") (.getBytes "d")))))
    (t/is (= [] (into [] (ks/seek-and-iterate f/*kv* (.getBytes "e") (.getBytes "f"))))))

  (t/testing "seek range before existing keys returns keys at start"
    (t/is (= [["a" 1]] (for [[^bytes k v] (into [] (ks/seek-and-iterate f/*kv* (.getBytes "0") (.getBytes "b")))]
                         [(String. k) (bu/bytes->long v)])))))

(t/deftest test-seek-and-iterate []
  (doseq [[^String k v] {"aa" 1 "b" 2 "bb" 3 "bcc" 4 "bd" 5 "dd" 6}]
    (ks/store f/*kv* (.getBytes k) (bu/long->bytes v)))

  (t/testing "seek within bounded prefix returns all matching keys"
    (t/is (= [["b" 2] ["bb" 3] ["bcc" 4] ["bd" 5]]
             (for [[^bytes k v] (into [] (ks/seek-and-iterate f/*kv* (partial bu/bytes=? (.getBytes "b")) (.getBytes "b")))]
               [(String. k) (bu/bytes->long v)]))))

  (t/testing "seek within bounded prefix before or after existing keys returns empty"
    (t/is (= [] (into [] (ks/seek-and-iterate f/*kv* (partial bu/bytes=? (.getBytes "0")) (.getBytes "0")))))
    (t/is (= [] (into [] (ks/seek-and-iterate f/*kv* (partial bu/bytes=? (.getBytes "e")) (.getBytes "0")))))))
