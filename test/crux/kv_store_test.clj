(ns crux.kv-store-test
  (:require [clojure.test :as t]
            [crux.byte-utils :as bu]
            [crux.fixtures :as f]
            [crux.kv-store :as ks]
            [crux.io :as cio])
  (:import [java.io Closeable]))

(t/use-fixtures :each f/with-each-kv-store-implementation f/with-kv-store)

(t/deftest test-store-and-value []
  (t/testing "store and retrieve and seek value"
    (ks/store f/*kv* (bu/long->bytes 1) (.getBytes "Crux"))
    (t/is (= "Crux" (String. ^bytes (ks/value f/*kv* (bu/long->bytes 1)))))
    (t/is (= [1 "Crux"] (let [[k v] (ks/seek f/*kv* (bu/long->bytes 1))]
                          [(bu/bytes->long k) (String. ^bytes v)]))))

  (t/testing "non existing key"
    (t/is (nil? (ks/value f/*kv* (bu/long->bytes 2))))))

(t/deftest test-can-store-all []
  (ks/store-all! f/*kv* (map (fn [i]
                               [(bu/long->bytes i) (bu/long->bytes (inc i))])
                             (range 10)))
  (doseq [i (range 10)]
    (t/is (= (inc i) (bu/bytes->long (ks/value f/*kv* (bu/long->bytes i)))))))

(t/deftest test-seek-and-iterate-range []
  (doseq [[^String k v] {"a" 1 "b" 2 "c" 3 "d" 4}]
    (ks/store f/*kv* (.getBytes k) (bu/long->bytes v)))

  (t/testing "seek range is exclusive"
    (t/is (= [["b" 2] ["c" 3]]
             (for [[^bytes k v] (into [] (ks/seek-and-iterate f/*kv*
                                                              #(neg? (bu/compare-bytes % (.getBytes "d")))
                                                              (.getBytes "b") ))]
               [(String. k) (bu/bytes->long v)]))))

  (t/testing "seek range after existing keys returns empty"
    (t/is (= [] (into [] (ks/seek-and-iterate f/*kv* #(neg? (bu/compare-bytes % (.getBytes "d"))) (.getBytes "d")))))
    (t/is (= [] (into [] (ks/seek-and-iterate f/*kv* #(neg? (bu/compare-bytes % (.getBytes "f")%)) (.getBytes "e"))))))

  (t/testing "seek range before existing keys returns keys at start"
    (t/is (= [["a" 1]] (for [[^bytes k v] (into [] (ks/seek-and-iterate f/*kv* #(neg? (bu/compare-bytes % (.getBytes "b"))) (.getBytes "0")))]
                         [(String. k) (bu/bytes->long v)])))))

(t/deftest test-seek-and-iterate-prefix []
  (doseq [[^String k v] {"aa" 1 "b" 2 "bb" 3 "bcc" 4 "bd" 5 "dd" 6}]
    (ks/store f/*kv* (.getBytes k) (bu/long->bytes v)))

  (t/testing "seek within bounded prefix returns all matching keys"
    (t/is (= [["b" 2] ["bb" 3] ["bcc" 4] ["bd" 5]]
             (for [[^bytes k v] (into [] (ks/seek-and-iterate f/*kv* (partial bu/bytes=? (.getBytes "b")) (.getBytes "b")))]
               [(String. k) (bu/bytes->long v)]))))

  (t/testing "seek within bounded prefix before or after existing keys returns empty"
    (t/is (= [] (into [] (ks/seek-and-iterate f/*kv* (partial bu/bytes=? (.getBytes "0")) (.getBytes "0")))))
    (t/is (= [] (into [] (ks/seek-and-iterate f/*kv* (partial bu/bytes=? (.getBytes "e")) (.getBytes "0")))))))


(t/deftest test-backup-and-restore-db
  (if (instance? crux.memdb.CruxMemKv f/*kv-store*)
    (t/is true "skipping")
    (let [backup-dir (cio/create-tmpdir "kv-store-backup")]
      (try
        (ks/store f/*kv* (bu/long->bytes 1) (.getBytes "Crux"))
        (ks/backup f/*kv* backup-dir)
        (with-open [restored-kv ^Closeable (ks/open (crux.core/kv backup-dir {:kv-store f/*kv-store*}))]
          (t/is (= "Crux" (String. ^bytes (ks/value restored-kv (bu/long->bytes 1)))))

          (t/testing "backup and original are different"
            (ks/store f/*kv* (bu/long->bytes 1) (.getBytes "Original"))
            (ks/store restored-kv (bu/long->bytes 1) (.getBytes "Backup"))
            (t/is (= "Original" (String. ^bytes (ks/value f/*kv* (bu/long->bytes 1)))))
            (t/is (= "Backup" (String. ^bytes (ks/value restored-kv (bu/long->bytes 1)))))))
        (finally
          (cio/delete-dir backup-dir))))))
