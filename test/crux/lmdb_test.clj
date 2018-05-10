(ns crux.lmdb-test
  (:require [clojure.test :as t]
            [crux.byte-utils :as bu]
            [crux.test-utils :as tu]
            [crux.kv-store :as ks]
            [crux.lmdb :as lmdb])
  (:import [java.io Closeable]))

;; Based on
;; https://github.com/LWJGL/lwjgl3/blob/master/modules/samples/src/test/java/org/lwjgl/demo/util/lmdb/LMDBDemo.java
(t/deftest test-lmdb-demo []
  (let [db-dir (tu/create-tmpdir "lmdb")]
    (try
      (with-open [kv ^Closeable (ks/open (lmdb/map->CruxLMDBKv {:db-dir db-dir}))]
        (ks/store kv (bu/long->bytes 1) (.getBytes "LMDB"))
        (t/is (= "LMDB" (String. ^bytes (ks/seek kv (bu/long->bytes 1))))))
      (finally
        (tu/delete-dir db-dir)))))

(t/deftest test-merge-adds-ints []
  (let [db-dir (tu/create-tmpdir "lmdb")
        k (.getBytes (String. "foo"))]
    (try
      (with-open [kv ^Closeable (ks/open (lmdb/map->CruxLMDBKv {:db-dir db-dir}))]
        (ks/store kv k (bu/long->bytes 1))
        (t/is (= 1 (bu/bytes->long (ks/seek kv k))))

        (ks/merge! kv k (bu/long->bytes 2))
        (t/is (= 3 (bu/bytes->long (ks/seek kv k)))))
      (finally
        (tu/delete-dir db-dir)))))


(t/deftest test-seek-and-iterate []
  (let [db-dir (tu/create-tmpdir "lmdb")]
    (try
      (with-open [kv ^Closeable (ks/open (lmdb/map->CruxLMDBKv {:db-dir db-dir}))]
        (doseq [[^String k v] {"a" 1 "b" 2 "c" 3 "d" 4}]
          (ks/store kv (.getBytes k) (bu/long->bytes v)))
        (t/is (= [["b" 2] ["c" 3]]
                 (for [[^bytes k v] (ks/seek-and-iterate kv (.getBytes "b") (.getBytes "d"))]
                   [(String. k) (bu/bytes->long v)]))))
      (finally
        (tu/delete-dir db-dir)))))

(t/deftest test-seek-and-iterate-bounded []
  (let [db-dir (tu/create-tmpdir "lmdb")]
    (try
      (with-open [kv ^Closeable (ks/open (lmdb/map->CruxLMDBKv {:db-dir db-dir}))]
        (doseq [[^String k v] {"aa" 1 "b" 2 "bb" 3 "bcc" 4 "bd" 5 "dd" 6}]
          (ks/store kv (.getBytes k) (bu/long->bytes v)))
        (t/is (= [["b" 2] ["bb" 3] ["bcc" 4] ["bd" 5]]
                 (for [[^bytes k v] (ks/seek-and-iterate-bounded kv (.getBytes "b"))]
                   [(String. k) (bu/bytes->long v)]))))
      (finally
        (tu/delete-dir db-dir)))))
