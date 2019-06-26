(ns crux.fixtures.kv
  (:require [clojure.test :as t]
            [crux.bootstrap :as b]
            [crux.index :as idx]
            [crux.io :as cio])
  (:import java.io.Closeable))

(def ^:dynamic *kv*)
(def ^:dynamic *kv-backend* "crux.kv.rocksdb.RocksKv")
(def ^:dynamic *check-and-store-index-version* true)

(defn without-kv-index-version [f]
  (binding [*check-and-store-index-version* false]
    (f)))

(defn with-kv-store [f]
  (let [db-dir (cio/create-tmpdir "kv-store")]
    (try
      (binding [*kv* (b/start-kv-store {:db-dir (str db-dir)
                                        :kv-backend *kv-backend*
                                        :crux.index/check-and-store-index-version *check-and-store-index-version*})]
        (with-open [*kv* ^Closeable *kv*]
          (f)))
      (finally
        (cio/delete-dir db-dir)))))

(defn with-memdb [f]
  (binding [*kv-backend* "crux.kv.memdb.MemKv"]
    (t/testing "MemDB"
      (f))))

(defn with-rocksdb [f]
  (binding [*kv-backend* "crux.kv.rocksdb.RocksKv"]
    (t/testing "RocksDB"
      (f))))

(defn with-rocksdb-jnr [f]
  (binding [*kv-backend* "crux.kv.rocksdb.jnr.RocksJNRKv"]
    (t/testing "RocksJNRDB"
      (f))))

(defn with-lmdb [f]
  (binding [*kv-backend* "crux.kv.lmdb.LMDBKv"]
    (t/testing "LMDB"
      (f))))

(defn with-lmdb-jnr [f]
  (binding [*kv-backend* "crux.kv.lmdb.jnr.LMDBJNRKv"]
    (t/testing "LMDBJNR"
      (f))))

(defn with-each-kv-store-implementation [f]
  (doseq [with-kv-store-implementation [with-memdb with-rocksdb with-rocksdb-jnr with-lmdb with-lmdb-jnr]]
    (with-kv-store-implementation f)))
