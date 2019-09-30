(ns crux.fixtures.kv
  (:require [crux.fixtures.api :as apif]
            [crux.io :as cio]
            [clojure.test :as t]))

(defn with-kv-dir [f]
  (let [db-dir (cio/create-tmpdir "kv-store")]
    (try
      (apif/with-opts {:crux.kv/db-dir (str db-dir)} f)
      (finally
        (cio/delete-dir db-dir)))))

(defn with-kv-backend [kv-backend f]
  (apif/with-opts {:kv-backend kv-backend} f))

(defn with-memdb [f]
  (with-kv-backend "crux.kv.memdb.MemKv"
    (fn []
      (t/testing "MemDB"
        (f)))))

(defn with-rocksdb [f]
  (with-kv-backend "crux.kv.rocksdb.RocksKv"
    (fn []
      (t/testing "RocksDB"
        (f)))))

(defn with-rocksdb-jnr [f]
  (with-kv-backend "crux.kv.rocksdb.jnr.RocksJNRKv"
    (fn []
      (t/testing "RocksJNRDB"
        (f)))))

(defn with-lmdb [f]
  (with-kv-backend "crux.kv.lmdb.LMDBKv"
    (fn []
      (t/testing "LMDB"
        (f)))))

(defn with-lmdb-jnr [f]
  (with-kv-backend "crux.kv.lmdb.jnr.LMDBJNRKv"
    (fn []
      (t/testing "LMDBJNR"
        (f)))))
