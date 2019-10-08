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
  (apif/with-opts {:kv-store kv-backend} f))

(defn with-memdb [f]
  (with-kv-backend 'crux.kv.memdb/kv
    (fn []
      (t/testing "MemDB"
        (f)))))

(defn with-rocksdb [f]
  (with-kv-backend 'crux.kv.rocksdb/kv
    (fn []
      (t/testing "RocksDB"
        (f)))))

(defn with-rocksdb-jnr [f]
  (with-kv-backend 'crux.kv.rocksdb.jnr/kv
    (fn []
      (t/testing "RocksJNRDB"
        (f)))))

(defn with-lmdb [f]
  (with-kv-backend 'crux.kv.lmdb/kv
    (fn []
      (t/testing "LMDB"
        (f)))))

(defn with-lmdb-jnr [f]
  (with-kv-backend 'crux.kv.lmdb.jnr/kv
    (fn []
      (t/testing "LMDBJNR"
        (f)))))
