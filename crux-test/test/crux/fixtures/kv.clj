(ns crux.fixtures.kv
  (:require [crux.fixtures :as f]
            [crux.fixtures.api :as apif]
            [crux.io :as cio]
            [clojure.test :as t]))

(defn with-kv-dir [f]
  (f/with-tmp-dir "kv-store" [db-dir]
    (apif/with-opts {:crux.kv/db-dir (str db-dir)} f)))

(defn with-kv-store [kv-store f]
  (apif/with-opts {:crux.node/kv-store kv-store} f))

(defn with-memdb [f]
  (with-kv-store 'crux.kv.memdb/kv
    (fn []
      (t/testing "MemDB"
        (f)))))

(defn with-rocksdb [f]
  (with-kv-store 'crux.kv.rocksdb/kv
    (fn []
      (t/testing "RocksDB"
        (f)))))

(defn with-rocksdb-jnr [f]
  (with-kv-store 'crux.kv.rocksdb.jnr/kv
    (fn []
      (t/testing "RocksJNRDB"
        (f)))))

(defn with-lmdb [f]
  (with-kv-store 'crux.kv.lmdb/kv
    (fn []
      (t/testing "LMDB"
        (f)))))

(defn with-lmdb-jnr [f]
  (with-kv-store 'crux.kv.lmdb.jnr/kv
    (fn []
      (t/testing "LMDBJNR"
        (f)))))
