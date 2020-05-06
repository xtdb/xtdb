(ns crux.fixtures.kv
  (:require [crux.fixtures :as f]
            [crux.io :as cio]
            [clojure.test :as t]
            [crux.fixtures :as fix]))

(defn ^:deprecated with-kv-dir [f]
  (fix/with-kv-dir f))

(defn with-kv-store [kv-store f]
  (fix/with-opts {:crux.node/kv-store kv-store} f))

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
