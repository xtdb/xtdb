(ns crux.fixtures.kv-only
  (:require [clojure.spec.alpha :as s]
            [clojure.test :as t]
            [crux.io :as cio]
            [crux.node :as n])
  (:import java.io.Closeable))

(def ^:dynamic *kv*)
(def ^:dynamic *kv-module* 'crux.kv.rocksdb/kv)
(def ^:dynamic *check-and-store-index-version* true)
(def ^:dynamic *sync* false)

(defn ^Closeable start-kv-store [opts]
  (n/start-module *kv-module* nil opts))

(defn with-kv-store [f]
  (let [db-dir (cio/create-tmpdir "kv-store")]
    (try
      (with-open [kv (start-kv-store {:crux.kv/db-dir (str db-dir)
                                      :crux.kv/sync? *sync*
                                      :crux.kv/check-and-store-index-version *check-and-store-index-version*})]
        (binding [*kv* kv]
          (f)))
      (finally
        (cio/delete-dir db-dir)))))

(defn without-kv-index-version [f]
  (binding [*check-and-store-index-version* false]
    (f)))

(defn with-memdb [f]
  (binding [*kv-module* 'crux.kv.memdb/kv]
    (t/testing "MemDB"
      (f))))

(defn with-rocksdb [f]
  (binding [*kv-module* 'crux.kv.rocksdb/kv]
    (t/testing "RocksDB"
      (f))))

(defn with-rocksdb-jnr [f]
  (binding [*kv-module* 'crux.kv.rocksdb.jnr/kv]
    (t/testing "RocksJNRDB"
      (f))))

(defn with-lmdb [f]
  (binding [*kv-module* 'crux.kv.lmdb/kv]
    (t/testing "LMDB"
      (f))))

(defn with-lmdb-jnr [f]
  (binding [*kv-module* 'crux.kv.lmdb.jnr/kv]
    (t/testing "LMDBJNR"
      (f))))

(defn with-each-kv-store-implementation [f]
  (doseq [with-kv-store-implementation [with-memdb with-rocksdb with-rocksdb-jnr with-lmdb with-lmdb-jnr]]
    (with-kv-store-implementation f)))
