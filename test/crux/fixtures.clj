(ns crux.fixtures
  (:require [crux.kv :as cr]
            [crux.kv-store :as kv-store]
            [crux.core]
            [crux.rocksdb]
            [crux.lmdb]
            [crux.memdb]
            [crux.io :as cio]
            [clojure.test :as t])
  (:import [java.io Closeable]
           [java.util Date]))

(defn random-person [] {:crux.kv/id (str (java.util.UUID/randomUUID))
                        :name      (rand-nth ["Ivan" "Petr" "Sergei" "Oleg" "Yuri" "Dmitry" "Fedor" "Denis"])
                        :last-name (rand-nth ["Ivanov" "Petrov" "Sidorov" "Kovalev" "Kuznetsov" "Voronoi"])
                        :sex       (rand-nth [:male :female])
                        :age       (rand-int 100)
                        :salary    (rand-int 100000)})

(def people (repeatedly random-person))

(defn transact-people!
  ([db people-mixins]
   (transact-people! db people-mixins (Date.)))
  ([db people-mixins ts]
   (let [people (->> people-mixins (map #(merge %1 %2) people))]
     (cr/-put db people ts)
     people)))

(def ^:dynamic *kv*)
(def ^:dynamic *kv-store* (crux.rocksdb/map->RocksKv {}))

(defn with-kv-store [f]
  (let [db-dir (cio/create-tmpdir "kv-store")]
    (binding [*kv* (kv-store/open (crux.core/kv db-dir {:kv-store *kv-store*}))]
      (try
        (with-open [*kv* ^Closeable *kv*]
          (f))
        (finally
          (cio/delete-dir db-dir))))))

(defn with-memdb [f]
  (binding [*kv-store* (crux.memdb/map->MemKv {})]
    (t/testing "MemDB"
      (f))))

(defn with-rocksdb [f]
  (binding [*kv-store* (crux.rocksdb/map->RocksKv {})]
    (t/testing "RocksDB"
      (f))))

(defn with-lmdb [f]
  (binding [*kv-store* (crux.lmdb/map->LMDBKv {})]
    (t/testing "LMDB"
      (f))))

(defn with-each-kv-store-implementation [f]
  (doseq [with-kv-store-implementation [with-memdb with-rocksdb with-lmdb]]
    (with-kv-store-implementation f)))
