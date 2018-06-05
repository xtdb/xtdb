(ns crux.fixtures
  (:require [crux.kv :as cr]
            [crux.kv-store :as kv-store]
            [crux.core]
            [crux.db]
            [crux.doc]
            [crux.rocksdb]
            [crux.lmdb]
            [crux.memdb]
            [crux.io :as cio]
            [clojure.test :as t])
  (:import [java.io Closeable]
           [java.util Date UUID]))

(defn random-person [] {:crux.kv/id (str (UUID/randomUUID))
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

;; TODO: this should either be simplified or cleaned up, depending on if
;; we keep KV.
(defn with-query-doc-index [f]
  (let [tx-log (crux.doc/->DocTxLog *kv*)]
    (with-redefs [crux.core/db crux.doc/db
                  transact-people! (fn this
                                     ([kv people-mixins]
                                      (this kv people-mixins (Date.)))
                                     ([kv people-mixins ts]
                                      (let [people (->> people-mixins (map #(merge %1 %2) people))
                                            tx-ops (vec (for [person people]
                                                          [:crux.tx/put
                                                           (let [id (:crux.kv/id person)]
                                                             (if (keyword? id)
                                                               id
                                                               (UUID/fromString id)))
                                                           person
                                                           ts]))]
                                        @(crux.db/submit-tx tx-log tx-ops)
                                        people)))]
      (t/testing "Doc"
        (f)))))

(defn with-query-kv-index [f]
  (t/testing "KV"
    (f)))

(defn with-each-query-index-implementation [f]
  (doseq [with-query-index-implementation [with-query-kv-index with-query-doc-index]]
    (with-query-index-implementation f)))
