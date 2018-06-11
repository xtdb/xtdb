(ns crux.fixtures
  (:require [crux.kv-store :as kv-store]
            [crux.bootstrap]
            [crux.db]
            [crux.doc]
            [crux.tx]
            [crux.rocksdb]
            [crux.lmdb]
            [crux.memdb]
            [crux.io :as cio]
            [clojure.test :as t])
  (:import [java.io Closeable]
           [java.util Date UUID]))

(defn random-person [] {:crux.db/id (str (UUID/randomUUID))
                        :name      (rand-nth ["Ivan" "Petr" "Sergei" "Oleg" "Yuri" "Dmitry" "Fedor" "Denis"])
                        :last-name (rand-nth ["Ivanov" "Petrov" "Sidorov" "Kovalev" "Kuznetsov" "Voronoi"])
                        :sex       (rand-nth [:male :female])
                        :age       (rand-int 100)
                        :salary    (rand-int 100000)})

(def people (repeatedly random-person))

(defn people->tx-ops [people ts]
  (vec (for [person people]
         [:crux.tx/put
          (let [id (:crux.db/id person)]
            (if (keyword? id)
              id
              (UUID/fromString id)))
          person
          ts])))

(defn transact-people!
  ([db people-mixins]
   (transact-people! db people-mixins (Date.)))
  ([db people-mixins ts]
   (let [tx-log (crux.tx/->DocTxLog db)
         people (->> people-mixins (map #(merge %1 %2) people))
         tx-ops (people->tx-ops people ts)]
     @(crux.db/submit-tx tx-log tx-ops)
     people)))

(def ^:dynamic *kv*)
(def ^:dynamic *kv-backend* "rocksdb")

(defn with-kv-store [f]
  (let [db-dir (cio/create-tmpdir "kv-store")]
    (binding [*kv* (crux.bootstrap/start-kv-store {:db-dir db-dir :kv-backend *kv-backend*})]
      (try
        (with-open [*kv* ^Closeable *kv*]
          (f))
        (finally
          (cio/delete-dir db-dir))))))

(defn with-memdb [f]
  (binding [*kv-backend* "memdb"]
    (t/testing "MemDB"
      (f))))

(defn with-rocksdb [f]
  (binding [*kv-backend* "rocksdb"]
    (t/testing "RocksDB"
      (f))))

(defn with-lmdb [f]
  (binding [*kv-backend* "lmdb"]
    (t/testing "LMDB"
      (f))))

(defn with-each-kv-store-implementation [f]
  (doseq [with-kv-store-implementation [with-memdb with-rocksdb with-lmdb]]
    (with-kv-store-implementation f)))
