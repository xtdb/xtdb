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

(defn random-person [] {:crux.db/id (UUID/randomUUID)
                        :name      (rand-nth ["Ivan" "Petr" "Sergei" "Oleg" "Yuri" "Dmitry" "Fedor" "Denis"])
                        :last-name (rand-nth ["Ivanov" "Petrov" "Sidorov" "Kovalev" "Kuznetsov" "Voronoi"])
                        :sex       (rand-nth [:male :female])
                        :age       (rand-int 100)
                        :salary    (rand-int 100000)})

(defn maps->tx-ops [maps ts]
  (vec (for [m maps]
         [:crux.tx/put
          (:crux.db/id m)
          m
          ts])))

(defn transact-entity-maps!
  ([kv entities]
   (transact-entity-maps! kv entities (Date.)))
  ([kv entities ts]
   (let [tx-log (crux.tx/->DocTxLog kv)
         tx-ops (maps->tx-ops entities ts)]
     @(crux.db/submit-tx tx-log tx-ops)
     entities)))

(defn entities->delete-tx-ops [entities ts]
  (vec (for [e entities]
         [:crux.tx/delete e ts])))

(defn delete-entities!
  ([kv entities]
   (delete-entities! kv entities (Date.)))
  ([kv entities ts]
   (let [tx-log (crux.tx/->DocTxLog kv)
         tx-ops (entities->delete-tx-ops entities ts)]
     @(crux.db/submit-tx tx-log tx-ops)
     entities)))

(defn transact-people!
  ([kv people-mixins]
   (transact-people! kv people-mixins (Date.)))
  ([kv people-mixins ts]
   (transact-entity-maps! kv (->> people-mixins (map merge (repeatedly random-person))) ts)))

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
