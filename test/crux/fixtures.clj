(ns crux.fixtures
  (:require [crux.kv :as cr]
            [crux.kv-store :as kv-store]
            [crux.core]
            [crux.rocksdb]
            [crux.lmdb]
            [crux.memdb]
            [crux.test-utils :as tu])
  (:import [java.util Date]))

(def next-eid (atom 0))

(defn random-person [] {:crux.kv/id (swap! next-eid dec)
                        :name      (rand-nth ["Ivan" "Petr" "Sergei" "Oleg" "Yuri" "Dmitry" "Fedor" "Denis"])
                        :last-name (rand-nth ["Ivanov" "Petrov" "Sidorov" "Kovalev" "Kuznetsov" "Voronoi"])
                        :sex       (rand-nth [:male :female])
                        :age       (rand-int 10)
                        :salary    (rand-int 100000)})

(def people (repeatedly random-person))

(defn transact-people!
  ([db people-mixins]
   (transact-people! db people-mixins (Date.)))
  ([db people-mixins ts]
   (let [people (->> people-mixins (map #(merge %1 %2) people))
         ids (cr/-put db people ts)]
     (map #(update % :crux.kv/id ids) people))))

(def ^:dynamic *kv*)
(def ^:dynamic *kv-store* (crux.rocksdb/map->CruxRocksKv {}))

(defn start-system [f]
  (let [db-dir (tu/create-tmpdir "test")]
    (binding [*kv* (kv-store/open (crux.core/kv db-dir {:kv-store *kv-store*}))]
      (try
        (f)
        (finally
          (kv-store/close *kv*)
          (kv-store/destroy *kv*)
          (tu/delete-dir db-dir))))))

(defn with-rocksdb [f]
  (binding [*kv-store* (crux.rocksdb/map->CruxRocksKv {})]
    (f)))

(defn with-memdb [f]
  (binding [*kv-store* (crux.memdb/map->CruxMemKv {})]
    (f)))

(defn with-lmdb [f]
  (binding [*kv-store* (crux.lmdb/map->CruxLMDBKv {})]
    (f)))
