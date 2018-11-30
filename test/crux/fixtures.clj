(ns crux.fixtures
  (:require [clojure.test :as t]
            [crux.api :as api]
            [crux.bootstrap :as b]
            [crux.db :as db]
            [crux.io :as cio]
            [crux.embedded-kafka :as ek]
            [crux.http-server :as srv]
            [crux.kv-store :as ks]
            [crux.tx :as tx])
  (:import java.io.Closeable
           java.util.UUID))

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
   (transact-entity-maps! kv entities (cio/next-monotonic-date)))
  ([kv entities ts]
   (let [tx-log (tx/->DocTxLog kv)
         tx-ops (maps->tx-ops entities ts)]
     @(db/submit-tx tx-log tx-ops)
     entities)))

(defn entities->delete-tx-ops [entities ts]
  (vec (for [e entities]
         [:crux.tx/delete e ts])))

(defn delete-entities!
  ([kv entities]
   (delete-entities! kv entities (cio/next-monotonic-date)))
  ([kv entities ts]
   (let [tx-log (tx/->DocTxLog kv)
         tx-ops (entities->delete-tx-ops entities ts)]
     @(db/submit-tx tx-log tx-ops)
     entities)))

(defn transact-people!
  ([kv people-mixins]
   (transact-people! kv people-mixins (cio/next-monotonic-date)))
  ([kv people-mixins ts]
   (transact-entity-maps! kv (->> people-mixins (map merge (repeatedly random-person))) ts)))

(def ^:dynamic *kv*)
(def ^:dynamic *kv-backend* "crux.rocksdb.RocksKv")

(defn with-kv-store [f]
  (let [db-dir (cio/create-tmpdir "kv-store")]
    (binding [*kv* (b/start-kv-store {:db-dir db-dir :kv-backend *kv-backend*})]
      (try
        (with-open [*kv* ^Closeable *kv*]
          (f))
        (finally
          (cio/delete-dir db-dir))))))

(defn with-memdb [f]
  (binding [*kv-backend* "crux.memdb.MemKv"]
    (t/testing "MemDB"
      (f))))

(defn with-rocksdb [f]
  (binding [*kv-backend* "crux.rocksdb.RocksKv"]
    (t/testing "RocksDB"
      (f))))

(defn with-lmdb [f]
  (binding [*kv-backend* "crux.lmdb.LMDBKv"]
    (t/testing "LMDB"
      (f))))

(defn with-each-kv-store-implementation [f]
  (doseq [with-kv-store-implementation [with-memdb with-rocksdb with-lmdb]]
    (with-kv-store-implementation f)))

(def ^:dynamic ^String *host* "localhost")
(def ^:dynamic *api-url*)

(defn with-local-node [f]
  (assert *kv*)
  (assert ek/*kafka-bootstrap-servers*)
  (let [server-port (cio/free-port)
        db-dir (ks/db-dir *kv*)]
    (.close ^Closeable *kv*)
    (binding [*api-url* (str "http://" *host* ":" server-port)]
      (with-open [local-node (api/start-local-node {:server-port server-port
                                                    :db-dir db-dir
                                                    :kv-backend *kv-backend*
                                                    :bootstrap-servers ek/*kafka-bootstrap-servers*})]
        (f)))))
