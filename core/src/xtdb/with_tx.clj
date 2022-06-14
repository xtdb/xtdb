(ns xtdb.with-tx
  (:require [xtdb.fork :as fork]
            [xtdb.kv.index-store :as index-store]
            [xtdb.kv.mutable-kv :as mut-kv]
            [xtdb.db :as db]
            [xtdb.cache]))

(defn ->forked-index-store-tx [index-store, valid-time, tx-id, tx]
  (let [delta-index-store (index-store/->kv-index-store {:kv-store (mut-kv/->mutable-kv-store)
                                                         :cav-cache (xtdb.cache/->cache {:cache-size (* 128 1024)})
                                                         :canonical-buffer-cache (xtdb.cache/->cache {:cache-size (* 128 1024)})
                                                         :stats-kvs-cache (xtdb.cache/->cache {:cache-size (* 128 1024)})})
        forked-index-store (fork/->ForkedIndexStore index-store delta-index-store valid-time, tx-id)]
    (db/begin-index-tx forked-index-store tx)))
