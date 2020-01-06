(ns crux.hook
  (:require [crux.index :as idx]
            [crux.db :as db]
            [crux.status :as status])
  (:import java.io.Closeable))

(defprotocol TxHooks
  (add-doc-hook! [this hook])
  (add-tx-hook! [this hook]))

;; (add-hook! this [init, end])

(defn wrap-index-hooks [indexer]
  (let [!doc-hooks (atom [])
        !tx-hooks (atom [])]
    (reify
      TxHooks
      (add-doc-hook! [_ hook]
        (swap! !doc-hooks conj hook))
      (add-tx-hook! [_ hook]
        (swap! !tx-hooks conj hook))

      Closeable
      (close [this]
        (.close indexer))

      status/Status
      (status-map [_]
        (status/status-map indexer))

      db/Indexer
      (index-doc [this content-hash doc]
        (let [latter-fns (seq (map (fn [f] (f {:content-hash content-hash
                                               :doc doc}))
                                   @!doc-hooks))]
          (let [ret (db/index-doc indexer content-hash doc)]
            (seq (map (fn [f] (f {:result ret}))
                      latter-fns))
            ret)))

      (index-tx [this tx-events tx-time tx-id]
        (let [latter-fns (seq (map (fn [f] (f {:tx-events tx-events
                                               :tx-time tx-time
                                               :tx-id tx-id}))
                                   @!tx-hooks))]
          (let [ret (db/index-tx indexer tx-events tx-time tx-id)]
            (seq (map (fn [f] (f {:result ret}))
                      latter-fns))
            ret)))

      (docs-exist? [this content-hashes]
        (db/docs-exist? indexer content-hashes))

      (store-index-meta [this k v]
        (db/store-index-meta indexer k v))

      (read-index-meta [this k]
        (db/read-index-meta indexer k)))))
