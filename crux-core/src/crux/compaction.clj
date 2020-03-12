(ns crux.compaction
  (:require [clojure.set :as set]
            [clojure.tools.logging :as log]
            [crux.db :as db]
            [crux.index :as idx]
            [crux.kv :as kv]
            [crux.node :as n]
            [crux.tx :as tx])
  (:import crux.codec.EntityTx
           java.util.Calendar))

;; todo, can be integrated:
(defn- tx-events->compaction-eids [tx-events]
  (->> tx-events
       (filter (comp #{:crux.tx/put :crux.tx/cas} first))
       (map second)))

(defn- entity-txes->content-hashes [txes]
  (set (for [^EntityTx entity-tx txes]
         (.content-hash entity-tx))))

(defn compact [object-store snapshot eid valid-time tx-time]
  (with-open [i (kv/new-iterator snapshot)]
    (let [[^EntityTx tx & txes] (idx/entity-history-seq-descending i eid valid-time tx-time)
          old-content-hashes (entity-txes->content-hashes txes)
          new-content-hashes (when tx
                               (with-open [i2 (kv/new-iterator snapshot)]
                                 (entity-txes->content-hashes (idx/entity-history-seq-ascending i2 eid (.vt tx) tx-time))))
          content-hashes-to-prune (set/difference old-content-hashes new-content-hashes)]
      (when (seq content-hashes-to-prune)
        (log/debug "Pruning" content-hashes-to-prune)
        (db/delete-objects object-store content-hashes-to-prune)))))

(defn valid-time-watermark [tt-vt-interval-s tx-time]
  (.getTime
   (doto (Calendar/getInstance)
     (.setTime tx-time)
     (.add Calendar/SECOND (- tt-vt-interval-s)))))

(defrecord CompactingIndexer [indexer object-store kv-store tt-vt-interval-s]
  db/Indexer
  (index-docs [this docs]
    (db/index-docs indexer docs))
  (index-tx [this tx tx-events]
    (db/index-tx indexer tx tx-events)
    (with-open [snapshot (kv/new-snapshot kv-store)]
      (let [vt-watermark (valid-time-watermark tt-vt-interval-s (::tx/tx-time tx))]
        (doseq [eid (tx-events->compaction-eids tx-events)]
          (compact object-store snapshot eid vt-watermark (::tx/tx-time tx))))))
  (missing-docs [this content-hashes]    (db/missing-docs indexer content-hashes))
  (store-index-meta [this k v]
    (db/store-index-meta indexer k v))
  (read-index-meta [this k]
    (db/read-index-meta indexer k))
  (latest-completed-tx [this]
    (db/latest-completed-tx indexer)))

(def interval-30-days (* 60 60 24 30))

(def module
  {::indexer 'crux.tx/kv-indexer
   ::n/indexer {:start-fn (fn [{:keys [::indexer ::n/object-store ::n/kv-store]} {:keys [::tt-vt-interval-s]}]
                            (->CompactingIndexer indexer object-store kv-store tt-vt-interval-s))
                :deps [::indexer ::n/object-store ::n/kv-store]
                :args {::tt-vt-interval-s {:doc "Interval in seconds between transaction-time and valid-time watermark"
                                           :default interval-30-days
                                           :crux.config/type :crux.config/nat-int}}}})
