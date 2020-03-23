(ns crux.compaction
  (:require [clojure.set :as set]
            [clojure.tools.logging :as log]
            [crux.bus :as bus]
            [crux.db :as db]
            [crux.index :as idx]
            [crux.kv :as kv]
            [crux.node :as n]
            [crux.tx :as tx])
  (:import crux.codec.EntityTx
           java.util.Calendar
           java.util.Date))

(defn- tx-events->compaction-eids [tx-events]
  (->> tx-events
       (filter (comp #{:crux.tx/put :crux.tx/cas} first))
       (map second)
       (distinct)))

(defn- entity-txes->content-hashes [txes]
  (set (for [^EntityTx entity-tx txes]
         (.content-hash entity-tx))))

(defn txes-to-compact [interval-s txes]
  (if interval-s
    (->> txes
         (partition-by
          (fn [^EntityTx tx]
            (int (/ (.getTime ^Date (.vt tx)) (* interval-s 1000)))))
         (mapcat rest))
    ;; All txes:
    txes))

(defn- compact [{:keys [object-store kv-store bus compact-interval-s]} snapshot eid valid-time tx-time]
  (with-open [i (kv/new-iterator snapshot)]
    (let [[^EntityTx tx & txes] (->> (idx/entity-history-seq-descending i eid valid-time tx-time)
                                     (take-while (fn [^EntityTx tx]
                                                   (db/get-single-object object-store snapshot (.content-hash tx))))
                                     (txes-to-compact compact-interval-s))
          old-content-hashes (entity-txes->content-hashes txes)
          new-content-hashes (when tx
                               (with-open [i2 (kv/new-iterator snapshot)]
                                 (entity-txes->content-hashes (idx/entity-history-seq-ascending i2 eid (.vt tx) tx-time))))
          content-hashes-to-prune (set/difference old-content-hashes new-content-hashes)]
      (when (seq content-hashes-to-prune)
        (log/debug "Pruning" content-hashes-to-prune)
        (->> (for [c content-hashes-to-prune]
               (idx/doc-idx-keys c (db/get-single-object object-store snapshot c)))
             (reduce into [])
             (idx/delete-doc-idx-keys kv-store))
        (db/delete-objects object-store content-hashes-to-prune))
      (bus/send bus {:crux.bus/event-type ::compacted
                     ::compacted-count (count content-hashes-to-prune)}))))

(defn valid-time-watermark [tt-vt-interval-s tx-time]
  (.getTime
   (doto (Calendar/getInstance)
     (.setTime tx-time)
     (.add Calendar/SECOND (- tt-vt-interval-s)))))

(defrecord CompactingIndexer [indexer object-store kv-store bus tt-vt-interval-s compact-interval-s]
  db/Indexer
  (index-docs [this docs]
    (db/index-docs indexer docs))
  (index-tx [this tx tx-events]
    (db/index-tx indexer tx tx-events)
    (with-open [snapshot (kv/new-snapshot kv-store)]
      (let [vt-watermark (valid-time-watermark tt-vt-interval-s (::tx/tx-time tx))]
        (doseq [eid (tx-events->compaction-eids tx-events)]
          (compact this snapshot eid vt-watermark (::tx/tx-time tx))))))
  (missing-docs [this content-hashes]
    (db/missing-docs indexer content-hashes))
  (store-index-meta [this k v]
    (db/store-index-meta indexer k v))
  (read-index-meta [this k]
    (db/read-index-meta indexer k))
  (latest-completed-tx [this]
    (db/latest-completed-tx indexer)))

(def interval-30-days (* 60 60 24 30))

(def module
  {::indexer 'crux.tx/kv-indexer
   ::n/indexer {:start-fn (fn [{:keys [::indexer ::n/object-store ::n/kv-store ::n/bus]}
                               {:keys [::tt-vt-interval-s ::compact-interval-s]}]
                            (->CompactingIndexer indexer object-store kv-store bus tt-vt-interval-s compact-interval-s))
                :deps [::indexer ::n/object-store ::n/kv-store ::n/bus]
                :args {::tt-vt-interval-s {:doc "Interval in seconds between transaction-time and valid-time watermark"
                                           :default interval-30-days
                                           :crux.config/type :crux.config/nat-int}
                       ::compact-interval-s {:doc "Compaction interval, example 1 day = 86400"
                                             :default nil
                                             :crux.config/type :crux.config/nat-int}}}})
