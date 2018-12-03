(ns crux.tx
  (:require [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [crux.byte-utils :as bu]
            [crux.db :as db]
            [crux.doc :as doc]
            [crux.index :as idx]
            [crux.io :as cio]
            [crux.kv-store :as ks])
  (:import crux.index.EntityTx
           [java.io Closeable Writer]))

(set! *unchecked-math* :warn-on-boxed)

(s/def ::id (s/conformer (comp str idx/new-id)))
(s/def ::doc (s/and (s/or :doc (s/and map? (s/conformer (comp str idx/new-id)))
                          :content-hash ::id)
                    (s/conformer second)))

(s/def ::put-op (s/cat :op #{:crux.tx/put}
                       :id ::id
                       :doc ::doc
                       :start-business-time (s/? inst?)
                       :end-business-time (s/? inst?)))

(s/def ::delete-op (s/cat :op #{:crux.tx/delete}
                          :id ::id
                          :start-business-time (s/? inst?)
                          :end-business-time (s/? inst?)))

(s/def ::cas-op (s/cat :op #{:crux.tx/cas}
                       :id ::id
                       :old-doc ::doc
                       :new-doc ::doc
                       :at-business-time (s/? inst?)))

(s/def ::evict-op (s/cat :op #{:crux.tx/evict}
                         :id ::id
                         :start-business-time (s/? inst?)
                         :end-business-time (s/? inst?)))

(s/def ::tx-op (s/and (s/or :put ::put-op
                            :delete ::delete-op
                            :cas ::cas-op
                            :evict ::evict-op)
                      (s/conformer (comp vec vals second))))

(s/def ::tx-ops (s/coll-of ::tx-op :kind vector?))

(defmulti tx-command (fn [kv snapshot object-store tx-log [op] transact-time tx-id] op))

(defn- in-range-pred [start end]
  #(and (or (nil? start)
            (not (pos? (compare start %))))
        (or (nil? end)
            (neg? (compare % end)))))

(defn- put-delete-kvs [snapshot k start-business-time end-business-time transact-time tx-id content-hash-bytes]
  (let [eid (idx/new-id k)
        start-business-time (or start-business-time transact-time)
        dates-in-history (when end-business-time
                           (map :bt (doc/entity-history snapshot eid)))
        dates-to-correct (->> (cons start-business-time dates-in-history)
                              (filter (in-range-pred start-business-time end-business-time))
                              (into (sorted-set)))]
    {:kvs (vec (for [business-time dates-to-correct]
                 [(idx/encode-entity+bt+tt+tx-id-key
                   (idx/id->bytes eid)
                   business-time
                   transact-time
                   tx-id)
                  content-hash-bytes]))}))

(defmethod tx-command :crux.tx/put [kv snapshot object-store tx-log [op k v start-business-time end-business-time] transact-time tx-id]
  (put-delete-kvs snapshot k start-business-time end-business-time transact-time tx-id (idx/id->bytes (idx/new-id v))))

(defmethod tx-command :crux.tx/delete [kv snapshot object-store tx-log [op k start-business-time end-business-time] transact-time tx-id]
  (put-delete-kvs snapshot k start-business-time end-business-time transact-time tx-id idx/nil-id-bytes))

(defmethod tx-command :crux.tx/cas [kv snapshot object-store tx-log [op k old-v new-v at-business-time :as cas-op] transact-time tx-id]
  (let [eid (idx/new-id k)
        business-time (or at-business-time transact-time)
        {:keys [content-hash]
         :as entity} (first (doc/entities-at snapshot [eid] business-time transact-time))
        old-v-bytes (idx/id->bytes old-v)
        new-v-id (idx/new-id new-v)]
    {:pre-commit-fn #(if (bu/bytes=? (idx/id->bytes content-hash) old-v-bytes)
                       true
                       (log/warn "CAS failure:" (pr-str cas-op)))
     :kvs [[(idx/encode-entity+bt+tt+tx-id-key
             (idx/id->bytes eid)
             business-time
             transact-time
             tx-id)
            (idx/id->bytes new-v-id)]]}))

(defmethod tx-command :crux.tx/evict [kv snapshot object-store tx-log [op k start-business-time end-business-time] transact-time tx-id]
  (let [eid (idx/new-id k)
        history-descending (doc/entity-history snapshot eid)
        start-business-time (or start-business-time (.bt ^EntityTx (last history-descending)))
        end-business-time (or end-business-time transact-time)]
    {:post-commit-fn #(when tx-log
                        (doseq [^EntityTx entity-tx history-descending
                                :let [content-hash (.content-hash entity-tx)]
                                :when (and ((in-range-pred start-business-time end-business-time) (.bt entity-tx))
                                           (get (db/get-objects object-store snapshot [content-hash]) content-hash))]
                          (db/submit-doc tx-log content-hash nil)))
     :kvs [[(idx/encode-entity+bt+tt+tx-id-key
             (idx/id->bytes eid)
             end-business-time
             transact-time
             tx-id)
            idx/nil-id-bytes]]}))

(defrecord DocIndexer [kv tx-log object-store]
  Closeable
  (close [_])

  db/Indexer
  (index-doc [_ content-hash doc]
    (when (and doc (not (contains? doc :crux.db/id)))
      (throw (IllegalArgumentException.
              (str "Missing required attribute :crux.db/id: " (pr-str doc)))))
    (let [content-hash (idx/new-id content-hash)
          existing-doc (with-open [snapshot (ks/new-snapshot kv)]
                         (get (db/get-objects object-store snapshot [content-hash]) content-hash))]
      (cond
        (and doc (nil? existing-doc))
        (do (db/put-objects object-store [[content-hash doc]])
            (doc/index-doc kv content-hash doc))

        (and (nil? doc) existing-doc)
        (do (db/delete-objects object-store [content-hash])
            (doc/delete-doc-from-index kv content-hash existing-doc)))))

  (index-tx [_ tx-ops tx-time tx-id]
    (with-open [snapshot (ks/new-snapshot kv)]
      (let [tx-command-results (for [tx-op tx-ops]
                                 (tx-command kv snapshot object-store tx-log tx-op tx-time tx-id))]
        (if (->> (for [{:keys [pre-commit-fn]} tx-command-results
                       :when pre-commit-fn]
                   (pre-commit-fn))
                 (doall)
                 (every? true?))
          (do (->> (map :kvs tx-command-results)
                   (reduce into (sorted-map-by bu/bytes-comparator))
                   (ks/store kv))
              (doseq [{:keys [post-commit-fn]} tx-command-results
                      :when post-commit-fn]
                (post-commit-fn)))
          (log/warn "Transaction aborted:" (pr-str tx-ops) tx-time tx-id)))))

  (store-index-meta [_ k v]
    (doc/store-meta kv k v))

  (read-index-meta [_ k]
    (doc/read-meta kv k)))

(defn conform-tx-ops [tx-ops]
  (let [conformed-ops (s/conform ::tx-ops tx-ops)]
    (if (s/invalid? conformed-ops)
      (throw (ex-info "Invalid input" (s/explain-data ::tx-ops tx-ops)))
      conformed-ops)))

(defn tx-ops->docs [tx-ops]
  (vec (for [[op id :as tx-op] tx-ops
             doc (filter map? tx-op)]
         (if (and (satisfies? idx/IdToBytes id)
                  (= (idx/new-id id) (idx/new-id (get doc :crux.db/id))))
           doc
           (throw (IllegalArgumentException.
                   (str "Document's id does not match the operation id: " (get doc :crux.db/id) " " id)))))))

(defrecord SubmittedTx [tx-id transact-time])

(defmethod print-method SubmittedTx [submitted-tx ^Writer w]
  (.write w "#crux/submitted-tx ")
  (print-method (into {} submitted-tx) w))

(defrecord DocTxLog [kv]
  db/TxLog
  (submit-doc [this content-hash doc]
    (db/index-doc (->DocIndexer kv this (doc/->DocObjectStore kv)) content-hash doc))

  (submit-tx [this tx-ops]
    (let [transact-time (cio/next-monotonic-date)
          tx-id (.getTime transact-time)
          conformed-tx-ops (conform-tx-ops tx-ops)
          indexer (->DocIndexer kv this (doc/->DocObjectStore kv))]
      (doseq [doc (tx-ops->docs tx-ops)]
        (db/submit-doc this (str (idx/new-id doc)) doc))
      (db/index-tx indexer conformed-tx-ops transact-time tx-id)
      (db/store-index-meta indexer :crux.tx-log/tx-time transact-time)
      (delay (->SubmittedTx tx-id transact-time)))))
