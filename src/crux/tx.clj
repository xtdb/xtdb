(ns crux.tx
  (:require [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [crux.byte-utils :as bu]
            [crux.doc :as doc]
            [crux.index :as idx]
            [crux.io :as cio]
            [crux.kv-store :as ks]
            [crux.db :as db])
  (:import [java.util Date]
           [crux.index EntityTx]))

(set! *unchecked-math* :warn-on-boxed)

(s/def ::id (s/conformer (comp str idx/new-id)))
(s/def ::doc (s/and (s/or :doc (s/and map? (s/conformer (comp str idx/new-id)))
                          :content-hash ::id)
                    (s/conformer second)))

(s/def ::put-op (s/cat :op #{:crux.tx/put}
                       :id ::id
                       :doc ::doc
                       :business-time (s/? inst?)))

(s/def ::delete-op (s/cat :op #{:crux.tx/delete}
                          :id ::id
                          :business-time (s/? inst?)))

(s/def ::cas-op (s/cat :op #{:crux.tx/cas}
                       :id ::id
                       :old-doc ::doc
                       :new-doc ::doc
                       :business-time (s/? inst?)))

(s/def ::evict-op (s/cat :op #{:crux.tx/evict}
                         :id ::id
                         :business-time (s/? inst?)))

(s/def ::tx-op (s/and (s/or :put ::put-op
                            :delete ::delete-op
                            :cas ::cas-op
                            :evict ::evict-op)
                      (s/conformer (comp vec vals second))))

(s/def ::tx-ops (s/coll-of ::tx-op :kind vector?))

(defmulti tx-command (fn [kv object-store tx-log [op] transact-time tx-id] op))

(defmethod tx-command :crux.tx/put [kv object-store tx-log [op k v business-time] transact-time tx-id]
  (let [eid (idx/new-id k)
        content-hash (idx/new-id v)
        business-time (or business-time transact-time)]
    {:kvs [[(idx/encode-entity+bt+tt+tx-id-key
             (idx/id->bytes eid)
             business-time
             transact-time
             tx-id)
            (idx/id->bytes content-hash)]]}))

(defmethod tx-command :crux.tx/delete [kv object-store tx-log [op k business-time] transact-time tx-id]
  (let [eid (idx/new-id k)
        business-time (or business-time transact-time)]
    {:kvs [[(idx/encode-entity+bt+tt+tx-id-key
             (idx/id->bytes eid)
             business-time
             transact-time
             tx-id)
            idx/nil-id-bytes]]}))

(defmethod tx-command :crux.tx/cas [kv object-store tx-log [op k old-v new-v business-time :as cas-op] transact-time tx-id]
  (let [eid (idx/new-id k)
        business-time (or business-time transact-time)
        {:keys [content-hash]
         :as entity} (with-open [snapshot (ks/new-snapshot kv)]
                       (first (doc/entities-at snapshot [eid] business-time transact-time)))
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

(defmethod tx-command :crux.tx/evict [kv object-store tx-log [op k business-time] transact-time tx-id]
  (let [eid (idx/new-id k)
        business-time (or business-time transact-time)]

    {:post-commit-fn #(when tx-log
                        (doseq [{:keys [content-hash]
                                 :as entity} (with-open [snapshot (ks/new-snapshot kv)]
                                               (doc/entity-history snapshot eid))
                                :let [doc (get (db/get-objects object-store [content-hash]) content-hash)]
                                :when (and doc (<= (compare (.bt ^EntityTx entity) business-time) 0))]
                          (db/submit-doc tx-log (idx/new-id doc) nil)))
     :kvs [[(idx/encode-entity+bt+tt+tx-id-key
             (idx/id->bytes eid)
             business-time
             transact-time
             tx-id)
            idx/nil-id-bytes]]}))

(defrecord DocIndexer [kv tx-log object-store]
  db/Indexer
  (index-doc [_ content-hash doc]
    (when (and doc (not (contains? doc :crux.db/id)))
      (throw (IllegalArgumentException.
              (str "Missing required attribute :crux.db/id: " (pr-str doc)))))
    (let [content-hash (idx/new-id content-hash)
          existing-doc (get (db/get-objects object-store [content-hash]) content-hash)]
      (cond
        (and doc (nil? existing-doc))
        (do (db/put-objects object-store [[content-hash doc]])
            (doc/index-doc kv content-hash doc))

        (and (nil? doc) existing-doc)
        (do (db/delete-objects object-store [content-hash])
            (doc/delete-doc-from-index kv content-hash existing-doc)))))

  (index-tx [_ tx-ops tx-time tx-id]
    (let [tx-command-results (for [tx-op tx-ops]
                               (tx-command kv object-store tx-log tx-op tx-time tx-id))]
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
        (log/warn "Transaction aborted:" (pr-str tx-ops)))))

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
      (delay {:tx-id tx-id
              :transact-time transact-time}))))
