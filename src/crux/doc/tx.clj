(ns crux.doc.tx
  (:require [clojure.spec.alpha :as s]
            [crux.byte-utils :as bu]
            [crux.doc :as doc]
            [crux.doc.index :as idx]
            [crux.kv-store :as ks]
            [crux.db :as db])
  (:import [java.util Date]))

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

(defmulti tx-command (fn [kv snapshot tx-log [op] transact-time tx-id] op))

(defmethod tx-command :crux.tx/put [kv snapshot tx-log [op k v business-time] transact-time tx-id]
  (let [eid (idx/new-id k)
        content-hash (idx/new-id v)
        business-time (or business-time transact-time)]
    [[(idx/encode-entity+bt+tt+tx-id-key
       eid
       business-time
       transact-time
       tx-id)
      (idx/id->bytes content-hash)]
     [(idx/encode-content-hash+entity-key content-hash eid)
      idx/empty-byte-array]]))

(defmethod tx-command :crux.tx/delete [kv snapshot tx-log [op k business-time] transact-time tx-id]
  (let [eid (idx/new-id k)
        business-time (or business-time transact-time)]
    [[(idx/encode-entity+bt+tt+tx-id-key
       eid
       business-time
       transact-time
       tx-id)
      idx/empty-byte-array]]))

(defmethod tx-command :crux.tx/cas [kv snapshot tx-log [op k old-v new-v business-time] transact-time tx-id]
  (let [eid (idx/new-id k)
        business-time (or business-time transact-time)
        old-content-hash (-> (doc/entities-at snapshot [eid] business-time transact-time)
                             (get eid)
                             :content-hash)
        old-v (idx/id->bytes old-v)
        new-v (idx/id->bytes new-v)]
    (when (bu/bytes=? (idx/id->bytes old-content-hash) old-v)
      [[(idx/encode-entity+bt+tt+tx-id-key
         eid
         business-time
         transact-time
         tx-id)
        new-v]
       [(idx/encode-content-hash+entity-key new-v eid)
        idx/empty-byte-array]])))

(defmethod tx-command :crux.tx/evict [kv snapshot tx-log [op k business-time] transact-time tx-id]
  (let [eid (idx/new-id k)
        business-time (or business-time transact-time)]
    (when tx-log
      (doseq [{:keys [content-hash bt]} (get (doc/entity-histories snapshot [eid]) eid)
              :when (and content-hash (<= (compare bt business-time) 0))]
        (db/submit-doc tx-log (str content-hash) nil)))
    [[(idx/encode-entity+bt+tt+tx-id-key
       eid
       business-time
       transact-time
       tx-id)
      idx/empty-byte-array]]))

(defn store-tx [kv tx-log tx-ops tx-time tx-id]
  (with-open [snapshot (ks/new-snapshot kv)]
    (->> (for [tx-op tx-ops]
           (tx-command kv snapshot tx-log tx-op tx-time tx-id))
         (reduce into (sorted-map-by bu/bytes-comparator))
         (ks/store kv))))

(defrecord DocIndexer [kv tx-log]
  db/Indexer
  (index-doc [_ content-hash doc]
    (doc/store-doc kv content-hash doc))

  (index-tx [_ tx-ops tx-time tx-id]
    (store-tx kv tx-log tx-ops tx-time tx-id))

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
  (for [tx-op tx-ops
        doc (filter map? tx-op)]
    doc))

(defrecord DocTxLog [kv]
  db/TxLog
  (submit-doc [this content-hash doc]
    (db/index-doc (->DocIndexer kv this) content-hash doc))

  (submit-tx [this tx-ops]
    (let [transact-time (Date.)
          tx-id (.getTime transact-time)
          conformed-tx-ops (conform-tx-ops tx-ops)
          indexer (->DocIndexer kv this)]
      (doseq [doc (tx-ops->docs tx-ops)]
        (db/submit-doc this (str (idx/new-id doc)) doc))
      (db/index-tx indexer conformed-tx-ops transact-time tx-id)
      (db/store-index-meta indexer :crux.tx-log/tx-time transact-time)
      (delay {:tx-id tx-id
              :transact-time transact-time}))))
