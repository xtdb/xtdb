(ns crux.tx
  (:require [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [crux.codec :as c]
            [crux.db :as db]
            [crux.index :as idx]
            [crux.io :as cio]
            [crux.kv :as kv]
            [crux.lru :as lru]
            [crux.memory :as mem]
            [taoensso.nippy :as nippy])
  (:import [crux.codec EntityTx Id]
           [java.io Closeable Writer]
           java.util.concurrent.TimeoutException
           java.util.Date))

(set! *unchecked-math* :warn-on-boxed)

(s/def ::id (s/conformer (comp str c/new-id)))
(s/def ::doc (s/and (s/or :doc (s/and map? (s/conformer (comp str c/new-id)))
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

(defmulti tx-command (fn [snapshot tx-log [op] transact-time tx-id] op))

(defn- in-range-pred [start end]
  #(and (or (nil? start)
            (not (pos? (compare start %))))
        (or (nil? end)
            (neg? (compare % end)))))

(defn- put-delete-kvs [snapshot k start-business-time end-business-time transact-time tx-id content-hash]
  (let [eid (c/new-id k)
        start-business-time (or start-business-time transact-time)
        dates-in-history (when end-business-time
                           (map :bt (idx/entity-history snapshot eid)))
        dates-to-correct (->> (cons start-business-time dates-in-history)
                              (filter (in-range-pred start-business-time end-business-time))
                              (into (sorted-set)))]
    {:kvs (vec (for [business-time dates-to-correct]
                 [(c/encode-entity+bt+tt+tx-id-key
                   (c/->id-buffer eid)
                   business-time
                   transact-time
                   tx-id)
                  content-hash]))}))

(defmethod tx-command :crux.tx/put [snapshot tx-log [op k v start-business-time end-business-time] transact-time tx-id]
  (put-delete-kvs snapshot k start-business-time end-business-time transact-time tx-id (c/->id-buffer (c/new-id v))))

(defmethod tx-command :crux.tx/delete [snapshot tx-log [op k start-business-time end-business-time] transact-time tx-id]
  (put-delete-kvs snapshot k start-business-time end-business-time transact-time tx-id c/nil-id-buffer))

(defmethod tx-command :crux.tx/cas [snapshot tx-log [op k old-v new-v at-business-time :as cas-op] transact-time tx-id]
  (let [eid (c/new-id k)
        business-time (or at-business-time transact-time)
        {:keys [content-hash]
         :as entity} (first (idx/entities-at snapshot [eid] business-time transact-time))]
    {:pre-commit-fn #(if (= (c/new-id content-hash)
                            (c/new-id old-v))
                       true
                       (log/warn "CAS failure:" (pr-str cas-op)))
     :kvs [[(c/encode-entity+bt+tt+tx-id-key
             (c/->id-buffer eid)
             business-time
             transact-time
             tx-id)
            (c/->id-buffer new-v)]]}))

(defmethod tx-command :crux.tx/evict [snapshot tx-log [op k start-business-time end-business-time] transact-time tx-id]
  (let [eid (c/new-id k)
        history-descending (idx/entity-history snapshot eid)
        start-business-time (or start-business-time (.bt ^EntityTx (last history-descending)))
        end-business-time (or end-business-time transact-time)]
    {:post-commit-fn #(when tx-log
                        (doseq [^EntityTx entity-tx history-descending
                                :when ((in-range-pred start-business-time end-business-time) (.bt entity-tx))]
                          (db/submit-doc tx-log (.content-hash entity-tx) nil)))
     :kvs [[(c/encode-entity+bt+tt+tx-id-key
             (c/->id-buffer eid)
             end-business-time
             transact-time
             tx-id)
            c/nil-id-buffer]]}))

(defrecord KvIndexer [kv tx-log object-store]
  Closeable
  (close [_])

  db/Indexer
  (index-doc [_ content-hash doc]
    (when (and doc (not (contains? doc :crux.db/id)))
      (throw (IllegalArgumentException.
              (str "Missing required attribute :crux.db/id: " (pr-str doc)))))
    (let [content-hash (c/new-id content-hash)
          existing-doc (with-open [snapshot (kv/new-snapshot kv)]
                         (db/get-single-object object-store snapshot content-hash))]
      (cond
        (and doc (nil? existing-doc))
        (do (db/put-objects object-store [[content-hash doc]])
            (idx/index-doc kv content-hash doc))

        (and (nil? doc) existing-doc)
        (do (db/delete-objects object-store [content-hash])
            (idx/delete-doc-from-index kv content-hash existing-doc)))))

  (index-tx [_ tx-ops tx-time tx-id]
    (with-open [snapshot (kv/new-snapshot kv)]
      (let [tx-command-results (for [tx-op tx-ops]
                                 (tx-command snapshot tx-log tx-op tx-time tx-id))]
        (if (->> (for [{:keys [pre-commit-fn]} tx-command-results
                       :when pre-commit-fn]
                   (pre-commit-fn))
                 (doall)
                 (every? true?))
          (do (->> (map :kvs tx-command-results)
                   (reduce into (sorted-map-by mem/buffer-comparator))
                   (kv/store kv))
              (doseq [{:keys [post-commit-fn]} tx-command-results
                      :when post-commit-fn]
                (post-commit-fn)))
          (log/warn "Transaction aborted:" (pr-str tx-ops) tx-time tx-id)))))

  (store-index-meta [_ k v]
    (idx/store-meta kv k v))

  (read-index-meta [_ k]
    (idx/read-meta kv k)))

(defn conform-tx-ops [tx-ops]
  (s/assert ::tx-ops tx-ops)
  (s/conform ::tx-ops tx-ops))

(defn tx-ops->docs [tx-ops]
  (vec (for [[op id :as tx-op] tx-ops
             doc (filter map? tx-op)]
         (if (and (satisfies? c/IdToBuffer id)
                  (= (c/new-id id) (c/new-id (get doc :crux.db/id))))
           doc
           (throw (IllegalArgumentException.
                   (str "Document's id does not match the operation id: " (get doc :crux.db/id) " " id)))))))

(defrecord KvTxLog [kv]
  db/TxLog
  (submit-doc [this content-hash doc]
    (db/index-doc (->KvIndexer kv this (idx/->KvObjectStore kv)) content-hash doc))

  (submit-tx [this tx-ops]
    (let [transact-time (cio/next-monotonic-date)
          tx-id (.getTime transact-time)
          conformed-tx-ops (conform-tx-ops tx-ops)
          indexer (->KvIndexer kv this (idx/->KvObjectStore kv))]
      (kv/store kv [[(c/encode-tx-log-key tx-id transact-time)
                     (nippy/fast-freeze conformed-tx-ops)]])
      (doseq [doc (tx-ops->docs tx-ops)]
        (db/submit-doc this (str (c/new-id doc)) doc))
      (db/index-tx indexer conformed-tx-ops transact-time tx-id)
      (db/store-index-meta indexer
                           :crux.tx-log/consumer-state
                           {:crux.kv.topic-partition/tx-log-0
                            {:lag 0
                             :time transact-time}})
      (delay {:crux.tx/tx-id tx-id
              :crux.tx/tx-time transact-time})))

  (new-tx-log-context [this]
    (kv/new-snapshot kv))

  (tx-log [this tx-log-context]
    (let [i (kv/new-iterator tx-log-context)]
      (for [[k v] (idx/all-keys-in-prefix i (c/encode-tx-log-key) true)]
        (assoc (c/decode-tx-log-key-from k)
               :crux.tx/tx-ops (nippy/fast-thaw (mem/->on-heap v)))))))

(def ^:const default-await-tx-timeout 10000)

(s/def :crux.tx-log/await-tx-timeout nat-int?)

(defn latest-completed-tx-time [indexer]
  (let [consumer-states (->> (db/read-index-meta indexer :crux.tx-log/consumer-state)
                             (vals)
                             (sort-by :time))
        consumer-states-without-lag (filter (comp zero? :lag) consumer-states)]
    (if (= consumer-states consumer-states-without-lag)
      (:time (last consumer-states))
      (:time (first consumer-states)))))

(defn await-no-consumer-lag [indexer {:crux.tx-log/keys [await-tx-timeout]
                                      :or {await-tx-timeout default-await-tx-timeout}}]
  (let [max-lag-fn #(some->> (db/read-index-meta indexer :crux.tx-log/consumer-state)
                             (vals)
                             (seq)
                             (map :lag)
                             (reduce max 0))]
    (if (cio/wait-while #(pos? (or (long (max-lag-fn)) Long/MAX_VALUE))
                        await-tx-timeout)
      (latest-completed-tx-time indexer)
      (throw (TimeoutException.
              (str "Timed out waiting for index to catch up, lag is: " (max-lag-fn)))))))

(defn await-tx-time [indexer transact-time {:crux.tx-log/keys [await-tx-timeout]
                                            :or {await-tx-timeout default-await-tx-timeout}}]
  (if (cio/wait-while #(pos? (compare transact-time
                                      (or (latest-completed-tx-time indexer)
                                          (Date. 0))))
                      await-tx-timeout)
    (latest-completed-tx-time indexer)
    (throw (TimeoutException.
            (str "Timed out waiting for: " transact-time
                 " index has: " (latest-completed-tx-time indexer))))))
