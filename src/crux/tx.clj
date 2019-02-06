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
            [crux.moberg :as moberg]
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

(defmulti tx-command (fn [object-store snapshot tx-log [op] transact-time tx-id] op))

(defn- in-range-pred [start end]
  #(and (or (nil? start)
            (not (pos? (compare start %))))
        (or (nil? end)
            (neg? (compare % end)))))

(defn- put-delete-kvs [object-store snapshot k start-business-time end-business-time transact-time tx-id content-hash]
  (let [eid (c/new-id k)
        start-business-time (or start-business-time transact-time)
        dates-in-history (when end-business-time
                           (map :bt (idx/entity-history snapshot eid)))
        dates-to-correct (->> (cons start-business-time dates-in-history)
                              (filter (in-range-pred start-business-time end-business-time))
                              (into (sorted-set)))]
    {:kvs (vec (for [business-time dates-to-correct]
                 [(c/encode-entity+bt+tt+tx-id-key-to
                   nil
                   (c/->id-buffer eid)
                   business-time
                   transact-time
                   tx-id)
                  content-hash]))}))

(defmethod tx-command :crux.tx/put [object-store snapshot tx-log [op k v start-business-time end-business-time] transact-time tx-id]
  (assoc (put-delete-kvs object-store snapshot k start-business-time end-business-time transact-time tx-id (c/->id-buffer (c/new-id v)))
         :pre-commit-fn #(let [content-hash (c/new-id v)
                               doc (db/get-single-object object-store snapshot content-hash)
                               correct-state? (not (nil? doc))]
                           (when-not correct-state?
                             (log/error "Put, incorrect doc state for:" content-hash "tx id:" tx-id))
                           correct-state?)))

(defmethod tx-command :crux.tx/delete [object-store snapshot tx-log [op k start-business-time end-business-time] transact-time tx-id]
  (put-delete-kvs object-store snapshot k start-business-time end-business-time transact-time tx-id c/nil-id-buffer))

(defmethod tx-command :crux.tx/cas [object-store snapshot tx-log [op k old-v new-v at-business-time :as cas-op] transact-time tx-id]
  (let [eid (c/new-id k)
        business-time (or at-business-time transact-time)
        {:keys [content-hash]
         :as entity} (first (idx/entities-at snapshot [eid] business-time transact-time))]
    {:pre-commit-fn #(if (= (c/new-id content-hash)
                            (c/new-id old-v))
                       (let [correct-state? (not (nil? (db/get-single-object object-store snapshot (c/new-id new-v))))]
                          (when-not correct-state?
                            (log/error "CAS, incorrect doc state for:" (c/new-id new-v) "tx id:" tx-id))
                          correct-state?)
                       (do (log/warn "CAS failure:" (pr-str cas-op))
                           false))
     :kvs [[(c/encode-entity+bt+tt+tx-id-key-to
             nil
             (c/->id-buffer eid)
             business-time
             transact-time
             tx-id)
            (c/->id-buffer new-v)]]}))

(defmethod tx-command :crux.tx/evict [object-store snapshot tx-log [op k start-business-time end-business-time] transact-time tx-id]
  (let [eid (c/new-id k)
        history-descending (idx/entity-history snapshot eid)
        start-business-time (or start-business-time (.bt ^EntityTx (last history-descending)))
        end-business-time (or end-business-time transact-time)]
    {:post-commit-fn #(when tx-log
                        (doseq [^EntityTx entity-tx history-descending
                                :when ((in-range-pred start-business-time end-business-time) (.bt entity-tx))]
                          (db/submit-doc tx-log (.content-hash entity-tx) nil)))
     :kvs [[(c/encode-entity+bt+tt+tx-id-key-to
             nil
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
    (log/debug "Indexing doc:" content-hash)
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
                                 (tx-command object-store snapshot tx-log tx-op tx-time tx-id))]
        (log/debug "Indexing tx-id:" tx-id "tx-ops:" (count tx-ops))
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
          (log/warn "Transaction aborted:" (pr-str tx-ops) (pr-str tx-time) tx-id)))))

  (docs-exist? [_ content-hashes]
    (with-open [snapshot (kv/new-snapshot kv)]
      (= (set content-hashes)
         (set (keys (db/get-objects object-store snapshot content-hashes))))))

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

;; For dev/testing:

(defrecord KvTxLog [kv]
  db/TxLog
  (submit-doc [this content-hash doc]
    (db/index-doc (->KvIndexer kv this (idx/->KvObjectStore kv)) content-hash doc))

  (submit-tx [this tx-ops]
    (let [transact-time (cio/next-monotonic-date)
          tx-id (.getTime transact-time)
          conformed-tx-ops (conform-tx-ops tx-ops)
          indexer (->KvIndexer kv this (idx/->KvObjectStore kv))]
      (kv/store kv [[(c/encode-tx-log-key-to nil tx-id transact-time)
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

  (tx-log [this tx-log-context from-tx-id]
    (let [i (kv/new-iterator tx-log-context)]
      (for [[k v] (idx/all-keys-in-prefix i (c/encode-tx-log-key-to nil from-tx-id) (c/encode-tx-log-key-to nil) true)]
        (assoc (c/decode-tx-log-key-from k)
               :crux.tx/tx-ops (nippy/fast-thaw (mem/->on-heap v)))))))

;; For StandaloneSystem.

(defrecord EventTxLog [event-log-kv]
  db/TxLog
  (submit-doc [this content-hash doc]
    (moberg/send-message event-log-kv ::event-log content-hash doc {::sub-topic :docs}))

  (submit-tx [this tx-ops]
    (let [conformed-tx-ops (conform-tx-ops tx-ops)]
      (doseq [doc (tx-ops->docs tx-ops)]
        (db/submit-doc this (str (c/new-id doc)) doc))
      (let [{:crux.moberg/keys [message-id message-time]}
            (moberg/send-message event-log-kv ::event-log nil conformed-tx-ops {::sub-topic :txs})]
        (delay {:crux.tx/tx-id message-id
                :crux.tx/tx-time message-time}))))

  (new-tx-log-context [this]
    (kv/new-snapshot event-log-kv))

  (tx-log [this tx-log-context from-tx-id]
    (let [i (kv/new-iterator tx-log-context)]
      (when-let [m (moberg/seek-message i ::event-log from-tx-id)]
        (for [m (->> (repeatedly #(moberg/next-message i ::event-log))
                     (take-while identity)
                     (cons m))
              :when (= :txs (get-in m [:crux.moberg/headers ::sub-topic]))]
          {:crux.tx/tx-ops (:crux.moberg/body m)
           :crux.tx/tx-id (:crux.moberg/message-id m)
           :crux.tx/tx-time (:crux.moberg/message-time m)})))))

(defn- event-log-consumer-main-loop [{:keys [running? event-log-kv indexer batch-size idle-sleep-ms]
                                      :or {batch-size 100
                                           idle-sleep-ms 100}}]
  (while @running?
    (with-open [snapshot (kv/new-snapshot event-log-kv)
                i (kv/new-iterator snapshot)]
      (let [next-offset (get-in (db/read-index-meta indexer :crux.tx-log/consumer-state)
                                [::event-log
                                 :next-offset])]
        (log/debug "Consuming from:" next-offset)
        (if-let [m (moberg/seek-message i ::event-log (or next-offset 0))]
          (let [last-message (->> (repeatedly #(moberg/next-message i ::event-log))
                                  (take-while identity)
                                  (cons m)
                                  (take batch-size)
                                  (reduce (fn [last-message m]
                                            (log/debug "Consuming message:" (pr-str m))
                                            (case (get-in m [:crux.moberg/headers ::sub-topic])
                                              :docs
                                              (db/index-doc indexer (:crux.moberg/key m) (:crux.moberg/body m))
                                              :txs
                                              (db/index-tx indexer
                                                           (:crux.moberg/body m)
                                                           (:crux.moberg/message-time m)
                                                           (:crux.moberg/message-id m)))
                                            m)
                                          nil))
                consumer-state {::event-log
                                {:lag (- (long (moberg/end-message-id event-log-kv ::event-log))
                                         (long (:crux.moberg/message-id last-message)))
                                 :next-offset (inc (long (:crux.moberg/message-id last-message)))
                                 :time (:crux.moberg/message-time last-message)}}]
            (log/debug "Event log consumer state:" (pr-str consumer-state))
            (db/store-index-meta indexer :crux.tx-log/consumer-state consumer-state))
          (Thread/sleep idle-sleep-ms))))))

(defn start-event-log-consumer ^java.io.Closeable [event-log-kv indexer]
  (let [running? (atom true)
        worker-thread (doto (Thread. #(try
                                        (event-log-consumer-main-loop {:running? running?
                                                                       :event-log-kv event-log-kv
                                                                       :indexer indexer})
                                        (catch Throwable t
                                          (log/error t "Event log consumer threw exception, consumption has stopped:")))
                                     "crux.tx.event-log-consumer-thread")
                        (.start))]
    (reify Closeable
      (close [_]
        (reset! running? false)
        (.join worker-thread)))))

(defn enrich-tx-ops-with-documents [snapshot object-store tx-ops]
  (vec (for [op tx-ops]
         (vec (for [x op]
                (or (when (satisfies? c/IdToBuffer x)
                      (db/get-single-object object-store snapshot x))
                    x))))))

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
  (let [seen-tx-time (atom (Date. 0))]
    (if (cio/wait-while #(pos? (compare transact-time
                                        (let [completed-tx-time (or (latest-completed-tx-time indexer)
                                                                    (Date. 0))]
                                          (reset! seen-tx-time completed-tx-time)
                                          completed-tx-time)))
                        await-tx-timeout)
      @seen-tx-time
      (throw (TimeoutException.
              (str "Timed out waiting for: " (pr-str transact-time)
                   " index has: " (pr-str @seen-tx-time)))))))
