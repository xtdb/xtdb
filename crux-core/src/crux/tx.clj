(ns crux.tx
  (:require [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [crux.codec :as c]
            [crux.backup :as backup]
            [crux.db :as db]
            [crux.index :as idx]
            [crux.io :as cio]
            [clojure.java.io :as io]
            [crux.kv :as kv]
            [crux.memory :as mem]
            [crux.moberg :as moberg]
            [crux.status :as status]
            crux.api
            crux.tx.event
            [taoensso.nippy :as nippy])
  (:import crux.codec.EntityTx
           crux.moberg.Message
           java.io.Closeable
           java.util.concurrent.TimeoutException
           java.util.Date))

(set! *unchecked-math* :warn-on-boxed)

(defn- in-range-pred [start end]
  #(and (or (nil? start)
            (not (pos? (compare start %))))
        (or (nil? end)
            (neg? (compare % end)))))

(defn- put-delete-kvs [object-store snapshot k start-valid-time end-valid-time transact-time tx-id content-hash]
  (let [eid (c/new-id k)
        start-valid-time (or start-valid-time transact-time)
        dates-in-history (when end-valid-time
                           (map :vt (idx/entity-history snapshot eid)))
        dates-to-correct (->> (cons start-valid-time dates-in-history)
                              (filter (in-range-pred start-valid-time end-valid-time))
                              (into (sorted-set)))]
    {:kvs (->> (for [valid-time dates-to-correct]
                 [[(c/encode-entity+vt+tt+tx-id-key-to
                    nil
                    (c/->id-buffer eid)
                    valid-time
                    transact-time
                    tx-id)
                   content-hash]
                  [(c/encode-entity+z+tx-id-key-to
                    nil
                    (c/->id-buffer eid)
                    (c/encode-entity-tx-z-number valid-time transact-time)
                    tx-id)
                   content-hash]])
               (reduce into []))}))

(defn tx-command-put [object-store snapshot tx-log [op k v start-valid-time end-valid-time] transact-time tx-id]
  (assoc (put-delete-kvs object-store snapshot k start-valid-time end-valid-time transact-time tx-id (c/->id-buffer (c/new-id v)))
         :pre-commit-fn #(let [content-hash (c/new-id v)
                               correct-state? (db/known-keys? object-store snapshot [content-hash])]
                           (when-not correct-state?
                             (log/error "Put, incorrect doc state for:" content-hash "tx id:" tx-id))
                           correct-state?)))

(defn tx-command-delete [object-store snapshot tx-log [op k start-valid-time end-valid-time] transact-time tx-id]
  (put-delete-kvs object-store snapshot k start-valid-time end-valid-time transact-time tx-id (c/nil-id-buffer)))

(defn tx-command-cas [object-store snapshot tx-log [op k old-v new-v at-valid-time :as cas-op] transact-time tx-id]
  (let [eid (c/new-id k)
        valid-time (or at-valid-time transact-time)
        {:keys [content-hash]
         :as entity} (first (idx/entities-at snapshot [eid] valid-time transact-time))]
    {:pre-commit-fn #(if (= (c/new-id content-hash)
                            (c/new-id old-v))
                       (let [correct-state? (not (nil? (db/get-single-object object-store snapshot (c/new-id new-v))))]
                         (when-not correct-state?
                           (log/error "CAS, incorrect doc state for:" (c/new-id new-v) "tx id:" tx-id))
                         correct-state?)
                       (do (log/warn "CAS failure:" (pr-str cas-op))
                           false))
     :kvs [[(c/encode-entity+vt+tt+tx-id-key-to
             nil
             (c/->id-buffer eid)
             valid-time
             transact-time
             tx-id)
            (c/->id-buffer new-v)]
           [(c/encode-entity+z+tx-id-key-to
             nil
             (c/->id-buffer eid)
             (c/encode-entity-tx-z-number valid-time transact-time)
             tx-id)
            (c/->id-buffer new-v)]]}))

(defn tx-command-evict [object-store snapshot tx-log [op k start-valid-time end-valid-time keep-latest? keep-earliest?] transact-time tx-id]
  (let [eid (c/new-id k)
        history-descending (idx/entity-history snapshot eid)
        start-valid-time (or start-valid-time (some-> ^EntityTx (last history-descending) (.vt)))
        end-valid-time (or end-valid-time transact-time)]
    {:post-commit-fn #(when (and tx-log start-valid-time)
                        (let [range-pred (in-range-pred start-valid-time end-valid-time)]
                          (doseq [^EntityTx entity-tx (cond->> (filter (fn [^EntityTx entity-tx]
                                                                         (range-pred (.vt entity-tx)))
                                                                       history-descending)
                                                        keep-latest? (rest)
                                                        keep-earliest? (butlast))]
                            ;; TODO: Direct interface call to help
                            ;; Graal, not sure why this is needed,
                            ;; fails with get-proxy-class issue
                            ;; otherwise.
                            (.submit_doc
                              ^crux.db.TxLog tx-log
                              (.content-hash entity-tx)
                              {:crux.db/id :crux.db/evicted}))))}))

(defn tx-command-unknown [object-store snapshot tx-log [op k start-valid-time end-valid-time keep-latest? keep-earliest?] transact-time tx-id]
  (throw (IllegalArgumentException. (str "Unknown tx-op:" op))))

(def ^:private tx-op->command
  {:crux.tx/put tx-command-put
   :crux.tx/delete tx-command-delete
   :crux.tx/cas tx-command-cas
   :crux.tx/evict tx-command-evict})

(defn evicted-doc?
  [doc]
  (= :crux.db/evicted (:crux.db/id doc)))

(defrecord KvIndexer [kv tx-log object-store]
  Closeable
  (close [_])

  db/Indexer
  (index-doc [_ content-hash doc]
    (log/debug "Indexing doc:" content-hash)
    (when (not (contains? doc :crux.db/id))
      (throw (IllegalArgumentException.
              (str "Missing required attribute :crux.db/id: " (pr-str doc)))))
    (let [content-hash (c/new-id content-hash)
          existing-doc (with-open [snapshot (kv/new-snapshot kv)]
                         (db/get-single-object object-store snapshot content-hash))]
      (db/put-objects object-store [[content-hash doc]])
      (if (evicted-doc? doc)
        (when existing-doc
          (idx/delete-doc-from-index kv content-hash existing-doc))
        (idx/index-doc kv content-hash doc))))

  (index-tx [_ tx-ops tx-time tx-id]
    (s/assert :crux.tx.event/tx-events tx-ops)
    (with-open [snapshot (kv/new-snapshot kv)]
      (let [tx-command-results (vec (for [[op :as tx-op] tx-ops]
                                      ((get tx-op->command op tx-command-unknown)
                                       object-store snapshot tx-log tx-op tx-time tx-id)))]
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
      (db/known-keys? object-store snapshot content-hashes)))

  (store-index-meta [_ k v]
    (idx/store-meta kv k v))

  (read-index-meta [_ k]
    (idx/read-meta kv k))

  status/Status
  (status-map [this]
    {:crux.index/index-version (idx/current-index-version kv)
     :crux.tx-log/consumer-state (db/read-index-meta this :crux.tx-log/consumer-state)}))

(defmulti conform-tx-op first)

(defmethod conform-tx-op ::put [tx-op] (let [[op doc & args] tx-op
                                            id (:crux.db/id doc)]
                                        (into [::put id doc] args)))

(defmethod conform-tx-op ::cas [tx-op] (let [[op old-doc new-doc & args] tx-op
                                            new-id (:crux.db/id new-doc)
                                            old-id (:crux.db/id old-doc)]
                                        (if (or (= nil old-id) (= new-id old-id))
                                          (into [::cas new-id old-doc new-doc] args)
                                          (throw (IllegalArgumentException.
                                                  (str "CAS, document id's do not match: " old-id " " new-id))))))

(defmethod conform-tx-op :default [tx-op] tx-op)

(defn tx-ops->docs [tx-ops]
  (let [conformed-tx-ops (into [] (for [tx-op tx-ops] (conform-tx-op tx-op)))]
    (vec (for [[op id & args] conformed-tx-ops
               doc (filter map? args)]
           doc))))

(defn tx-ops->tx-events [tx-ops]
  (let [conformed-tx-ops (into [] (for [tx-op tx-ops] (conform-tx-op tx-op)))
        tx-events (mapv (fn [[op id & args]]
                          (into [op (str (c/new-id id))]
                                (for [arg args]
                                  (if (map? arg)
                                    (-> arg c/new-id str)
                                    arg))))
                        conformed-tx-ops)]
    (s/assert :crux.tx.event/tx-events tx-events)
    tx-events))

;; For StandaloneSystem.

(s/def ::event-log-dir string?)
(s/def ::event-log-kv-backend :crux.kv/kv-backend)
(s/def ::event-log-sync-interval-ms nat-int?)

(defrecord EventTxLog [event-log-kv]
  db/TxLog
  (submit-doc [this content-hash doc]
    (moberg/send-message event-log-kv ::event-log content-hash doc {::sub-topic :docs}))

  (submit-tx [this tx-ops]
    (s/assert :crux.api/tx-ops tx-ops)
    (doseq [doc (tx-ops->docs tx-ops)]
      (db/submit-doc this (str (c/new-id doc)) doc))
    (let [tx-events (tx-ops->tx-events tx-ops)
          m (moberg/send-message event-log-kv ::event-log nil tx-events {::sub-topic :txs})]
      (delay {:crux.tx/tx-id (.id m)
              :crux.tx/tx-time (.time m)})))

  (new-tx-log-context [this]
    (kv/new-snapshot event-log-kv))

  (tx-log [this tx-log-context from-tx-id]
    (let [i (kv/new-iterator tx-log-context)]
      (when-let [m (moberg/seek-message i ::event-log from-tx-id)]
        (for [^Message m (->> (repeatedly #(moberg/next-message i ::event-log))
                              (take-while identity)
                              (cons m))
              :when (= :txs (get (.headers m) ::sub-topic))]
          {:crux.api/tx-ops (.body m)
           :crux.tx/tx-id (.message-id m)
           :crux.tx/tx-time (.message-time m)}))))

  Closeable
  (close [_]
    (.close ^Closeable event-log-kv))

  backup/ISystemBackup
  (write-checkpoint [this {:keys [crux.backup/checkpoint-directory]}]
    (kv/backup event-log-kv (io/file checkpoint-directory "event-log-kv-store"))))

(defn- event-log-consumer-main-loop [{:keys [running? event-log-kv indexer batch-size idle-sleep-ms]
                                      :or {batch-size 100
                                           idle-sleep-ms 10}}]
  (when-not (db/read-index-meta indexer :crux.tx-log/consumer-state)
    (db/store-index-meta
     indexer
     :crux.tx-log/consumer-state {::event-log {:lag 0
                                               :next-offset 0
                                               :time nil}}))
  (while @running?
    (with-open [snapshot (kv/new-snapshot event-log-kv)
                i (kv/new-iterator snapshot)]
      (let [next-offset (get-in (db/read-index-meta indexer :crux.tx-log/consumer-state)
                                [::event-log
                                 :next-offset])]
        (if-let [m (moberg/seek-message i ::event-log next-offset)]
          (let [_ (log/debug "Consuming from:" next-offset)
                ^Message last-message (loop [m m
                                             n 1]
                                        (do (log/debug "Consuming message:" (pr-str (moberg/message->edn m)))
                                            (case (get (.headers m) ::sub-topic)
                                              :docs
                                              (db/index-doc indexer (.key m) (.body m))
                                              :txs
                                              (db/index-tx indexer
                                                           (.body m)
                                                           (.message-time m)
                                                           (.message-id m)))
                                            (if (>= n (long batch-size))
                                              m
                                              (if-let [next-m (moberg/next-message i ::event-log)]
                                                (recur next-m (inc n))
                                                m))))
                end-offset (moberg/end-message-id-offset event-log-kv ::event-log)
                next-offset (inc (long (.message-id last-message)))
                lag (- end-offset next-offset)
                _ (when (pos? lag)
                    (log/debug "Falling behind" ::event-log "at:" next-offset "end:" end-offset))
                consumer-state {::event-log
                                {:lag lag
                                 :next-offset next-offset
                                 :time (.message-time last-message)}}]
            (log/debug "Event log consumer state:" (pr-str consumer-state))
            (db/store-index-meta indexer :crux.tx-log/consumer-state consumer-state)))))
    (Thread/sleep idle-sleep-ms)))

(defn start-event-log-consumer ^java.io.Closeable [event-log-kv indexer event-log-sync-interval-ms]
  (let [running? (atom true)
        worker-thread (doto (Thread. #(try
                                        (event-log-consumer-main-loop {:running? running?
                                                                       :event-log-kv event-log-kv
                                                                       :indexer indexer})
                                        (catch Throwable t
                                          (log/fatal t "Event log consumer threw exception, consumption has stopped:")))
                                     "crux.tx.event-log-consumer-thread")
                        (.start))
        fsync-thread (when event-log-sync-interval-ms
                       (log/debug "Using event log fsync interval ms:" event-log-sync-interval-ms)
                       (doto (Thread. #(while @running?
                                         (try
                                           (Thread/sleep event-log-sync-interval-ms)
                                           (kv/fsync event-log-kv)
                                           (catch Throwable t
                                             (log/error t "Event log fsync threw exception:"))))
                                      "crux.tx.event-log-fsync-thread")
                         (.start)))]
    (reify Closeable
      (close [_]
        (reset! running? false)
        (.join worker-thread)
        (some-> fsync-thread (.join))))))

(defn enrich-tx-ops-with-documents [snapshot object-store tx-ops]
  (vec (for [op tx-ops]
         (vec (for [x op]
                (or (when (satisfies? c/IdToBuffer x)
                      (db/get-single-object object-store snapshot x))
                    x))))))

(def ^:const default-await-tx-timeout 10000)

(s/def :crux.tx-log/await-tx-timeout nat-int?)

(defn latest-completed-tx-time [consumer-state]
  (let [consumer-states (->> consumer-state
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
    (if (cio/wait-while #(if-let [max-lag (max-lag-fn)]
                           (pos? (long max-lag))
                           true)
                        await-tx-timeout)
      (latest-completed-tx-time (db/read-index-meta indexer :crux.tx-log/consumer-state))
      (throw (TimeoutException.
               (str "Timed out waiting for index to catch up, lag is: " (or (max-lag-fn)
                                                                            "unknown")))))))

(defn await-tx-time [indexer transact-time {:crux.tx-log/keys [await-tx-timeout]
                                            :or {await-tx-timeout default-await-tx-timeout}}]
  (let [seen-tx-time (atom (Date. 0))]
    (if (cio/wait-while #(pos? (compare transact-time
                                        (let [completed-tx-time (or (latest-completed-tx-time
                                                                     (db/read-index-meta indexer :crux.tx-log/consumer-state))
                                                                    (Date. 0))]
                                          (reset! seen-tx-time completed-tx-time)
                                          completed-tx-time)))
                        await-tx-timeout)
      @seen-tx-time
      (throw (TimeoutException.
              (str "Timed out waiting for: " (pr-str transact-time)
                   " index has: " (pr-str @seen-tx-time)))))))
