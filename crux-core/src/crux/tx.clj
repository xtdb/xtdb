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
            [crux.status :as status]
            [crux.tx.polling :as p]
            crux.api
            crux.tx.event
            [crux.query :as q]
            [taoensso.nippy :as nippy])
  (:import crux.codec.EntityTx
           crux.tx.consumer.Message
           java.io.Closeable
           [java.util.concurrent ExecutorService Executors TimeoutException TimeUnit]
           java.util.Date))

(set! *unchecked-math* :warn-on-boxed)

(def ^:private date? (partial instance? Date))

(s/def ::tx-id nat-int?)
(s/def ::tx-time date?)

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

(defn tx-command-put [indexer kv object-store snapshot tx-log [op k v start-valid-time end-valid-time] transact-time tx-id]
  (assoc (put-delete-kvs object-store snapshot k start-valid-time end-valid-time transact-time tx-id (c/->id-buffer (c/new-id v)))
         :pre-commit-fn #(let [content-hash (c/new-id v)
                               correct-state? (db/known-keys? object-store snapshot [content-hash])]
                           (when-not correct-state?
                             (log/error "Put, incorrect doc state for:" content-hash "tx id:" tx-id))
                           correct-state?)))

(defn tx-command-delete [indexer kv object-store snapshot tx-log [op k start-valid-time end-valid-time] transact-time tx-id]
  (put-delete-kvs object-store snapshot k start-valid-time end-valid-time transact-time tx-id (c/nil-id-buffer)))

(defn tx-command-cas [indexer kv object-store snapshot tx-log [op k old-v new-v at-valid-time :as cas-op] transact-time tx-id]
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
                       (do (log/warn "CAS failure:" (pr-str cas-op) "was:" (c/new-id content-hash))
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

(defn tx-command-evict [indexer kv object-store snapshot tx-log [op k start-valid-time end-valid-time keep-latest? keep-earliest?] transact-time tx-id]
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
                              {:crux.db/id eid :crux.db/evicted? true}))))}))

(declare tx-op->command tx-command-unknown tx-ops->docs tx-ops->tx-events)

(def ^:private tx-fn-eval-cache (memoize eval))

(defn log-tx-fn-error [fn-result fn-id body args-id args]
  (log/warn fn-result "Transaction function failure:" fn-id (pr-str body) args-id (pr-str args)))

(def tx-fns-enabled? (Boolean/parseBoolean (System/getenv "CRUX_ENABLE_TX_FNS")))

(defn tx-command-fn [indexer kv object-store snapshot tx-log [op k args-v :as tx-op] transact-time tx-id]
  (if-not tx-fns-enabled?
    (throw (IllegalArgumentException. (str "Transaction functions not enabled: " (pr-str tx-op))))
    (let [fn-id (c/new-id k)
          db (q/db kv object-store transact-time transact-time)
          {:crux.db.fn/keys [body] :as fn-doc} (q/entity db fn-id)
          {:crux.db.fn/keys [args] :as args-doc} (db/get-single-object object-store snapshot (c/new-id args-v))
          args-id (:crux.db/id args-doc)
          fn-result (try
                      (let [tx-ops (apply (tx-fn-eval-cache body) db (eval args))
                            _ (s/assert :crux.api/tx-ops tx-ops)
                            docs (tx-ops->docs tx-ops)
                            {arg-docs true docs false} (group-by (comp boolean :crux.db.fn/args) docs)]
                        ;; TODO: might lead to orphaned and unevictable
                        ;; argument docs if the transaction fails. As
                        ;; these nested docs never go to the doc topic,
                        ;; it's slightly less of an issue. It's done
                        ;; this way to support nested fns.
                        (doseq [arg-doc arg-docs]
                          (db/index-doc indexer (c/new-id arg-doc) arg-doc))
                        {:docs (vec docs)
                         :ops-result (vec (for [[op :as tx-op] (tx-ops->tx-events tx-ops)]
                                            ((get tx-op->command op tx-command-unknown)
                                             indexer kv object-store snapshot tx-log tx-op transact-time tx-id)))})
                      (catch Throwable t
                        t))]
      (if (instance? Throwable fn-result)
        {:pre-commit-fn (fn []
                          (log-tx-fn-error fn-result fn-id body args-id args)
                          false)}
        (let [{:keys [docs ops-result]} fn-result]
          {:pre-commit-fn #(do (doseq [doc docs]
                                 (db/index-doc indexer (c/new-id doc) doc))
                               (every? true? (for [{:keys [pre-commit-fn]} ops-result
                                                   :when pre-commit-fn]
                                               (pre-commit-fn))))
           :kvs (vec (apply concat
                            (when args-doc
                              [[(c/encode-entity+vt+tt+tx-id-key-to
                                 nil
                                 (c/->id-buffer args-id)
                                 transact-time
                                 transact-time
                                 tx-id)
                                (c/->id-buffer args-v)]
                               [(c/encode-entity+z+tx-id-key-to
                                 nil
                                 (c/->id-buffer args-id)
                                 (c/encode-entity-tx-z-number transact-time transact-time)
                                 tx-id)
                                (c/->id-buffer args-v)]])
                            (map :kvs ops-result)))
           :post-commit-fn #(doseq [{:keys [post-commit-fn]} ops-result
                                    :when post-commit-fn]
                              (post-commit-fn))})))))

(defn tx-command-unknown [indexer kv object-store snapshot tx-log [op k start-valid-time end-valid-time keep-latest? keep-earliest?] transact-time tx-id]
  (throw (IllegalArgumentException. (str "Unknown tx-op: " op))))

(def ^:private tx-op->command
  {:crux.tx/put tx-command-put
   :crux.tx/delete tx-command-delete
   :crux.tx/cas tx-command-cas
   :crux.tx/evict tx-command-evict
   :crux.tx/fn tx-command-fn})

(def ^:dynamic *current-tx*)

(defrecord KvIndexer [kv tx-log object-store ^ExecutorService stats-executor]
  Closeable
  (close [_]
    (when stats-executor
      (doto stats-executor
        (.shutdown)
        (.awaitTermination 60000 TimeUnit/MILLISECONDS))))

  db/Indexer
  (index-doc [_ content-hash doc]
    (log/debug "Indexing doc:" content-hash)
    (when (not (contains? doc :crux.db/id))
      (throw (IllegalArgumentException.
              (str "Missing required attribute :crux.db/id: " (pr-str doc)))))
    (let [content-hash (c/new-id content-hash)
          evicted? (idx/evicted-doc? doc)]
      (when-let [normalized-doc (if evicted?
                                  (when-let [existing-doc (with-open [snapshot (kv/new-snapshot kv)]
                                                            (db/get-single-object object-store snapshot content-hash))]
                                    (idx/delete-doc-from-index kv content-hash existing-doc))
                                  (idx/index-doc kv content-hash doc))]
        (let [stats-fn #(idx/update-predicate-stats kv evicted? normalized-doc)]
          (if stats-executor
            (.submit stats-executor ^Runnable stats-fn)
            (stats-fn))))
      (db/put-objects object-store [[content-hash doc]])))

  (index-tx [this tx-events tx-time tx-id]
    (s/assert :crux.tx.event/tx-events tx-events)
    (with-open [snapshot (kv/new-snapshot kv)]
      (binding [*current-tx* {:crux.tx/tx-id tx-id
                              :crux.tx/tx-time tx-time
                              :crux.tx.event/tx-events tx-events}]
        (let [tx-command-results (vec (for [[op :as tx-event] tx-events]
                                        ((get tx-op->command op tx-command-unknown)
                                         this kv object-store snapshot tx-log tx-event tx-time tx-id)))]
          (log/debug "Indexing tx-id:" tx-id "tx-events:" (count tx-events))
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
            (do (log/warn "Transaction aborted:" (pr-str tx-events) (pr-str tx-time) tx-id)
                (kv/store kv [[(c/encode-failed-tx-id-key-to nil tx-id) c/empty-buffer]])))))))

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

(defn tx-events->tx-ops [snapshot object-store tx-events]
  (let [tx-ops (vec (for [[op id & args] tx-events]
                      (cond->> (for [arg args]
                                 (or (when (satisfies? c/IdToBuffer arg)
                                       (db/get-single-object object-store snapshot arg))
                                     arg))
                        (contains? #{:crux.tx/delete
                                     :crux.tx/evict
                                     :crux.tx/fn} op) (cons (c/new-id id))
                        true (cons op)
                        true (vec))))]
    (s/assert :crux.api/tx-ops tx-ops)
    tx-ops))

(defn latest-completed-tx-time [consumer-state]
  (let [consumer-states (->> consumer-state
                             (vals)
                             (sort-by :time))
        consumer-states-without-lag (filter (comp zero? :lag) consumer-states)]
    (if (= consumer-states consumer-states-without-lag)
      (:time (last consumer-states))
      (:time (first consumer-states)))))

(defn await-no-consumer-lag [indexer timeout-ms]
  (let [max-lag-fn #(some->> (db/read-index-meta indexer :crux.tx-log/consumer-state)
                             (vals)
                             (seq)
                             (map :lag)
                             (reduce max 0))]
    (if (cio/wait-while #(if-let [max-lag (max-lag-fn)]
                           (pos? (long max-lag))
                           true)
                        timeout-ms)
      (latest-completed-tx-time (db/read-index-meta indexer :crux.tx-log/consumer-state))
      (throw (TimeoutException.
               (str "Timed out waiting for index to catch up, lag is: " (or (max-lag-fn)
                                                                            "unknown")))))))

(defn await-tx-time [indexer transact-time timeout-ms]
  (let [seen-tx-time (atom (Date. 0))]
    (if (cio/wait-while #(pos? (compare transact-time
                                        (let [completed-tx-time (or (latest-completed-tx-time
                                                                     (db/read-index-meta indexer :crux.tx-log/consumer-state))
                                                                    (Date. 0))]
                                          (reset! seen-tx-time completed-tx-time)
                                          completed-tx-time)))
                        timeout-ms)
      @seen-tx-time
      (throw (TimeoutException.
              (str "Timed out waiting for: " (pr-str transact-time)
                   " index has: " (pr-str @seen-tx-time)))))))
