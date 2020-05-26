(ns ^:no-doc crux.tx
  (:require [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [crux.bus :as bus]
            [crux.codec :as c]
            [crux.db :as db]
            [crux.index :as idx]
            [crux.io :as cio]
            [crux.kv :as kv]
            [crux.query :as q]
            [crux.tx.conform :as txc]
            [crux.tx.event :as txe]
            [clojure.set :as set])
  (:import crux.codec.EntityTx
           java.io.Closeable
           java.time.Duration
           [java.util.concurrent Executors ExecutorService TimeoutException TimeUnit]
           java.util.Date))

(set! *unchecked-math* :warn-on-boxed)

(def ^:private date? (partial instance? Date))

(s/def ::tx-id nat-int?)
(s/def ::tx-time date?)
(s/def ::submitted-tx (s/keys :req [::tx-id ::tx-time]))
(s/def ::committed? boolean?)
(s/def ::av-count nat-int?)
(s/def ::bytes-indexed nat-int?)
(s/def ::doc-ids (s/coll-of #(instance? crux.codec.Id %) :kind set?))

(defmethod bus/event-spec ::indexing-docs [_] (s/keys :req-un [::doc-ids]))
(defmethod bus/event-spec ::indexed-docs [_] (s/keys :req-un [::doc-ids ::av-count ::bytes-indexed]))
(defmethod bus/event-spec ::indexing-tx [_] (s/keys :req [::submitted-tx]))
(defmethod bus/event-spec ::indexed-tx [_] (s/keys :req [::submitted-tx ::txe/tx-events], :req-un [::committed?]))

(defprotocol EntityHistory
  (with-entity-history-seq-ascending [_ eid valid-time f])
  (with-entity-history-seq-descending [_ eid valid-time f])
  (entity-at [_ eid valid-time tx-time])
  (with-etxs [_ etxs]))

(defn- etx->vt [^EntityTx etx]
  (.vt etx))

(defn- merge-histories [keyfn compare left right]
  (lazy-seq
   (cond
     (nil? (seq left)) right
     (nil? (seq right)) left
     :else (let [[l & more-left] left
                 [r & more-right] right]
             (case (Integer/signum (compare (keyfn l) (keyfn r)))
               -1 (cons l (merge-histories keyfn compare more-left right))
               0 (cons r (merge-histories keyfn compare more-left more-right))
               1 (cons r (merge-histories keyfn compare left more-right)))))))

(defrecord IndexStore+NewETXs [index-store etxs]
  EntityHistory
  (with-entity-history-seq-ascending [_ eid valid-time f]
    (with-open [nested-index-store (db/open-nested-index-store index-store)
                history (db/open-entity-history nested-index-store eid :asc
                                                {:start {:crux.db/valid-time valid-time}})]
      (f (merge-histories etx->vt compare
                          (iterator-seq history)
                          (->> (get etxs eid)
                               (drop-while (comp neg? #(compare % valid-time) etx->vt)))))))

  (with-entity-history-seq-descending [_ eid valid-time f]
    (with-open [nested-index-store (db/open-nested-index-store index-store)
                history (db/open-entity-history nested-index-store eid :desc
                                                {:start {:crux.db/valid-time valid-time}})]
      (f (merge-histories etx->vt #(compare %2 %1)
                          (iterator-seq history)
                          (->> (reverse (get etxs eid))
                               (drop-while (comp pos? #(compare % valid-time) etx->vt)))))))

  (entity-at [_ eid valid-time tx-time]
    (->> (merge-histories etx->vt #(compare %2 %1)
                          (some->> (db/entity-as-of index-store eid valid-time tx-time)
                                   vector)
                          (some->> (reverse (get etxs eid))
                                   (drop-while (comp pos? #(compare % valid-time) etx->vt))
                                   first
                                   vector))
         first))

  (with-etxs [_ new-etxs]
    (->IndexStore+NewETXs index-store
                          (merge-with #(merge-histories etx->vt compare %1 %2)
                                      etxs
                                      (->> new-etxs (group-by #(.eid ^EntityTx %)))))))



(defmulti index-tx-event
  (fn [[op :as tx-event] tx tx-consumer]
    op))

(defn- put-delete-etxs [k start-valid-time end-valid-time content-hash {::keys [tx-time tx-id]} {:keys [history]}]
  (let [eid (c/new-id k)
        ->new-entity-tx (fn [vt]
                          (c/->EntityTx eid vt tx-time tx-id content-hash))

        start-valid-time (or start-valid-time tx-time)]

    (if end-valid-time
      (when-not (= start-valid-time end-valid-time)
        (with-entity-history-seq-descending history eid end-valid-time
          (fn [entity-history]
            (into (->> (cons start-valid-time
                             (->> (map etx->vt entity-history)
                                  (take-while #(neg? (compare start-valid-time %)))))
                       (remove #{end-valid-time})
                       (mapv ->new-entity-tx))

                  [(if-let [entity-to-restore ^EntityTx (first entity-history)]
                     (-> entity-to-restore
                         (assoc :vt end-valid-time))

                     (c/->EntityTx eid end-valid-time tx-time tx-id c/nil-id-buffer))]))))

      (->> (cons start-valid-time
                 (when-let [visible-entity (some-> (entity-at history eid start-valid-time tx-time)
                                                   (select-keys [:tx-time :tx-id :content-hash]))]
                   (with-entity-history-seq-ascending history eid start-valid-time
                     (fn [entity-history]
                       (->> entity-history
                            (remove #{start-valid-time})
                            (take-while #(= visible-entity (select-keys % [:tx-time :tx-id :content-hash])))
                            (mapv etx->vt))))))

           (map ->new-entity-tx)))))

(defmethod index-tx-event :crux.tx/put [[op k v start-valid-time end-valid-time] tx tx-consumer]
  {:etxs (put-delete-etxs k start-valid-time end-valid-time (c/new-id v) tx tx-consumer)})

(defmethod index-tx-event :crux.tx/delete [[op k start-valid-time end-valid-time] tx tx-consumer]
  {:etxs (put-delete-etxs k start-valid-time end-valid-time nil tx tx-consumer)})

(defmethod index-tx-event :crux.tx/match [[op k v at-valid-time :as match-op]
                                          {::keys [tx-time tx-id] :as tx}
                                          {:keys [history] :as tx-consumer}]
  {:pre-commit-fn #(let [{:keys [content-hash] :as entity} (entity-at history
                                                                      (c/new-id k)
                                                                      (or at-valid-time tx-time)
                                                                      tx-time)]
                     (or (= (c/new-id content-hash) (c/new-id v))
                         (log/debug "crux.tx/match failure:" (cio/pr-edn-str match-op) "was:" (c/new-id content-hash))))})

(defmethod index-tx-event :crux.tx/cas [[op k old-v new-v at-valid-time :as cas-op]
                                        {::keys [tx-time tx-id] :as tx}
                                        {:keys [history document-store] :as tx-consumer}]
  (let [eid (c/new-id k)
        valid-time (or at-valid-time tx-time)]

    {:pre-commit-fn #(let [{:keys [content-hash] :as entity} (entity-at history eid valid-time tx-time)
                           current-id (c/new-id content-hash)
                           expected-id (c/new-id old-v)]
                       ;; see juxt/crux#362 - we'd like to just compare content hashes here, but
                       ;; can't rely on the old content-hashing returning the same hash for the same document
                       (or (= current-id expected-id)
                           (let [docs (db/fetch-docs document-store #{current-id expected-id})]
                             (= (get docs current-id)
                                (get docs expected-id)))
                           (log/warn "CAS failure:" (cio/pr-edn-str cas-op) "was:" (c/new-id content-hash))))

     :etxs (put-delete-etxs eid valid-time nil (c/new-id new-v) tx tx-consumer)}))

(def evict-time-ranges-env-var "CRUX_EVICT_TIME_RANGES")
(def ^:dynamic *evict-all-on-legacy-time-ranges?* (= (System/getenv evict-time-ranges-env-var) "EVICT_ALL"))

(defmethod index-tx-event :crux.tx/evict [[op k & legacy-args] tx {:keys [history] :as tx-consumer}]
  (let [eid (c/new-id k)]
    {:pre-commit-fn #(cond
                       (empty? legacy-args) true

                       (not *evict-all-on-legacy-time-ranges?*)
                       (throw (IllegalArgumentException. (str "Evict no longer supports time-range parameters. "
                                                              "See https://github.com/juxt/crux/pull/438 for more details, and what to do about this message.")))

                       :else (do
                               (log/warnf "Evicting '%s' for all valid-times, '%s' set"
                                          k evict-time-ranges-env-var)
                               true))

     :evict-eids #{k}}))

(def ^:private tx-fn-eval-cache (memoize eval))

(defn log-tx-fn-error [fn-result fn-id body args-id args]
  (log/warn fn-result "Transaction function failure:" fn-id (cio/pr-edn-str body) args-id (cio/pr-edn-str args)))

(def tx-fns-enabled? (Boolean/parseBoolean (System/getenv "CRUX_ENABLE_TX_FNS")))

(defmethod index-tx-event :crux.tx/fn [[op k args-v :as tx-op]
                                       {::keys [tx-time tx-id] :as tx}
                                       {:keys [indexer index-store document-store nested-fn-args query-engine], :as tx-consumer}]
  (when-not tx-fns-enabled?
    (throw (IllegalArgumentException. (str "Transaction functions not enabled: " (cio/pr-edn-str tx-op)))))

  (let [fn-id (c/new-id k)
        db (q/db query-engine tx-time tx-time)
        {:crux.db.fn/keys [body] :as fn-doc} (q/entity db index-store fn-id)
        {:crux.db.fn/keys [args] :as args-doc} (let [arg-id (c/new-id args-v)]
                                                 (or (get nested-fn-args arg-id)
                                                     (-> (db/fetch-docs document-store #{arg-id})
                                                         (get arg-id))))
        args-id (:crux.db/id args-doc)

        {:keys [conformed-tx-ops fn-error]} (try
                                              {:conformed-tx-ops (->> (apply (tx-fn-eval-cache body) db args)
                                                                      (mapv txc/conform-tx-op))}

                                    (catch Throwable t
                                      {:fn-error t}))]

    (if fn-error
      {:pre-commit-fn (fn []
                        (log-tx-fn-error fn-error fn-id body args-id args)
                        false)}

      (let [docs (->> conformed-tx-ops
                      (into {} (mapcat :docs)))
            {arg-docs true docs false} (group-by (comp boolean :crux.db.fn/args val) docs)
            _ (db/index-docs indexer (into {} docs))
            _ (db/submit-docs document-store (into {} docs))
            op-results (vec (for [[op :as tx-event] (map txc/->tx-event conformed-tx-ops)]
                              (index-tx-event tx-event tx
                                              (-> tx-consumer
                                                  (update :nested-fn-args (fnil into {}) arg-docs)))))]
        {:pre-commit-fn #(every? true? (for [{:keys [pre-commit-fn]} op-results
                                             :when pre-commit-fn]
                                         (pre-commit-fn)))
         :etxs (cond-> (mapcat :etxs op-results)
                 args-doc (conj (c/->EntityTx args-id tx-time tx-time tx-id args-v)))

         :evict-eids (into #{} (mapcat :evict-eids) op-results)}))))

(defmethod index-tx-event :default [[op & _] tx tx-consumer]
  (throw (IllegalArgumentException. (str "Unknown tx-op: " op))))

(def ^:dynamic *current-tx*)

;; TODO: Rename :crux.kv/stats on next index bump.
(defn- update-stats [{:keys [indexer ^ExecutorService stats-executor] :as tx-consumer} docs-stats]
  (let [stats-fn ^Runnable #(->> (apply merge-with + (db/read-index-meta indexer :crux.kv/stats) docs-stats)
                                 (db/store-index-meta indexer :crux.kv/stats))]
    (if stats-executor
      (.submit stats-executor stats-fn)
      (stats-fn))))

(defn index-docs [{:keys [bus indexer] :as tx-consumer} docs]
  (when-let [missing-ids (seq (remove :crux.db/id (vals docs)))]
    (throw (IllegalArgumentException.
            (str "Missing required attribute :crux.db/id: " (cio/pr-edn-str missing-ids)))))

  (with-open [index-store (db/open-index-store indexer)]
    (when (seq docs)
      (bus/send bus {:crux/event-type ::indexing-docs, :doc-ids (set (keys docs))})

      (let [{:keys [bytes-indexed indexed-docs]} (db/index-docs indexer docs)]
        (update-stats tx-consumer (->> (vals indexed-docs)
                                       (map idx/doc-predicate-stats)))

        (bus/send bus {:crux/event-type ::indexed-docs,
                       :doc-ids (set (keys docs))
                       :av-count (->> (vals indexed-docs) (apply concat) (count))
                       :bytes-indexed bytes-indexed})))))

(defn index-tx [{:keys [bus indexer kv-store document-store] :as tx-consumer}
                {::keys [tx-time tx-id] :as tx}
                tx-events]
  (s/assert ::txe/tx-events tx-events)

  (log/debug "Indexing tx-id:" tx-id "tx-events:" (count tx-events))
  (bus/send bus {:crux/event-type ::indexing-tx, ::submitted-tx tx})

  (with-open [index-store (db/open-index-store indexer)]
    (binding [*current-tx* (assoc tx ::txe/tx-events tx-events)]
      (let [tx-consumer (assoc tx-consumer :index-store index-store)
            res (reduce (fn [{:keys [history] :as acc} tx-event]
                          (let [{:keys [pre-commit-fn etxs evict-eids]} (index-tx-event tx-event tx (assoc tx-consumer :history history))]
                            (if (and pre-commit-fn (not (pre-commit-fn)))
                              (reduced ::aborted)
                              (-> acc
                                  (update :history with-etxs etxs)
                                  (update :evict-eids set/union evict-eids)))))

                        {:history (->IndexStore+NewETXs index-store {})
                         :evict-eids #{}}

                        tx-events)
            committed? (not= res ::aborted)]

        (if committed?
          (do
            (when-let [evict-eids (not-empty (:evict-eids res))]
              (let [{:keys [tombstones]} (db/unindex-eids indexer evict-eids)]
                (db/submit-docs document-store tombstones)))

            (db/index-entity-txs indexer tx (->> (get-in res [:history :etxs]) (mapcat val))))

          (do
            (log/warn "Transaction aborted:" (cio/pr-edn-str tx-events) (cio/pr-edn-str tx-time) tx-id)
            (db/mark-tx-as-failed indexer tx)))

        (bus/send bus {:crux/event-type ::indexed-tx,
                       ::submitted-tx tx,
                       :committed? committed?
                       ::txe/tx-events tx-events})))))

(defn- index-tx-log [{:keys [!error tx-log indexer document-store] :as tx-consumer} {::keys [^Duration poll-sleep-duration]}]
  (log/info "Started tx-consumer")
  (try
    (while true
      (let [consumed-txs? (when-let [tx-stream (try
                                                 (db/open-tx-log tx-log (::tx-id (db/latest-completed-tx indexer)))
                                                 (catch InterruptedException e (throw e))
                                                 (catch Exception e
                                                   (log/warn e "Error reading TxLog, will retry")))]
                            (try
                              (let [tx-log-entries (iterator-seq tx-stream)
                                    consumed-txs? (not (empty? tx-log-entries))]
                                (doseq [tx-log-entry (partition-all 1000 tx-log-entries)]
                                  (doseq [docs (->> tx-log-entry
                                                    (mapcat ::txe/tx-events)
                                                    (txc/tx-events->docs document-store)
                                                    (partition-all 100))]
                                    (index-docs tx-consumer docs)
                                    (when (Thread/interrupted)
                                      (throw (InterruptedException.))))

                                  (doseq [{:keys [::txe/tx-events] :as tx} tx-log-entry
                                          :let [tx (select-keys tx [::tx-time ::tx-id])]]
                                    (index-tx tx-consumer tx tx-events)

                                    (when (Thread/interrupted)
                                      (throw (InterruptedException.)))))
                                consumed-txs?)
                              (finally
                                (.close ^Closeable tx-stream))))]
        (when (Thread/interrupted)
          (throw (InterruptedException.)))
        (when-not consumed-txs?
          (Thread/sleep (.toMillis poll-sleep-duration)))))

    (catch InterruptedException e)
    (catch Throwable e
      (reset! !error e)
      (log/error e "Error consuming transactions")))

  (log/info "Shut down tx-consumer"))

(defrecord TxConsumer [^Thread executor-thread ^ExecutorService stats-executor !error indexer document-store tx-log bus]
  db/TxConsumer
  (consumer-error [_] @!error)

  Closeable
  (close [_]
    (when executor-thread
      (.interrupt executor-thread)
      (.join executor-thread))
    (when stats-executor
      (doto stats-executor
        (.shutdown)
        (.awaitTermination 60000 TimeUnit/MILLISECONDS)))))

(def tx-consumer
  {:start-fn (fn [{:crux.node/keys [indexer document-store tx-log bus query-engine]} args]
               (let [stats-executor (Executors/newSingleThreadExecutor (cio/thread-factory "crux.tx.update-stats-thread"))
                     tx-consumer (map->TxConsumer
                                  {:!error (atom nil)
                                   :indexer indexer
                                   :document-store document-store
                                   :tx-log tx-log
                                   :bus bus
                                   :query-engine query-engine
                                   :stats-executor stats-executor})]
                 (assoc tx-consumer
                        :executor-thread
                        (doto (Thread. #(index-tx-log tx-consumer args))
                          (.setName "crux-tx-consumer")
                          (.start)))))
   :deps [:crux.node/indexer :crux.node/document-store :crux.node/tx-log :crux.node/bus :crux.node/query-engine]
   :args {::poll-sleep-duration {:default (Duration/ofMillis 100)
                                 :doc "How long to sleep between polling for new transactions"
                                 :crux.config/type :crux.config/duration}}})

(defn await-tx [indexer tx-consumer {::keys [tx-id] :as tx} timeout]
  (let [seen-tx (atom nil)]
    (if (cio/wait-while #(if-let [err (db/consumer-error tx-consumer)]
                           (throw (Exception. "Transaction consumer aborted" err))
                           (let [latest-completed-tx (db/latest-completed-tx indexer)]
                             (reset! seen-tx latest-completed-tx)
                             (or (nil? latest-completed-tx)
                                 (pos? (compare tx-id (::tx-id latest-completed-tx))))))
                        timeout)
      @seen-tx
      (throw (TimeoutException.
              (str "Timed out waiting for: " (cio/pr-edn-str tx)
                   " index has: " (cio/pr-edn-str @seen-tx)))))))

(defn await-tx-time [indexer tx-consumer tx-time timeout]
  (let [seen-tx (atom nil)]
    (if (cio/wait-while #(if-let [err (db/consumer-error tx-consumer)]
                           (throw (Exception. "Transaction consumer aborted" err))
                           (let [latest-completed-tx (db/latest-completed-tx indexer)]
                             (reset! seen-tx latest-completed-tx)
                             (or (nil? latest-completed-tx)
                                 (pos? (compare tx-time (::tx-time latest-completed-tx))))))
                        timeout)
      @seen-tx
      (throw (TimeoutException.
              (str "Timed out waiting for: " (cio/pr-edn-str tx-time)
                   " index has: " (cio/pr-edn-str @seen-tx)))))))
