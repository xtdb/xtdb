(ns ^:no-doc crux.tx
  (:require [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [crux.error :as err]
            [crux.bus :as bus]
            [crux.cache.nop :as nop-cache]
            [crux.codec :as c]
            [crux.db :as db]
            [crux.io :as cio]
            [crux.tx.conform :as txc]
            [crux.tx.event :as txe]
            [crux.api :as api]
            [crux.fork :as fork]
            [crux.kv.index-store :as kvi]
            [crux.mem-kv :as mem-kv]
            [crux.system :as sys])
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

(s/def ::ingester-error #(instance? Exception %))
(defmethod bus/event-spec ::ingester-error [_] (s/keys :req [::ingester-error]))

(defn- etx->vt [^EntityTx etx]
  (.vt etx))

(defmulti index-tx-event
  (fn [[op :as tx-event] tx tx-ingester]
    op))

(defn- put-delete-etxs [k start-valid-time end-valid-time content-hash
                        {::keys [tx-time tx-id], :keys [crux.db/valid-time]} {:keys [index-snapshot]}]
  (let [eid (c/new-id k)
        ->new-entity-tx (fn [vt]
                          (c/->EntityTx eid vt tx-time tx-id content-hash))

        start-valid-time (or start-valid-time valid-time tx-time)]

    (if end-valid-time
      (when-not (= start-valid-time end-valid-time)
        (let [entity-history (db/entity-history index-snapshot eid :desc {:start {:crux.db/valid-time end-valid-time}})]
          (into (->> (cons start-valid-time
                           (->> (map etx->vt entity-history)
                                (take-while #(neg? (compare start-valid-time %)))))
                     (remove #{end-valid-time})
                     (mapv ->new-entity-tx))

                [(if-let [entity-to-restore ^EntityTx (first entity-history)]
                   (-> entity-to-restore
                       (assoc :vt end-valid-time))

                   (c/->EntityTx eid end-valid-time tx-time tx-id c/nil-id-buffer))])))

      (->> (cons start-valid-time
                 (when-let [visible-entity (some-> (db/entity-as-of index-snapshot eid start-valid-time tx-time)

                                                   (select-keys [:tx-time :tx-id :content-hash]))]
                   (->> (db/entity-history index-snapshot eid :asc {:start {:crux.db/valid-time start-valid-time}})
                        (remove (comp #{start-valid-time} :valid-time))
                        (take-while #(= visible-entity (select-keys % [:tx-time :tx-id :content-hash])))
                        (mapv etx->vt))))

           (map ->new-entity-tx)))))

(defmethod index-tx-event :crux.tx/put [[op k v start-valid-time end-valid-time] tx tx-ingester]
  {:etxs (put-delete-etxs k start-valid-time end-valid-time (c/new-id v) tx tx-ingester)})

(defmethod index-tx-event :crux.tx/delete [[op k start-valid-time end-valid-time] tx tx-ingester]
  {:etxs (put-delete-etxs k start-valid-time end-valid-time nil tx tx-ingester)})

(defmethod index-tx-event :crux.tx/match [[op k v at-valid-time :as match-op]
                                          {::keys [tx-time tx-id], :keys [crux.db/valid-time], :as tx}
                                          {:keys [index-snapshot] :as tx-ingester}]
  {:pre-commit-fn #(let [content-hash (db/entity-as-of-resolver index-snapshot
                                                                (c/new-id k)
                                                                (or at-valid-time valid-time tx-time)
                                                                tx-time)]
                     (or (= (c/new-id content-hash) (c/new-id v))
                         (log/debug "crux.tx/match failure:" (cio/pr-edn-str match-op) "was:" (c/new-id content-hash))))})

(defmethod index-tx-event :crux.tx/cas [[op k old-v new-v at-valid-time :as cas-op]
                                        {::keys [tx-time tx-id], :keys [crux.db/valid-time] :as tx}
                                        {:keys [index-snapshot document-store] :as tx-ingester}]
  (let [eid (c/new-id k)
        valid-time (or at-valid-time valid-time tx-time)]

    {:pre-commit-fn #(let [content-hash (db/entity-as-of-resolver index-snapshot eid valid-time tx-time)
                           current-id (c/new-id content-hash)
                           expected-id (c/new-id old-v)]
                       ;; see juxt/crux#362 - we'd like to just compare content hashes here, but
                       ;; can't rely on the old content-hashing returning the same hash for the same document
                       (or (= current-id expected-id)
                           (let [docs (db/fetch-docs document-store #{current-id expected-id})]
                             (= (get docs current-id)
                                (get docs expected-id)))
                           (log/warn "CAS failure:" (cio/pr-edn-str cas-op) "was:" (c/new-id content-hash))))

     :etxs (put-delete-etxs eid valid-time nil (c/new-id new-v) tx tx-ingester)}))

(def evict-time-ranges-env-var "CRUX_EVICT_TIME_RANGES")
(def ^:dynamic *evict-all-on-legacy-time-ranges?* (= (System/getenv evict-time-ranges-env-var) "EVICT_ALL"))

(defmethod index-tx-event :crux.tx/evict [[op k & legacy-args] tx _]
  (let [eid (c/new-id k)]
    {:pre-commit-fn #(cond
                       (empty? legacy-args) true

                       (not *evict-all-on-legacy-time-ranges?*)
                       (throw (err/illegal-arg :evict-with-time-range
                                               {::err/message (str "Evict no longer supports time-range parameters. "
                                                                   "See https://github.com/juxt/crux/pull/438 for more details, and what to do about this message.")}))

                       :else (do
                               (log/warnf "Evicting '%s' for all valid-times, '%s' set"
                                          k evict-time-ranges-env-var)
                               true))

     :evict-eids #{k}}))

(def ^:private tx-fn-eval-cache (memoize eval))

;; for tests
(def ^:private !last-tx-fn-error (atom nil))

(defn- reset-tx-fn-error []
  (first (reset-vals! !last-tx-fn-error nil)))

(defn- ->tx-fn [{body :crux.db/fn
                 legacy-body :crux.db.fn/body}]
  (or (tx-fn-eval-cache body)
      (when legacy-body
        (let [f (tx-fn-eval-cache legacy-body)]
          (fn [ctx & args]
            (apply f (api/db ctx) args))))))

(defrecord TxFnContext [db-provider indexing-tx]
  api/DBProvider
  (db [ctx] (api/db db-provider (:crux.tx/tx-time indexing-tx)))
  (db [ctx valid-time] (api/db db-provider valid-time))
  (db [ctx valid-time tx-time] (api/db db-provider valid-time tx-time))
  (open-db [ctx] (api/open-db db-provider (:crux.tx/tx-time indexing-tx)))
  (open-db [ctx valid-time] (api/open-db db-provider valid-time))
  (open-db [ctx valid-time tx-time] (api/open-db db-provider valid-time tx-time))

  api/TransactionFnContext
  (indexing-tx [_] indexing-tx))

(defmethod index-tx-event :crux.tx/fn [[op k args-doc :as tx-op]
                                       {:crux.tx/keys [tx-time tx-id] :as tx}
                                       {:keys [query-engine document-store index-snapshot], :as tx-ingester}]
  (let [fn-id (c/new-id k)
        {args-doc-id :crux.db/id, :crux.db.fn/keys [args tx-events failed?]} args-doc
        args-content-hash (c/new-id args-doc)

        {:keys [tx-events docs failed?]}
        (cond
          tx-events {:tx-events tx-events
                     :docs (txc/tx-events->docs document-store tx-events)}

          failed? (do
                    (log/warn "Transaction function failed when originally evaluated:"
                              fn-id args-doc-id
                              (pr-str (select-keys args-doc [:crux.db.fn/exception
                                                             :crux.db.fn/message
                                                             :crux.db.fn/ex-data])))
                    {:failed? true})

          :else (try
                  (let [ctx (->TxFnContext query-engine tx)
                        db (api/db query-engine tx-time)
                        res (apply (->tx-fn (api/entity db fn-id)) ctx args)]
                    (if (false? res)
                      {:failed? true}

                      (let [conformed-tx-ops (mapv txc/conform-tx-op res)
                            tx-events (mapv txc/->tx-event conformed-tx-ops)]
                        {:tx-events tx-events
                         :docs (into {args-content-hash {:crux.db/id args-doc-id
                                                         :crux.db.fn/tx-events tx-events}}
                                     (mapcat :docs)
                                     conformed-tx-ops)})))

                  (catch Throwable t
                    (reset! !last-tx-fn-error t)
                    (log/warn t "Transaction function failure:" fn-id args-doc-id)

                    {:failed? true
                     :fn-error t
                     :docs {args-content-hash {:crux.db.fn/failed? true
                                               :crux.db.fn/exception (symbol (.getName (class t)))
                                               :crux.db.fn/message (ex-message t)
                                               :crux.db.fn/ex-data (ex-data t)}}})))]
    (if failed?
      {:pre-commit-fn (constantly false)
       :docs docs}

      {:tx-events tx-events
       :docs docs})))

(defmethod index-tx-event :default [[op & _] tx tx-ingester]
  (throw (err/illegal-arg :unknown-tx-op {:op op})))

(defn- update-stats [{:keys [index-store ^ExecutorService stats-executor] :as tx-ingester} docs-stats]
  (let [stats-fn ^Runnable #(->> (apply merge-with + (db/read-index-meta index-store :crux/attribute-stats) docs-stats)
                                 (db/store-index-meta index-store :crux/attribute-stats))]
    (if stats-executor
      (.submit stats-executor stats-fn)
      (stats-fn))))

(defn- doc-predicate-stats [doc]
  (->> (for [[k v] doc]
         [k (count (c/vectorize-value v))])
       (into {})))

(defn index-docs [{:keys [bus index-store] :as tx-ingester} docs]
  (when-let [missing-ids (seq (remove :crux.db/id (vals docs)))]
    (throw (err/illegal-arg :missing-eid {::err/message "Missing required attribute :crux.db/id"
                                          :docs missing-ids})))

  (when (seq docs)
    (bus/send bus {:crux/event-type ::indexing-docs, :doc-ids (set (keys docs))})

    (let [{:keys [bytes-indexed indexed-docs]} (db/index-docs index-store docs)]
      (update-stats tx-ingester (->> (vals indexed-docs)
                                     (map doc-predicate-stats)))

      (bus/send bus {:crux/event-type ::indexed-docs,
                     :doc-ids (set (keys docs))
                     :av-count (->> (vals indexed-docs) (apply concat) (count))
                     :bytes-indexed bytes-indexed}))))

(defn with-tx-fn-args [[op & args :as evt] {:keys [document-store]}]
  (case op
    :crux.tx/fn (let [[fn-eid arg-doc-id] args]
                  (cond-> [op fn-eid]
                    arg-doc-id (conj (-> (db/fetch-docs document-store #{arg-doc-id})
                                         (get arg-doc-id)))))
    evt))

(defrecord InFlightTx [tx !state !tx-events !error
                       forked-index-store forked-document-store
                       query-engine index-store document-store bus
                       stats-executor]
  db/DocumentStore
  (submit-docs [_ docs]
    (db/submit-docs forked-document-store docs))

  (fetch-docs [_ ids]
    (db/fetch-docs forked-document-store ids))

  api/DBProvider
  (db [ctx] (api/db query-engine (:crux.tx/tx-time tx)))
  (db [ctx valid-time] (api/db query-engine valid-time))
  (db [ctx valid-time tx-time] (api/db query-engine valid-time tx-time))
  (open-db [ctx] (api/open-db query-engine (:crux.tx/tx-time tx)))
  (open-db [ctx valid-time] (api/open-db query-engine valid-time))
  (open-db [ctx valid-time tx-time] (api/open-db query-engine valid-time tx-time))

  db/InFlightTx
  (index-tx-events [this tx-events]
    (when (not= @!state :open)
      (throw (IllegalStateException. "Transaction marked as " (name @!state))))

    (swap! !tx-events into tx-events)

    (try
      (index-docs this (txc/tx-events->docs forked-document-store tx-events))
      (let [forked-deps {:index-store forked-index-store
                         :document-store forked-document-store
                         :query-engine (assoc query-engine :index-store forked-index-store)}
            abort? (loop [[tx-event & more-tx-events] tx-events]
                     (when tx-event
                       (let [{:keys [new-tx-events abort?]}
                             (with-open [index-snapshot (db/open-index-snapshot forked-index-store)]
                               (let [{:keys [pre-commit-fn tx-events evict-eids etxs docs]}
                                     (index-tx-event (-> tx-event
                                                         (with-tx-fn-args forked-deps))
                                                     tx
                                                     (-> forked-deps
                                                         (assoc :index-snapshot index-snapshot)))]
                                 (db/submit-docs forked-document-store docs)

                                 (if (and pre-commit-fn (not (pre-commit-fn)))
                                   {:abort? true}

                                   (do
                                     (doto forked-index-store
                                       (db/index-docs docs)
                                       (db/unindex-eids evict-eids)
                                       (db/index-entity-txs tx etxs))
                                     {:new-tx-events tx-events}))))]
                         (or abort?
                             (recur (concat new-tx-events more-tx-events))))))]
        (when abort?
          (reset! !state :abort-only))

        (not abort?))

      (catch Throwable e
        (reset! !error e)
        (reset! !state :abort-only)
        (bus/send bus {:crux/event-type ::ingester-error, ::ingester-error e})
        (throw e))))

  (commit [this]
    (when-not (compare-and-set! !state :open :committed)
      (throw (IllegalStateException. "Transaction marked as " (name @!state))))

    (when (:fork-at tx)
      (throw (IllegalStateException. "Can't commit from fork.")))

    (when-let [evict-eids (not-empty (fork/newly-evicted-eids forked-index-store))]
      (let [{:keys [tombstones]} (db/unindex-eids index-store evict-eids)]
        (db/submit-docs document-store tombstones)))

    (when-let [new-docs (fork/new-docs forked-document-store)]
      (db/index-docs index-store new-docs)
      (update-stats this (map doc-predicate-stats (vals new-docs)))

      (db/submit-docs document-store new-docs))

    (db/index-entity-txs index-store tx (fork/new-etxs forked-index-store))

    (bus/send bus {:crux/event-type ::indexed-tx,
                   ::submitted-tx tx,
                   :committed? true
                   ::txe/tx-events @!tx-events}))

  (abort [_]
    (swap! !state (fn [state]
                    (if-not (contains? #{:open :abort-only} state)
                      (throw (IllegalStateException. "Transaction marked as " (name @!state)))
                      :aborted)))

    (log/debug "Transaction aborted:" (pr-str tx))

    (db/submit-docs document-store
                    (->> (fork/new-docs forked-document-store)
                         (into {} (filter (comp :crux.db.fn/failed? val)))))

    (db/mark-tx-as-failed index-store tx)

    (bus/send bus {:crux/event-type ::indexed-tx,
                   ::submitted-tx tx,
                   :committed? false
                   ::txe/tx-events @!tx-events})))

(defrecord TxIngester [!error index-store document-store bus query-engine ^ExecutorService stats-executor]
  db/TxIngester
  (begin-tx [_ {:keys [fork-at], ::keys [tx-time] :as tx}]
    (log/debug "Indexing tx-id:" (::tx-id tx))
    (bus/send bus {:crux/event-type ::indexing-tx, ::submitted-tx tx})

    (let [forked-index-store (fork/->forked-index-store index-store (kvi/->kv-index-store {:kv-store (mem-kv/->kv-store)
                                                                                           :value-cache (nop-cache/->nop-cache {})
                                                                                           :cav-cache (nop-cache/->nop-cache {})
                                                                                           :canonical-buffer-cache (nop-cache/->nop-cache {})})
                                                        (::db/valid-time fork-at)
                                                        (::tx-time fork-at))
          forked-document-store (fork/->forked-document-store document-store)]
      (->InFlightTx tx (atom :open) (atom []) !error
                    forked-index-store
                    forked-document-store
                    (assoc query-engine
                           :index-store forked-index-store
                           :document-store forked-document-store)
                    index-store document-store bus
                    stats-executor)))
  (ingester-error [_] @!error)

  Closeable
  (close [_]
    (when stats-executor
      (doto stats-executor
        (.shutdown)
        (.awaitTermination 60000 TimeUnit/MILLISECONDS)))))

(defn ->tx-ingester {::sys/deps {:index-store :crux/index-store
                                 :document-store :crux/document-store
                                 :bus :crux/bus
                                 :query-engine :crux/query-engine}
                     ::sys/args {:stats-executor? {:default true
                                                   :spec ::sys/boolean}}}
  [{:keys [stats-executor?] :as deps}]
  (map->TxIngester (assoc deps
                          :!error (atom nil)
                          :stats-executor (when stats-executor?
                                            (Executors/newSingleThreadExecutor (cio/thread-factory "crux.tx.update-stats-thread"))))))

(defn- index-tx-log [{:keys [tx-ingester index-store ^Duration poll-sleep-duration]} open-next-txs]
  (log/info "Started tx-consumer")
  (try
    (while true
      (let [consumed-txs? (when-let [^crux.api.ICursor
                                     txs (try
                                           (open-next-txs (::tx-id (db/latest-completed-tx index-store)))
                                           (catch InterruptedException e (throw e))
                                           (catch Exception e
                                             (log/warn e "Error polling for txs, will retry")))]
                            (try
                              (let [txs (iterator-seq txs)
                                    consumed-txs? (not (empty? txs))]
                                (doseq [{:keys [::txe/tx-events] :as tx} txs
                                        :let [tx (select-keys tx [::tx-time ::tx-id])]]

                                  (s/assert ::txe/tx-events tx-events)

                                  (let [in-flight-tx (db/begin-tx tx-ingester tx)
                                        res (db/index-tx-events in-flight-tx tx-events)]
                                    (if res
                                      (db/commit in-flight-tx)
                                      (db/abort in-flight-tx)))

                                  (when (Thread/interrupted)
                                    (throw (InterruptedException.))))

                                consumed-txs?)
                              (finally
                                (.close txs))))]
        (when (Thread/interrupted)
          (throw (InterruptedException.)))
        (when-not consumed-txs?
          (Thread/sleep (.toMillis poll-sleep-duration)))))

    (catch InterruptedException e))

  (log/info "Shut down tx-consumer"))

(defn ->polling-tx-consumer {::sys/deps {:index-store :crux/index-store
                                         :tx-ingester :crux/tx-ingester}
                             ::sys/args {:poll-sleep-duration {:spec ::sys/duration
                                                               :default (Duration/ofMillis 100)
                                                               :doc "How long to sleep between polling for new transactions"}}}
  [opts open-next-txs]
  (let [executor-thread (doto (Thread. #(index-tx-log opts open-next-txs))
                          (.setName "crux-polling-tx-consumer")
                          (.start))]
    (reify Closeable
      (close [_]
        (.interrupt executor-thread)
        (.join executor-thread)))))
