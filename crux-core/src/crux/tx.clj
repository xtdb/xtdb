(ns ^:no-doc crux.tx
  (:require [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [crux.api :as api]
            [crux.bus :as bus]
            [crux.codec :as c]
            [crux.db :as db]
            [crux.error :as err]
            [crux.fork :as fork]
            [crux.io :as cio]
            [crux.system :as sys]
            [crux.tx.conform :as txc]
            [crux.tx.event :as txe])
  (:import clojure.lang.MapEntry
           crux.api.ICursor
           crux.codec.EntityTx
           java.io.Closeable
           java.time.Duration
           [java.util.concurrent CompletableFuture Future TimeoutException]
           java.util.Date
           java.util.function.BiConsumer))

(set! *unchecked-math* :warn-on-boxed)

(def ^:private date? (partial instance? Date))

(s/def ::tx-id nat-int?)
(s/def ::tx-time date?)
(s/def ::tx (s/keys :opt [::tx-id ::tx-time]))
(s/def ::submitted-tx (s/keys :req [::tx-id ::tx-time]))
(s/def ::committed? boolean?)
(s/def ::av-count nat-int?)
(s/def ::bytes-indexed nat-int?)
(s/def ::doc-ids (s/coll-of #(instance? crux.codec.Id %) :kind set?))
(s/def ::eids (s/coll-of c/valid-id? :kind set?))
(s/def ::evicting-eids ::eids)

(defmethod bus/event-spec ::indexing-tx [_]
  (s/keys :req-un [::submitted-tx]))

(defmethod bus/event-spec ::committing-tx [_]
  (s/keys :req-un [::submitted-tx ::evicting-eids ::doc-ids]))

(defmethod bus/event-spec ::aborting-tx [_]
  (s/keys :req-un [::submitted-tx]))

(defmethod bus/event-spec ::indexed-tx [_]
  (s/keys :req [::txe/tx-events],
          :req-un [::submitted-tx ::committed?]
          :opt-un [::doc-ids ::av-count ::bytes-indexed]))

(s/def ::ingester-error #(instance? Exception %))
(defmethod bus/event-spec ::ingester-error [_] (s/keys :req-un [::ingester-error]))

(defn- etx->vt [^EntityTx etx]
  (.vt etx))

(defmulti index-tx-event
  (fn [[op :as _tx-event] _tx _in-flight-tx]
    op))

(alter-meta! #'index-tx-event assoc :arglists '([tx-event tx in-flight-tx]))

(defn- put-delete-etxs [k start-valid-time end-valid-time content-hash
                        {::keys [tx-time tx-id], :keys [crux.db/valid-time]}
                        {:keys [index-snapshot]}]
  (let [eid (c/new-id k)
        ->new-entity-tx (fn [vt]
                          (c/->EntityTx eid vt tx-time tx-id content-hash))

        start-valid-time (or start-valid-time valid-time tx-time)]

    (if end-valid-time
      (when-not (= start-valid-time end-valid-time)
        (let [entity-history (db/entity-history index-snapshot eid :desc {:start-valid-time end-valid-time})]
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
                 (when-let [visible-entity (some-> (db/entity-as-of index-snapshot eid start-valid-time tx-id)

                                                   (select-keys [:tx-time :tx-id :content-hash]))]
                   (->> (db/entity-history index-snapshot eid :asc {:start-valid-time start-valid-time})
                        (remove (comp #{start-valid-time} :valid-time))
                        (take-while #(= visible-entity (select-keys % [:tx-time :tx-id :content-hash])))
                        (mapv etx->vt))))

           (map ->new-entity-tx)))))

(defmethod index-tx-event :crux.tx/put [[_op k v start-valid-time end-valid-time] tx in-flight-tx]
  {:etxs (put-delete-etxs k start-valid-time end-valid-time (c/new-id v) tx in-flight-tx)})

(defmethod index-tx-event :crux.tx/delete [[_op k start-valid-time end-valid-time] tx in-flight-tx]
  {:etxs (put-delete-etxs k start-valid-time end-valid-time nil tx in-flight-tx)})

(defmethod index-tx-event :crux.tx/match [[_op k v at-valid-time :as match-op]
                                          {::keys [tx-time tx-id], :keys [crux.db/valid-time]}
                                          {:keys [index-snapshot]}]
  (let [content-hash (db/entity-as-of-resolver index-snapshot
                                               (c/new-id k)
                                               (or at-valid-time valid-time tx-time)
                                               tx-id)
        match? (= (c/new-id content-hash) (c/new-id v))]
    (when-not match?
      (log/debug "crux.tx/match failure:" (cio/pr-edn-str match-op) "was:" (c/new-id content-hash)))

    {:abort? (not match?)}))

(defmethod index-tx-event :crux.tx/cas [[_op k old-v new-v at-valid-time :as cas-op]
                                        {::keys [tx-time tx-id], :keys [crux.db/valid-time] :as tx}
                                        {:keys [index-snapshot] :as in-flight-tx}]
  (let [eid (c/new-id k)
        valid-time (or at-valid-time valid-time tx-time)
        content-hash (db/entity-as-of-resolver index-snapshot eid valid-time tx-id)
        current-id (c/new-id content-hash)
        expected-id (c/new-id old-v)]
    (if (or (= current-id expected-id)
            ;; see juxt/crux#362 - we'd like to just compare content hashes here, but
            ;; can't rely on the old content-hashing returning the same hash for the same document
            (let [docs (db/fetch-docs in-flight-tx #{current-id expected-id})]
              (= (get docs current-id)
                 (get docs expected-id))))
      {:etxs (put-delete-etxs eid valid-time nil (c/new-id new-v) tx in-flight-tx)}
      (do
        (log/warn "CAS failure:" (cio/pr-edn-str cas-op) "was:" (c/new-id content-hash))
        {:abort? true}))))

(def evict-time-ranges-env-var "CRUX_EVICT_TIME_RANGES")
(def ^:dynamic *evict-all-on-legacy-time-ranges?* (= (System/getenv evict-time-ranges-env-var) "EVICT_ALL"))

(defmethod index-tx-event :crux.tx/evict [[_op k & legacy-args] _ _]
  (cond
    (empty? legacy-args) {:evict-eids #{k}}

    (not *evict-all-on-legacy-time-ranges?*)
    (throw (err/illegal-arg :evict-with-time-range
                            {::err/message (str "Evict no longer supports time-range parameters. "
                                                "See https://github.com/juxt/crux/pull/438 for more details, and what to do about this message.")}))

    :else (do
            (log/warnf "Evicting '%s' for all valid-times, '%s' set"
                       k evict-time-ranges-env-var)
            {:evict-eids #{k}})))

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
  (db [_] (api/db db-provider))
  (db [_ valid-time tx-time] (api/db db-provider valid-time tx-time))
  (db [_ valid-time-or-basis] (api/db db-provider valid-time-or-basis))

  (open-db [_] (api/open-db db-provider))
  (open-db [_ valid-time tx-time] (api/open-db db-provider valid-time tx-time))
  (open-db [_ valid-time-or-basis] (api/open-db db-provider valid-time-or-basis))

  api/TransactionFnContext
  (indexing-tx [_] indexing-tx))

(defmethod index-tx-event :crux.tx/fn [[_op k args-content-hash] tx in-flight-tx]
  (let [fn-id (c/new-id k)
        {args-doc-id :crux.db/id,
         :crux.db.fn/keys [args tx-events failed?]
         :as args-doc} (when args-content-hash
                         (-> (db/fetch-docs in-flight-tx #{args-content-hash})
                             (get args-content-hash)))]
    (cond
      tx-events {:tx-events tx-events
                 :docs (db/fetch-docs in-flight-tx (txc/tx-events->doc-hashes tx-events))}

      failed? (do
                (log/warn "Transaction function failed when originally evaluated:"
                          fn-id args-doc-id
                          (pr-str (select-keys args-doc [:crux.db.fn/exception
                                                         :crux.db.fn/message
                                                         :crux.db.fn/ex-data])))
                {:abort? true})

      :else (try
              (let [ctx (->TxFnContext in-flight-tx tx)
                    db (api/db ctx)
                    res (apply (->tx-fn (api/entity db fn-id)) ctx args)]
                (if (false? res)
                  {:abort? true
                   :docs (when args-doc-id
                           {args-content-hash {:crux.db/id args-doc-id
                                               :crux.db.fn/failed? true}})}

                  (let [conformed-tx-ops (mapv txc/conform-tx-op res)
                        tx-events (mapv txc/->tx-event conformed-tx-ops)]
                    {:tx-events tx-events
                     :docs (into (if args-doc-id
                                   {args-content-hash {:crux.db/id args-doc-id
                                                       :crux.db.fn/tx-events tx-events}}
                                   {})
                                 (mapcat :docs)
                                 conformed-tx-ops)})))

              (catch Throwable t
                (reset! !last-tx-fn-error t)
                (log/warn t "Transaction function failure:" fn-id args-doc-id)

                {:abort? true
                 :docs (when args-doc-id
                         {args-content-hash {:crux.db/id args-doc-id
                                             :crux.db.fn/failed? true
                                             :crux.db.fn/exception (symbol (.getName (class t)))
                                             :crux.db.fn/message (ex-message t)
                                             :crux.db.fn/ex-data (ex-data t)}})})))))

(defmethod index-tx-event :default [[op & _] _tx _in-flight-tx]
  (throw (err/illegal-arg :unknown-tx-op {:op op})))

(defn- tx-fn-doc? [doc]
  (some #{:crux.db.fn/args
          :crux.db.fn/tx-events
          :crux.db.fn/failed?}
        (keys doc)))

(defn- without-tx-fn-docs [docs]
  (into {} (remove (comp tx-fn-doc? val) docs)))

(defn- arg-docs-to-replace [document-store tx-events]
  (->> (db/fetch-docs document-store (for [[op :as tx-event] tx-events
                                           :when (= op :crux.tx/fn)
                                           :let [[_op _fn-id arg-doc-id] tx-event]]
                                       arg-doc-id))
       (into {}
             (map (fn [[arg-doc-id arg-doc]]
                    (MapEntry/create arg-doc-id
                                     {:crux.db/id (:crux.db/id arg-doc)
                                      :crux.db.fn/failed? true}))))))

(defn- index-docs [{:keys [index-store-tx !tx]} docs]
  (when (seq docs)
    (when-let [missing-ids (seq (remove :crux.db/id (vals docs)))]
      (throw (err/illegal-arg :missing-eid {::err/message "Missing required attribute :crux.db/id"
                                            :docs missing-ids})))
    (let [{:keys [bytes-indexed indexed-docs]} (db/index-docs index-store-tx docs)
          av-count (->> (vals indexed-docs) (apply concat) (count))]
      (swap! !tx
             (fn [tx]
               (-> tx
                   (update :av-count + av-count)
                   (update :bytes-indexed + bytes-indexed)
                   (update :doc-ids into (map c/new-id) (keys indexed-docs))))))))

(defrecord InFlightTx [tx fork-at !tx-state !tx
                       index-store-tx document-store-tx
                       db-provider bus]
  db/DocumentStore
  (submit-docs [_ docs]
    (db/submit-docs document-store-tx docs))

  (fetch-docs [_ ids]
    (db/fetch-docs document-store-tx ids))

  api/DBProvider
  (db [_] (api/db db-provider tx))
  (db [_ valid-time-or-basis] (api/db db-provider valid-time-or-basis))
  (db [_ valid-time tx-time] (api/db db-provider valid-time tx-time))
  (open-db [_] (api/open-db db-provider tx))
  (open-db [_ valid-time-or-basis] (api/open-db db-provider valid-time-or-basis))
  (open-db [_ valid-time tx-time] (api/open-db db-provider valid-time tx-time))

  db/InFlightTx
  (index-tx-events [this tx-events]
    (try
      (let [tx-state @!tx-state]
        (when (not= tx-state :open)
          (throw (IllegalStateException. (format "Transaction marked as '%s'" (name tx-state))))))

      (swap! !tx update :tx-events into tx-events)

      (let [doc-hashes (set (txc/tx-events->doc-hashes tx-events))
            docs (db/fetch-docs document-store-tx doc-hashes)
            fetched-doc-hashes (set (keys docs))]
        (when-not (= fetched-doc-hashes doc-hashes)
          (throw (IllegalStateException. (str "missing docs: " (pr-str (set/difference doc-hashes fetched-doc-hashes))))))

        (index-docs this (-> docs without-tx-fn-docs)))

      (with-open [index-snapshot (db/open-index-snapshot index-store-tx)]
        (let [deps (assoc this :index-snapshot index-snapshot)
              abort? (loop [[tx-event & more-tx-events] tx-events]
                       (when tx-event
                         (let [{:keys [docs abort? evict-eids etxs], new-tx-events :tx-events}
                               (index-tx-event tx-event tx deps)]
                           (if abort?
                             (do
                               (when-let [docs (seq (concat docs (arg-docs-to-replace document-store-tx more-tx-events)))]
                                 (db/submit-docs document-store-tx docs))
                               true)

                             (do
                               (index-docs this (-> docs without-tx-fn-docs))
                               (db/index-entity-txs index-store-tx etxs)
                               (let [{:keys [tombstones]} (when (seq evict-eids)
                                                            (swap! !tx update :evicted-eids into evict-eids)
                                                            (db/unindex-eids index-store-tx evict-eids))]
                                 (when-let [docs (seq (concat docs tombstones))]
                                   (db/submit-docs document-store-tx docs)))

                               (if (Thread/interrupted)
                                 (throw (InterruptedException.))
                                 (recur (concat new-tx-events more-tx-events))))))))]
          (when abort?
            (reset! !tx-state :abort-only))

          (not abort?)))

      (catch Throwable e
        (reset! !tx-state :abort-only)
        (throw e))))

  (commit [_]
    (when-not (compare-and-set! !tx-state :open :committed)
      (throw (IllegalStateException. (str "Transaction marked as " (name @!tx-state)))))

    (when fork-at
      (throw (IllegalStateException. "Can't commit from fork.")))

    (let [{:keys [evicted-eids doc-ids tx-events]} @!tx]
      ;; these two come before the committing tx bus message
      ;; because Lucene relies on both of them before indexing/evicting docs
      (fork/commit-doc-store-tx document-store-tx)

      (bus/send bus {:crux/event-type ::committing-tx,
                     :submitted-tx tx
                     :evicting-eids evicted-eids
                     :doc-ids doc-ids})

      (db/commit-index-tx index-store-tx)

      (bus/send bus (into {:crux/event-type ::indexed-tx,
                           :submitted-tx tx,
                           :committed? true
                           ::txe/tx-events tx-events}
                          (select-keys @!tx [:doc-ids :av-count :bytes-indexed])))))

  (abort [_]
    (swap! !tx-state (fn [tx-state]
                       (if-not (contains? #{:open :abort-only} tx-state)
                         (throw (IllegalStateException. "Transaction marked as " tx-state))
                         :aborted)))

    (when (:fork-at tx)
      (throw (IllegalStateException. "Can't abort from fork.")))

    (bus/send bus {:crux/event-type ::aborting-tx,
                   :submitted-tx tx})

    (log/debug "Transaction aborted:" (pr-str tx))

    (fork/abort-doc-store-tx document-store-tx)
    (db/abort-index-tx index-store-tx)

    (bus/send bus {:crux/event-type ::indexed-tx,
                   :submitted-tx tx,
                   :committed? false
                   ::txe/tx-events (:tx-events @!tx)})))

(defrecord TxIndexer [index-store document-store bus query-engine]
  db/TxIndexer
  (begin-tx [_ tx fork-at]
    (when-not fork-at
      (log/debug "Indexing tx-id:" (::tx-id tx))
      (bus/send bus {:crux/event-type ::indexing-tx, :submitted-tx tx}))

    (let [index-store-tx (db/begin-index-tx index-store tx fork-at)
          document-store-tx (fork/begin-document-store-tx document-store)]
      (->InFlightTx tx fork-at
                    (atom :open)
                    (atom {:doc-ids #{}
                           :evicted-eids #{}
                           :av-count 0
                           :bytes-indexed 0
                           :tx-events []})
                    index-store-tx
                    document-store-tx
                    (assoc query-engine
                           :index-store index-store-tx
                           :document-store document-store-tx)
                    bus))))

(defn ->tx-indexer {::sys/deps {:index-store :crux/index-store
                                :document-store :crux/document-store
                                :bus :crux/bus
                                :query-engine :crux/query-engine}}
  [deps]
  (map->TxIndexer deps))

(defrecord TxIngester [index-store !error ^Future job]
  db/TxIngester
  (ingester-error [_] @!error)

  db/LatestCompletedTx
  (latest-completed-tx [_] (db/latest-completed-tx index-store))

  Closeable
  (close [_]
    (.cancel job true)
    (log/info "Shut down tx-ingester")))

(defn ->tx-ingester {::sys/deps {:tx-indexer :crux/tx-indexer
                                 :index-store :crux/index-store
                                 :tx-log :crux/tx-log
                                 :bus :crux/bus}}
  [{:keys [tx-log tx-indexer bus index-store]}]
  (log/info "Started tx-ingester")
  (let [!error (atom nil)
        job (db/subscribe-async tx-log
                                (::tx-id (db/latest-completed-tx index-store))
                                (fn [_fut tx]
                                  (try
                                    (let [in-flight-tx (db/begin-tx tx-indexer
                                                                    (select-keys tx [::tx-time ::tx-id])
                                                                    nil)]
                                      (if (db/index-tx-events in-flight-tx (::txe/tx-events tx))
                                        (db/commit in-flight-tx)
                                        (db/abort in-flight-tx)))
                                    (catch Throwable t
                                      (reset! !error t)
                                      (bus/send bus {:crux/event-type ::ingester-error, :ingester-error t})
                                      (throw t)))))]
    (->TxIngester index-store !error job)))

(defn await-tx
  ([deps awaited-tx]
   (await-tx deps awaited-tx {}))

  ([{:keys [bus tx-ingester]} awaited-tx
    {:keys [^Duration timeout, indexed-tx-event ingester-error-event]
     :or {indexed-tx-event ::indexed-tx
          ingester-error-event ::ingester-error}}]
   (let [tx-k (some (set (keys awaited-tx)) [::tx-id ::tx-time])
         tx-v (get awaited-tx tx-k)
         fut (bus/await bus {:crux/event-types #{indexed-tx-event ingester-error-event :crux.node/node-closing}
                             :->result (letfn [(tx->result [tx]
                                                 (when (and tx (not (neg? (compare (get tx tx-k) tx-v))))
                                                   {:tx tx}))]
                                         (fn
                                           ([] (or (when-let [ingester-error (db/ingester-error tx-ingester)]
                                                     {:ingester-error ingester-error})
                                                   (tx->result (db/latest-completed-tx tx-ingester))))
                                           ([{:keys [crux/event-type] :as ev}]
                                            (condp = event-type
                                              indexed-tx-event (tx->result (:submitted-tx ev))
                                              ingester-error-event {:ingester-error (:ingester-error ev)}
                                              :crux.node/node-closing {:node-closing? true}))))})

         {:keys [timeout? ingester-error node-closing? tx]} (if timeout
                                                              (deref fut (.toMillis timeout) {:timeout? true})
                                                              (deref fut))]

     (when timeout?
       (future-cancel fut))

     (cond
       ingester-error (throw (Exception. "Transaction ingester aborted." ingester-error))
       timeout? (throw (TimeoutException. (str "Timed out waiting for: " (pr-str awaited-tx)
                                               ", index has: " (pr-str (db/latest-completed-tx tx-ingester)))))
       node-closing? (throw (InterruptedException. "Node closed."))
       tx tx))))

(def ^java.util.concurrent.ThreadFactory subscription-thread-factory
  (cio/thread-factory "crux-tx-subscription"))

(defn handle-polling-subscription [tx-log after-tx-id {:keys [^Duration poll-sleep-duration]} f]
  (let [fut (CompletableFuture.)]
    (letfn [(handle-txs [^ICursor txs after-tx-id]
              (try
                (reduce (fn [_last-tx-id tx]
                          (when (Thread/interrupted)
                            (throw (InterruptedException.)))

                          (f fut tx)
                          (::tx-id tx))
                        after-tx-id
                        (iterator-seq txs))
                (finally
                  (.close txs))))

            (poll-txs []
              (loop [after-tx-id after-tx-id]
                (let [last-tx-id (if-let [txs (try
                                                (db/open-tx-log tx-log after-tx-id)
                                                (catch InterruptedException e (throw e))
                                                (catch Exception e
                                                  (log/warn e "Error polling for txs, will retry")))]
                                   (handle-txs txs after-tx-id)
                                   after-tx-id)]
                  (cond
                    (.isDone fut) nil
                    (Thread/interrupted) (throw (InterruptedException.))
                    :else (do
                            (when (= after-tx-id last-tx-id)
                              (Thread/sleep (.toMillis poll-sleep-duration)))
                            (recur last-tx-id))))))]

      (let [thread (doto (.newThread subscription-thread-factory
                                     (fn []
                                       (try
                                         (poll-txs)
                                         (.complete fut nil)
                                         (catch Throwable t
                                           (.completeExceptionally fut t)))))
                     (.start))]
        (doto fut
          (.whenComplete (reify BiConsumer
                           (accept [_ v e]
                             (when-not (instance? InterruptedException e)
                               (.interrupt thread))))))))))
