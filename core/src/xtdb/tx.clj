(ns ^:no-doc xtdb.tx
  (:require [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [xtdb.api :as xt]
            [xtdb.bus :as bus]
            [xtdb.codec :as c]
            [xtdb.db :as db]
            [xtdb.error :as err]
            [xtdb.fork :as fork]
            [xtdb.io :as xio]
            [xtdb.system :as sys]
            [xtdb.tx.conform :as txc]
            [xtdb.tx.event :as txe])
  (:import clojure.lang.MapEntry
           java.io.Closeable
           [java.util.concurrent CompletableFuture]
           [java.util.concurrent ExecutorService ThreadFactory]
           java.util.function.BiFunction
           xtdb.codec.EntityTx))

(set! *unchecked-math* :warn-on-boxed)

(s/def ::submitted-tx (s/keys :req [::xt/tx-id ::xt/tx-time]))
(s/def ::committed? boolean?)
(s/def ::av-count nat-int?)
(s/def ::bytes-indexed nat-int?)
(s/def ::doc-ids (s/coll-of #(instance? xtdb.codec.Id %) :kind set?))
(s/def ::eids (s/coll-of c/valid-id? :kind set?))
(s/def ::evicting-eids ::eids)

(defmethod bus/event-spec ::indexing-tx [_]
  (s/keys :req-un [::submitted-tx]))

(defmethod bus/event-spec ::indexed-tx [_]
  (s/keys :req [::txe/tx-events],
          :req-un [::submitted-tx ::committed?]
          :opt-un [::doc-ids ::av-count ::bytes-indexed]))

(s/def ::ingester-error #(instance? Exception %))
(defmethod bus/event-spec ::ingester-error [_] (s/keys :req-un [::ingester-error]))

(defn- tx-fn-doc? [doc]
  (some #{:crux.db.fn/args
          :crux.db.fn/tx-events
          :crux.db.fn/failed?}
        (keys doc)))

(defn- without-tx-fn-docs [docs]
  (into {} (remove (comp tx-fn-doc? val)) docs))

(defn- strict-fetch-docs [document-store doc-hashes]
  (let [doc-hashes (set doc-hashes)
        docs (db/fetch-docs document-store doc-hashes)
        fetched-doc-hashes (set (keys docs))]
    (when-not (= fetched-doc-hashes doc-hashes)
      (throw (IllegalStateException. (str "missing docs: " (pr-str (set/difference doc-hashes fetched-doc-hashes))))))

    docs))

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

(defn- update-tx-stats [tx {:keys [bytes-indexed doc-ids av-count]}]
  (-> tx
      (update :av-count + av-count)
      (update :bytes-indexed + bytes-indexed)
      (update :doc-ids into doc-ids)))

(defn- etx->vt [^EntityTx etx]
  (.vt etx))

(defmulti index-tx-event
  (fn [[op :as _tx-event] _tx _in-flight-tx]
    op))

(alter-meta! #'index-tx-event assoc :arglists '([tx-event tx in-flight-tx]))

(defn- put-delete-etxs [k start-valid-time end-valid-time content-hash
                        {::xt/keys [tx-time tx-id valid-time]}
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
                                          {::xt/keys [tx-time tx-id valid-time]}
                                          {:keys [index-snapshot]}]
  (let [content-hash (db/entity-as-of-resolver index-snapshot
                                               (c/new-id k)
                                               (or at-valid-time valid-time tx-time)
                                               tx-id)
        match? (= (c/new-id content-hash) (c/new-id v))]
    (when-not match?
      (log/debug "match failure:" (xio/pr-edn-str match-op) "was:" (c/new-id content-hash)))

    {:abort? (not match?)}))

(defmethod index-tx-event :crux.tx/cas [[_op k old-v new-v at-valid-time :as cas-op]
                                        {::xt/keys [tx-time tx-id valid-time] :as tx}
                                        {:keys [document-store-tx index-snapshot] :as in-flight-tx}]
  (let [eid (c/new-id k)
        valid-time (or at-valid-time valid-time tx-time)
        content-hash (db/entity-as-of-resolver index-snapshot eid valid-time tx-id)
        current-id (c/new-id content-hash)
        expected-id (c/new-id old-v)]
    (if (or (= current-id expected-id)
            ;; see xtdb/xtdb#362 - we'd like to just compare content hashes here, but
            ;; can't rely on the old content-hashing returning the same hash for the same document
            (let [docs (strict-fetch-docs document-store-tx #{current-id expected-id})]
              (= (get docs current-id)
                 (get docs expected-id))))
      {:etxs (put-delete-etxs eid valid-time nil (c/new-id new-v) tx in-flight-tx)}
      (do
        (log/warn "CAS failure:" (xio/pr-edn-str cas-op) "was:" (c/new-id content-hash))
        {:abort? true}))))

(def evict-time-ranges-env-var "XTDB_EVICT_TIME_RANGES")
(def ^:dynamic *evict-all-on-legacy-time-ranges?* (= (System/getenv evict-time-ranges-env-var) "EVICT_ALL"))

(defmethod index-tx-event :crux.tx/evict [[_op k & legacy-args] _ _]
  (cond
    (empty? legacy-args) {:evict-eids #{k}}

    (not *evict-all-on-legacy-time-ranges?*)
    (throw (err/illegal-arg :evict-with-time-range
                            {::err/message (str "Evict no longer supports time-range parameters. "
                                                "See https://github.com/xtdb/xtdb/pull/438 for more details, and what to do about this message.")}))

    :else (do
            (log/warnf "Evicting '%s' for all valid-times, '%s' set"
                       k evict-time-ranges-env-var)
            {:evict-eids #{k}})))

(def ^:private tx-fn-eval-cache (memoize eval))

;; for tests
(def ^:private !last-tx-fn-error (atom nil))

(defn- reset-tx-fn-error []
  (first (reset-vals! !last-tx-fn-error nil)))

(defrecord TxFnContext [db-provider indexing-tx]
  xt/DBProvider
  (db [_] (xt/db db-provider))
  (db [_ valid-time tx-time] (xt/db db-provider valid-time tx-time))
  (db [_ valid-time-or-basis] (xt/db db-provider valid-time-or-basis))

  (open-db [_] (xt/open-db db-provider))
  (open-db [_ valid-time tx-time] (xt/open-db db-provider valid-time tx-time))
  (open-db [_ valid-time-or-basis] (xt/open-db db-provider valid-time-or-basis))

  xt/TransactionFnContext
  (indexing-tx [_] (select-keys indexing-tx [::xt/tx-time ::xt/tx-id])))

(defmethod index-tx-event :crux.tx/fn [[_op k args-content-hash] tx {:keys [document-store-tx] :as in-flight-tx}]
  (let [fn-id (c/new-id k)
        {args-doc-id :xt/id,
         :crux.db.fn/keys [args tx-events failed?]
         :as args-doc} (when args-content-hash
                         (-> (strict-fetch-docs document-store-tx #{args-content-hash})
                             (get args-content-hash)
                             c/crux->xt))]
    (cond
      tx-events {:tx-events tx-events
                 :docs (strict-fetch-docs document-store-tx (txc/tx-events->doc-hashes tx-events))}

      failed? (do
                (log/warn "Transaction function failed when originally evaluated:"
                          fn-id args-doc-id
                          (pr-str (select-keys args-doc [:crux.db.fn/exception
                                                         :crux.db.fn/message
                                                         :crux.db.fn/ex-data])))
                {:abort? true})

      :else (let [ctx (->TxFnContext in-flight-tx tx)
                  db (xt/db ctx)
                  {fn-body :xt/fn, :as tx-fn} (xt/entity db fn-id)]
              (cond
                (nil? tx-fn) {:abort? true
                              :docs (when args-doc-id
                                      {args-content-hash {:xt/id args-doc-id
                                                          :crux.db.fn/failed? true
                                                          :crux.db.fn/message (format "Missing tx-fn: `%s`" fn-id)}})}

                (nil? fn-body) (if (or (:crux.db/fn tx-fn) (:crux.db.fn/body tx-fn))
                                 (let [msg (format "Legacy Crux tx-fn found: `%s` - see XTDB (v1.19.0) migration guide." (:xt/id tx-fn))]
                                   (log/error msg)
                                   (throw (IllegalStateException. msg)))
                                 {:abort? true
                                  :docs (when args-doc-id
                                          {args-content-hash {:xt/id args-doc-id
                                                              :crux.db.fn/failed? true
                                                              :crux.db.fn/message (format "tx-fn missing `:xtdb/fn` key: `%s`" (:xt/id tx-fn))}})})

                :else
                (try
                  (let [res (apply (tx-fn-eval-cache fn-body) ctx args)]
                    (if (false? res)
                      {:abort? true
                       :docs (when args-doc-id
                               {args-content-hash {:xt/id args-doc-id
                                                   :crux.db.fn/failed? true}})}

                      (let [conformed-tx-ops (mapv txc/conform-tx-op res)
                            tx-events (mapv txc/->tx-event conformed-tx-ops)]
                        {:tx-events tx-events
                         :docs (into (if args-doc-id
                                       {args-content-hash {:xt/id args-doc-id
                                                           :crux.db.fn/tx-events tx-events}}
                                       {})
                                     (mapcat :docs)
                                     conformed-tx-ops)})))

                  (catch Throwable t
                    (reset! !last-tx-fn-error t)
                    (log/warn t "Transaction function failure:" fn-id args-doc-id)

                    {:abort? true
                     :docs (when args-doc-id
                             {args-content-hash {:xt/id args-doc-id
                                                 :crux.db.fn/failed? true
                                                 :crux.db.fn/exception (symbol (.getName (class t)))
                                                 :crux.db.fn/message (ex-message t)
                                                 :crux.db.fn/ex-data (ex-data t)}})})))))))

(defmethod index-tx-event :default [[op & _] _tx _in-flight-tx]
  (throw (err/illegal-arg :unknown-tx-op {:op op})))

(defrecord InFlightTx [!tx-state !tx !tx-stats !docs
                       index-store-tx document-store-tx
                       db-provider bus]
  db/DocumentStore
  (submit-docs [_ docs]
    (db/submit-docs document-store-tx docs))

  (fetch-docs [_ ids]
    (db/fetch-docs document-store-tx ids))

  xt/DBProvider
  (db [_] (xt/db db-provider @!tx))
  (db [_ valid-time-or-basis] (xt/db db-provider valid-time-or-basis))
  (db [_ valid-time tx-time] (xt/db db-provider valid-time tx-time))
  (open-db [_] (xt/open-db db-provider @!tx))
  (open-db [_ valid-time-or-basis] (xt/open-db db-provider valid-time-or-basis))
  (open-db [_ valid-time tx-time] (xt/open-db db-provider valid-time tx-time))

  db/InFlightTx
  (index-tx-docs [_ docs]
    (let [stats (db/index-docs index-store-tx docs)]
      (swap! !tx-stats update-tx-stats stats)
      (swap! !docs merge docs)))

  (index-tx-events [this tx]
    (log/debug "Indexing tx-id:" (::xt/tx-id tx))
    (bus/send bus {::xt/event-type ::indexing-tx,
                   :submitted-tx (select-keys tx [::xt/tx-id ::xt/tx-time ::xt/tx-ops])})

    (db/index-tx index-store-tx tx)

    (try
      (let [tx-state @!tx-state]
        (when (not= tx-state :open)
          (throw (IllegalStateException. (format "Transaction marked as '%s'" (name tx-state))))))

      (reset! !tx tx)

      (with-open [index-snapshot (db/open-index-snapshot index-store-tx)]
        (let [deps (assoc this :index-snapshot index-snapshot)
              {::txe/keys [tx-events]} tx
              [abort? indexed-docs]
              (loop [[tx-event & more-tx-events] tx-events
                     indexed-docs []]
                (if-not tx-event
                  [false indexed-docs]
                  (let [{:keys [docs abort? evict-eids etxs], new-tx-events :tx-events}
                        (index-tx-event tx-event tx deps)
                        docs (->> docs (xio/map-vals c/xt->crux))]
                    (if abort?
                      (let [new-docs (seq (concat docs (arg-docs-to-replace document-store-tx more-tx-events)))]
                        (when new-docs (db/submit-docs document-store-tx new-docs))
                        [true indexed-docs])

                      (do
                        (let [docs-to-index (-> docs without-tx-fn-docs)]

                          (when (seq docs-to-index)
                            (when-let [missing-ids (seq (remove :crux.db/id (vals docs-to-index)))]
                              (throw (err/illegal-arg :missing-eid {::err/message "Missing required attribute :crux.db/id"
                                                                    :docs missing-ids})))

                            (db/index-tx-docs this docs-to-index))

                          (db/index-entity-txs index-store-tx etxs)
                          (let [{:keys [tombstones]} (when (seq evict-eids)
                                                       (db/unindex-eids index-store-tx nil evict-eids))]
                            (when-let [docs (seq (concat docs tombstones))]
                              (db/submit-docs document-store-tx docs)))

                            (if (Thread/interrupted)
                              (throw (InterruptedException.))
                              (recur (concat new-tx-events more-tx-events)
                                     (into indexed-docs docs-to-index)))))))))]
          (when abort?
            (reset! !tx-state :abort-only))

          {:committing? (not abort?)
           :indexed-docs indexed-docs}))

      (catch Throwable e
        (reset! !tx-state :abort-only)
        (throw e))))

  (commit [_ tx]
    (when-not (compare-and-set! !tx-state :open :committed)
      (throw (IllegalStateException. (str "Transaction marked as " (name @!tx-state)))))

    (fork/commit-doc-store-tx document-store-tx)
    (db/commit-index-tx index-store-tx)

    (log/debug "Transaction committed:" (pr-str tx))

    (bus/send bus (into {::xt/event-type ::indexed-tx,
                         :submitted-tx (select-keys tx [::xt/tx-id ::xt/tx-time ::xt/tx-ops]),
                         :committed? true
                         ::txe/tx-events (::txe/tx-events tx)}
                        (select-keys @!tx-stats [:doc-ids :av-count :bytes-indexed]))))

  (abort [_ tx]
    (swap! !tx-state (fn [tx-state]
                       (if-not (contains? #{:open :abort-only} tx-state)
                         (throw (IllegalStateException. "Transaction marked as " tx-state))
                         :aborted)))

    (fork/abort-doc-store-tx document-store-tx)
    (db/abort-index-tx index-store-tx tx @!docs)

    (log/debug "Transaction aborted:" (pr-str tx))

    (bus/send bus {::xt/event-type ::indexed-tx,
                   :submitted-tx (select-keys tx [::xt/tx-id ::xt/tx-time])
                   :committed? false
                   ::txe/tx-events (::txe/tx-events tx)})))

(defrecord TxIndexer [index-store document-store bus query-engine]
  db/TxIndexer
  (begin-tx [_]
    (let [index-store-tx (db/begin-index-tx index-store)
          document-store-tx (fork/begin-document-store-tx document-store)]
      (->InFlightTx (atom :open)
                    (atom nil)
                    (atom {:av-count 0, :bytes-indexed 0, :doc-ids #{}})
                    (atom {})
                    index-store-tx
                    document-store-tx
                    (assoc query-engine
                           :index-store index-store-tx
                           :document-store document-store-tx)
                    bus))))

(defn ->tx-indexer {::sys/deps {:index-store :xtdb/index-store
                                :document-store :xtdb/document-store
                                :bus :xtdb/bus
                                :query-engine :xtdb/query-engine}}
  [deps]
  (map->TxIndexer deps))

(defprotocol ISecondaryIndices
  (register-index!
    [_ after-tx-id process-tx-f]
    [_ after-tx-id opts process-tx-f]
    "NOTE: alpha API, may break between releases."))

(defrecord SecondaryIndices [!secondary-indices]
  ISecondaryIndices
  (register-index! [this after-tx-id process-tx-f]
    (register-index! this after-tx-id {} process-tx-f))
  (register-index! [_ after-tx-id opts process-tx-f]
    (swap! !secondary-indices conj (merge opts {:after-tx-id after-tx-id
                                                :process-tx-f process-tx-f}))))

(defn ->secondary-indices [_]
  (->SecondaryIndices (atom #{})))

(defrecord TxIngester [index-store !error ^Closeable job]
  db/TxIngester
  (ingester-error [_] @!error)

  db/LatestCompletedTx
  (latest-completed-tx [_] (db/latest-completed-tx index-store))

  Closeable
  (close [_]
    (.close job)
    (log/info "Shut down tx-ingester")))

(def ^ThreadFactory docs-fetcher-thread-factory
  (xio/thread-factory "xtdb-tx-docs-fetch"))

(def ^ThreadFactory docs-encoder-thread-factory
  (xio/thread-factory "xtdb-tx-docs-encoder"))

(def ^ThreadFactory txs-processor-thread-factory
  (xio/thread-factory "xtdb-tx-processor"))

(def ^ThreadFactory stats-processor-thread-factory
  (xio/thread-factory "xtdb-stats-processor"))

(defn- validate-tx-time-override! [latest-tx tx tx-time-override]
  (let [tx-log-time (::xt/tx-time tx)
        {latest-tx-time ::xt/tx-time, :as lctx} latest-tx]
    (cond
      (and latest-tx-time (pos? (compare latest-tx-time tx-time-override)))
      (do
        (log/warn "overridden tx-time before latest completed tx-time, aborting tx"
                  (pr-str {:latest-completed-tx lctx
                           :new-tx (dissoc tx ::txe/tx-events)}))
        false)

      (neg? (compare tx-log-time tx-time-override))
      (do
        (log/warn "overridden tx-time after tx-log clock time, aborting tx"
                  (pr-str {:tx-log-time tx-log-time
                           :new-tx (dissoc tx ::txe/tx-events)}))
        false)

      :else
      true)))

(defn ->tx-ingester {::sys/args {:batch-preferred-doc-count
                                 {:default 1024
                                  :spec ::sys/pos-int}}
                     ::sys/deps {:tx-indexer :xtdb/tx-indexer
                                 :index-store :xtdb/index-store
                                 :document-store :xtdb/document-store
                                 :tx-log :xtdb/tx-log
                                 :bus :xtdb/bus
                                 :secondary-indices :xtdb/secondary-indices}}
  [{:keys [tx-log tx-indexer document-store bus index-store secondary-indices batch-preferred-doc-count]}]
  (log/info "Started tx-ingester")

  (let [!error (atom nil)
        secondary-indices @(:!secondary-indices secondary-indices)
        with-tx-ops? (some :with-tx-ops? secondary-indices)
        latest-xtdb-tx-id (::xt/tx-id (db/latest-completed-tx index-store))]
    (letfn [(process-tx-f [document-store {:keys [::xt/tx-id ::txe/tx-events] :as tx}]
              (let [tx (cond-> tx
                         with-tx-ops? (assoc ::xt/tx-ops (txc/tx-events->tx-ops document-store tx-events)))]
                (doseq [{:keys [after-tx-id process-tx-f]} secondary-indices
                        :when (or (nil? after-tx-id) (< ^long after-tx-id ^long tx-id))]
                  #_{:clj-kondo/ignore [:invalid-arity]}
                  (process-tx-f tx))))

            (set-ingester-error! [t]
              (when (compare-and-set! !error nil t)
                (when-not (instance? InterruptedException t)
                  (log/fatal t "Ingester error occurred"))
                (bus/send bus {::xt/event-type ::ingester-error, :ingester-error t})))

            ;; See https://github.com/xtdb/xtdb/pull/1808
            (batch-transactions [txs]
              (lazy-seq
                (loop [batch []
                       doc-hashes #{}
                       txs txs]
                  (if-not (seq txs)
                    (when (seq batch) [{:batch batch, :doc-hashes doc-hashes}])
                    (let [tx (first txs)
                          new-batch (conj batch tx)
                          new-doc-hashes (txc/tx-events->doc-hashes (::txe/tx-events tx))
                          unioned-doc-hashes (into doc-hashes new-doc-hashes)]
                      (if (<= (long batch-preferred-doc-count) (count unioned-doc-hashes))
                        (if (seq batch)
                          (cons {:batch batch, :doc-hashes doc-hashes} (batch-transactions txs))
                          (cons {:batch new-batch, :doc-hashes unioned-doc-hashes} (batch-transactions (rest txs))))
                        (recur new-batch unioned-doc-hashes (rest txs))))))))]

      ;; catching all the secondary indices up to where XTDB is
      (when (and latest-xtdb-tx-id (seq secondary-indices))
        (let [after-tx-id (let [secondary-tx-ids (into #{} (map :after-tx-id) secondary-indices)]
                            (when (every? some? secondary-tx-ids)
                              (apply min secondary-tx-ids)))]
          (try
            (when (or (nil? after-tx-id)
                      (< ^long after-tx-id ^long latest-xtdb-tx-id))
              (with-open [log (db/open-tx-log tx-log after-tx-id)]
                (doseq [{::keys [tx-id] :as tx} (->> (iterator-seq log)
                                                     (take-while (comp #(<= ^long % ^long latest-xtdb-tx-id) ::xt/tx-id)))]
                  (process-tx-f document-store (assoc tx :committing? (not (db/tx-failed? index-store tx-id)))))))
            (catch Throwable t
              (set-ingester-error! t)
              (throw t)))))

      (let [latest-tx! (atom (db/latest-completed-tx index-store))
            txs-docs-fetch-executor (xio/bounded-thread-pool 1 1 docs-fetcher-thread-factory)
            txs-docs-encode-executor (xio/bounded-thread-pool 1 1 docs-encoder-thread-factory)
            txs-index-executor (xio/bounded-thread-pool 1 5 txs-processor-thread-factory)
            stats-executor (xio/bounded-thread-pool 1 1 stats-processor-thread-factory)

            job (db/subscribe tx-log
                              latest-xtdb-tx-id
                              (fn [^CompletableFuture fut txs]
                                (letfn [(submit-job! [^ExecutorService executor ^Callable f & args]
                                          (.submit executor ^Callable (fn []
                                                                        (try
                                                                          (when-not (.isDone fut)
                                                                            (apply f args))

                                                                          (catch Throwable t
                                                                            (set-ingester-error! t)
                                                                            (.completeExceptionally fut t))))))

                                        (stats-fn [docs]
                                          (db/index-stats index-store docs))

                                        (txs-index-fn [{:keys [tx docs in-flight-tx]}]
                                          (let [tx-time-override (get-in tx [::xt/submit-tx-opts ::xt/tx-time])
                                                [tx abort?] (if tx-time-override
                                                              (if (validate-tx-time-override! @latest-tx! tx tx-time-override)
                                                                [(assoc tx ::xt/tx-time tx-time-override) false]
                                                                [tx true])
                                                              [tx false])
                                                _ (reset! latest-tx! tx)
                                                {:keys [committing?, indexed-docs]} (when-not abort? (db/index-tx-events in-flight-tx tx))]

                                            (process-tx-f in-flight-tx (assoc tx :committing? committing?))

                                            (if committing?
                                              (do
                                                (db/commit in-flight-tx tx)
                                                (submit-job! stats-executor stats-fn (into docs indexed-docs)))
                                              (db/abort in-flight-tx tx))))

                                        (txs-doc-encoder-fn [txs]
                                          (doseq [{:keys [docs tx] :as m} txs]
                                            (let [in-flight-tx (db/begin-tx tx-indexer)]
                                              (db/index-tx-docs in-flight-tx docs)
                                              (submit-job! txs-index-executor txs-index-fn (assoc m :tx tx :in-flight-tx in-flight-tx)))))

                                        (txs-doc-fetch-fn [txs doc-hashes]
                                          (let [docs (strict-fetch-docs document-store doc-hashes)
                                                txs (doall
                                                     (for [tx txs]
                                                       {:tx tx
                                                        :docs (-> (select-keys docs (txc/tx-events->doc-hashes (::txe/tx-events tx)))
                                                                  without-tx-fn-docs)}))]
                                            (submit-job! txs-docs-encode-executor txs-doc-encoder-fn txs)))]
                                  (try
                                    (when (Thread/interrupted)
                                      (throw (InterruptedException.)))

                                    (doseq [{:keys [batch doc-hashes]} (batch-transactions txs)]
                                      (submit-job! txs-docs-fetch-executor txs-doc-fetch-fn batch doc-hashes))

                                    (catch Throwable t
                                      (set-ingester-error! t)
                                      (throw t))))))]

        (->TxIngester index-store !error
                      (reify Closeable
                        (close [_]
                          (.cancel job false)
                          @(.handle job (reify BiFunction
                                          (apply [_ _v _e]
                                            (doseq [^ExecutorService executor [txs-docs-fetch-executor txs-docs-encode-executor txs-index-executor]]
                                              (.shutdownNow executor))
                                            (doseq [^ExecutorService executor [txs-docs-fetch-executor txs-docs-encode-executor txs-index-executor]]
                                              (.awaitTermination executor 60, java.util.concurrent.TimeUnit/SECONDS))

                                            (.shutdown ^ExecutorService stats-executor)
                                            (.awaitTermination ^ExecutorService stats-executor 60, java.util.concurrent.TimeUnit/SECONDS)))))))))))
