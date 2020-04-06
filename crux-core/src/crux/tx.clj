(ns ^:no-doc crux.tx
  (:require [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            crux.api
            [crux.bus :as bus]
            [crux.codec :as c]
            [crux.db :as db]
            [crux.index :as idx]
            [crux.io :as cio]
            [crux.kv :as kv]
            [crux.memory :as mem]
            [crux.query :as q]
            [taoensso.nippy :as nippy]
            [clojure.set :as set]
            [crux.status :as status]
            [crux.tx.conform :as txc]
            crux.tx.event)
  (:import [crux.codec EntityTx]
           java.io.Closeable
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
(defmethod bus/event-spec ::indexed-tx [_] (s/keys :req [::submitted-tx], :req-un [::committed?]))

(defn- conform-tx-event [[op & args]]
  (-> (case op
        ::put (zipmap [:eid :content-hash :start-valid-time :end-valid-time] args)
        ::delete (zipmap [:eid :start-valid-time :end-valid-time] args)
        ::cas (zipmap [:eid :old-content-hash :new-content-hash :valid-time] args)
        ::match (zipmap [:eid :content-hash :valid-time] args)
        ::evict (let [[eid & args] args
                      [start-valid-time end-valid-time] (filter inst? args)
                      [keep-latest? keep-earliest?] (filter boolean? args)]
                  {:eid eid
                   :start-valid-time start-valid-time, :end-valid-time end-valid-time
                   :keep-latest? keep-latest?, :keep-earliest? keep-earliest?})
        ::fn (zipmap [:fn-eid :args-content-hash] args))
      (assoc :op op)))

(defn tx-event->doc-hashes [tx-event]
  (keep (conform-tx-event tx-event) [:content-hash :old-content-hash :new-content-hash :args-content-hash]))

(defn tx-event->tx-op [[op id & args] snapshot object-store]
  (into [op]
        (concat (when (contains? #{:crux.tx/delete :crux.tx/evict :crux.tx/fn} op)
                  [(c/new-id id)])

                (for [arg args]
                  (or (when (satisfies? c/IdToBuffer arg)
                        (or (db/get-single-object object-store snapshot arg)
                            {:crux.db/id (c/new-id id)
                             :crux.db/evicted? true}))
                      arg)))))

(defprotocol EntityHistory
  (with-entity-history-seq-ascending [_ eid valid-time tx-time f])
  (with-entity-history-seq-descending [_ eid valid-time tx-time f])
  (all-content-hashes [_ eid])
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

(defrecord Snapshot+NewETXs [snapshot etxs]
  EntityHistory
  (with-entity-history-seq-ascending [_ eid valid-time tx-time f]
    (with-open [i (kv/new-iterator snapshot)]
      (f (merge-histories etx->vt compare
                          (idx/entity-history-seq-ascending i eid valid-time tx-time)
                          (->> (get etxs eid)
                               (drop-while (comp neg? #(compare % valid-time) etx->vt)))))))

  (with-entity-history-seq-descending [_ eid valid-time tx-time f]
    (with-open [i (kv/new-iterator snapshot)]
      (f (merge-histories etx->vt #(compare %2 %1)
                          (idx/entity-history-seq-descending i eid valid-time tx-time)
                          (->> (reverse (get etxs eid))
                               (drop-while (comp pos? #(compare % valid-time) etx->vt)))))))

  (entity-at [_ eid valid-time tx-time]
    (->> (merge-histories etx->vt #(compare %2 %1)
                          (some->> (with-open [i (kv/new-iterator snapshot)]
                                     (idx/entity-at (idx/new-entity-as-of-index i valid-time tx-time) eid))
                                   vector)
                          (some->> (reverse (get etxs eid))
                                   (drop-while (comp pos? #(compare % valid-time) etx->vt))
                                   first
                                   vector))
         first))

  (all-content-hashes [_ eid]
    (with-open [i (kv/new-iterator snapshot)]
      (into (set (->> (idx/all-keys-in-prefix i (c/encode-indexed-content-hash-to nil (c/->id-buffer eid)))
                      (map c/decode-indexed-content-hash-from)
                      (map :content-hash)))
            (set (->> (get etxs eid)
                      (map #(.content-hash ^EntityTx %)))))))

  (with-etxs [_ new-etxs]
    (->Snapshot+NewETXs snapshot
                        (merge-with #(merge-histories etx->vt compare %1 %2)
                                    etxs
                                    (->> new-etxs (group-by #(.eid ^EntityTx %)))))))

(defn etx->kvs [^EntityTx etx]
  [[(c/encode-entity+vt+tt+tx-id-key-to
     nil
     (c/->id-buffer (.eid etx))
     (.vt etx)
     (.tt etx)
     (.tx-id etx))
    (c/->id-buffer (.content-hash etx))]
   [(c/encode-entity+z+tx-id-key-to
     nil
     (c/->id-buffer (.eid etx))
     (c/encode-entity-tx-z-number (.vt etx) (.tt etx))
     (.tx-id etx))
    (c/->id-buffer (.content-hash etx))]])

(defmulti index-tx-event
  (fn [[op :as tx-event] tx deps]
    op))

(defn- put-delete-etxs [k start-valid-time end-valid-time content-hash {:crux.tx/keys [tx-time tx-id]} {:keys [history]}]
  (let [eid (c/new-id k)
        ->new-entity-tx (fn [vt]
                          (c/->EntityTx eid vt tx-time tx-id content-hash))

        start-valid-time (or start-valid-time tx-time)]

    (if end-valid-time
      (when-not (= start-valid-time end-valid-time)
        (with-entity-history-seq-descending history eid end-valid-time tx-time
          (fn [entity-history]
            (into (->> (cons start-valid-time
                             (->> (map etx->vt entity-history)
                                  (take-while #(neg? (compare start-valid-time %)))))
                       (remove #{end-valid-time})
                       (mapv ->new-entity-tx))

                  [(if-let [entity-to-restore ^EntityTx (first entity-history)]
                     (-> entity-to-restore
                         (assoc :vt end-valid-time))

                     (c/->EntityTx eid end-valid-time tx-time tx-id (c/nil-id-buffer)))]))))

      (->> (cons start-valid-time
                 (when-let [visible-entity (some-> (entity-at history eid start-valid-time tx-time)
                                                   (select-keys [:tx-time :tx-id :content-hash]))]
                   (with-entity-history-seq-ascending history eid start-valid-time tx-time
                     (fn [entity-history]
                       (->> entity-history
                            (remove #{start-valid-time})
                            (take-while #(= visible-entity (select-keys % [:tx-time :tx-id :content-hash])))
                            (mapv etx->vt))))))

           (map ->new-entity-tx)))))

(defmethod index-tx-event :crux.tx/put [[op k v start-valid-time end-valid-time] tx {:keys [kv-store object-store] :as deps}]
  ;; This check shouldn't be required, under normal operation - the ingester checks for this before indexing
  ;; keeping this around _just in case_ - e.g. if we're refactoring the ingest code
  {:pre-commit-fn (fn []
                    (let [content-hash (c/new-id v)]
                      (assert (with-open [snapshot (kv/new-snapshot kv-store)]
                                (or (idx/doc-indexed? snapshot k content-hash)
                                    (idx/evicted-doc? (db/get-single-object object-store snapshot content-hash))))
                              (format "Put, incorrect doc state for: '%s', tx-id '%s'"
                                      content-hash (:crux.tx/tx-id tx)))
                      true))

   :etxs (put-delete-etxs k start-valid-time end-valid-time (c/new-id v) tx deps)})

(defmethod index-tx-event :crux.tx/delete [[op k start-valid-time end-valid-time] tx deps]
  {:etxs (put-delete-etxs k start-valid-time end-valid-time nil tx deps)})

(defmethod index-tx-event :crux.tx/match [[op k v at-valid-time :as match-op]
                                          {:crux.tx/keys [tx-time tx-id] :as tx}
                                          {:keys [history] :as deps}]
  {:pre-commit-fn #(let [{:keys [content-hash] :as entity} (entity-at history
                                                                      (c/new-id k)
                                                                      (or at-valid-time tx-time)
                                                                      tx-time)]
                     (or (= (c/new-id content-hash) (c/new-id v))
                         (log/debug "crux.tx/match failure:" (cio/pr-edn-str match-op) "was:" (c/new-id content-hash))))})

(defmethod index-tx-event :crux.tx/cas [[op k old-v new-v at-valid-time :as cas-op]
                                        {:crux.tx/keys [tx-time tx-id] :as tx}
                                        {:keys [object-store history snapshot] :as deps}]
  (let [eid (c/new-id k)
        valid-time (or at-valid-time tx-time)]

    {:pre-commit-fn #(let [{:keys [content-hash] :as entity} (entity-at history eid valid-time tx-time)]
                       ;; see juxt/crux#362 - we'd like to just compare content hashes here, but
                       ;; can't rely on the old content-hashing returning the same hash for the same document
                       (if (or (= (c/new-id content-hash) (c/new-id old-v))
                               (= (db/get-single-object object-store snapshot (c/new-id content-hash))
                                  (db/get-single-object object-store snapshot (c/new-id old-v))))
                         (let [correct-state? (not (nil? (db/get-single-object object-store snapshot (c/new-id new-v))))]
                           (when-not correct-state?
                             (log/error "CAS, incorrect doc state for:" (c/new-id new-v) "tx id:" tx-id))
                           correct-state?)
                         (do (log/warn "CAS failure:" (cio/pr-edn-str cas-op) "was:" (c/new-id content-hash))
                             false)))

     :etxs (put-delete-etxs eid valid-time nil (c/new-id new-v) tx deps)}))

(def evict-time-ranges-env-var "CRUX_EVICT_TIME_RANGES")
(def ^:dynamic *evict-all-on-legacy-time-ranges?* (= (System/getenv evict-time-ranges-env-var) "EVICT_ALL"))

(defmethod index-tx-event :crux.tx/evict [[op k & legacy-args] tx {:keys [history] :as deps}]
  (let [eid (c/new-id k)
        content-hashes (all-content-hashes history eid)]
    {:pre-commit-fn #(cond
                       (empty? legacy-args) true

                       (not *evict-all-on-legacy-time-ranges?*)
                       (throw (IllegalArgumentException. (str "Evict no longer supports time-range parameters. "
                                                              "See https://github.com/juxt/crux/pull/438 for more details, and what to do about this message.")))

                       :else (do
                               (log/warnf "Evicting '%s' for all valid-times, '%s' set"
                                          k evict-time-ranges-env-var)
                               true))

     :tombstones (into {} (for [content-hash content-hashes]
                            [content-hash {:crux.db/id eid, :crux.db/evicted? true}]))}))

(def ^:private tx-fn-eval-cache (memoize eval))

(defn log-tx-fn-error [fn-result fn-id body args-id args]
  (log/warn fn-result "Transaction function failure:" fn-id (cio/pr-edn-str body) args-id (cio/pr-edn-str args)))

(def tx-fns-enabled? (Boolean/parseBoolean (System/getenv "CRUX_ENABLE_TX_FNS")))

(defmethod index-tx-event :crux.tx/fn [[op k args-v :as tx-op]
                                       {:crux.tx/keys [tx-time tx-id] :as tx}
                                       {:keys [object-store kv-store indexer tx-log snapshot nested-fn-args], :as deps}]
  (when-not tx-fns-enabled?
    (throw (IllegalArgumentException. (str "Transaction functions not enabled: " (cio/pr-edn-str tx-op)))))

  (let [fn-id (c/new-id k)
        db (q/db kv-store object-store nil tx-time tx-time)
        {:crux.db.fn/keys [body] :as fn-doc} (q/entity db snapshot fn-id)
        {:crux.db.fn/keys [args] :as args-doc} (let [arg-id (c/new-id args-v)]
                                                 (or (get nested-fn-args arg-id)
                                                     (db/get-single-object object-store snapshot arg-id)))
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
            op-results (vec (for [[op :as tx-event] (map txc/->tx-event conformed-tx-ops)]
                              (index-tx-event tx-event tx
                                              (-> deps
                                                  (update :nested-fn-args (fnil into {}) arg-docs)))))]
        {:pre-commit-fn #(every? true? (for [{:keys [pre-commit-fn]} op-results
                                             :when pre-commit-fn]
                                         (pre-commit-fn)))
         :etxs (cond-> (mapcat :etxs op-results)
                 args-doc (conj (c/->EntityTx args-id tx-time tx-time tx-id args-v)))

         :tombstones (into {} (mapcat :tombstones) op-results)}))))

(defmethod index-tx-event :default [[op & _] tx deps]
  (throw (IllegalArgumentException. (str "Unknown tx-op: " op))))

(def ^:dynamic *current-tx*)

(defn- update-stats [docs-stats {:keys [kv-store ^ExecutorService stats-executor]}]
  (let [stats-fn ^Runnable #(idx/update-predicate-stats kv-store docs-stats)]
    (if stats-executor
      (.submit stats-executor stats-fn)
      (stats-fn))))

(defn- unindex-docs [tombstones {:keys [kv-store object-store snapshot] :as deps}]
  (let [existing-docs (db/get-objects object-store snapshot (keys tombstones))]
    (db/put-objects object-store tombstones)

    (kv/delete kv-store (->> existing-docs
                             (mapcat (fn [[k doc]] (idx/doc-idx-kvs k doc)))
                             (map first)))

    (update-stats (->> (vals existing-docs) (map #(idx/doc-predicate-stats % true)))
                  deps)))

(defrecord KvIndexer [object-store kv-store bus ^ExecutorService stats-executor]
  Closeable
  (close [_]
    (when stats-executor
      (doto stats-executor
        (.shutdown)
        (.awaitTermination 60000 TimeUnit/MILLISECONDS))))

  db/Indexer
  (index-docs [this docs]
    (when-let [missing-ids (seq (remove :crux.db/id (vals docs)))]
      (throw (IllegalArgumentException.
              (str "Missing required attribute :crux.db/id: " (cio/pr-edn-str missing-ids)))))

    (with-open [snapshot (kv/new-snapshot kv-store)]
      (db/put-objects object-store (->> docs (into {} (filter (comp idx/evicted-doc? val)))))

      (when-let [docs-to-upsert (->> docs
                                     (into {} (remove (fn [[k doc]]
                                                        (or (idx/doc-indexed? snapshot (:crux.db/id doc) k)
                                                            (idx/evicted-doc? doc)))))
                                     not-empty)]
        (bus/send bus {::bus/event-type ::indexing-docs, :doc-ids (set (keys docs))})

        (let [doc-idx-kvs (when (seq docs-to-upsert)
                            (->> docs-to-upsert
                                 (mapcat (fn [[k doc]] (idx/doc-idx-kvs k doc)))))

              _ (some->> (seq doc-idx-kvs) (kv/store kv-store))

              docs-stats (->> (vals docs-to-upsert)
                              (map #(idx/doc-predicate-stats % false)))]

          (db/put-objects object-store docs-to-upsert)

          (bus/send bus {::bus/event-type ::indexed-docs,
                         :doc-ids (set (keys docs))
                         :av-count (->> (vals docs) (apply concat) (count))
                         :bytes-indexed (->> doc-idx-kvs
                                             (apply concat)
                                             (transduce (map mem/capacity) +))})

          (update-stats docs-stats this)))))

  (index-tx [this {:crux.tx/keys [tx-time tx-id] :as tx} tx-events]
    (s/assert :crux.tx.event/tx-events tx-events)

    (log/debug "Indexing tx-id:" tx-id "tx-events:" (count tx-events))
    (bus/send bus {::bus/event-type ::indexing-tx, ::submitted-tx tx})

    (with-open [snapshot (kv/new-snapshot kv-store)]
      (binding [*current-tx* (assoc tx :crux.tx.event/tx-events tx-events)]
        (let [deps {:object-store object-store
                    :kv-store kv-store
                    :indexer this
                    :snapshot snapshot}

              res (reduce (fn [{:keys [history] :as acc} tx-event]
                            (let [{:keys [pre-commit-fn etxs tombstones]} (index-tx-event tx-event tx (assoc deps :history history))]
                              (if (and pre-commit-fn (not (pre-commit-fn)))
                                (reduced ::aborted)
                                (-> acc
                                    (update :history with-etxs etxs)
                                    (update :tombstones merge tombstones)))))

                          {:history (->Snapshot+NewETXs snapshot {})
                           :tombstones {}}

                          tx-events)

              committed? (not= res ::aborted)]

          (if committed?
            (let [tombstones (not-empty (:tombstones res))]
              (when tombstones
                (unindex-docs tombstones deps))

              (kv/store kv-store (->> (conj (->> (get-in res [:history :etxs]) (mapcat val) (mapcat etx->kvs))
                                            (idx/meta-kv ::latest-completed-tx tx))
                                      (into (sorted-map-by mem/buffer-comparator)))))

            (do
              (log/warn "Transaction aborted:" (cio/pr-edn-str tx-events) (cio/pr-edn-str tx-time) tx-id)
              (kv/store kv-store [(idx/meta-kv ::latest-completed-tx tx)
                                  [(c/encode-failed-tx-id-key-to nil tx-id) c/empty-buffer]])))

          (bus/send bus {::bus/event-type ::indexed-tx, ::submitted-tx tx, :committed? committed?})

          {:tombstones (when committed?
                         (:tombstones res))}))))

  (store-index-meta [_ k v]
    (idx/store-meta kv-store k v))

  (read-index-meta [_  k]
    (idx/read-meta kv-store k))

  (latest-completed-tx [this]
    (db/read-index-meta this ::latest-completed-tx))

  status/Status
  (status-map [this]
    {:crux.index/index-version (idx/current-index-version kv-store)
     :crux.doc-log/consumer-state (db/read-index-meta this :crux.doc-log/consumer-state)
     :crux.tx-log/consumer-state (db/read-index-meta this :crux.tx-log/consumer-state)}))

(def kv-indexer
  {:start-fn (fn [{:crux.node/keys [object-store kv-store bus]} args]
               (->KvIndexer object-store kv-store bus
                            (Executors/newSingleThreadExecutor (cio/thread-factory "crux.tx.update-stats-thread"))))
   :deps [:crux.node/kv-store :crux.node/object-store :crux.node/bus]})

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
