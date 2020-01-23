(ns crux.tx
  (:require [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [crux.codec :as c]
            [crux.backup :as backup]
            [crux.bus :as bus]
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
  (:import [crux.codec EntityTx EntityValueContentHash]
           crux.tx.consumer.Message
           java.io.Closeable
           [java.util.concurrent ExecutorService Executors TimeoutException TimeUnit]
           java.util.Date))

(set! *unchecked-math* :warn-on-boxed)

(def ^:private date? (partial instance? Date))

(s/def ::tx-id nat-int?)
(s/def ::tx-time date?)
(s/def ::submitted-tx (s/keys :req [::tx-id ::tx-time]))
(s/def ::committed? boolean?)

(s/def ::doc-ids (s/coll-of #(instance? crux.codec.Id %) :kind set?))
(defmethod bus/event-spec ::indexing-docs [_] (s/keys :req-un [::doc-ids]))
(defmethod bus/event-spec ::indexed-docs [_] (s/keys :req-un [::doc-ids]))

(defmethod bus/event-spec ::indexing-tx [_] (s/keys :req [::submitted-tx]))
(defmethod bus/event-spec ::indexed-tx [_] (s/keys :req [::submitted-tx], :req-un [::committed?]))

(defmulti conform-tx-op first)

(defmethod conform-tx-op ::put [tx-op]
  (let [[op doc & args] tx-op
        id (:crux.db/id doc)]
    (into [::put id doc] args)))

(defmethod conform-tx-op ::cas [tx-op]
  (let [[op old-doc new-doc & args] tx-op
        new-id (:crux.db/id new-doc)
        old-id (:crux.db/id old-doc)]
    (if (or (= nil old-id) (= new-id old-id))
      (into [::cas new-id old-doc new-doc] args)
      (throw (IllegalArgumentException.
              (str "CAS, document ids do not match: " old-id " " new-id))))))

(defmethod conform-tx-op :default [tx-op] tx-op)

(defn tx-op->docs [tx-op]
  (let [[op id & args] (conform-tx-op tx-op)]
    (filter map? args)))

(defn tx-ops->id-and-docs [tx-ops]
  (s/assert :crux.api/tx-ops tx-ops)
  (map #(vector (str (c/new-id %)) %) (mapcat tx-op->docs tx-ops)))

(defn tx-op->tx-event [tx-op]
  (let [[op id & args] (conform-tx-op tx-op)]
    (doto (into [op (str (c/new-id id))]
                (for [arg args]
                  (if (map? arg)
                    (-> arg c/new-id str)
                    arg)))
      (->> (s/assert :crux.tx.event/tx-event)))))

(defn tx-event->tx-op [[op id & args] snapshot object-store]
  (doto (into [op]
              (concat (when (contains? #{:crux.tx/delete :crux.tx/evict :crux.tx/fn} op)
                        [(c/new-id id)])

                      (for [arg args]
                        (or (when (satisfies? c/IdToBuffer arg)
                              (or (db/get-single-object object-store snapshot arg)
                                  {:crux.db/id (c/new-id id)
                                   :crux.db/evicted? true}))
                            arg))))
    (->> (s/assert :crux.api/tx-op))))

(defprotocol EntityHistory
  (entity-history-seq-ascending [_ eid valid-time tx-time])
  (entity-history-seq-descending [_ eid valid-time tx-time])
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
  (entity-history-seq-ascending [_ eid valid-time tx-time]
    (merge-histories etx->vt compare
                     (idx/entity-history-seq-ascending snapshot eid valid-time tx-time)
                     (->> (get etxs eid)
                          (drop-while (comp neg? #(compare % valid-time) etx->vt)))))

  (entity-history-seq-descending [_ eid valid-time tx-time]
    (merge-histories etx->vt #(compare %2 %1)
                     (idx/entity-history-seq-descending snapshot eid valid-time tx-time)
                     (->> (reverse (get etxs eid))
                          (drop-while (comp pos? #(compare % valid-time) etx->vt)))))

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
      (into (set (->> (idx/all-keys-in-prefix i (c/encode-aecv-key-to nil (c/->id-buffer :crux.db/id) (c/->id-buffer eid)))
                      (map c/decode-aecv-key->evc-from)
                      (map #(.content-hash ^EntityValueContentHash %))))
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
        (let [entity-history (entity-history-seq-descending history eid end-valid-time tx-time)]
          (concat (->> (cons start-valid-time
                             (->> (map etx->vt entity-history)
                                  (take-while #(neg? (compare start-valid-time %)))))
                       (remove #{end-valid-time})
                       (map ->new-entity-tx))

                  [(if-let [entity-to-restore ^EntityTx (first entity-history)]
                     (-> entity-to-restore
                         (assoc :vt end-valid-time))

                     (c/->EntityTx eid end-valid-time tx-time tx-id (c/nil-id-buffer)))])))

      (->> (cons start-valid-time
                 (when-let [visible-entity (some-> (entity-at history eid start-valid-time tx-time)
                                                   (select-keys [:tx-time :tx-id :content-hash]))]
                   (->> (entity-history-seq-ascending history eid start-valid-time tx-time)
                        (remove #{start-valid-time})
                        (take-while #(= visible-entity (select-keys % [:tx-time :tx-id :content-hash])))
                        (mapv etx->vt))))

           (map ->new-entity-tx)))))

(defmethod index-tx-event :crux.tx/put [[op k v start-valid-time end-valid-time] tx {:keys [object-store snapshot] :as deps}]
  ;; This check shouldn't be required, under normal operation - the ingester checks for this before indexing
  ;; keeping this around _just in case_ - e.g. if we're refactoring the ingest code
  {:pre-commit-fn #(let [content-hash (c/new-id v)
                         correct-state? (db/known-keys? object-store snapshot [content-hash])]
                     (when-not correct-state?
                       (log/error "Put, incorrect doc state for:" content-hash "tx id:" (:crux.tx/tx-id tx)))
                     correct-state?)

   :etxs (put-delete-etxs k start-valid-time end-valid-time (c/new-id v) tx deps)})

(defmethod index-tx-event :crux.tx/delete [[op k start-valid-time end-valid-time] tx deps]
  {:etxs (put-delete-etxs k start-valid-time end-valid-time nil tx deps)})

(defmethod index-tx-event :crux.tx/cas [[op k old-v new-v at-valid-time :as cas-op] {:crux.tx/keys [tx-time tx-id] :as tx} {:keys [object-store history snapshot] :as deps}]
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

(defmethod index-tx-event :crux.tx/evict [[op k & legacy-args] tx
                                          {:keys [history ^crux.db.DocumentStore document-store] :as deps}]
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
                                       {:keys [object-store kv-store indexer tx-log snapshot], :as deps}]
  (when-not tx-fns-enabled?
    (throw (IllegalArgumentException. (str "Transaction functions not enabled: " (cio/pr-edn-str tx-op)))))

  (let [fn-id (c/new-id k)
        db (q/db kv-store object-store tx-time tx-time)
        {:crux.db.fn/keys [body] :as fn-doc} (q/entity db fn-id)
        {:crux.db.fn/keys [args] :as args-doc} (db/get-single-object object-store snapshot (c/new-id args-v))
        args-id (:crux.db/id args-doc)
        fn-result (try
                    (let [tx-ops (apply (tx-fn-eval-cache body) db (eval args))
                          _ (when tx-ops (s/assert :crux.api/tx-ops tx-ops))
                          docs (mapcat tx-op->docs tx-ops)
                          {arg-docs true docs false} (group-by (comp boolean :crux.db.fn/args) docs)]
                      ;; TODO: might lead to orphaned and unevictable
                      ;; argument docs if the transaction fails. As
                      ;; these nested docs never go to the doc topic,
                      ;; it's slightly less of an issue. It's done
                      ;; this way to support nested fns.
                      (db/index-docs indexer (->> arg-docs (into {} (map (juxt c/new-id identity)))))
                      {:docs (vec docs)
                       :ops-result (vec (for [[op :as tx-event] (map tx-op->tx-event tx-ops)]
                                          (index-tx-event tx-event tx deps)))})

                    (catch Throwable t
                      t))]

    (if (instance? Throwable fn-result)
      {:pre-commit-fn (fn []
                        (log-tx-fn-error fn-result fn-id body args-id args)
                        false)}

      (let [{:keys [docs ops-result]} fn-result]
        {:pre-commit-fn #(do (db/index-docs indexer (->> docs (into {} (map (juxt c/new-id identity)))))
                             (every? true? (for [{:keys [pre-commit-fn]} ops-result
                                                 :when pre-commit-fn]
                                             (pre-commit-fn))))
         :etxs (cond-> (mapcat :etxs ops-result)
                 args-doc (conj (c/->EntityTx args-id tx-time tx-time tx-id args-v)))

         :tombstones (into {} (mapcat :tombstones) ops-result)}))))

(defmethod index-tx-event :default [[op & _] tx deps]
  (throw (IllegalArgumentException. (str "Unknown tx-op: " op))))

(def ^:dynamic *current-tx*)

(defrecord KvIndexer [object-store kv-store tx-log document-store bus ^ExecutorService stats-executor]
  Closeable
  (close [_]
    (when stats-executor
      (doto stats-executor
        (.shutdown)
        (.awaitTermination 60000 TimeUnit/MILLISECONDS))))

  db/Indexer
  (index-docs [_ docs]
    (when-let [missing-ids (seq (remove :crux.db/id (vals docs)))]
      (throw (IllegalArgumentException.
              (str "Missing required attribute :crux.db/id: " (cio/pr-edn-str missing-ids)))))

    (bus/send bus {::bus/event-type ::indexing-docs, :doc-ids (set (keys docs))})

    (let [{docs-to-evict true, docs-to-upsert false} (group-by (comp boolean idx/evicted-doc? val) docs)

          _ (when (seq docs-to-upsert)
              (->> docs-to-upsert
                   (mapcat (fn [[k doc]] (idx/doc-idx-keys k doc)))
                   (idx/store-doc-idx-keys kv-store)))

          docs-to-remove (when (seq docs-to-evict)
                           (with-open [snapshot (kv/new-snapshot kv-store)]
                             (let [existing-docs (db/get-objects object-store snapshot (keys docs-to-evict))]
                               (->> existing-docs
                                    (mapcat (fn [[k doc]] (idx/doc-idx-keys k doc)))
                                    (idx/delete-doc-idx-keys kv-store))

                               existing-docs)))

          docs-stats (concat (->> (vals docs-to-upsert)
                                  (map #(idx/doc-predicate-stats % false)))

                             (->> (vals docs-to-remove)
                                  (map #(idx/doc-predicate-stats % true))))]

      (db/put-objects object-store docs)

      (bus/send bus {::bus/event-type ::indexed-docs, :doc-ids (set (keys docs))})

      (let [stats-fn ^Runnable #(idx/update-predicate-stats kv-store docs-stats)]
        (if stats-executor
          (.submit stats-executor stats-fn)
          (stats-fn)))))

  (index-tx [this {:crux.tx/keys [tx-time tx-id] :as tx} tx-events]
    (s/assert :crux.tx.event/tx-events tx-events)

    (log/debug "Indexing tx-id:" tx-id "tx-events:" (count tx-events))
    (bus/send bus {::bus/event-type ::indexing-tx, ::submitted-tx tx})

    (with-open [snapshot (kv/new-snapshot kv-store)]
      (binding [*current-tx* (assoc tx :crux.tx.event/tx-events tx-events)]
        (let [deps {:object-store object-store
                    :kv-store kv-store
                    :tx-log tx-log
                    :document-store document-store
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

              committed? (not= res ::aborted)
              completed-tx-kv (idx/meta-kv :crux.tx/latest-completed-tx tx)]

          (if (not= res ::aborted)
            (do
              (kv/store kv-store (->> (conj (->> (get-in res [:history :etxs]) (mapcat val) (mapcat etx->kvs))
                                            completed-tx-kv)
                                      (into (sorted-map-by mem/buffer-comparator))))
              (when-let [tombstones (not-empty (:tombstones res))]
                (db/index-docs this tombstones)
                (db/submit-docs document-store tombstones)))

            (do (log/warn "Transaction aborted:" (cio/pr-edn-str tx-events) (cio/pr-edn-str tx-time) tx-id)
                (kv/store kv-store [completed-tx-kv
                                    [(c/encode-failed-tx-id-key-to nil tx-id) c/empty-buffer]])))

          (bus/send bus {::bus/event-type ::indexed-tx, ::submitted-tx tx, :committed? committed?})
          tx))))

  (docs-exist? [_ content-hashes]
    (with-open [snapshot (kv/new-snapshot kv-store)]
      (let [docs (db/get-objects object-store snapshot content-hashes)]
        (every? (fn [content-hash]
                  (when-let [doc (get docs content-hash)]
                    (idx/doc-indexed? kv-store (:crux.db/id doc) content-hash)))
                content-hashes))))

  (store-index-meta [_ k v]
    (idx/store-meta kv-store k v))

  (read-index-meta [_  k]
    (idx/read-meta kv-store k))

  status/Status
  (status-map [this]
    {:crux.index/index-version (idx/current-index-version kv-store)
     :crux.tx/latest-completed-tx (db/read-index-meta this :crux.tx/latest-completed-tx)
     :crux.doc-log/consumer-state (db/read-index-meta this :crux.doc-log/consumer-state)
     :crux.tx-log/consumer-state (db/read-index-meta this :crux.tx-log/consumer-state)}))

(def kv-indexer
  {:start-fn (fn [{:crux.node/keys [object-store kv-store tx-log document-store bus]} args]
               (->KvIndexer object-store kv-store tx-log document-store bus
                            (Executors/newSingleThreadExecutor (cio/thread-factory "crux.tx.update-stats-thread"))))
   :deps [:crux.node/kv-store :crux.node/tx-log :crux.node/document-store :crux.node/object-store :crux.node/bus]})

(defn await-tx [indexer {::keys [tx-id] :as tx} timeout-ms]
  (let [seen-tx (atom nil)]
    (if (cio/wait-while #(let [latest-completed-tx (db/read-index-meta indexer :crux.tx/latest-completed-tx)]
                           (reset! seen-tx latest-completed-tx)
                           (or (nil? latest-completed-tx)
                               (pos? (compare tx-id (:crux.tx/tx-id latest-completed-tx)))))
                        timeout-ms)
      @seen-tx
      (throw (TimeoutException.
              (str "Timed out waiting for: " (cio/pr-edn-str tx)
                   " index has: " (cio/pr-edn-str @seen-tx)))))))

(defn await-tx-time [indexer transact-time timeout-ms]
  (let [seen-tx (atom nil)]
    (if (cio/wait-while #(let [latest-completed-tx (db/read-index-meta indexer :crux.tx/latest-completed-tx)]
                           (reset! seen-tx latest-completed-tx)
                           (or (nil? latest-completed-tx)
                               (pos? (compare transact-time (:crux.tx/tx-time latest-completed-tx)))))
                        timeout-ms)
      @seen-tx
      (throw (TimeoutException.
              (str "Timed out waiting for: " (cio/pr-edn-str transact-time)
                   " index has: " (cio/pr-edn-str @seen-tx)))))))
