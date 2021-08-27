(ns xtdb.node
  (:require [clojure.java.io :as io]
            [clojure.pprint :as pp]
            [clojure.tools.logging :as log]
            [xtdb.api :as xt]
            [xtdb.bus :as bus]
            [xtdb.codec :as c]
            [xtdb.db :as db]
            [xtdb.error :as err]
            [xtdb.io :as xio]
            [xtdb.query :as q]
            [xtdb.query-state :as qs]
            [xtdb.status :as status]
            [xtdb.system :as sys]
            [xtdb.tx :as tx]
            [xtdb.tx.conform :as txc]
            [xtdb.tx.event :as txe])
  (:import [java.io Closeable Writer]
           [java.time Duration Instant]
           java.util.concurrent.locks.StampedLock
           java.util.concurrent.TimeoutException
           java.util.Date))

(def crux-version
  (when-let [pom-file (io/resource "META-INF/maven/com.xtdb/xtdb-core/pom.properties")]
    (with-open [in (io/reader pom-file)]
      (let [{:strs [version revision]} (xio/load-properties in)]
        {:xtdb.version/version version
         :xtdb.version/revision revision}))))

(defn- ensure-node-open [{:keys [closed?]}]
  (when @closed?
    (throw (IllegalStateException. "Crux node is closed"))))

(defn- await-tx [{:keys [bus tx-ingester]} tx-k awaited-tx ^Duration timeout]
  (let [tx-v (get awaited-tx tx-k)
        fut (bus/await bus {:xt/event-types #{::tx/indexed-tx ::tx/ingester-error ::node-closing}
                            :->result (letfn [(tx->result [tx]
                                                (when (and tx (not (neg? (compare (get tx tx-k) tx-v))))
                                                  {:tx tx}))]
                                        (fn
                                          ([] (or
                                               (tx->result (db/latest-completed-tx tx-ingester))
                                               (when-let [ingester-error (db/ingester-error tx-ingester)]
                                                 {:ingester-error ingester-error})))
                                          ([{:keys [xt/event-type] :as ev}]
                                           (case event-type
                                             ::tx/indexed-tx (tx->result (:submitted-tx ev))
                                             ::tx/ingester-error {:ingester-error (:ingester-error ev)}
                                             ::node-closing {:node-closing? true}))))})

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
      tx tx)))

(defn- query-expired? [{:keys [finished-at] :as query} ^Duration max-age]
  (when finished-at
    (let [time-since-query ^Duration (Duration/between (.toInstant ^Date finished-at) (Instant/now))]
      (neg? (.compareTo max-age time-since-query)))))

(defn slow-query? [{:keys [started-at finished-at] :as query} {:keys [^Duration slow-queries-min-threshold]}]
  (let [time-taken (Duration/between (.toInstant ^Date started-at) (.toInstant ^Date finished-at))]
    (neg? (.compareTo slow-queries-min-threshold time-taken))))

(defn- clean-completed-queries [queries {:keys [recent-queries-max-age recent-queries-max-count]}]
  (->> queries
       (remove (fn [query] (query-expired? query recent-queries-max-age)))
       (sort-by :finished-at #(compare %2 %1))
       (take recent-queries-max-count)))

(defn- clean-slowest-queries [queries {:keys [slow-queries-max-age slow-queries-max-count]}]
  (->> queries
       (remove (fn [query] (query-expired? query slow-queries-max-age)))
       (sort-by (fn [{:keys [^Date started-at ^Date finished-at]}]
                  (- (.getTime started-at) (.getTime finished-at))))
       (take slow-queries-max-count)))

(defrecord XtdbNode [kv-store tx-log document-store index-store tx-ingester bus query-engine
                     !running-queries close-fn !system closed? ^StampedLock lock]
  Closeable
  (close [_]
    (when close-fn
      (xio/with-write-lock lock
        (when (not @closed?)
          (bus/send bus {:xt/event-type ::node-closing})
          (close-fn)
          (reset! closed? true)))))

  xt/DBProvider
  (db [this] (xt/db this {}))
  (db [this valid-time-or-basis]
    (if (instance? Date valid-time-or-basis)
      (xt/db this {:xt/valid-time valid-time-or-basis})
      (xt/db query-engine valid-time-or-basis)))
  (db [this valid-time tx-time]
    (xt/db this {:xt/valid-time valid-time, :xt/tx-time tx-time}))

  (open-db [this] (xt/open-db this {}))
  (open-db [this valid-time tx-time]
    (xt/open-db this {:xt/valid-time valid-time :xt/tx-time tx-time}))
  (open-db [this valid-time-or-basis]
    (if (instance? Date valid-time-or-basis)
      (xt/open-db this {:xt/valid-time valid-time-or-basis})
      (xio/with-read-lock lock
        (ensure-node-open this)
        (xt/open-db query-engine valid-time-or-basis))))

  xt/PXtdb
  (status [this]
    (xio/with-read-lock lock
      (ensure-node-open this)
      (letfn [(status [m]
                (merge (status/status-map m)
                       (when (map? m)
                         (into {} (mapcat status) (vals m)))))]
        (merge crux-version
               (status (dissoc @!system :xtdb/node))))))

  (tx-committed? [this {:keys [:xt/tx-id] :as submitted-tx}]
    (xio/with-read-lock lock
      (ensure-node-open this)
      (let [{latest-tx-id :xt/tx-id, :as latest-tx} (xt/latest-completed-tx this)]
        (cond
          (nil? tx-id) (throw (err/illegal-arg :invalid-tx {:tx submitted-tx}))

          (or (nil? latest-tx-id) (pos? (compare tx-id latest-tx-id)))
          (throw (err/node-out-of-sync {:requested submitted-tx, :available latest-tx}))

          :else (not (db/tx-failed? index-store tx-id))))))

  (sync [this] (xt/sync this nil))

  (sync [this timeout]
    (when-let [tx (db/latest-submitted-tx tx-log)]
      (-> (xt/await-tx this tx timeout)
          :xt/tx-time)))

  (sync [this tx-time timeout]
    (defonce warn-on-deprecated-sync
      (log/warn "(sync tx-time <timeout?>) is deprecated, replace with either (await-tx-time tx-time <timeout?>) or, preferably, (await-tx tx <timeout?>)"))
    (:xt/tx-time (await-tx this :xt/tx-time {:xt/tx-time tx-time} timeout)))

  (await-tx [this submitted-tx]
    (xt/await-tx this submitted-tx nil))

  (await-tx [this submitted-tx timeout]
    (await-tx this :xt/tx-id submitted-tx timeout))

  (await-tx-time [this tx-time]
    (xt/await-tx-time this tx-time nil))

  (await-tx-time [this tx-time timeout]
    (:xt/tx-time (await-tx this :xt/tx-time {:xt/tx-time tx-time} timeout)))

  (listen [_ {:xt/keys [event-type] :as event-opts} f]
    (case event-type
      :xtdb/indexed-tx
      (bus/listen bus
                  (assoc event-opts :xt/event-types #{::tx/indexed-tx})
                  (fn [{:keys [submitted-tx ::txe/tx-events] :as ev}]
                    (f (merge {:xt/event-type :xtdb/indexed-tx}
                              (select-keys ev [:committed?])
                              (select-keys submitted-tx [:xt/tx-time :xt/tx-id])
                              (when (:with-tx-ops? event-opts)
                                {:xt/tx-ops (txc/tx-events->tx-ops document-store tx-events)})))))))

  (latest-completed-tx [this]
    (xio/with-read-lock lock
      (ensure-node-open this)
      (db/latest-completed-tx index-store)))

  (latest-submitted-tx [_]
    (db/latest-submitted-tx tx-log))

  (attribute-stats [this]
    (xio/with-read-lock lock
      (ensure-node-open this)
      (with-open [snapshot (db/open-index-snapshot index-store)]
        (->> (db/all-attrs snapshot)
             (into {} (map (juxt identity #(db/doc-count snapshot %))))))))

  (active-queries [_]
    (map qs/->QueryState (vals (:in-progress @!running-queries))))

  (recent-queries [this]
    (let [running-queries (swap! !running-queries update :completed clean-completed-queries this)]
      (map qs/->QueryState (:completed running-queries))))

  (slowest-queries [this]
    (let [running-queries (swap! !running-queries update :slowest clean-slowest-queries this)]
      (map qs/->QueryState (:slowest running-queries))))

  xt/PXtdbSubmitClient
  (submit-tx-async [this tx-ops]
    (let [tx-ops (xt/conform-tx-ops tx-ops)
          conformed-tx-ops (mapv txc/conform-tx-op tx-ops)]
      (xio/with-read-lock lock
        (ensure-node-open this)
        (db/submit-docs document-store (->> conformed-tx-ops
                                            (into {} (comp (mapcat :docs)))
                                            (xio/map-vals c/xt->crux)))
        (db/submit-tx tx-log (mapv txc/->tx-event conformed-tx-ops)))))

  (submit-tx [this tx-ops]
    @(xt/submit-tx-async this tx-ops))

  (open-tx-log ^xtdb.api.ICursor [this after-tx-id with-ops?]
    (let [with-ops? (boolean with-ops?)]
      (xio/with-read-lock lock
        (ensure-node-open this)
        (if (let [latest-submitted-tx-id (:xt/tx-id (xt/latest-submitted-tx this))]
              (or (nil? latest-submitted-tx-id)
                  (and after-tx-id (>= after-tx-id latest-submitted-tx-id))))
          xio/empty-cursor

          (let [latest-completed-tx-id (:xt/tx-id (xt/latest-completed-tx this))
                tx-log-iterator (db/open-tx-log tx-log after-tx-id)
                tx-log (->> (iterator-seq tx-log-iterator)
                            (remove #(db/tx-failed? index-store (:xt/tx-id %)))
                            (take-while (comp #(<= % latest-completed-tx-id) :xt/tx-id))
                            (map (if with-ops?
                                   (fn [{:keys [xtdb.tx.event/tx-events] :as tx-log-entry}]
                                     (-> tx-log-entry
                                         (dissoc :xtdb.tx.event/tx-events)
                                         (assoc :xt/tx-ops (txc/tx-events->tx-ops document-store tx-events))))
                                   (fn [tx-log-entry]
                                     (-> tx-log-entry
                                         (update :xtdb.tx.event/tx-events
                                                 (fn [evts]
                                                   (->> evts
                                                        (mapv (fn [evt]
                                                                (-> evt
                                                                    (update 0 txc/crux-op->xt-op)
                                                                    (update 1 c/new-id))))))))))))]
            (xio/->cursor (fn []
                            (.close tx-log-iterator))
                          tx-log)))))))

(defmethod print-method XtdbNode [node ^Writer w] (.write w "#<XtdbNode>"))
(defmethod pp/simple-dispatch XtdbNode [it] (print-method it *out*))

(defn- swap-finished-query! [!running-queries {:keys [query-id] :as query} {:keys [bus] :as  node-opts}]
  (loop []
    (let [queries @!running-queries
          query (merge (get-in queries [:in-progress query-id])
                       query)
          slow-query? (slow-query? query node-opts)]
      (if-not (compare-and-set! !running-queries
                                queries
                                (-> queries
                                    (update :in-progress dissoc query-id)
                                    (update :completed conj query)
                                    (update :completed clean-completed-queries node-opts)
                                    (cond-> slow-query? (-> (update :slowest conj query)
                                                            (update :slowest clean-slowest-queries node-opts)))))
        (recur)
        (when slow-query?
          (bus/send bus {:xt/event-type :slow-query
                         :query query}))))))

(defn attach-current-query-listeners [!running-queries {:keys [bus] :as node-opts}]
  (bus/listen bus {:xt/event-types #{:xtdb.query/submitted-query
                                     :xtdb.query/completed-query
                                     :xtdb.query/failed-query}}
              (fn [{::q/keys [query-id query error] :keys [xt/event-type]}]
                (case event-type
                  :xtdb.query/submitted-query
                  (swap! !running-queries assoc-in [:in-progress query-id] {:query-id query-id
                                                                            :started-at (Date.)
                                                                            :query query
                                                                            :status :in-progress})

                  :xtdb.query/failed-query
                  (swap-finished-query! !running-queries
                                        {:query-id query-id
                                         :finished-at (Date.)
                                         :status :failed
                                         :error error}
                                        node-opts)

                  :xtdb.query/completed-query
                  (swap-finished-query! !running-queries
                                        {:query-id query-id
                                         :finished-at (Date.)
                                         :status :completed}
                                        node-opts)))))

(defn ->node {::sys/deps {:index-store :xtdb/index-store
                          :tx-ingester :xtdb/tx-ingester
                          :bus :xtdb/bus
                          :document-store :xtdb/document-store
                          :tx-log :xtdb/tx-log
                          :query-engine :xtdb/query-engine}
              ::sys/args {:await-tx-timeout {:doc "Default timeout for awaiting transactions being indexed."
                                             :default nil
                                             :spec ::sys/duration}
                          :recent-queries-max-age {:doc "How long to keep recently ran queries on the query queue"
                                                   :default (Duration/ofMinutes 5)
                                                   ::spec ::sys/duration}
                          :recent-queries-max-count {:doc "Max number of finished queries to retain on the query queue"
                                                     :default 20
                                                     :spec ::sys/nat-int}
                          :slow-queries-max-age {:doc "How long to retain queries on the slow query queue"
                                                 :default (Duration/ofHours 24)
                                                 :spec ::sys/duration}
                          :slow-queries-max-count {:doc "Max number of finished queries to retain on the slow query queue"
                                                   :default 100
                                                   :spec ::sys/nat-int}
                          :slow-queries-min-threshold {:doc "Minimum threshold for a query to be considered slow."
                                                       :default (Duration/ofMinutes 1)
                                                       :spec ::sys/duration}}}
  [opts]
  (map->XtdbNode (merge opts
                        {:!running-queries (doto (atom {:in-progress {} :completed '()})
                                             (attach-current-query-listeners opts))
                         :closed? (atom false)
                         :lock (StampedLock.)
                         :!system (atom nil)})))
