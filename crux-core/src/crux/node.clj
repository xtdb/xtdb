(ns crux.node
  (:require [clojure.java.io :as io]
            [clojure.pprint :as pp]
            [clojure.tools.logging :as log]
            [crux.api :as api]
            [crux.bus :as bus]
            [crux.codec :as c]
            [crux.db :as db]
            [crux.error :as err]
            [crux.io :as cio]
            [crux.query :as q]
            [crux.query-state :as qs]
            [crux.status :as status]
            [crux.system :as sys]
            [crux.tx :as tx]
            [crux.tx.conform :as txc]
            [crux.tx.event :as txe])
  (:import [java.io Closeable Writer]
           [java.time Duration Instant]
           java.util.concurrent.locks.StampedLock
           java.util.Date))

(def crux-version
  (when-let [pom-file (io/resource "META-INF/maven/juxt/crux-core/pom.properties")]
    (with-open [in (io/reader pom-file)]
      (let [{:strs [version revision]} (cio/load-properties in)]
        {:crux.version/version version
         :crux.version/revision revision}))))

(defn- ensure-node-open [{:keys [closed?]}]
  (when @closed?
    (throw (IllegalStateException. "Crux node is closed"))))

(defn- query-expired? [{:keys [finished-at]} ^Duration max-age]
  (when finished-at
    (let [time-since-query ^Duration (Duration/between (.toInstant ^Date finished-at) (Instant/now))]
      (neg? (.compareTo max-age time-since-query)))))

(defn slow-query? [{:keys [started-at finished-at]} {:keys [^Duration slow-queries-min-threshold]}]
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

(defrecord CruxNode [kv-store tx-log document-store index-store tx-ingester bus query-engine
                     !running-queries close-fn !system closed? ^StampedLock lock]
  Closeable
  (close [_]
    (when close-fn
      (cio/with-write-lock lock
        (when (not @closed?)
          (bus/send bus {:crux/event-type ::node-closing})
          (close-fn)
          (reset! closed? true)))))

  api/DBProvider
  (db [this] (api/db this {}))
  (db [this valid-time-or-basis]
    (if (instance? Date valid-time-or-basis)
      (api/db this {:crux.db/valid-time valid-time-or-basis})
      (api/db query-engine valid-time-or-basis)))
  (db [this valid-time tx-time]
    (api/db this {:crux.db/valid-time valid-time, :crux.tx/tx-time tx-time}))

  (open-db [this] (api/open-db this {}))
  (open-db [this valid-time tx-time]
    (api/open-db this {:crux.db/valid-time valid-time :crux.tx/tx-time tx-time}))
  (open-db [this valid-time-or-basis]
    (if (instance? Date valid-time-or-basis)
      (api/open-db this {:crux.db/valid-time valid-time-or-basis})
      (cio/with-read-lock lock
        (ensure-node-open this)
        (api/open-db query-engine valid-time-or-basis))))

  api/PCruxNode
  (status [this]
    (cio/with-read-lock lock
      (ensure-node-open this)
      (letfn [(status [m]
                (merge (status/status-map m)
                       (when (map? m)
                         (into {} (mapcat status) (vals m)))))]
        (merge crux-version
               (status (dissoc @!system :crux/node))))))

  (tx-committed? [this {:keys [::tx/tx-id] :as submitted-tx}]
    (cio/with-read-lock lock
      (cio/with-read-lock lock
        (ensure-node-open this)
        (let [{latest-tx-id ::tx/tx-id, :as latest-tx} (api/latest-completed-tx this)]
          (if (and tx-id (or (nil? latest-tx-id) (pos? (compare tx-id latest-tx-id))))
            (throw (err/node-out-of-sync {:requested submitted-tx, :available latest-tx}))
            (not (db/tx-failed? index-store tx-id)))))))

  (sync [this] (api/sync this nil))

  (sync [this timeout]
    (when-let [tx (db/latest-submitted-tx tx-log)]
      (-> (api/await-tx this tx timeout)
          :crux.tx/tx-time)))

  (sync [this tx-time timeout]
    (defonce warn-on-deprecated-sync
      (log/warn "(sync tx-time <timeout?>) is deprecated, replace with either (await-tx-time tx-time <timeout?>) or, preferably, (await-tx tx <timeout?>)"))
    (::tx/tx-time (tx/await-tx this {::tx/tx-time tx-time} {:timeout timeout})))

  (await-tx [this submitted-tx]
    (api/await-tx this submitted-tx nil))

  (await-tx [this submitted-tx timeout]
    (tx/await-tx this submitted-tx {:timeout timeout}))

  (await-tx-time [this tx-time]
    (api/await-tx-time this tx-time nil))

  (await-tx-time [this tx-time timeout]
    (::tx/tx-time (tx/await-tx this {::tx/tx-time tx-time} {:timeout timeout})))

  (listen [_ {:crux/keys [event-type] :as event-opts} f]
    (case event-type
      :crux/indexed-tx
      (bus/listen bus
                  (assoc event-opts :crux/event-types #{::tx/indexed-tx})
                  (fn [{:keys [submitted-tx ::txe/tx-events] :as ev}]
                    (f (merge {:crux/event-type :crux/indexed-tx}
                              (select-keys ev [:committed?])
                              (select-keys submitted-tx [::tx/tx-time ::tx/tx-id])
                              (when (:with-tx-ops? event-opts)
                                {:crux/tx-ops (txc/tx-events->tx-ops document-store tx-events)})))))))

  (latest-completed-tx [this]
    (cio/with-read-lock lock
      (ensure-node-open this)
      (db/latest-completed-tx index-store)))

  (latest-submitted-tx [_]
    (db/latest-submitted-tx tx-log))

  (attribute-stats [this]
    (cio/with-read-lock lock
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

  api/PCruxIngestClient
  (submit-tx [this tx-ops]
    @(api/submit-tx-async this tx-ops))

  (open-tx-log ^crux.api.ICursor [this after-tx-id with-ops?]
    (let [with-ops? (boolean with-ops?)]
      (cio/with-read-lock lock
        (ensure-node-open this)
        (if (let [latest-submitted-tx-id (::tx/tx-id (api/latest-submitted-tx this))]
              (or (nil? latest-submitted-tx-id)
                  (and after-tx-id (>= after-tx-id latest-submitted-tx-id))))
          cio/empty-cursor

          (let [latest-completed-tx-id (::tx/tx-id (api/latest-completed-tx this))
                tx-log-iterator (db/open-tx-log tx-log after-tx-id)
                tx-log (->> (iterator-seq tx-log-iterator)
                            (remove #(db/tx-failed? index-store (:crux.tx/tx-id %)))
                            (take-while (comp #(<= % latest-completed-tx-id) ::tx/tx-id))
                            (map (if with-ops?
                                   (fn [{:keys [crux.tx.event/tx-events] :as tx-log-entry}]
                                     (-> tx-log-entry
                                         (dissoc :crux.tx.event/tx-events)
                                         (assoc :crux.api/tx-ops (txc/tx-events->tx-ops document-store tx-events))))
                                   (fn [tx-log-entry]
                                     (-> tx-log-entry
                                         (update :crux.tx.event/tx-events
                                                 (fn [evts]
                                                   (->> evts (mapv #(update % 1 c/new-id))))))))))]
            (cio/->cursor (fn []
                            (.close tx-log-iterator))
                          tx-log))))))
  api/PCruxAsyncIngestClient
  (submit-tx-async [this tx-ops]
    (let [tx-ops (api/conform-tx-ops tx-ops)
          conformed-tx-ops (mapv txc/conform-tx-op tx-ops)]
      (cio/with-read-lock lock
        (ensure-node-open this)
        (db/submit-docs document-store (into {} (mapcat :docs) conformed-tx-ops))
        (db/submit-tx tx-log (mapv txc/->tx-event conformed-tx-ops))))))

(defmethod print-method CruxNode [_node ^Writer w] (.write w "#<CruxNode>"))
(defmethod pp/simple-dispatch CruxNode [it] (print-method it *out*))

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
          (bus/send bus {:crux/event-type :slow-query
                         :query query}))))))

(defn attach-current-query-listeners [!running-queries {:keys [bus] :as node-opts}]
  (bus/listen bus {:crux/event-types #{:crux.query/submitted-query
                                       :crux.query/completed-query
                                       :crux.query/failed-query}}
              (fn [{::q/keys [query-id query error] :keys [crux/event-type]}]
                (case event-type
                  :crux.query/submitted-query
                  (swap! !running-queries assoc-in [:in-progress query-id] {:query-id query-id
                                                                            :started-at (Date.)
                                                                            :query query
                                                                            :status :in-progress})

                  :crux.query/failed-query
                  (swap-finished-query! !running-queries
                                        {:query-id query-id
                                         :finished-at (Date.)
                                         :status :failed
                                         :error error}
                                        node-opts)

                  :crux.query/completed-query
                  (swap-finished-query! !running-queries
                                        {:query-id query-id
                                         :finished-at (Date.)
                                         :status :completed}
                                        node-opts)))))

(defn ->node {::sys/deps {:index-store :crux/index-store
                          :tx-ingester :crux/tx-ingester
                          :bus :crux/bus
                          :document-store :crux/document-store
                          :tx-log :crux/tx-log
                          :query-engine :crux/query-engine}
              ::sys/args {:await-tx-timeout {:doc "Default timeout for awaiting transactions being indexed."
                                             :default nil
                                             :spec ::sys/duration}
                          :recent-queries-max-age {:doc "How long to keep recently ran queries on the query queue"
                                                   :default (Duration/ofMinutes 5)
                                                   :crux.config/type :crux.config/duration}
                          :recent-queries-max-count {:doc "Max number of finished queries to retain on the query queue"
                                                     :default 20
                                                     :crux.config/type :crux.config/nat-int}
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
  (map->CruxNode (merge opts
                        {:!running-queries (doto (atom {:in-progress {} :completed '()})
                                             (attach-current-query-listeners opts))
                         :closed? (atom false)
                         :lock (StampedLock.)
                         :!system (atom nil)})))
