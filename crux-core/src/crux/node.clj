(ns crux.node
  (:require [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [com.stuartsierra.dependency :as dep]
            [crux.api :as api]
            [crux.codec :as c]
            [crux.db :as db]
            [crux.io :as cio]
            [crux.kv :as kv]
            [crux.lru :as lru]
            [crux.query :as q]
            [crux.status :as status]
            [crux.query-state :as qs]
            [crux.system :as sys]
            [crux.tx :as tx]
            [crux.tx.event :as txe]
            [crux.bus :as bus]
            [crux.tx.conform :as txc])
  (:import (crux.api ICruxAPI ICruxAsyncIngestAPI NodeOutOfSyncException ICursor
                     QueryState QueryState$QueryStatus QueryState$QueryError)
           (java.io Closeable Writer)
           (java.lang AutoCloseable)
           java.util.function.Consumer
           java.util.Date
           [java.util.concurrent Executors TimeoutException]
           java.util.concurrent.locks.StampedLock
           (java.time Duration Instant)))

(def crux-version
  (when-let [pom-file (io/resource "META-INF/maven/juxt/crux-core/pom.properties")]
    (with-open [in (io/reader pom-file)]
      (let [{:strs [version revision]} (cio/load-properties in)]
        {:crux.version/version version
         :crux.version/revision revision}))))

(defn- ensure-node-open [{:keys [closed?]}]
  (when @closed?
    (throw (IllegalStateException. "Crux node is closed"))))

(defn- await-tx [{:keys [bus] :as node} tx-k tx ^Duration timeout]
  (let [tx-v (get tx tx-k)

        {:keys [timeout? ingester-error node-closed? tx] :as res}
        (bus/await bus {:crux/event-types #{::tx/indexed-tx ::tx/ingester-error ::node-closed}
                        :->result (letfn [(tx->result [tx]
                                            (when (and tx (not (neg? (compare (get tx tx-k) tx-v))))
                                              {:tx tx}))]
                                    (fn
                                      ([] (tx->result (api/latest-completed-tx node)))
                                      ([{:keys [crux/event-type] :as ev}]
                                       (case event-type
                                         ::tx/indexed-tx (tx->result (::tx/submitted-tx ev))
                                         ::tx/ingester-error {:ingester-error (::tx/ingester-error ev)}
                                         ::node-closed {:node-closed? true}))))
                        :timeout timeout
                        :timeout-value {:timeout? true}})]
    (cond
      ingester-error (throw (Exception. "Transaction ingester aborted." ingester-error))
      timeout? (throw (TimeoutException. (str "Timed out waiting for: " (pr-str tx)
                                              ", index has: " (pr-str (api/latest-completed-tx node)))))
      node-closed? (throw (InterruptedException. "Node closed."))
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

(defrecord CruxNode [kv-store tx-log document-store indexer tx-ingester bus query-engine
                     !running-queries close-fn !system closed? ^StampedLock lock]
  ICruxAPI
  (db [this] (.db this nil nil))
  (db [this valid-time] (.db this valid-time nil))

  (db [this valid-time tx-time]
    (cio/with-read-lock lock
      (ensure-node-open this)
      (api/db query-engine valid-time tx-time)))

  (openDB [this] (.openDB this nil nil))
  (openDB [this valid-time] (.openDB this valid-time nil))
  (openDB [this valid-time tx-time]
    (cio/with-read-lock lock
      (ensure-node-open this)
      (api/open-db query-engine valid-time tx-time)))

  (status [this]
    (cio/with-read-lock lock
      (ensure-node-open this)
      (letfn [(status [m]
                (merge (status/status-map m)
                       (when (map? m)
                         (into {} (mapcat status) (vals m)))))]
        (merge crux-version
               (status (dissoc @!system :crux/node))))))

  (attributeStats [this]
    (cio/with-read-lock lock
      (ensure-node-open this)
      (db/read-index-meta indexer :crux/attribute-stats)))

  (submitTx [this tx-ops]
    (cio/with-read-lock lock
      (ensure-node-open this)
      (let [conformed-tx-ops (mapv txc/conform-tx-op tx-ops)]
        (db/submit-docs document-store (into {} (mapcat :docs) conformed-tx-ops))
        @(db/submit-tx tx-log (mapv txc/->tx-event conformed-tx-ops)))))

  (hasTxCommitted [this {:keys [::tx/tx-id ::tx/tx-time] :as submitted-tx}]
    (cio/with-read-lock lock
      (ensure-node-open this)
      (let [{latest-tx-id ::tx/tx-id, latest-tx-time ::tx/tx-time} (.latestCompletedTx this)]
        (if (and tx-id (or (nil? latest-tx-id) (pos? (compare tx-id latest-tx-id))))
          (throw
           (NodeOutOfSyncException.
            (format "Node hasn't indexed the transaction: requested: %s, available: %s" tx-time latest-tx-time)
            tx-time latest-tx-time))
          (not (db/tx-failed? indexer tx-id))))))

  (openTxLog ^ICursor [this after-tx-id with-ops?]
    (cio/with-read-lock lock
      (ensure-node-open this)
      (if (let [latest-submitted-tx-id (::tx/tx-id (api/latest-submitted-tx this))]
            (or (nil? latest-submitted-tx-id)
                (and after-tx-id (>= after-tx-id latest-submitted-tx-id))))
        (cio/->cursor #() [])

        (let [latest-completed-tx-id (::tx/tx-id (api/latest-completed-tx this))
              tx-log-iterator (db/open-tx-log tx-log after-tx-id)
              tx-log (->> (iterator-seq tx-log-iterator)
                          (remove #(db/tx-failed? indexer (:crux.tx/tx-id %)))
                          (take-while (comp #(<= % latest-completed-tx-id) ::tx/tx-id))
                          (map (if with-ops?
                                 (fn [{:keys [crux.tx/tx-id crux.tx.event/tx-events] :as tx-log-entry}]
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
                        tx-log)))))

  (sync [this timeout]
    (when-let [tx (db/latest-submitted-tx (:tx-log this))]
      (-> (api/await-tx this tx timeout)
          :crux.tx/tx-time)))

  (awaitTxTime [this tx-time timeout]
    (::tx/tx-time (await-tx this ::tx/tx-time {::tx/tx-time tx-time} timeout)))

  (awaitTx [this submitted-tx timeout]
    (await-tx this ::tx/tx-id submitted-tx timeout))

  (listen [this {:crux/keys [event-type] :as event-opts} consumer]
    (case event-type
      :crux/indexed-tx
      (bus/listen bus
                  (assoc event-opts :crux/event-types #{::tx/indexed-tx})
                  (fn [{:keys [::tx/submitted-tx ::txe/tx-events] :as ev}]
                    (.accept ^Consumer consumer
                             (merge {:crux/event-type :crux/indexed-tx}
                                    (select-keys ev [:committed?])
                                    (select-keys submitted-tx [::tx/tx-time ::tx/tx-id])
                                    (when (:with-tx-ops? event-opts)
                                      {:crux/tx-ops (txc/tx-events->tx-ops document-store tx-events)})))))))

  (latestCompletedTx [this]
    (cio/with-read-lock lock
      (ensure-node-open this)
      (db/latest-completed-tx indexer)))

  (latestSubmittedTx [this]
    (db/latest-submitted-tx tx-log))

  (activeQueries [this]
    (map qs/->QueryState (vals (:in-progress @!running-queries))))

  (recentQueries [this]
    (let [running-queries (swap! !running-queries update :completed clean-completed-queries this)]
      (map qs/->QueryState (:completed running-queries))))

  (slowestQueries [this]
    (let [running-queries (swap! !running-queries update :slowest clean-slowest-queries this)]
      (map qs/->QueryState (:slowest running-queries))))

  ICruxAsyncIngestAPI
  (submitTxAsync [this tx-ops]
    (cio/with-read-lock lock
      (ensure-node-open this)
      (let [conformed-tx-ops (mapv txc/conform-tx-op tx-ops)]
        (db/submit-docs document-store (into {} (mapcat :docs) conformed-tx-ops))
        (db/submit-tx tx-log (mapv txc/->tx-event conformed-tx-ops)))))

  Closeable
  (close [_]
    (when close-fn
      (cio/with-write-lock lock
        (when (not @closed?)
          (close-fn)
          (reset! closed? true)
          (bus/send bus {:crux/event-type ::node-closed}))))))

(defmethod print-method CruxNode [node ^Writer w] (.write w "#<CruxNode>"))

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
  (bus/listen bus {:crux/event-types #{:crux.query/submitted-query}}
              (fn [{::q/keys [query-id query]}]
                (swap! !running-queries assoc-in [:in-progress query-id] {:query-id query-id
                                                                          :started-at (Date.)
                                                                          :query query
                                                                          :status :in-progress})))

  (bus/listen bus {:crux/event-types #{:crux.query/failed-query}}
              (fn [{::q/keys [query-id error]}]
                (swap-finished-query! !running-queries
                                      {:query-id query-id
                                       :finished-at (Date.)
                                       :status :failed
                                       :error error}
                                      node-opts)))

  (bus/listen bus {:crux/event-types #{:crux.query/completed-query}}
              (fn [{::q/keys [query-id]}]
                (swap-finished-query! !running-queries
                                      {:query-id query-id
                                       :finished-at (Date.)
                                       :status :completed}
                                      node-opts))))

(defn- ->node {::sys/deps {:indexer :crux/indexer
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
