(ns crux.node
  (:require [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [com.stuartsierra.dependency :as dep]
            [crux.api :as api]
            [crux.backup :as backup]
            [crux.codec :as c]
            [crux.config :as cc]
            [crux.db :as db]
            [crux.io :as cio]
            [crux.kv :as kv]
            [crux.lru :as lru]
            crux.object-store
            [crux.query :as q]
            [crux.status :as status]
            [crux.topology :as topo]
            [crux.tx :as tx]
            [crux.tx.event :as txe]
            [crux.bus :as bus]
            [crux.tx.conform :as txc])
  (:import [crux.api ICruxAPI ICruxAsyncIngestAPI NodeOutOfSyncException ICursor]
           (java.io Closeable PrintWriter)
           java.util.function.Consumer
           [java.util.concurrent Executors]
           java.util.concurrent.locks.StampedLock))

(def crux-version
  (when-let [pom-file (io/resource "META-INF/maven/juxt/crux-core/pom.properties")]
    (with-open [in (io/reader pom-file)]
      (let [{:strs [version revision]} (cio/load-properties in)]
        {:crux.version/version version
         :crux.version/revision revision}))))

(defn- ensure-node-open [{:keys [closed?]}]
  (when @closed?
    (throw (IllegalStateException. "Crux node is closed"))))

(defrecord CruxNode [kv-store tx-log document-store indexer tx-consumer object-store bus query-engine
                     options close-fn status-fn closed? ^StampedLock lock]
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
      ;; we don't have status-fn set when other components use node as a dependency within the topology
      (if status-fn
        (status-fn)
        (into {} (mapcat status/status-map) [indexer kv-store object-store tx-log]))))

  (attributeStats [this]
    (cio/with-read-lock lock
      (ensure-node-open this)
      (db/read-index-meta indexer :crux.kv/stats)))

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
              tx-log (-> (iterator-seq tx-log-iterator)
                         (->> (remove #(db/tx-failed? indexer (:crux.tx/tx-id %)))
                              (take-while (comp #(<= % latest-completed-tx-id) ::tx/tx-id)))
                         (cond->> with-ops? (map (fn [{:keys [crux.tx/tx-id crux.tx.event/tx-events]
                                                       :as tx-log-entry}]
                                                   (-> tx-log-entry
                                                       (dissoc :crux.tx.event/tx-events)
                                                       (assoc :crux.api/tx-ops (txc/tx-events->tx-ops document-store tx-events)))))))]

          (cio/->cursor (fn []
                          (.close tx-log-iterator))
                        tx-log)))))

  (sync [this timeout]
    (when-let [tx (db/latest-submitted-tx (:tx-log this))]
      (-> (api/await-tx this tx timeout)
          :crux.tx/tx-time)))

  (awaitTxTime [this tx-time timeout]
    (cio/with-read-lock lock
      (ensure-node-open this)
      (-> (tx/await-tx-time indexer tx-consumer tx-time (or timeout (:crux.tx-log/await-tx-timeout options)))
          :crux.tx/tx-time)))

  (awaitTx [this submitted-tx timeout]
    (cio/with-read-lock lock
      (ensure-node-open this)
      (tx/await-tx indexer tx-consumer submitted-tx (or timeout (:crux.tx-log/await-tx-timeout options)))))

  (listen [this {:crux/keys [event-type] :as event-opts} consumer]
    (case event-type
      :crux/indexed-tx
      (bus/listen bus
                  (assoc event-opts :crux/event-type ::tx/indexed-tx)
                  (fn [{:keys [::tx/submitted-tx ::txe/tx-events] :as ev}]
                    (.accept ^Consumer consumer
                             (merge {:crux/event-type :crux/indexed-tx}
                                    (select-keys ev [:committed?])
                                    (select-keys submitted-tx [::tx/tx-time ::tx/tx-id])
                                    (when (:with-tx-ops? event-opts)
                                      {:crux/tx-ops (txc/tx-events->tx-ops document-store tx-events)})))))))

  (latestCompletedTx [this]
    (db/latest-completed-tx indexer))

  (latestSubmittedTx [this]
    (db/latest-submitted-tx tx-log))

  ICruxAsyncIngestAPI
  (submitTxAsync [this tx-ops]
    (cio/with-read-lock lock
      (ensure-node-open this)
      (let [conformed-tx-ops (mapv txc/conform-tx-op tx-ops)]
        (db/submit-docs document-store (into {} (mapcat :docs) conformed-tx-ops))
        (db/submit-tx tx-log (mapv txc/->tx-event conformed-tx-ops)))))

  backup/INodeBackup
  (write-checkpoint [this {:keys [crux.backup/checkpoint-directory] :as opts}]
    (cio/with-read-lock lock
      (ensure-node-open this)
      (kv/backup kv-store (io/file checkpoint-directory "kv-store"))

      (when (satisfies? backup/INodeBackup tx-log)
        (backup/write-checkpoint tx-log opts))))

  Closeable
  (close [_]
    (cio/with-write-lock lock
      (when (and (not @closed?) close-fn) (close-fn))
      (reset! closed? true))))

(defmethod print-method CruxNode [node ^PrintWriter w] (.write w "#<CruxNode>"))

(def ^:private node-component
  {:start-fn (fn [{::keys [indexer tx-consumer document-store object-store tx-log kv-store bus query-engine]} node-opts]
               (map->CruxNode {:options node-opts
                               :kv-store kv-store
                               :tx-log tx-log
                               :indexer indexer
                               :tx-consumer tx-consumer
                               :document-store document-store
                               :object-store object-store
                               :bus bus
                               :query-engine query-engine
                               :closed? (atom false)
                               :lock (StampedLock.)}))
   :deps #{::indexer ::tx-consumer ::kv-store ::bus ::document-store ::object-store ::tx-log ::query-engine}
   :args {:crux.tx-log/await-tx-timeout {:doc "Default timeout for awaiting transactions being indexed."
                                         :default nil
                                         :crux.config/type :crux.config/duration}}})

(def base-topology
  {::kv-store 'crux.kv.memdb/kv
   ::object-store 'crux.object-store/kv-object-store
   ::indexer 'crux.kv-indexer/kv-indexer
   ::tx-consumer 'crux.tx/tx-consumer
   ::bus 'crux.bus/bus
   ::query-engine 'crux.query/query-engine
   ::node 'crux.node/node-component})

(defn start ^crux.api.ICruxAPI [options]
  (let [[{::keys [node] :as components} close-fn] (topo/start-topology options)]
    (-> node
        (assoc :status-fn (fn []
                            (merge crux-version
                                   (into {} (mapcat status/status-map) (vals (dissoc components ::node)))))
               :close-fn close-fn)
        (vary-meta assoc ::topology components))))
