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
            [crux.index :as idx]
            [crux.io :as cio]
            [crux.kv :as kv]
            crux.object-store
            [crux.query :as q]
            [crux.status :as status]
            [crux.topology :as topo]
            [crux.tx :as tx]
            [crux.bus :as bus])
  (:import (crux.api ICruxAPI ICruxAsyncIngestAPI NodeOutOfSyncException ITxLog)
           java.io.Closeable
           java.util.Date
           (java.util.concurrent Executors TimeoutException)
           java.util.concurrent.locks.StampedLock))

(s/check-asserts (if-let [check-asserts (System/getProperty "clojure.spec.compile-asserts")]
                   (Boolean/parseBoolean check-asserts)
                   true))

(def crux-version
  (when-let [pom-file (io/resource "META-INF/maven/juxt/crux-core/pom.properties")]
    (with-open [in (io/reader pom-file)]
      (let [{:strs [version revision]} (cio/load-properties in)]
        {:crux.version/version version
         :crux.version/revision revision}))))

(defn- ensure-node-open [{:keys [closed?]}]
  (when @closed?
    (throw (IllegalStateException. "Crux node is closed"))))

(defn- with-bitemp-inst-defaults [bitemp-inst ^ICruxAPI node]
  (db/->bitemp-inst (merge (.latestCompletedTx node)
                           {::db/valid-time (Date.)}
                           bitemp-inst)))

(defn- await-inst [{::tx/keys [tx-id tx-time] :as bitemp-inst} ^ICruxAPI node timeout]
  (when (or tx-id tx-time)
    (let [seen-tx (atom nil)]
      (or (cio/wait-while #(let [latest-completed-tx (.latestCompletedTx node)]
                             (reset! seen-tx latest-completed-tx)
                             (or (nil? latest-completed-tx)
                                 (and tx-id (pos? (compare tx-id (::tx/tx-id latest-completed-tx))))
                                 (and tx-time (pos? (compare tx-time (::tx/tx-time latest-completed-tx))))))
                          timeout)
          (throw (TimeoutException. (str "Timed out waiting for: " (cio/pr-edn-str bitemp-inst)
                                         " index has: " (cio/pr-edn-str @seen-tx)))))

      @seen-tx)))

(defrecord CruxNode [kv-store tx-log document-store indexer object-store bus
                     options close-fn status-fn closed? ^StampedLock lock]
  ICruxAPI
  (at [this] (.at this nil))
  (at [this bitemp-inst] (.at this bitemp-inst nil))
  (at [this bitemp-inst timeout]
    (cio/with-read-lock lock
      (ensure-node-open this)

      (let [bitemp-inst (-> bitemp-inst
                            (with-bitemp-inst-defaults this)
                            (doto (await-inst this (or timeout (:crux.tx-log/await-tx-timeout options)))))]
        (q/->db {:kv-store kv-store
                 :object-store object-store
                 :bus bus
                 :bitemp-inst bitemp-inst}))))


  (openAt [this] (.openAt this nil))
  (openAt [this bitemp-inst] (.openAt this bitemp-inst nil))
  (openAt [this bitemp-inst timeout]
    (cio/with-read-lock lock
      (ensure-node-open this)

      (let [bitemp-inst (-> bitemp-inst
                            (with-bitemp-inst-defaults this)
                            (doto (await-inst this (or timeout (:crux.tx-log/await-tx-timeout options)))))]
        (q/->snapshot {:kv-store kv-store
                       :object-store object-store
                       :bus bus
                       :bitemp-inst bitemp-inst}))))

  ;; HACK db for backward compatibility
  (db [this] (.db this nil nil))
  (db [this valid-time] (.db this valid-time nil))

  (db [this valid-time tx-time]
    (cio/with-read-lock lock
      (ensure-node-open this)
      (let [latest-tx-time (:crux.tx/tx-time (.latestCompletedTx this))
            _ (when (and tx-time (or (nil? latest-tx-time) (pos? (compare tx-time latest-tx-time))))
                (throw (NodeOutOfSyncException. (format "node hasn't indexed the requested transaction: requested: %s, available: %s"
                                                        tx-time latest-tx-time)
                                                tx-time latest-tx-time)))
            tx-time (or tx-time latest-tx-time)
            valid-time (or valid-time (Date.))]

        (.at this (db/->bitemp-inst {::db/valid-time valid-time
                                     ::tx/tx-time tx-time})))))

  (document [this content-hash]
    (cio/with-read-lock lock
      (ensure-node-open this)
      (with-open [snapshot (kv/new-snapshot kv-store)]
        (-> (db/get-single-object object-store snapshot (c/new-id content-hash))
            (idx/keep-non-evicted-doc)))))

  (documents [this content-hash-set]
    (cio/with-read-lock lock
      (ensure-node-open this)
      (with-open [snapshot (kv/new-snapshot kv-store)]
        (->> (db/get-objects object-store snapshot (map c/new-id content-hash-set))
             (into {} (remove (comp idx/evicted-doc? val)))))))

  (history [this eid]
    (cio/with-read-lock lock
      (ensure-node-open this)
      (with-open [snapshot (kv/new-snapshot kv-store)]
        (mapv c/entity-tx->edn (idx/entity-history snapshot eid)))))

  (historyRange [this eid valid-time-start transaction-time-start valid-time-end transaction-time-end]
    (cio/with-read-lock lock
      (ensure-node-open this)
      (with-open [snapshot (kv/new-snapshot kv-store)]
        (->> (idx/entity-history-range snapshot eid valid-time-start transaction-time-start valid-time-end transaction-time-end)
             (mapv c/entity-tx->edn)
             (sort-by (juxt :crux.db/valid-time :crux.tx/tx-time))))))

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
      (idx/read-meta kv-store :crux.kv/stats)))

  (submitTx [this tx-ops]
    (cio/with-read-lock lock
      (ensure-node-open this)
      (db/submit-docs document-store (tx/tx-ops->id-and-docs tx-ops))
      (db/->bitemp-inst @(db/submit-tx tx-log tx-ops))))

  (hasTxCommitted [this {:keys [::tx/tx-id ::tx/tx-time] :as submitted-tx}]
    (cio/with-read-lock lock
      (ensure-node-open this)
      (.at this submitted-tx)
      (nil? (kv/get-value (kv/new-snapshot kv-store) (c/encode-failed-tx-id-key-to nil tx-id)))))

  (openTxLog ^ITxLog [this from-tx-id with-ops?]
    (cio/with-read-lock lock
      (ensure-node-open this)
      (let [tx-log-iterator (db/open-tx-log tx-log from-tx-id)
            snapshot (kv/new-snapshot kv-store)
            tx-log (-> (iterator-seq tx-log-iterator)
                       (->> (filter
                             #(nil?
                               (kv/get-value snapshot
                                             (c/encode-failed-tx-id-key-to nil (:crux.tx/tx-id %))))))
                       (cond->> with-ops? (map (fn [{:keys [crux.tx/tx-id
                                                            crux.tx.event/tx-events] :as tx-log-entry}]
                                                 (-> tx-log-entry
                                                     (dissoc :crux.tx.event/tx-events)
                                                     (assoc :crux.api/tx-ops
                                                            (->> tx-events
                                                                 (mapv #(tx/tx-event->tx-op % snapshot object-store)))))))))]

        (db/->closeable-tx-log-iterator (fn []
                                          (.close snapshot)
                                          (.close tx-log-iterator))
                                        tx-log))))

  (latestCompletedTx [this]
    (db/->bitemp-inst (db/read-index-meta indexer ::tx/latest-completed-tx)))

  (latestSubmittedTx [this]
    (db/->bitemp-inst (db/latest-submitted-tx tx-log)))

  (sync [this timeout]
    (when-let [tx (db/latest-submitted-tx tx-log)]
      (->> (await-inst tx this (or timeout (:crux.tx-log/await-tx-timeout options)))
           ::tx/tx-time)))

  (awaitTxTime [this tx-time timeout]
    (cio/with-read-lock lock
      (ensure-node-open this)
      (-> (await-inst {::tx/tx-time tx-time} this (or timeout (:crux.tx-log/await-tx-timeout options)))
          ::tx/tx-time)))

  (awaitTx [this submitted-tx timeout]
    (cio/with-read-lock lock
      (ensure-node-open this)
      (await-inst submitted-tx this (or timeout (:crux.tx-log/await-tx-timeout options)))))

  ICruxAsyncIngestAPI
  (submitTxAsync [this tx-ops]
    (cio/with-read-lock lock
      (ensure-node-open this)
      (db/submit-docs document-store (tx/tx-ops->id-and-docs tx-ops))
      (db/submit-tx tx-log tx-ops)))

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

(def ^:private node-component
  {:start-fn (fn [{::keys [indexer document-store object-store tx-log kv-store bus]} node-opts]
               (map->CruxNode {:options node-opts
                               :kv-store kv-store
                               :tx-log tx-log
                               :indexer indexer
                               :document-store document-store
                               :object-store object-store
                               :bus bus
                               :closed? (atom false)
                               :lock (StampedLock.)}))
   :deps #{::indexer ::kv-store ::bus ::document-store ::object-store ::tx-log}
   :args {:crux.tx-log/await-tx-timeout {:doc "Default timeout for awaiting transactions being indexed."
                                         :default nil
                                         :crux.config/type :crux.config/duration}}})

(def base-topology
  {::kv-store 'crux.kv.memdb/kv
   ::object-store 'crux.object-store/kv-object-store
   ::indexer 'crux.tx/kv-indexer
   ::bus 'crux.bus/bus
   ::node 'crux.node/node-component})

(defn start ^crux.api.ICruxAPI [options]
  (let [[{::keys [node] :as components} close-fn] (topo/start-topology options)]
    (-> node
        (assoc :status-fn (fn []
                            (merge crux-version
                                   (into {} (mapcat status/status-map) (vals (dissoc components ::node)))))
               :close-fn close-fn)
        (vary-meta assoc ::topology components))))
