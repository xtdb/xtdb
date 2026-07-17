(ns xtdb.log
  (:require [clojure.tools.logging :as log]
            [integrant.core :as ig]
            [xtdb.api :as xt]
            [xtdb.db-catalog :as db]
            [xtdb.error :as err]
            [xtdb.node :as xtn]
            [xtdb.table :as table]
            [xtdb.util :as util])
  (:import (java.time Duration Instant)
           (java.util HashMap List)
           java.util.concurrent.TimeUnit
           (xtdb.api TransactionKey Xtdb$Config)
           (xtdb.api.log Log Log$Factory)
           (xtdb.database Database DatabaseStorage Database$Config)
           xtdb.api.TableRef
           (xtdb.util MsgIdUtil)))

(set! *unchecked-math* :warn-on-boxed)

(def ^:private forbidden-schemas #{"xt" "information_schema" "pg_catalog"})

(defn forbidden-table? [^TableRef table]
  (contains? forbidden-schemas (.getSchemaName table)))

(defn forbidden-table-ex [table]
  (err/incorrect :xtdb/forbidden-table (format "Cannot write to table: %s" (table/ref->schema+table table))
                 {:table table}))


(defmulti ->log-cluster-factory
  (fn [k _opts]
    (when-let [ns (namespace k)]
      (doseq [k [(symbol ns)
                 (symbol (str ns "." (name k)))]]

        (try
          (require k)
          (catch Throwable _))))
    k)
  :default ::default)

(defmethod ->log-cluster-factory ::default [k _]
  (throw (err/incorrect :xtdb/unknown-log-cluster-type (format "Unknown log cluster type: %s" k)
                        {:log-cluster-type k})))

(defmethod ->log-cluster-factory :kafka [_ opts] (->log-cluster-factory :xtdb.kafka/cluster opts))

(defmethod xtn/apply-config! ::clusters [^Xtdb$Config config _ clusters]
  (doseq [[cluster-alias [tag opts]] clusters]
    (.logCluster config (str (symbol cluster-alias)) (->log-cluster-factory tag opts)))
  config)

(defmulti ->log-factory
  (fn [k _opts]
    (when-let [ns (namespace k)]
      (doseq [k [(symbol ns)
                 (symbol (str ns "." (name k)))]]

        (try
          (require k)
          (catch Throwable _))))
    k)
  :default ::default)

(defmethod ->log-factory ::default [k _]
  (throw (err/incorrect :xtdb/unknown-log-type (format "Unknown log type: %s" k)
                        {:log-type k})))

(defmethod ->log-factory ::in-memory [_ {:keys [instant-src epoch]}]
  (cond-> (Log/getInMemoryLog)
    instant-src (.instantSource instant-src)
    epoch (.epoch epoch)))

(defmethod ->log-factory ::local [_ {:keys [path instant-src epoch instant-source-for-non-tx-msgs?]}]
  (cond-> (Log/localLog (util/->path path))
    instant-src (.instantSource instant-src)
    epoch (.epoch epoch)
    instant-source-for-non-tx-msgs? (.useInstantSourceForNonTx)))

(defmethod ->log-factory :in-memory [_ opts] (->log-factory ::in-memory opts))
(defmethod ->log-factory :local [_ opts] (->log-factory ::local opts))
(defmethod ->log-factory :kafka [_ opts] (->log-factory :xtdb.kafka/kafka opts))

(defmethod xtn/apply-config! :xtdb/log [^Xtdb$Config config _ [tag opts]]
  (.log config (->log-factory tag opts)))

(defn await-tx [^Database db, ^long tx-id, ^Duration timeout]
  (.awaitTxBlocking db tx-id timeout))

(defn await-source [^Database db, ^long msg-id, ^Duration timeout]
  (.awaitSourceBlocking db msg-id timeout))

(defn sync-db [^Database db, ^Duration timeout]
  (let [msg-id (.getLatestSubmittedMsgId (.getSourceLog db))]
    (await-source db msg-id timeout)))

(defn await-node [node token timeout]
  (.awaitAll (db/<-node node) token timeout))

(defn sync-node [node timeout]
  (.syncAll (db/<-node node) timeout))

(defn send-flush-block-msg! [^Database db]
  (.sendFlushBlockMessage db))
