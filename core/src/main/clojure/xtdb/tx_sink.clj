(ns xtdb.tx-sink 
  (:require [integrant.core :as ig]
            [xtdb.log :as log]
            [xtdb.node :as xtn]
            xtdb.node.impl
            xtdb.serde
            [xtdb.util :as util]
            [xtdb.time :as time])
  (:import (xtdb.api TxSinkConfig Xtdb Xtdb$Config)
           (xtdb.api.log Log Log$Message$Tx)
           (xtdb.indexer Indexer$TxSink LiveIndex$Tx)
           (xtdb.table TableRef)))

(defn read-table-rows [^TableRef table ^LiveIndex$Tx live-idx-tx]
  (let [live-table (.liveTable live-idx-tx table)
        start-pos (.getStartPos live-table)
        live-relation (.getLiveRelation live-table)
        pos (.getRowCount live-relation)
        iid-vec (.vectorFor live-relation "_iid")
        valid-from-vec (.vectorFor live-relation "_valid_from")
        valid-to-vec (.vectorFor live-relation "_valid_to")
        op-vec (.vectorFor live-relation "op")
        put-vec (.vectorFor op-vec "put")]
    {:db (.getDbName table)
     :schema (.getSchemaName table)
     :table (.getTableName table)
     :ops (into []
            (for [i (range start-pos pos)]
              (let [leg (.getLeg op-vec i)]
                (cond-> {:op (keyword leg)
                         :iid (.getObject iid-vec i)
                         :valid-from (time/->instant (.getObject valid-from-vec i))
                         :valid-to (when-not (= Long/MAX_VALUE (.getLong valid-to-vec i))
                                     (time/->instant (.getObject valid-to-vec i)))}
                  (= leg "put") (assoc :doc (.getObject put-vec i))))))}))

(defn ->encode-fn [fmt]
  (case fmt
    :transit+json xtdb.serde/write-transit))

(defmethod xtn/apply-config! :xtdb/tx-sink [^Xtdb$Config config _ {:keys [output-log format enable db-name]}]
  (.txSink config
           (cond-> (TxSinkConfig.)
             (some? enable) (.enable enable)
             (some? output-log) (.outputLog (log/->log-factory (first output-log) (second output-log)))
             (some? db-name) (.dbName db-name)
             (some? format) (.format (str (symbol format))))))

(defmethod ig/expand-key ::for-db [k {:keys [base ^TxSinkConfig tx-sink-conf db-name]}]
  {k {:tx-sink-conf tx-sink-conf
      :output-log (ig/ref ::output-log)
      :db-name db-name}
   ::output-log {:tx-sink-conf tx-sink-conf
                 :base base
                 :db-name db-name}})

(defrecord TxSink [^Log output-log encode-as-bytes db-name]
  Indexer$TxSink
  (onCommit [_ tx-key live-idx-tx]
    (util/with-open [live-idx-snap (.openSnapshot live-idx-tx)]
      (let [live-tables (.getLiveTables live-idx-snap)]
        (.appendMessage output-log
                        (-> {:transaction {:id tx-key}
                             :system-time (let [live-table (.liveTable live-idx-tx (first live-tables))
                                                start-pos (.getStartPos live-table)
                                                live-relation (.getLiveRelation live-table)
                                                system-from-vec (.vectorFor live-relation "_system_from")]
                                            (time/->instant (.getObject system-from-vec start-pos)))
                             :source {;:version "1.0.0" ;; TODO
                                      :db db-name}
                             :tables (->> live-tables
                                          (map #(read-table-rows % live-idx-tx))
                                          (into []))}
                            encode-as-bytes
                            Log$Message$Tx.))))))

(defmethod ig/init-key ::for-db [_ {:keys [^TxSinkConfig tx-sink-conf output-log db-name]}]
  (when (and tx-sink-conf
             (.getEnable tx-sink-conf)
             (= db-name (.getDbName tx-sink-conf)))
    (map->TxSink {:output-log output-log
                  :encode-as-bytes (->encode-fn (keyword (.getFormat tx-sink-conf)))
                  :db-name db-name})))

(defmethod ig/init-key ::output-log [_ {:keys [^TxSinkConfig tx-sink-conf db-name]
                                        {:keys [log-clusters]} :base}]
  (when (and tx-sink-conf
             (.getEnable tx-sink-conf)
             (= db-name (.getDbName tx-sink-conf)))
    (.openLog (.getOutputLog tx-sink-conf) log-clusters)))

(defmethod ig/halt-key! ::output-log [_ output-log]
  (util/close output-log))

(defn open! ^Xtdb [node-opts]
  (let [config (doto (xtn/->config node-opts)
                 (-> (.getCompactor) (.threads 0))
                 (.setServer nil)
                 (some-> (.getTxSink) (.enable true)))]
    (.open config)))
