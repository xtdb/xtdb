(ns xtdb.tx-sink 
  (:require [integrant.core :as ig]
            [xtdb.log :as log]
            [xtdb.node :as xtn]
            [xtdb.util :as util]
            xtdb.serde)
  (:import (xtdb.api TxSinkConfig Xtdb$Config)
           (xtdb.api.log Log Log$Message$Tx)
           (xtdb.indexer Indexer$TxSink LiveIndex$Tx)
           (xtdb.table TableRef)))

(defn read-table-rows [^TableRef table ^LiveIndex$Tx live-idx-tx]
  (let [live-table (.liveTable live-idx-tx table)
        start-pos (.getStartPos live-table)
        live-relation (.getLiveRelation live-table)
        pos (.getRowCount live-relation)
        iid-vec (.vectorFor live-relation "_iid")
        system-from-vec (.vectorFor live-relation "_system_from")
        valid-from-vec (.vectorFor live-relation "_valid_from")
        valid-to-vec (.vectorFor live-relation "_valid_to")
        op-vec (.vectorFor live-relation "op")
        put-vec (.vectorFor op-vec "put")]
    {:db (.getDbName table)
     :schema (.getSchemaName table)
     :table (.getTableName table)
     :ops (into []
            (for [i (range start-pos pos)]
              (let [leg (.getLeg op-vec i)
                    put? (= leg "put")
                    data (when put? (.getObject put-vec i))]
                (cond-> {:op (keyword leg)
                         :iid (.getObject iid-vec i)
                         :system-from (.getObject system-from-vec i)
                         :valid-from (.getObject valid-from-vec i)
                         :valid-to (when-not (= Long/MAX_VALUE (.getLong valid-to-vec i))
                                     (.getObject valid-to-vec i))}
                  put? (assoc :payload data)))))}))

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
      (->> {:transaction {:id tx-key}
            :source {;:version "1.0.0" ;; TODO
                     :db db-name}
            :tables (->> (.getLiveTables live-idx-snap)
                         (map #(read-table-rows % live-idx-tx))
                         (into []))}
           encode-as-bytes
           Log$Message$Tx.
           (.appendMessage output-log)))))

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
