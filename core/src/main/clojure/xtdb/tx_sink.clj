(ns xtdb.tx-sink 
  (:require [clojure.string :as str]
            [integrant.core :as ig]
            [xtdb.log :as log]
            [xtdb.node :as xtn]
            [xtdb.util :as util]
            xtdb.serde)
  (:import (xtdb.api TxSinkConfig TxSinkConfig$TableFilter Xtdb$Config)
           (xtdb.api.log Log Log$Message$Tx)
           (xtdb.indexer Indexer$TxSink LiveIndex$Tx)
           (xtdb.table TableRef)))

(defn- table-name [^TableRef table-ref]
  (str/join "." [(.getDbName table-ref)
                 (.getSchemaName table-ref)
                 (.getTableName table-ref)]))

(defn read-table-rows [table ^LiveIndex$Tx live-idx-tx]
  (let [table-name (table-name table)
        live-table (.liveTable live-idx-tx table)
        start-pos (.getStartPos live-table)
        live-relation (.getLiveRelation live-table)
        pos (.getRowCount live-relation)
        iid-vec (.vectorFor live-relation "_iid")
        system-from-vec (.vectorFor live-relation "_system_from")
        valid-from-vec (.vectorFor live-relation "_valid_from")
        valid-to-vec (.vectorFor live-relation "_valid_to")
        op-vec (.vectorFor live-relation "op")
        put-vec (.vectorFor op-vec "put")]
    (doall
      (for [i (range start-pos pos)]
        (let [leg (.getLeg op-vec i)
              put? (= leg "put")
              data (when put? (.getObject put-vec i))]
          (cond-> {:table table-name
                   :iid (.getObject iid-vec i)
                   :system-from (.getObject system-from-vec i)
                   :valid-from (.getObject valid-from-vec i)
                   :valid-to (.getObject valid-to-vec i)
                   :op (keyword leg)}
            put? (assoc :data data)))))))

(defn ->encode-fn [fmt]
  (case fmt
    :transit+json xtdb.serde/write-transit))

(defmethod xtn/apply-config! :tx-sink [^Xtdb$Config config _ {:keys [output-log format table-filter enable]}]
  (let [table-filter (when-let [{:keys [include exclude]} table-filter]
                       (TxSinkConfig$TableFilter.
                         (if (some? include) (set include) #{})
                         (if (some? exclude) (set exclude) #{})))]
    (.txSink config
             (cond-> (TxSinkConfig.)
               (some? enable) (.enable enable)
               (some? output-log) (.outputLog (log/->log-factory (first output-log) (second output-log)))
               (some? format) (.format (str (symbol format)))
               (some? table-filter) (.tableFilter table-filter)))))

(defmethod ig/expand-key :xtdb/tx-sink [k {:keys [base ^TxSinkConfig tx-sink-conf]}]
  {k {:tx-sink-conf tx-sink-conf
      :output-log (ig/ref ::output-log)}
   ::output-log {:tx-sink-conf tx-sink-conf
                 :base base}})

(defmethod ig/init-key :xtdb/tx-sink [_ {:keys [^TxSinkConfig tx-sink-conf ^Log output-log]}]
  (when (and tx-sink-conf (.getEnable tx-sink-conf))
    (let [encode-as-bytes (->encode-fn (keyword (.getFormat tx-sink-conf)))
          table-filter (.getTableFilter tx-sink-conf)]
      (reify Indexer$TxSink
        (onCommit [_ _tx-key live-idx-tx]
          (util/with-open [live-idx-snap (.openSnapshot live-idx-tx)]
            (doseq [^TableRef table (.getLiveTables live-idx-snap)
                    :when (.test table-filter (table-name table))]
              (doseq [row (read-table-rows table live-idx-tx)]
                (->> row
                     encode-as-bytes
                     Log$Message$Tx.
                     (.appendMessage output-log))))))))))

(defmethod ig/init-key ::output-log [_ {:keys [^TxSinkConfig tx-sink-conf]
                                        {:keys [log-clusters]} :base}]
  (when (and tx-sink-conf (.getEnable tx-sink-conf))
    (.openLog (.getOutputLog tx-sink-conf) log-clusters)))

(defmethod ig/halt-key! ::output-log [_ output-log]
  (util/close output-log))
