(ns xtdb.tx-sink 
  (:require [clojure.string :as str]
            [integrant.core :as ig]
            [xtdb.log :as log]
            [xtdb.node :as xtn]
            [xtdb.util :as util])
  (:import (java.lang AutoCloseable)
           (xtdb.api TxSinkConfig Xtdb$Config)
           (xtdb.api.log Log$Message$Tx)
           (xtdb.indexer Indexer$TxSink LiveIndex$Tx)
           (xtdb.table TableRef)))

(defn read-table-rows [^TableRef table ^LiveIndex$Tx live-idx-tx]
  (let [table-name (str/join "." [(.getDbName table)
                                  (.getSchemaName table)
                                  (.getTableName table)])
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
    :edn #(-> % pr-str (.getBytes "UTF-8"))
    :json (requiring-resolve 'jsonista.core/write-value-as-bytes)))

(defmethod xtn/apply-config! :tx-sink [^Xtdb$Config config _ {:keys [output-log format]}]
  (.txSink config
           (cond-> (TxSinkConfig.)
             (some? output-log) (.outputLog (log/->log-factory (first output-log) (second output-log)))
             (some? format) (.format (str (symbol format))))))

(defmethod ig/expand-key :xtdb/tx-sink [k {:keys [base tx-sink-conf]}]
  {k {:base base
      :tx-sink-conf tx-sink-conf}})

(defmethod ig/init-key :xtdb/tx-sink [_ {:keys [^TxSinkConfig tx-sink-conf]
                                         {:keys [log-clusters]} :base}]
  (when tx-sink-conf
    (let [encode-as-bytes (->encode-fn (keyword (.getFormat tx-sink-conf)))
          log (.openLog (.getOutputLog tx-sink-conf) log-clusters)]
      (reify
        Indexer$TxSink
        (onCommit [_ _tx-key live-idx-tx]
          (util/with-open [live-idx-snap (.openSnapshot live-idx-tx)]
            (doseq [table (.getLiveTables live-idx-snap)]
              (doseq [row (read-table-rows table live-idx-tx)]
                (->> row
                     encode-as-bytes
                     Log$Message$Tx.
                     (.appendMessage log))))))

        AutoCloseable
        (close [_]
          (util/close log))))))

(defmethod ig/halt-key! :xtdb/tx-sink [_ ^AutoCloseable tx-sink]
  (util/close tx-sink))
