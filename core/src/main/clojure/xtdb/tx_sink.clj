(ns xtdb.tx-sink
  (:require [integrant.core :as ig]
            [xtdb.log :as log]
            [xtdb.node :as xtn]
            xtdb.node.impl
            xtdb.serde
            [xtdb.util :as util]
            [xtdb.time :as time])
  (:import (xtdb.api TxSinkConfig Xtdb Xtdb$Config)
           (xtdb.api.log Log Log$Message Log$Message$Tx)
           (xtdb.arrow Relation)
           (xtdb.catalog BlockCatalog)
           (xtdb.indexer Indexer$TxSink LiveIndex$Tx)
           (xtdb.table TableRef)))

(defn- read-relation-rows
  "Read rows from a relation into a vector of maps.
   Auto-detects whether _system_from column exists."
  ([rel] (read-relation-rows rel 0))
  ([^Relation rel start]
   (let [row-count (.getRowCount rel)
         start (long start)
         iid-vec (.vectorFor rel "_iid")
         system-from-vec (.vectorFor rel "_system_from")
         valid-from-vec (.vectorFor rel "_valid_from")
         valid-to-vec (.vectorFor rel "_valid_to")
         op-vec (.vectorFor rel "op")
         put-vec (when op-vec (.vectorFor op-vec "put"))]
     (into []
           (for [i (range start row-count)]
             (let [leg (when op-vec (.getLeg op-vec i))]
               (cond-> {:iid (.getObject iid-vec i)
                        :valid-from (time/->instant (.getObject valid-from-vec i))
                        :system-from (.getObject system-from-vec i)
                        :valid-to (let [vt (.getLong valid-to-vec i)]
                                    (when-not (= Long/MAX_VALUE vt)
                                      (time/->instant (.getObject valid-to-vec i))))}
                 leg (assoc :op (keyword leg))
                 (and (= leg "put") put-vec) (assoc :doc (.getObject put-vec i)))))))))

(defn read-table-rows [^TableRef table ^LiveIndex$Tx live-idx-tx]
  (let [live-table (.liveTable live-idx-tx table)
        start-pos (.getStartPos live-table)
        live-relation (.getLiveRelation live-table)]
    {:db (.getDbName table)
     :schema (.getSchemaName table)
     :table (.getTableName table)
     :ops (read-relation-rows live-relation start-pos)}))

(defn ->encode-fn [fmt]
  (case fmt
    :transit+json #(xtdb.serde/write-transit % :json)
    :transit+msgpack #(xtdb.serde/write-transit % :msgpack)))

(defn ->decode-fn [fmt]
  (case fmt
    :transit+json #(xtdb.serde/read-transit % :json)
    :transit+msgpack #(xtdb.serde/read-transit % :msgpack)))

(defmethod xtn/apply-config! :xtdb/tx-sink [^Xtdb$Config config _ {:keys [output-log format enable db-name initial-scan]}]
  (.txSink config
           (cond-> (TxSinkConfig.)
             (some? enable) (.enable enable)
             (some? initial-scan) (.initialScan initial-scan)
             (some? output-log) (.outputLog (log/->log-factory (first output-log) (second output-log)))
             (some? db-name) (.dbName db-name)
             (some? format) (.format (str (symbol format))))))

(defmethod ig/expand-key ::for-db [k {:keys [base ^TxSinkConfig tx-sink-conf db-name]}]
  {k {:tx-sink-conf tx-sink-conf
      :output-log (ig/ref ::output-log)
      :block-cat (ig/ref :xtdb/block-catalog)
      :db-name db-name}
   ::output-log {:tx-sink-conf tx-sink-conf
                 :base base
                 :db-name db-name}})

(defn gt [a b] (pos? (compare a b)))

(defrecord TxSink [^Log output-log encode-as-bytes ^BlockCatalog block-cat db-name last-tx-key]
  Indexer$TxSink
  (onCommit [_ tx-key live-idx-tx]
    (when (or (nil? last-tx-key) (gt tx-key last-tx-key))
      (util/with-open [live-idx-snap (.openSnapshot live-idx-tx)]
        (let [live-tables (.getLiveTables live-idx-snap)]
          @(.appendMessage output-log
                           (-> {:transaction {:id tx-key}
                                :system-time (let [live-table (.liveTable live-idx-tx (first live-tables))
                                                   start-pos (.getStartPos live-table)
                                                   live-relation (.getLiveRelation live-table)
                                                   system-from-vec (.vectorFor live-relation "_system_from")]
                                               (time/->instant (.getObject system-from-vec start-pos)))
                                :source {;:version "1.0.0" ;; TODO
                                         :db db-name
                                         :block-idx (inc (or (.getCurrentBlockIndex block-cat) -1))}
                                :tables (->> live-tables
                                             (map #(read-table-rows % live-idx-tx))
                                             (into []))}
                               encode-as-bytes
                               Log$Message$Tx.)))))))

(defmethod ig/init-key ::for-db [_ {:keys [^TxSinkConfig tx-sink-conf ^Log output-log block-cat db-name]}]
  (when (and tx-sink-conf
             (.getEnable tx-sink-conf)
             (= db-name (.getDbName tx-sink-conf)))
    (let [last-message (try
                         (let [decode-record (->decode-fn (keyword (.getFormat tx-sink-conf)))]
                           (->> (.readLastMessage output-log)
                                Log$Message/.encode
                                decode-record))
                         (catch Exception _ nil))]
      (map->TxSink {:output-log output-log
                    :encode-as-bytes (->encode-fn (keyword (.getFormat tx-sink-conf)))
                    :block-cat block-cat
                    :db-name db-name
                    :last-tx-key (some-> last-message :transaction :id)}))))

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
                 (.setFlightSql nil)
                 (some-> (.getTxSink) (.enable true)))]
    (.open config)))
