(ns xtdb.tx-sink
  (:require [integrant.core :as ig]
            [xtdb.log :as log]
            [xtdb.node :as xtn]
            xtdb.node.impl
            [xtdb.serde :as serde]
            [xtdb.time :as time]
            [xtdb.util :as util])
  (:import (org.apache.arrow.memory BufferAllocator)
           (xtdb.api TxSinkConfig Xtdb Xtdb$Config)
           (xtdb.api.log Log Log$Message Log$Message$Tx)
           (xtdb.arrow Relation RelationReader)
           (xtdb.catalog BlockCatalog)
           (xtdb.indexer Indexer$TxSink LiveIndex$Tx)
           (xtdb.storage BufferPool)
           (xtdb.table TableRef)
           (xtdb.trie Trie)))

(defn read-relation-rows
  ([rel] (read-relation-rows rel 0))
  ([^RelationReader rel start]
   (let [row-count (.getRowCount rel)
         iid-vec (.vectorFor rel "_iid")
         system-from-vec (.vectorFor rel "_system_from")
         valid-from-vec (.vectorFor rel "_valid_from")
         valid-to-vec (.vectorFor rel "_valid_to")
         op-vec (.vectorFor rel "op")
         put-vec (.vectorFor op-vec "put")]
     (into []
           (for [i (range start row-count)]
             (let [leg (.getLeg op-vec i)]
               (cond-> {:iid (.getObject iid-vec i)
                        :valid-from (time/->instant (.getObject valid-from-vec i))
                        :system-from (time/->instant (.getObject system-from-vec i))
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

(defn- read-l0-data-file
  [^BufferAllocator allocator ^BufferPool buffer-pool data-path]
  (let [footer (.getFooter buffer-pool data-path)
        schema (.getSchema footer)
        batch-count (count (.getRecordBatches footer))]
    (->> (range batch-count)
         (mapcat (fn [batch-idx]
                   (with-open [rb (.getRecordBatchSync buffer-pool data-path batch-idx)
                               rel (Relation/fromRecordBatch allocator schema rb)]
                     (read-relation-rows rel))))
         reverse
         (into []))))

(defn read-l0-events
  [^BufferAllocator allocator ^BufferPool buffer-pool table-entries]
  (for [{:keys [^TableRef table trie-key]} table-entries
        event (read-l0-data-file allocator buffer-pool (Trie/dataFilePath table trie-key))]
    {:table table :event event}))

(defn group-events-by-system-time
  [events]
  (->> (group-by #(get-in % [:event :system-from]) events)
       (map (fn [[k entries]]
              [k (-> (group-by :table entries)
                     (update-vals #(mapv :event %)))]))
       (sort-by first)))

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
      :block-cat (ig/ref :xtdb/block-catalog)
      :db-name db-name}
   ::output-log {:tx-sink-conf tx-sink-conf
                 :base base
                 :db-name db-name}})

(defn gt [a b] (pos? (compare a b)))

(defrecord TxSink [^Log output-log encode-fn ^BlockCatalog block-cat db-name last-tx-key]
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
                               encode-fn
                               Log$Message$Tx.)))))))

(defmethod ig/init-key ::for-db [_ {:keys [^TxSinkConfig tx-sink-conf ^Log output-log block-cat db-name]}]
  (when (and tx-sink-conf
             (.getEnable tx-sink-conf)
             (= db-name (.getDbName tx-sink-conf)))
    (let [encode-fn (->encode-fn (keyword (.getFormat tx-sink-conf)))
          last-message (try
                         (let [decode-record (->decode-fn (keyword (.getFormat tx-sink-conf)))]
                           (->> (.readLastMessage output-log)
                                Log$Message/.encode
                                decode-record))
                         (catch Exception _ nil))]
      (map->TxSink {:output-log output-log
                    :encode-fn encode-fn
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
