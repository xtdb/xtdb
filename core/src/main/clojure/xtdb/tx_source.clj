(ns xtdb.tx-source
  (:require [clojure.tools.logging :as log]
            [integrant.core :as ig]
            [xtdb.error :as err]
            [xtdb.log :as xt-log]
            [xtdb.node :as xtn]
            xtdb.node.impl
            [xtdb.serde :as serde]
            [xtdb.time :as time]
            [xtdb.trie-catalog :as trie-cat]
            [xtdb.util :as util])
  (:import (org.apache.arrow.memory BufferAllocator)
           (xtdb TaggedValue)
           (xtdb.api TxSourceConfig Xtdb Xtdb$Config)
           (xtdb.api.log Log Log$Message Log$Message$Tx)
           (xtdb.arrow Relation RelationReader)
           (xtdb.catalog BlockCatalog TableCatalog)
           (xtdb.indexer Indexer$TxSource)
           (xtdb.storage BufferPool)
           (xtdb.table TableRef)
           (xtdb.trie Trie TrieCatalog)))

(defn read-relation-rows [^RelationReader rel]
  (->> (.getAsMaps rel)
       (map (fn [{:xt/keys [iid valid-from system-from valid-to]
                  :keys [^TaggedValue op]}]
              (let [op-tag (.getTag op)]
                (cond-> {:iid iid
                         :valid-from (time/->instant valid-from)
                         :system-from (time/->instant system-from)
                         :valid-to (let [vt (time/->instant valid-to)]
                                     (when-not (= vt time/end-of-time)
                                       vt))
                         :op op-tag}
                  (= op-tag :put) (assoc :doc (.getValue op))))))
       (into [])))

(defn read-table-rows-from-ipc [^BufferAllocator allocator ^String db-name ^String schema-and-table ^bytes ipc-bytes]
  (let [table-ref (TableRef/parse db-name schema-and-table)]
    (with-open [loader (Relation/streamLoader allocator ipc-bytes)
                rel (Relation. allocator (.getSchema loader))]
      (.loadNextPage loader rel)
      {:db (.getDbName table-ref)
       :schema (.getSchemaName table-ref)
       :table (.getTableName table-ref)
       :ops (map #(dissoc % :system-from) (read-relation-rows rel))})))

(defn ->encode-fn [fmt]
  (case fmt
    :transit+json #(xtdb.serde/write-transit % :json)
    :transit+json-verbose #(xtdb.serde/write-transit % :json-verbose)
    :transit+msgpack #(xtdb.serde/write-transit % :msgpack)))

(defn ->decode-fn [fmt]
  (case fmt
    (:transit+json :transit+json-verbose) #(xtdb.serde/read-transit % :json)
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

(defn events->tx-output
  [system-time table->events db-name block-idx encode-fn]
  (let [; NOTE: We get the tx-key from the xt.txs table
        txs-events (or (get table->events #xt/table "xt/txs")
                       (throw (err/fault :xtdb.tx-source/missing-txs-table
                                         "Expected xt.txs table in transaction events"
                                         {:system-time system-time
                                          :tables (keys table->events)})))
        _ (when (not= 1 (count txs-events))
            (throw (err/fault :xtdb.tx-source/unexpected-txs-count
                              (format "Expected exactly 1 xt.txs event per transaction, got %d" (count txs-events))
                              {:system-time system-time
                               :count (count txs-events)})))
        tx-key (serde/->TxKey (-> txs-events first :doc :xt/id)
                              (time/->instant system-time))]
    {:tx-key tx-key
     :message (-> {:transaction {:id tx-key}
                   :system-time (time/->instant system-time)
                   :source {:db db-name
                            :block-idx block-idx}
                   :tables (->> table->events
                                (map (fn [[^TableRef table events]]
                                       {:db (.getDbName table)
                                        :schema (.getSchemaName table)
                                        :table (.getTableName table)
                                        :ops (map #(dissoc % :system-from) events)}))
                                (into []))}
                  encode-fn
                  Log$Message$Tx.)}))

(def ^:dynamic *after-block-hook* nil)

(defn gt [a b] (pos? (compare a b)))

(defn- output-block-txs!
  [^Log output-log grouped-txs db-name block-idx encode-fn last-key]
  (reduce (fn [last-key [system-time events]]
            (let [{tx-key' :tx-key :keys [message]} (events->tx-output system-time events db-name block-idx encode-fn)]
              (if (or (nil? last-key) (gt tx-key' last-key))
                (do
                  @(.appendMessage output-log message)
                  tx-key')
                last-key)))
          last-key
          grouped-txs))

(defn backfill-from-l0!
  [^BufferAllocator al ^BufferPool bp ^Log output-log db-name encode-fn ^TrieCatalog trie-cat last-message refresh!]
  (letfn [(fetch-new-blocks [last-block-idx]
            (refresh!)
            (->> (trie-cat/l0-blocks trie-cat)
                 (drop-while #(>= (or last-block-idx -1) (:block-idx %)))))]
    (log/info "Backfill starting")
    (loop [last-key (some-> last-message :transaction :id)
           ;; Decrement because the last block may not have been fully processed
           ;; (e.g. if the last message was from the live index before a flush)
           last-block-idx (some-> last-message :source :block-idx dec)]
      (if-let [blocks (seq (fetch-new-blocks last-block-idx))]
        (let [[new-last-key new-last-block-idx]
              (reduce (fn [[last-key _] {:keys [block-idx tables]}]
                        (log/debugf "Backfill processing block %d (%d tables)" block-idx (count tables))
                        (let [grouped-txs (->> tables (read-l0-events al bp) group-events-by-system-time)
                              new-last-key (output-block-txs! output-log grouped-txs db-name block-idx encode-fn last-key)]
                          (when *after-block-hook* (*after-block-hook* block-idx))
                          [new-last-key block-idx]))
                      [last-key last-block-idx]
                      blocks)]
          (recur new-last-key new-last-block-idx))
        last-key))))

(defmethod xtn/apply-config! :xtdb/tx-source [^Xtdb$Config config _ {:keys [output-log format enable db-name initial-scan]}]
  (.txSource config
             (cond-> (TxSourceConfig.)
               (some? enable) (.enable enable)
               (some? initial-scan) (.initialScan initial-scan)
               (some? output-log) (.outputLog (xt-log/->log-factory (first output-log) (second output-log)))
               (some? db-name) (.dbName db-name)
               (some? format) (.format (str (symbol format))))))

(defmethod ig/expand-key ::for-db [k {:keys [^TxSourceConfig tx-source-conf] :as opts}]
  {k (into {:output-log (ig/ref ::output-log)
            :block-cat (ig/ref :xtdb/block-catalog)
            :allocator (ig/ref :xtdb.db-catalog/allocator)
            :buffer-pool (ig/ref :xtdb/buffer-pool)
            :table-cat (ig/ref :xtdb/table-catalog)
            :trie-cat (ig/ref :xtdb/trie-catalog)}
           opts)
   ::output-log (select-keys opts [:tx-source-conf :base :db-name])})

(defrecord TxSource [^BufferAllocator allocator ^Log output-log encode-fn ^BlockCatalog block-cat db-name last-tx-key]
  Indexer$TxSource
  (onCommit [_ resolved-tx]
    (let [tx-key (serde/->TxKey (.getTxId resolved-tx)
                                (time/micros->instant (.getSystemTimeMicros resolved-tx)))]
      (when (or (nil? last-tx-key) (gt tx-key last-tx-key))
        (let [table-data (.getTableData resolved-tx)]
          @(.appendMessage output-log
                           (-> {:transaction {:id tx-key}
                                :system-time (.getSystemTime tx-key)
                                :source {:db db-name
                                         :block-idx (inc (or (.getCurrentBlockIndex block-cat) -1))}
                                :tables (->> table-data
                                             (map (fn [[schema-and-table ipc-bytes]]
                                                    (read-table-rows-from-ipc allocator db-name schema-and-table ipc-bytes)))
                                             (into []))}
                               encode-fn
                               Log$Message$Tx.)))))))

(defmethod ig/init-key ::for-db [_ {:keys [^TxSourceConfig tx-source-conf ^Log output-log
                                           ^BlockCatalog block-cat allocator buffer-pool
                                           ^TableCatalog table-cat
                                           ^TrieCatalog trie-cat db-name]}]
  (when (and tx-source-conf
             (.getEnable tx-source-conf)
             (= db-name (.getDbName tx-source-conf)))
    (let [encode-fn (->encode-fn (keyword (.getFormat tx-source-conf)))
          last-message (try
                         (let [decode-record (->decode-fn (keyword (.getFormat tx-source-conf)))]
                           (->> (.readLastMessage output-log)
                                Log$Message/.encode
                                decode-record))
                         (catch Exception _ nil))
          refresh! (fn []
                     (.refresh block-cat (BlockCatalog/getLatestBlock buffer-pool))
                     (let [block-idx (or (.getCurrentBlockIndex block-cat) -1)]
                       (.refresh table-cat block-idx)
                       (.refresh trie-cat block-idx)))
          last-tx-key (when (or last-message (.getInitialScan tx-source-conf))
                        (backfill-from-l0! allocator buffer-pool output-log
                                           db-name encode-fn trie-cat
                                           last-message refresh!))]
      (log/info "Tx-source now processing live transactions")
      (map->TxSource {:allocator allocator
                      :output-log output-log
                      :encode-fn encode-fn
                      :block-cat block-cat
                      :db-name db-name
                      :last-tx-key last-tx-key}))))

(defmethod ig/init-key ::output-log [_ {:keys [^TxSourceConfig tx-source-conf db-name]
                                        {:keys [log-clusters]} :base}]
  (when (and tx-source-conf
             (.getEnable tx-source-conf)
             (= db-name (.getDbName tx-source-conf)))
    (.openLog (.getOutputLog tx-source-conf) log-clusters)))

(defmethod ig/halt-key! ::output-log [_ output-log]
  (util/close output-log))

(defn open! ^Xtdb [node-opts]
  (let [config (doto (xtn/->config node-opts)
                 (-> (.getCompactor) (.threads 0))
                 (.setServer nil)
                 (.setFlightSql nil)
                 (some-> (.getTxSource) (.enable true))
                 (.readOnlyDatabases true))]
    (.open config)))
