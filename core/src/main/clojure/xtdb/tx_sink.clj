(ns xtdb.tx-sink
  (:require [clojure.tools.logging :as logging]
            [integrant.core :as ig]
            [xtdb.log :as log]
            [xtdb.node :as xtn]
            xtdb.node.impl
            [xtdb.serde :as serde]
            [xtdb.trie-catalog :as trie-cat]
            [xtdb.util :as util]
            [xtdb.time :as time])
  (:import (org.apache.arrow.memory BufferAllocator)
           (xtdb.api TxSinkConfig Xtdb Xtdb$Config)
           (xtdb.api.log Log Log$Message Log$Message$Tx)
           (xtdb.arrow Relation)
           (xtdb.catalog BlockCatalog)
           (xtdb.indexer Indexer$TxSink LiveIndex$Tx)
           (xtdb.storage BufferPool)
           (xtdb.table TableRef)
           (xtdb.trie Trie TrieCatalog)))

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

;; ============================================================================
;; L0 Initial Scan Functions
;; ============================================================================

(defn l0-tries-for-table
  "Get all L0 tries for a table (both :live and :garbage states).
   L0 files are never deleted, only marked as garbage after compaction."
  [trie-cat ^TableRef table]
  (let [{:keys [live garbage]} (-> (trie-cat/trie-state trie-cat table)
                                   (get-in [:tries [0 nil []]]))]
    (concat live garbage)))

(defn build-l0-index
  "Build an inverted index of block-idx -> [{:table TableRef, :trie-key String}].
   Returns a sorted map for natural iteration order.
   Asserts one trie-key per table per block-idx."
  [^TrieCatalog trie-cat]
  (->> (.getTables trie-cat)
       (mapcat (fn [table]
                 (->> (l0-tries-for-table trie-cat table)
                      (map (fn [{:keys [block-idx trie-key]}]
                             [block-idx {:table table :trie-key trie-key}])))))
       (group-by first)
       (map (fn [[block-idx pairs]]
              (let [entries (mapv second pairs)]
                ;; Assert one trie-key per table per block
                (assert (= (count entries) (count (distinct (map :table entries))))
                        (str "Multiple trie-keys for same table in block " block-idx))
                [block-idx entries])))
       (into (sorted-map))))

(defn read-l0-file-events
  "Read all events from an L0 data file.
   Returns a vector of event maps."
  [^BufferAllocator al ^BufferPool bp ^TableRef table ^String trie-key]
  (let [data-path (Trie/dataFilePath table trie-key)
        footer (.getFooter bp data-path)
        schema (.getSchema footer)
        batch-count (count (.getRecordBatches footer))]
    (with-open [rel (Relation. al schema)]
      (into []
            (mapcat (fn [batch-idx]
                      (with-open [rb (.getRecordBatchSync bp data-path batch-idx)]
                        (.load rel rb)
                        (read-relation-rows rel))))
            (range batch-count)))))

(defn read-l0-block
  "Read all events from L0 files for a given block.
   Returns a map of table -> events."
  [^BufferAllocator al ^BufferPool bp table-entries]
  (->> table-entries
       (map (fn [{:keys [^TableRef table trie-key]}]
              [table (read-l0-file-events al bp table trie-key)]))
       (into {})))

(defn- group-events-by-system-time
  "Group events by system-from time (which identifies transactions).
   Returns a sorted map of system-time -> {:table -> events}."
  [table->events]
  (let [all-events (for [[table events] table->events
                         event events]
                     (assoc event :_table table))]
    (->> all-events
         (group-by :system-from)
         (into (sorted-map)))))

(defn- gt [a b] (pos? (compare a b)))

(defn- events->tx-output
  "Convert events for a single transaction into the TxSink output format."
  [system-time table-events db-name block-idx encode-fn]
  (let [;; Find tx-id from xt.txs table
        txs-events (->> table-events
                        (filter (fn [event]
                                  (when-let [^TableRef table (:_table event)]
                                    (and (= "xt" (.getSchemaName table))
                                         (= "txs" (.getTableName table)))))))
        tx-id (some-> txs-events first :doc (get "_id"))
        tx-key (when tx-id
                 (serde/->TxKey tx-id (time/->instant system-time)))

        ;; Group events by table
        events-by-table (group-by :_table table-events)

        tables (->> events-by-table
                    (map (fn [[^TableRef table events]]
                           {:db (.getDbName table)
                            :schema (.getSchemaName table)
                            :table (.getTableName table)
                            :ops (mapv (fn [e]
                                         (cond-> {:op (:op e)
                                                  :iid (:iid e)
                                                  :valid-from (:valid-from e)
                                                  :valid-to (:valid-to e)}
                                           (= (:op e) :put) (assoc :doc (:doc e))))
                                       events)}))
                    (into []))]
    (when tx-key
      {:tx-key tx-key
       :message (-> {:transaction {:id tx-key}
                     :system-time (time/->instant system-time)
                     :source {:db db-name
                              :block-idx block-idx}
                     :tables tables}
                    encode-fn
                    Log$Message$Tx.)})))

(defn process-l0-block
  "Process an L0 block: read events, group by transaction, output to log.
   Returns the last tx-key processed."
  [^BufferAllocator al ^BufferPool bp ^Log output-log
   block-idx table-entries db-name encode-fn last-tx-key]
  (let [table->events (read-l0-block al bp table-entries)
        txs-by-time (group-events-by-system-time table->events)]
    (reduce (fn [last-key [system-time events]]
              (if-let [{:keys [tx-key message]} (events->tx-output system-time events db-name block-idx encode-fn)]
                (do
                  ;; Only output if newer than last-tx-key
                  (when (or (nil? last-key) (gt tx-key last-key))
                    @(.appendMessage output-log message))
                  tx-key)
                last-key))
            last-tx-key
            txs-by-time)))

(defn initial-scan!
  "Perform initial scan of L0 files and output transactions.
   Uses a catch-up loop that re-checks for new blocks after each pass.
   Returns the last tx-key processed."
  [^BufferAllocator al ^BufferPool bp ^TrieCatalog trie-cat ^Log output-log
   {:keys [db-name encode-fn last-tx-key process-block-fn]}]
  (let [process-block (or process-block-fn process-l0-block)]
    (loop [last-processed-block -1
           current-last-tx-key last-tx-key]
      (.refresh trie-cat)
      (let [l0-index (build-l0-index trie-cat)
            blocks-to-process (if (neg? last-processed-block)
                                l0-index
                                (subseq l0-index > last-processed-block))]
        (if (empty? blocks-to-process)
          (do
            (logging/debug "Initial scan complete, switching to live mode")
            current-last-tx-key)
          (let [new-last-tx-key
                (reduce (fn [last-key [block-idx table-entries]]
                          (logging/debugf "Processing L0 block %d with %d tables"
                                          block-idx (count table-entries))
                          (process-block al bp output-log block-idx table-entries
                                         db-name encode-fn last-key))
                        current-last-tx-key
                        blocks-to-process)
                last-block-idx (long (first (last blocks-to-process)))]
            (recur last-block-idx new-last-tx-key)))))))

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
      :allocator (ig/ref :xtdb.db-catalog/allocator)
      :buffer-pool (ig/ref :xtdb/buffer-pool)
      :trie-cat (ig/ref :xtdb/trie-catalog)
      :db-name db-name}
   ::output-log {:tx-sink-conf tx-sink-conf
                 :base base
                 :db-name db-name}})

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

(defmethod ig/init-key ::for-db [_ {:keys [^TxSinkConfig tx-sink-conf ^Log output-log
                                           block-cat allocator buffer-pool trie-cat db-name]}]
  (when (and tx-sink-conf
             (.getEnable tx-sink-conf)
             (= db-name (.getDbName tx-sink-conf)))
    (let [encode-fn (->encode-fn (keyword (.getFormat tx-sink-conf)))
          last-message (try
                         (let [decode-record (->decode-fn (keyword (.getFormat tx-sink-conf)))]
                           (->> (.readLastMessage output-log)
                                Log$Message/.encode
                                decode-record))
                         (catch Exception _ nil))
          last-tx-key (some-> last-message :transaction :id)
          ;; Perform initial scan if enabled and no last message
          last-tx-key (if (and (.getInitialScan tx-sink-conf) (nil? last-message))
                        (do
                          (logging/info "Starting initial scan from L0 files")
                          (initial-scan! allocator buffer-pool trie-cat output-log
                                         {:db-name db-name
                                          :encode-fn encode-fn
                                          :last-tx-key last-tx-key}))
                        last-tx-key)]
      (map->TxSink {:output-log output-log
                    :encode-as-bytes encode-fn
                    :block-cat block-cat
                    :db-name db-name
                    :last-tx-key last-tx-key}))))

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
