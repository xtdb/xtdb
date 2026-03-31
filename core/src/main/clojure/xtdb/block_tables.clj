(ns xtdb.block-tables
  (:require [xtdb.error :as err]
            [xtdb.expression :as expr]
            [xtdb.expression.constraints :as constraints]
            [xtdb.table :as table]
            [xtdb.table-catalog :as table-cat]
            [xtdb.trie-catalog :as trie-cat]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr])
  (:import (java.util Map)
           (org.apache.arrow.memory BufferAllocator)
           (xtdb ICursor)
           (xtdb.arrow Relation RelationReader VectorReader)
           (xtdb.block.proto Block TableBlock)
           (xtdb.catalog BlockCatalog)
           (xtdb.database Database)
           (xtdb.log.proto TrieDetails)
           (xtdb.operator SelectionSpec)
           (xtdb.storage BufferPool)
           (xtdb.table TableRef)
           (xtdb.time InstantUtil)
           (xtdb.trie Trie)
           (xtdb.util HyperLogLog StringUtil)))

(set! *unchecked-math* :warn-on-boxed)

(defn- map->vec-types [m]
  (update-vals m types/->type))

(def block-tables
  (-> '{xt/block_files {block_idx :utf8
                        tx_id [:? :i64]
                        system_time [:? :timestamp-tz :micro "UTC"]
                        latest_processed_msg_id :i64
                        table_names :utf8
                        boundary_replica_msg_id [:? :i64]
                        file_size :i64}

        xt/table_block_files {table_name :utf8
                              block_idx :utf8
                              row_count :i64
                              fields :utf8
                              hlls :utf8
                              partition_count :i32}

        xt/table_block_file_tries {table_name :utf8
                                   block_idx :utf8
                                   level :i32
                                   recency [:? :utf8]
                                   part :utf8
                                   trie_key :utf8
                                   data_file_size :i64
                                   trie_state [:? :utf8]
                                   row_count [:? :i64]
                                   temporal_metadata [:? :utf8]}}
      (update-vals map->vec-types)))

(defn block-table
  "Returns the table schema if this is a block table, nil otherwise."
  [table-ref]
  (get block-tables (table/ref->schema+table table-ref)))

;;; helpers

(defn- ->env [derived-table-schema args]
  {:var-types derived-table-schema
   :param-types (if args (expr/->param-types args) {})})

(def ^:private ^:const batch-size 1024)

(defn- take-batch
  "Takes up to `n` elements from an Iterator, returning a vector."
  [^java.util.Iterator iter ^long n]
  (loop [acc (transient []), i 0]
    (if (and (< i n) (.hasNext iter))
      (recur (conj! acc (.next iter)) (inc i))
      (persistent! acc))))

(defn- emit-batch
  [^BufferAllocator allocator derived-table-schema col-names col-preds schema args rows c]
  (util/with-open [out-rel (Relation. allocator ^Map (update-keys derived-table-schema str))]
    (.writeRows out-rel (into-array java.util.Map rows))
    (let [out-rel-view (reduce (fn [^RelationReader rel ^SelectionSpec col-pred]
                                 (.select rel (.select col-pred allocator rel schema args)))
                               (-> out-rel
                                   (->> (filter (comp (set col-names) #(.getName ^VectorReader %))))
                                   (vr/rel-reader (.getRowCount out-rel))
                                   (vr/with-absent-cols allocator col-names))
                               (vals col-preds))]
      (.accept c out-rel-view))))

;;; xt.block_files

(defn- ->lex-hex ^String [^long v]
  (.getAsLexHex StringUtil/INSTANCE v))

(defn- <-lex-hex ^long [^String s]
  (.getFromLexHex StringUtil/INSTANCE s))

(defn- range-bounds->from-to
  "Converts structured range bounds from extract-range into a [from, to) long pair."
  [{[lower-op lower-val] :lower, [upper-op upper-val] :upper}]
  (let [from (let [v (<-lex-hex (str lower-val))]
               (case lower-op :>= v, :> (inc v)))
        to (let [v (<-lex-hex (str upper-val))]
             (case upper-op :<= (inc v), :< v))]
    [from to]))

(defn- block->row [^Block block ^long file-size]
  (let [has-tx? (.hasLatestCompletedTx block)]
    {"block_idx" (->lex-hex (.getBlockIndex block))
     "tx_id" (when has-tx? (.getTxId (.getLatestCompletedTx block)))
     "system_time" (when has-tx? (InstantUtil/fromMicros (.getSystemTime (.getLatestCompletedTx block))))
     "latest_processed_msg_id" (.getLatestProcessedMsgId block)
     "table_names" (pr-str (vec (.getTableNamesList block)))
     "boundary_replica_msg_id" (when (.hasBoundaryReplicaMsgId block) (.getBoundaryReplicaMsgId block))
     "file_size" file-size}))

(defn- try-read-block
  "Attempts to read a block file at the given index. Returns a row map or nil."
  [^BufferPool buffer-pool ^long block-idx]
  (try
    (let [path (BlockCatalog/blockFilePath block-idx)
          ^bytes ba (.getByteArray buffer-pool path)
          block (Block/parseFrom ba)]
      (block->row block (alength ba)))
    (catch Exception _e
      nil)))

(defn- ->block-files-cursor
  [^Database db ^BufferAllocator allocator col-names col-preds selects schema args]
  (let [derived-table-schema (get block-tables 'xt/block_files)
        env (->env derived-table-schema args)
        bounds (when-let [form (get selects "block_idx")]
                 (constraints/extract-range form env args))]

    (when-not bounds
      (throw (err/incorrect :xtdb/missing-block-idx-bounds
                            "Queries on xt.block_files require block_idx bounds (e.g. WHERE block_idx = '...' or WHERE block_idx BETWEEN '...' AND '...')")))

    (let [[^long from-idx ^long to-idx] (range-bounds->from-to bounds)
          ^BufferPool buffer-pool (.getBufferPool db)
          idx (volatile! from-idx)]

      (reify ICursor
        (getCursorType [_] "block-table")
        (getChildCursors [_] [])
        (tryAdvance [_ c]
          (boolean
           (when (< ^long @idx to-idx)
             (let [batch (loop [acc (transient []), i (long 0)]
                           (if (and (< i batch-size) (< (+ ^long @idx i) to-idx))
                             (let [row (try-read-block buffer-pool (+ ^long @idx i))]
                               (recur (if row (conj! acc row) acc) (inc i)))
                             (do (vswap! idx (fn [^long v] (+ v i)))
                                 (persistent! acc))))]
               (when (seq batch)
                 (emit-batch allocator derived-table-schema col-names col-preds schema args batch c)
                 true)))))
        (close [_])))))

;;; xt.table_block_files

(defn- table-block->row [^String table-name ^long block-idx {:keys [^long row-count fields hlls partitions]}]
  {"table_name" table-name
   "block_idx" (->lex-hex block-idx)
   "row_count" row-count
   "fields" (pr-str (update-vals fields #(types/field->col-type %)))
   "hlls" (pr-str (update-vals hlls #(long (HyperLogLog/estimate %))))
   "partition_count" (int (count partitions))})

(defn- resolve-table-block
  "Resolves a table-block file given db name, table name and block idx. Returns parsed map or nil."
  [^BufferPool buffer-pool ^String db-name ^String table-name ^long block-idx]
  (let [table-ref (table/->ref db-name table-name)
        table-path (Trie/getTablePath table-ref)
        obj-key (table-cat/->table-block-metadata-obj-key table-path block-idx)]
    (try
      (-> (.getByteArray buffer-pool obj-key)
          (TableBlock/parseFrom)
          (table-cat/<-table-block))
      (catch Exception _e
        nil))))

(defn- ->table-block-files-cursor
  [^Database db ^BufferAllocator allocator col-names col-preds selects schema args]
  (let [derived-table-schema (get block-tables 'xt/table_block_files)
        env (->env derived-table-schema args)
        table-name (when-let [form (get selects "table_name")]
                     (constraints/extract-equality form env args))
        block-idx (when-let [form (get selects "block_idx")]
                    (constraints/extract-equality form env args))]

    (when-not (and table-name block-idx)
      (throw (err/incorrect :xtdb/missing-table-block-predicates
                            "Queries on xt.table_block_files require table_name = '...' AND block_idx = '...'")))

    (let [^BufferPool buffer-pool (.getBufferPool db)
          block-idx (<-lex-hex (str block-idx))
          table-block (resolve-table-block buffer-pool (.getName db) (str table-name) block-idx)
          rows (if table-block
                 [(table-block->row (str table-name) block-idx table-block)]
                 [])
          done? (volatile! false)]

      (reify ICursor
        (getCursorType [_] "table-block-file")
        (getChildCursors [_] [])
        (tryAdvance [_ c]
          (boolean
           (when (and (seq rows) (not @done?))
             (vreset! done? true)
             (emit-batch allocator derived-table-schema col-names col-preds schema args rows c)
             true)))
        (close [_])))))

;;; xt.table_block_file_tries

(defn- trie-details->rows [^String table-name ^String block-idx-hex partitions]
  (vec
   (for [{:keys [^int level recency part tries]} partitions
         ^TrieDetails td tries
         :let [trie-meta (some-> (.getTrieMetadata td) trie-cat/<-trie-metadata)]]
     {"table_name" table-name
      "block_idx" block-idx-hex
      "level" (int level)
      "recency" (some-> recency str)
      "part" (pr-str part)
      "trie_key" (.getTrieKey td)
      "data_file_size" (.getDataFileSize td)
      "trie_state" (when (.hasTrieState td)
                     (name (condp = (.getTrieState td)
                             xtdb.log.proto.TrieState/LIVE :live
                             xtdb.log.proto.TrieState/NASCENT :nascent
                             xtdb.log.proto.TrieState/GARBAGE :garbage)))
      "row_count" (:row-count trie-meta)
      "temporal_metadata" (some-> trie-meta (dissoc :row-count :iid-bloom) not-empty pr-str)})))

(defn- ->table-block-file-tries-cursor
  [^Database db ^BufferAllocator allocator col-names col-preds selects schema args]
  (let [derived-table-schema (get block-tables 'xt/table_block_file_tries)
        env (->env derived-table-schema args)
        table-name (when-let [form (get selects "table_name")]
                     (constraints/extract-equality form env args))
        block-idx (when-let [form (get selects "block_idx")]
                    (constraints/extract-equality form env args))]

    (when-not (and table-name block-idx)
      (throw (err/incorrect :xtdb/missing-table-block-predicates
                            "Queries on xt.table_block_file_tries require table_name = '...' AND block_idx = '...'")))

    (let [^BufferPool buffer-pool (.getBufferPool db)
          block-idx-hex (str block-idx)
          block-idx (<-lex-hex block-idx-hex)
          table-block (resolve-table-block buffer-pool (.getName db) (str table-name) block-idx)
          rows (if table-block
                 (trie-details->rows (str table-name) block-idx-hex (:partitions table-block))
                 [])
          iter (.iterator ^Iterable rows)]

      (reify ICursor
        (getCursorType [_] "table-block-file-tries")
        (getChildCursors [_] [])
        (tryAdvance [_ c]
          (boolean
           (when (.hasNext iter)
             (let [batch (take-batch iter batch-size)]
               (emit-batch allocator derived-table-schema col-names col-preds schema args batch c)
               true))))
        (close [_])))))

;;; dispatch

(defn ->cursor
  [^Database db ^BufferAllocator allocator ^TableRef table
   col-names col-preds selects schema args]
  (case (table/ref->schema+table table)
    xt/block_files (->block-files-cursor db allocator col-names col-preds selects schema args)
    xt/table_block_files (->table-block-files-cursor db allocator col-names col-preds selects schema args)
    xt/table_block_file_tries (->table-block-file-tries-cursor db allocator col-names col-preds selects schema args)))
