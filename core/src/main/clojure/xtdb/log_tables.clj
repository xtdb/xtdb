(ns xtdb.log-tables
  (:require [clojure.tools.logging :as log]
            [xtdb.error :as err]
            [xtdb.table :as table]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr])
  (:import (java.util Map)
           (org.apache.arrow.memory BufferAllocator)
           (xtdb ICursor)
           (xtdb.api.log Log Log$Record SourceMessage SourceMessage$Tx
                         SourceMessage$FlushBlock SourceMessage$TriesAdded
                         SourceMessage$AttachDatabase SourceMessage$DetachDatabase
                         SourceMessage$BlockUploaded SourceMessage$LegacyTx
                         ReplicaMessage ReplicaMessage$ResolvedTx
                         ReplicaMessage$TriesAdded ReplicaMessage$BlockBoundary
                         ReplicaMessage$BlockUploaded ReplicaMessage$NoOp)
           (xtdb.arrow Relation RelationReader VectorReader)
           (xtdb.database Database)
           (xtdb.operator SelectionSpec)
           (xtdb.table TableRef)))

(set! *unchecked-math* :warn-on-boxed)

(defn- map->vec-types [m]
  (update-vals m types/->type))

(def log-tables
  (-> '{xt/source_log_msgs {msg_id :i64, log_offset :i64,
                            log_timestamp [:timestamp-tz :micro "UTC"],
                            msg :utf8, tx_ops [:? :utf8]}
        xt/replica_log_msgs {msg_id :i64, log_offset :i64,
                             log_timestamp [:timestamp-tz :micro "UTC"],
                             msg :utf8}}
      (update-vals map->vec-types)))

(defn log-table
  "Returns the table schema if this is a log table, nil otherwise."
  [table-ref]
  (get log-tables (table/ref->schema+table table-ref)))

(defn- source-msg->edn [^SourceMessage msg]
  (pr-str
   (condp instance? msg
     SourceMessage$Tx
     (let [^SourceMessage$Tx tx msg]
       {:type :tx
        :default-tz (str (.getDefaultTz tx))
        :user (.getUser tx)
        :system-time (.getSystemTime tx)})

     SourceMessage$FlushBlock
     {:type :flush-block
      :expected-block-idx (.getExpectedBlockIdx ^SourceMessage$FlushBlock msg)}

     SourceMessage$TriesAdded
     (let [^SourceMessage$TriesAdded m msg]
       {:type :tries-added
        :storage-version (.getStorageVersion m)
        :storage-epoch (.getStorageEpoch m)})

     SourceMessage$AttachDatabase
     {:type :attach-database
      :db-name (.getDbName ^SourceMessage$AttachDatabase msg)}

     SourceMessage$DetachDatabase
     {:type :detach-database
      :db-name (.getDbName ^SourceMessage$DetachDatabase msg)}

     SourceMessage$BlockUploaded
     (let [^SourceMessage$BlockUploaded m msg]
       {:type :block-uploaded
        :block-index (.getBlockIndex m)
        :storage-version (.getStorageVersion m)
        :storage-epoch (.getStorageEpoch m)})

     SourceMessage$LegacyTx
     {:type :legacy-tx}

     {:type :unknown})))

(defn- decode-tx-ops
  "Decodes Arrow-encoded tx-ops bytes into an EDN string."
  [^BufferAllocator allocator ^bytes tx-ops-bytes]
  (when tx-ops-bytes
    (try
      (with-open [rel (Relation/openFromArrowStream allocator tx-ops-bytes)]
        (pr-str (util/->clj (.getAsMaps rel))))
      (catch Exception e
        (log/warn e "Failed to decode tx-ops from source log message")
        "<error decoding tx-ops>"))))

(defn- replica-msg->edn [^ReplicaMessage msg]
  (pr-str
   (condp instance? msg
     ReplicaMessage$ResolvedTx
     (let [^ReplicaMessage$ResolvedTx m msg]
       {:type :resolved-tx
        :tx-id (.getTxId m)
        :system-time (.getSystemTime m)
        :committed (.getCommitted m)
        :error (some-> (.getError m) str)})

     ReplicaMessage$TriesAdded
     (let [^ReplicaMessage$TriesAdded m msg]
       {:type :tries-added
        :storage-version (.getStorageVersion m)
        :storage-epoch (.getStorageEpoch m)
        :source-msg-id (.getSourceMsgId m)})

     ReplicaMessage$BlockBoundary
     (let [^ReplicaMessage$BlockBoundary m msg]
       {:type :block-boundary
        :block-index (.getBlockIndex m)
        :latest-processed-msg-id (.getLatestProcessedMsgId m)})

     ReplicaMessage$BlockUploaded
     (let [^ReplicaMessage$BlockUploaded m msg]
       {:type :block-uploaded
        :block-index (.getBlockIndex m)
        :storage-version (.getStorageVersion m)
        :storage-epoch (.getStorageEpoch m)})

     ReplicaMessage$NoOp
     {:type :no-op}

     {:type :unknown})))

(defn- resolve-expr-value
  "Resolves a literal value or param reference from a predicate expression form."
  [expr args]
  (cond
    (number? expr) (long expr)

    (symbol? expr)
    (when args
      (let [^xtdb.arrow.RelationReader args-rel args]
        (when-let [^VectorReader col (.readerForName args-rel (str expr))]
          (.getObject col 0))))

    (and (seq? expr) (= 'param (first expr)))
    (let [[_ param-name _param-type] expr
          param-str (str param-name)]
      (when args
        (let [^xtdb.arrow.RelationReader args-rel args]
          (when-let [^VectorReader col (.readerForName args-rel param-str)]
            (.getObject col 0)))))

    :else nil))

(defn- extract-msg-id-bounds
  "Extracts [from-msg-id to-msg-id) bounds from the selects map for msg_id.
  BETWEEN is inclusive, so we add 1 to the upper bound for end-exclusive semantics."
  [selects args]
  (when-let [pred (get selects "msg_id")]
    (when (seq? pred)
      (let [op (first pred)]
        (case op
          between
          (let [[_ _col lower upper] pred
                from (resolve-expr-value lower args)
                to (resolve-expr-value upper args)]
            (when (and from to)
              [(long from) (inc (long to))]))

          ;; fallback: try to extract >= and <= from an `and` form
          and
          (let [clauses (rest pred)
                bounds (reduce (fn [acc clause]
                                 (if (seq? clause)
                                   (let [[op _col val] clause
                                         v (some-> (resolve-expr-value val args) long)]
                                     (if v
                                       (let [v (long v)]
                                         (case op
                                           >= (assoc acc :from v)
                                           > (assoc acc :from (inc v))
                                           <= (assoc acc :to (inc v))
                                           < (assoc acc :to v)
                                           acc))
                                       acc))
                                   acc))
                               {} clauses)]
            (when (and (:from bounds) (:to bounds))
              [(:from bounds) (:to bounds)]))

          nil)))))

(def ^:private ^:const batch-size 1024)

(defn- take-batch
  "Takes up to `n` elements from an Iterator, returning a vector."
  [^java.util.Iterator iter ^long n]
  (loop [acc (transient []), i 0]
    (if (and (< i n) (.hasNext iter))
      (recur (conj! acc (.next iter)) (inc i))
      (persistent! acc))))

(defn- records->rows [records source? decode-tx-ops? allocator]
  (if source?
    (mapv (fn [^Log$Record rec]
            (let [msg (.getMessage rec)]
              {"msg_id" (.getMsgId rec)
               "log_offset" (.getLogOffset rec)
               "log_timestamp" (.getLogTimestamp rec)
               "msg" (source-msg->edn msg)
               "tx_ops" (when (and decode-tx-ops? (instance? SourceMessage$Tx msg))
                          (decode-tx-ops allocator (.getTxOps ^SourceMessage$Tx msg)))}))
          records)
    (mapv (fn [^Log$Record rec]
            {"msg_id" (.getMsgId rec)
             "log_offset" (.getLogOffset rec)
             "log_timestamp" (.getLogTimestamp rec)
             "msg" (replica-msg->edn (.getMessage rec))})
          records)))

(defn- emit-batch
  "Writes a batch of row maps into a Relation, applies col-preds, and yields to the consumer."
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

(defn ->cursor
  "Creates a streaming cursor for log table queries.
  Pulls records from the log in batches of ~1024, converting each batch to a Relation."
  [^Database db ^BufferAllocator allocator ^TableRef table
   col-names col-preds selects schema args]
  (let [table-sym (table/ref->schema+table table)
        source? (= 'xt/source_log_msgs table-sym)
        ^Log log (if source? (.getSourceLog db) (.getReplicaLog db))
        derived-table-schema (log-table table)
        bounds (extract-msg-id-bounds selects args)]

    (when-not bounds
      (throw (err/incorrect :xtdb/missing-log-bounds
                            "Queries on log tables require msg_id bounds (e.g. WHERE msg_id BETWEEN x AND y)")))

    (let [[^long from-msg-id ^long to-msg-id] bounds
          record-seq (.readRecords log from-msg-id to-msg-id)
          record-iter (.iterator record-seq)
          decode-tx-ops? (and source? (contains? col-names "tx_ops"))]

      (reify ICursor
        (getCursorType [_] "log-table")
        (getChildCursors [_] [])
        (tryAdvance [_ c]
          (boolean
           (when (.hasNext record-iter)
             (let [batch (take-batch record-iter batch-size)
                   rows (records->rows batch source? decode-tx-ops? allocator)]
               (emit-batch allocator derived-table-schema col-names col-preds schema args rows c)
               true))))
        (close [_])))))
