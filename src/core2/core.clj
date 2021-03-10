(ns core2.core
  (:require [clojure.tools.logging :as log]
            [clojure.set :as set]
            [core2.buffer-pool :as bp]
            [core2.indexer :as indexer]
            [core2.log :as l]
            [core2.metadata :as meta]
            [core2.object-store :as os]
            [core2.temporal :as temporal]
            [core2.tx :as tx]
            [core2.types :as t]
            [core2.util :as util])
  (:import core2.buffer_pool.BufferPool
           [core2.indexer Indexer TransactionIndexer]
           [core2.log LogReader LogRecord LogWriter]
           core2.metadata.IMetadataManager
           core2.temporal.ITemporalManager
           core2.object_store.ObjectStore
           java.io.Closeable
           java.nio.file.Path
           java.time.Duration
           [java.util LinkedHashMap LinkedHashSet Set]
           [java.util.concurrent Executors ExecutorService Future TimeoutException]
           [org.apache.arrow.memory BufferAllocator RootAllocator]
           [org.apache.arrow.vector.complex DenseUnionVector StructVector]
           [org.apache.arrow.vector.types Types$MinorType UnionMode]
           [org.apache.arrow.vector.types.pojo ArrowType ArrowType$Union Schema]
           [org.apache.arrow.vector VectorSchemaRoot TimeStampVector]))

(set! *unchecked-math* :warn-on-boxed)

(definterface TxProducer
  (^java.util.concurrent.CompletableFuture submitTx [_]))

(defn- ->doc-k-types [tx-ops]
  (let [doc-k-types (LinkedHashMap.)]
    (doseq [{:keys [op] :as tx-op} tx-ops
            [k v] (case op
                    :put (:doc tx-op)
                    :delete (select-keys tx-op [:_id]))]
      (let [^Set field-types (.computeIfAbsent doc-k-types k (util/->jfn (fn [_] (LinkedHashSet.))))]
        (.add field-types (t/->arrow-type (type v)))))

    doc-k-types))

(defn- validate-tx-ops [tx-ops]
  (doseq [{:keys [op _valid-time _valid-time-end] :as tx-op} tx-ops]
    (case op
      :put (assert (contains? (:doc tx-op) :_id))
      :delete (assert (and (contains? tx-op :_id)
                           (set/subset? (set (keys tx-op)) #{:_id :_valid-time :_valid-time-end}))))

    (when _valid-time
      (assert (inst? _valid-time)))
    (when _valid-time-end
      (assert (inst? _valid-time-end)))))

(defn- ->doc-field [k v-types]
  (let [v-types (sort-by #(.getFlatbufID (.getTypeID ^ArrowType %)) v-types)]
    (apply t/->field
           (name k)
           (ArrowType$Union. UnionMode/Dense
                             (int-array (for [^ArrowType v-type v-types]
                                          (.getFlatbufID (.getTypeID v-type)))))
           false
           (for [^ArrowType v-type v-types]
             (t/->field (str "type-" (.getFlatbufID (.getTypeID v-type))) v-type false)))))

(def ^:private ^org.apache.arrow.vector.types.pojo.Field valid-time-field
  (t/->field "_valid-time" (t/primitive-type->arrow-type :timestampmilli) true))

(def ^:private ^org.apache.arrow.vector.types.pojo.Field valid-time-end-field
  (t/->field "_valid-time-end" (t/primitive-type->arrow-type :timestampmilli) true))

(defn serialize-tx-ops ^java.nio.ByteBuffer [tx-ops ^BufferAllocator allocator]
  (validate-tx-ops tx-ops)
  (let [put-k-types (->doc-k-types tx-ops)
        document-field (apply t/->field "document" (.getType Types$MinorType/STRUCT) false
                              (for [[k v-types] put-k-types]
                                (->doc-field k v-types)))
        delete-id-field (->doc-field :_id (:_id put-k-types))
        tx-schema (Schema. [(t/->field "tx-ops" (.getType Types$MinorType/DENSEUNION) false
                                       (t/->field "put" (.getType Types$MinorType/STRUCT) false
                                                  document-field
                                                  valid-time-field
                                                  valid-time-end-field)
                                       (t/->field "delete" (.getType Types$MinorType/STRUCT) false
                                                  delete-id-field
                                                  valid-time-field
                                                  valid-time-end-field))])]
    (with-open [root (VectorSchemaRoot/create tx-schema allocator)]
      (let [^DenseUnionVector tx-ops-duv (.getVector root "tx-ops")]

        (dotimes [tx-op-n (count tx-ops)]
          (let [{:keys [op _valid-time _valid-time-end] :as tx-op} (nth tx-ops tx-op-n)
                op-type-id (case op :put 0, :delete 1)
                ^StructVector op-vec (.getStruct tx-ops-duv op-type-id)
                tx-op-offset (util/write-type-id tx-ops-duv tx-op-n op-type-id)
                valid-time-vec (.getChild op-vec "_valid-time" TimeStampVector)
                valid-time-end-vec (.getChild op-vec "_valid-time-end" TimeStampVector)]
            (case op
              :put (let [^StructVector document-vec (.getChild op-vec "document" StructVector)]
                     (.setIndexDefined op-vec tx-op-offset)
                     (.setIndexDefined document-vec tx-op-offset)

                     (let [{:keys [doc]} tx-op]
                       (doseq [[k v] doc
                               :let [^DenseUnionVector value-duv (.getChild document-vec (name k) DenseUnionVector)]]
                         (if (some? v)
                           (let [type-id (.getFlatbufID (.getTypeID ^ArrowType (t/->arrow-type (type v))))
                                 value-offset (util/write-type-id value-duv tx-op-offset type-id)]
                             (t/set-safe! (.getVectorByType value-duv type-id) value-offset v))

                           (util/set-value-count value-duv (inc (.getValueCount value-duv)))))))

              :delete (let [id (:_id tx-op)
                            ^DenseUnionVector id-duv (.getChild op-vec "_id" DenseUnionVector)]
                        (.setIndexDefined op-vec tx-op-offset)

                        (if (some? id)
                          (let [type-id (.getFlatbufID (.getTypeID ^ArrowType (t/->arrow-type (type id))))
                                value-offset (util/write-type-id id-duv tx-op-offset type-id)]
                            (t/set-safe! (.getVectorByType id-duv type-id) value-offset id))

                          (util/set-value-count id-duv (inc (.getValueCount id-duv))))))

            (if _valid-time
              (t/set-safe! valid-time-vec tx-op-n _valid-time)
              (t/set-null! valid-time-vec tx-op-n))
            (if _valid-time-end
              (t/set-safe! valid-time-end-vec tx-op-n _valid-time)
              (t/set-null! valid-time-end-vec tx-op-n))))

        (util/set-vector-schema-root-row-count root (count tx-ops))

        (.syncSchema root)

        (util/root->arrow-ipc-byte-buffer root :stream)))))

(defn log-record->tx-instant ^core2.tx.TransactionInstant [^LogRecord record]
  (tx/->TransactionInstant (.offset record) (.time record)))

(deftype LogTxProducer [^LogWriter log-writer, ^BufferAllocator allocator]
  TxProducer
  (submitTx [_this tx-ops]
    (-> (.appendRecord log-writer (serialize-tx-ops tx-ops allocator))
        (util/then-apply
          (fn [result]
            (log-record->tx-instant result)))))

  Closeable
  (close [_]
    (util/try-close log-writer)
    (util/try-close allocator)))

(defn ->local-tx-producer
  (^core2.core.LogTxProducer [^Path node-dir]
   (->local-tx-producer node-dir {}))
  (^core2.core.LogTxProducer [^Path node-dir log-writer-opts]
   (LogTxProducer. (l/->local-directory-log-writer (.resolve node-dir "log") log-writer-opts)
                   (RootAllocator.))))

(definterface IIngestLoop
  (^void ingestLoop [])
  (^void start [])
  (^core2.tx.TransactionInstant latestCompletedTx [])
  (^core2.tx.TransactionInstant awaitTx [^core2.tx.TransactionInstant tx])
  (^core2.tx.TransactionInstant awaitTx [^core2.tx.TransactionInstant tx, ^java.time.Duration timeout]))

(deftype IngestLoop [^LogReader log-reader
                     ^TransactionIndexer indexer
                     ^ExecutorService pool
                     ^:volatile-mutable ^Future ingest-future
                     ingest-opts]
  IIngestLoop
  (ingestLoop [this]
    (let [{:keys [^Duration poll-sleep-duration ^long batch-size],
           :or {poll-sleep-duration (Duration/ofMillis 100)
                batch-size 100}} ingest-opts
          poll-sleep-ms (.toMillis poll-sleep-duration)]
      (try
        (while true
          (if-let [log-records (not-empty (.readRecords log-reader (some-> (.latestCompletedTx indexer) .tx-id) batch-size))]
            (doseq [^LogRecord record log-records]
              (if (Thread/interrupted)
                (throw (InterruptedException.))
                (.indexTx indexer (log-record->tx-instant record) (.record record))))
            (Thread/sleep poll-sleep-ms)))
        (catch InterruptedException _)
        (catch Throwable t
          (if (Thread/interrupted)
            (log/warn t "exception while closing")
            (do (log/fatal t "ingest loop stopped")
                (throw t)))))))

  (start [this]
    (set! (.ingest-future this)
          (.submit pool ^Runnable #(.ingestLoop this))))

  (latestCompletedTx [_this] (.latestCompletedTx indexer))

  (awaitTx [this tx] (.awaitTx this tx nil))

  (awaitTx [_this tx timeout]
    (if tx
      (let [{:keys [^Duration poll-sleep-duration],
             :or {poll-sleep-duration (Duration/ofMillis 100)}} ingest-opts
            poll-sleep-ms (.toMillis poll-sleep-duration)
            end-ns (+ (System/nanoTime) (.toNanos timeout))
            tx-id (.tx-id tx)]
        (loop []
          (let [latest-completed-tx (.latestCompletedTx indexer)]
            (cond
              (and latest-completed-tx
                   (>= (.tx-id latest-completed-tx) tx-id))
              latest-completed-tx

              (some-> ingest-future (.isDone))
              (do @ingest-future (throw (IllegalStateException. "node closed")))

              (or (nil? timeout)
                  (neg? (- (System/nanoTime) end-ns)))
              (do
                (Thread/sleep poll-sleep-ms)
                (recur))

              :else (throw (TimeoutException.))))))
      (.latestCompletedTx indexer)))

  Closeable
  (close [_]
    (future-cancel ingest-future)
    (util/shutdown-pool pool)))

(defn ->ingest-loop
  (^core2.core.IngestLoop
   [^LogReader log-reader
    ^TransactionIndexer indexer]
   (->ingest-loop log-reader indexer {}))

  (^core2.core.IngestLoop
   [^LogReader log-reader
    ^TransactionIndexer indexer
    ingest-opts]

   (let [pool (Executors/newSingleThreadExecutor (util/->prefix-thread-factory "ingest-loop-"))]
     (doto (IngestLoop. log-reader indexer pool nil ingest-opts)
       (.start)))))

(deftype Node [^BufferAllocator allocator
               ^LogReader log-reader
               ^ObjectStore object-store
               ^IMetadataManager metadata-manager
               ^ITemporalManager temporal-manager
               ^Indexer indexer
               ^IngestLoop ingest-loop
               ^BufferPool buffer-pool]
  Closeable
  (close [_]
    (util/try-close ingest-loop)
    (util/try-close indexer)
    (util/try-close metadata-manager)
    (util/try-close buffer-pool)
    (util/try-close object-store)
    (util/try-close log-reader)
    (util/try-close allocator)))

(defn ->local-node
  (^core2.core.Node [^Path node-dir]
   (->local-node node-dir {}))
  (^core2.core.Node [^Path node-dir opts]
   (let [object-dir (.resolve node-dir "objects")
         log-dir (.resolve node-dir "log")
         allocator (RootAllocator.)
         log-reader (l/->local-directory-log-reader log-dir)
         object-store (os/->file-system-object-store object-dir opts)
         buffer-pool (bp/->memory-mapped-buffer-pool (.resolve node-dir "buffers") allocator object-store)
         metadata-manager (meta/->metadata-manager allocator object-store buffer-pool)
         temporal-manager (temporal/->temporal-manager allocator buffer-pool metadata-manager)
         indexer (indexer/->indexer allocator object-store metadata-manager temporal-manager opts)
         ingest-loop (->ingest-loop log-reader indexer opts)]
     (Node. allocator log-reader object-store metadata-manager temporal-manager indexer ingest-loop buffer-pool))))

(defn -main [& [node-dir :as args]]
  (if node-dir
    (let [node-dir (util/->path node-dir)]
      (->local-node node-dir)
      (println "core2 started in" (str node-dir)))
    (binding [*out* *err*]
      (println "node directory argument required")
      (System/exit 1))))
