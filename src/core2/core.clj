(ns core2.core
  (:require [clojure.tools.logging :as log]
            [core2.buffer-pool :as bp]
            [core2.ingest :as ingest]
            [core2.log :as l]
            [core2.metadata :as meta]
            [core2.object-store :as os]
            [core2.types :as t]
            [core2.util :as util])
  (:import clojure.lang.MapEntry
           core2.buffer_pool.BufferPool
           [core2.ingest Ingester TransactionIngester TransactionInstant]
           [core2.log LogReader LogRecord LogWriter]
           core2.object_store.ObjectStore
           [java.io Closeable]
           [java.nio ByteBuffer]
           [java.nio.channels ClosedByInterruptException]
           [java.nio.file Path]
           java.nio.file.attribute.FileAttribute
           [java.time Duration Instant]
           [java.util.concurrent CompletableFuture Future Executors ExecutorService TimeUnit TimeoutException]
           [org.apache.arrow.memory ArrowBuf BufferAllocator RootAllocator]
           [org.apache.arrow.vector ValueVector VectorSchemaRoot VectorLoader]
           [org.apache.arrow.vector.complex DenseUnionVector StructVector]
           org.apache.arrow.vector.types.pojo.Schema
           org.apache.arrow.vector.types.Types$MinorType))

(set! *unchecked-math* :warn-on-boxed)

(definterface TxProducer
  (^java.util.concurrent.CompletableFuture submitTx [_]))

(defn serialize-tx-ops ^java.nio.ByteBuffer [tx-ops ^BufferAllocator allocator]
  (let [put-op-fields (->> tx-ops
                           (into {} (keep (fn [{:keys [op] :as tx-op}]
                                            (when (= :put op)
                                              (MapEntry/create tx-op
                                                               (for [[k v] (sort-by key (:doc tx-op))]
                                                                 (t/->field (subs (str k) 1) (t/->arrow-type (type v)) true))))))))

        put-op-struct-idxs (->> (vals put-op-fields)
                                (into {} (comp (distinct)
                                               (map-indexed (fn [idx key-fields]
                                                              ;; boxing idx
                                                              (MapEntry/create key-fields idx))))))

        tx-schema (Schema. [(t/->field "tx-ops" (.getType Types$MinorType/DENSEUNION) false
                                       (t/->field "put" (.getType Types$MinorType/STRUCT) false
                                                  (apply t/->field "document" (.getType Types$MinorType/DENSEUNION) false
                                                         (for [[key-fields idx] (sort-by val put-op-struct-idxs)]
                                                           (apply t/->field (str "struct" idx) (.getType Types$MinorType/STRUCT) true key-fields))))
                                       (t/->field "delete" (.getType Types$MinorType/STRUCT) true))])]

    (with-open [root (VectorSchemaRoot/create tx-schema allocator)]
      (let [^DenseUnionVector tx-ops-duv (.getVector root "tx-ops")
            tx-ops-duw (util/->dense-union-writer tx-ops-duv)

            ^StructVector put-vec (.getStruct tx-ops-duv 0)
            ^DenseUnionVector document-vec (.getChild put-vec "document" DenseUnionVector)
            document-duw (util/->dense-union-writer document-vec)]

        (doseq [{:keys [op] :as tx-op} tx-ops]
          (let [tx-op-offset (.writeTypeId tx-ops-duw (case op :put 0, :delete 1))]
            (case op
              :put (do
                     (.setIndexDefined put-vec tx-op-offset)

                     (let [{:keys [doc]} tx-op
                           doc-duv-type-id (put-op-struct-idxs (put-op-fields tx-op))
                           doc-duv-offset (.writeTypeId document-duw doc-duv-type-id)
                           ^StructVector struct-vec (.getStruct document-vec doc-duv-type-id)]

                       (.setIndexDefined struct-vec doc-duv-offset)

                       (doseq [[k v] doc
                               :let [value-vec (.getChild struct-vec (subs (str k) 1) ValueVector)]]
                         (if (some? v)
                           (t/set-safe! value-vec doc-duv-offset v)
                           (t/set-null! value-vec doc-duv-offset)))))

              :delete nil)))

        (.end document-duw)
        (.end tx-ops-duw)

        (.setRowCount root (count tx-ops))

        (.syncSchema root)

        (util/root->byte-buffer root)))))

(defn log-record->tx-instant ^core2.ingest.TransactionInstant [^LogRecord record]
  (ingest/->TransactionInstant (.offset record) (.time record)))

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

(defn latest-metadata-buffer ^java.util.concurrent.CompletableFuture [^ObjectStore os ^BufferPool bp]
  (-> (.listObjects os "metadata-*")

      (util/then-compose
        (fn [ks]
          (if-let [metadata-path (last (sort ks))]
            (.getBuffer bp metadata-path)
            (CompletableFuture/completedFuture nil))))))

(defn with-metadata [^ArrowBuf metadata-buffer, ^BufferAllocator allocator, f]
  (when metadata-buffer
    (with-open [metadata-buffer metadata-buffer]
      (let [footer (util/read-arrow-footer metadata-buffer)
            metadata-batch (first (.getRecordBatches footer))]
        (with-open [metadata-root (VectorSchemaRoot/create (.getSchema footer) allocator)
                    record-batch (util/->arrow-record-batch-view metadata-batch metadata-buffer)]
          (.load (VectorLoader. metadata-root) record-batch)
          (f metadata-root))))))

(defn latest-row-id ^java.util.concurrent.CompletableFuture [^ObjectStore os, ^BufferPool bp, ^BufferAllocator allocator]
  (-> (latest-metadata-buffer os bp)

      (util/then-apply
        (fn [metadata-buffer]
          (with-metadata metadata-buffer allocator
            (fn [metadata-root]
              (meta/max-value metadata-root "_tx-id" "_row-id")))))))

(defn latest-completed-tx ^java.util.concurrent.CompletableFuture [^ObjectStore os, ^BufferPool bp, ^BufferAllocator allocator]
  (-> (latest-metadata-buffer os bp)

      (util/then-apply
        (fn [metadata-buffer]
          (with-metadata metadata-buffer allocator
            (fn [metadata-root]
              (when-let [max-tx-id (meta/max-value metadata-root "_tx-id")]
                (->> (util/local-date-time->date (meta/max-value metadata-root "_tx-time"))
                     (ingest/->TransactionInstant max-tx-id)))))))))

(definterface IIngestLoop
  (^void ingestLoop [])
  (^void start [])
  (^core2.ingest.TransactionInstant latestCompletedTx [])
  (^core2.ingest.TransactionInstant awaitTx [^core2.ingest.TransactionInstant tx])
  (^core2.ingest.TransactionInstant awaitTx [^core2.ingest.TransactionInstant tx, ^java.time.Duration timeout]))

(deftype IngestLoop [^LogReader log-reader
                     ^TransactionIngester ingester
                     ^:volatile-mutable ^TransactionInstant latest-completed-tx
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
          (if-let [log-records (not-empty (.readRecords log-reader (some-> latest-completed-tx .tx-id) batch-size))]
            (doseq [^LogRecord record log-records]
              (if (Thread/interrupted)
                (throw (InterruptedException.))
                (set! (.latest-completed-tx this)
                      (.indexTx ingester (log-record->tx-instant record) (.record record)))))
            (Thread/sleep poll-sleep-ms)))
        (catch ClosedByInterruptException e
          (log/warn e "channel interrupted while closing"))
        (catch InterruptedException _)
        (catch Throwable t
          (log/fatal t "ingest loop stopped")
          (throw t)))))

  (start [this]
    (set! (.ingest-future this)
          (.submit pool ^Runnable #(.ingestLoop this))))

  (latestCompletedTx [_this] latest-completed-tx)

  (awaitTx [this tx] (.awaitTx this tx nil))

  (awaitTx [_this tx timeout]
    (if tx
      (let [{:keys [^Duration poll-sleep-duration],
             :or {poll-sleep-duration (Duration/ofMillis 100)}} ingest-opts
            poll-sleep-ms (.toMillis poll-sleep-duration)
            end-ns (+ (System/nanoTime) (.toNanos timeout))
            tx-id (.tx-id tx)]
        (loop []
          (let [latest-completed-tx latest-completed-tx]
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
      latest-completed-tx))

  Closeable
  (close [_]
    (future-cancel ingest-future)
    (util/shutdown-pool pool)))

(defn ->ingest-loop
  (^core2.core.IngestLoop
   [^LogReader log-reader
    ^TransactionIngester ingester
    ^TransactionInstant latest-completed-tx]
   (->ingest-loop log-reader ingester latest-completed-tx {}))

  (^core2.core.IngestLoop
   [^LogReader log-reader
    ^TransactionIngester ingester
    ^TransactionInstant latest-completed-tx
    ingest-opts]

   (let [pool (Executors/newSingleThreadExecutor (util/->prefix-thread-factory "ingest-loop-"))]
     (doto (IngestLoop. log-reader ingester latest-completed-tx pool nil ingest-opts)
       (.start)))))

(deftype Node [^BufferAllocator allocator
               ^LogReader log-reader
               ^ObjectStore object-store
               ^Ingester ingester
               ^IngestLoop ingest-loop
               ^BufferPool buffer-pool]
  Closeable
  (close [_]
    (util/try-close ingest-loop)
    (util/try-close ingester)
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
         latest-row-id @(latest-row-id object-store buffer-pool allocator)
         latest-completed-tx @(latest-completed-tx object-store buffer-pool allocator)
         ingester (ingest/->ingester allocator object-store latest-row-id opts)
         ingest-loop (->ingest-loop log-reader ingester latest-completed-tx opts)]
     (Node. allocator log-reader object-store ingester ingest-loop buffer-pool))))

(defn -main [& [node-dir :as args]]
  (if node-dir
    (let [node-dir (util/->path node-dir)]
      (->local-node node-dir)
      (println "core2 started in" (str node-dir)))
    (binding [*out* *err*]
      (println "node directory argument required")
      (System/exit 1))))
