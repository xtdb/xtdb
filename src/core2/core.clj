(ns core2.core
  (:require [clojure.java.io :as io]
            [core2.ingest :as ingest]
            [core2.log :as log]
            [core2.metadata :as meta]
            [core2.object-store :as os]
            [core2.types :as t]
            [core2.util :as util])
  (:import clojure.lang.MapEntry
           [core2.ingest Ingester TransactionIngester TransactionInstant]
           [core2.log LogReader LogRecord LogWriter]
           core2.object_store.ObjectStore
           [java.io ByteArrayOutputStream Closeable]
           [java.nio ByteBuffer ByteOrder]
           [java.nio.channels Channels SeekableByteChannel]
           java.nio.charset.StandardCharsets
           [java.nio.file Files OpenOption StandardOpenOption]
           java.nio.file.attribute.FileAttribute
           [java.time Duration Instant]
           [java.util.concurrent CompletableFuture Executors ExecutorService TimeUnit]
           org.apache.arrow.flatbuf.Footer
           [org.apache.arrow.memory BufferAllocator RootAllocator]
           [org.apache.arrow.vector ValueVector VectorSchemaRoot]
           [org.apache.arrow.vector.complex DenseUnionVector StructVector]
           [org.apache.arrow.vector.ipc ArrowFileReader ArrowStreamWriter]
           org.apache.arrow.vector.ipc.message.ArrowFooter
           org.apache.arrow.vector.types.pojo.Schema
           org.apache.arrow.vector.types.Types$MinorType))

(set! *unchecked-math* :warn-on-boxed)

(definterface TxProducer
  (^java.util.concurrent.CompletableFuture submitTx [_ ]))

(defn- root->byte-buffer [^VectorSchemaRoot root]
  (with-open [baos (ByteArrayOutputStream.)
              sw (ArrowStreamWriter. root nil (Channels/newChannel baos))]
    (doto sw
      (.start)
      (.writeBatch)
      (.end))
    (ByteBuffer/wrap (.toByteArray baos))))

(definterface DenseUnionWriter
  (^int writeTypeId [^byte type-id])
  (^void end []))

(deftype DenseUnionWriterImpl [^DenseUnionVector duv
                               ^:unsynchronized-mutable ^int value-count
                               ^ints offsets]
  DenseUnionWriter
  (writeTypeId [this type-id]
    (while (< (.getValueCapacity duv) (inc value-count))
      (.reAlloc duv))

    (let [offset (aget offsets type-id)
          offset-buffer (.getOffsetBuffer duv)]
      (.setTypeId duv value-count type-id)
      (.setInt offset-buffer (* DenseUnionVector/OFFSET_WIDTH value-count) offset)

      (set! (.value-count this) (inc value-count))
      (aset offsets type-id (inc offset))

      offset))

  (end [_]
    (.setValueCount duv value-count)))

(defn ->dense-union-writer ^core2.core.DenseUnionWriter [^DenseUnionVector duv]
  (->DenseUnionWriterImpl duv (.getValueCount duv) (int-array (count (.getChildren (.getField duv))))))

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
            tx-ops-duw (->dense-union-writer tx-ops-duv)

            ^StructVector put-vec (.getStruct tx-ops-duv 0)
            ^DenseUnionVector document-vec (.getChild put-vec "document" DenseUnionVector)
            document-duw (->dense-union-writer document-vec)]

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

        (root->byte-buffer root)))))

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
    (.close ^Closeable log-writer)
    (.close allocator)))

(defn ->local-tx-producer
  (^core2.core.LogTxProducer [node-dir]
   (->local-tx-producer node-dir {}))
  (^core2.core.LogTxProducer [node-dir log-writer-opts]
   (LogTxProducer. (log/->local-directory-log-writer (io/file node-dir "log") log-writer-opts)
                   (RootAllocator.))))

(defn block-stream [path ^BufferAllocator allocator]
  (when path
    ;; `Stream`, when we go to Java
    (reify
      clojure.lang.IReduceInit
      (reduce [_ f init]
        (with-open [file-ch (Files/newByteChannel path (into-array OpenOption #{StandardOpenOption/READ}))
                    file-reader (ArrowFileReader. file-ch allocator)]
          (let [root (.getVectorSchemaRoot file-reader)]
            (loop [v init]
              (if (.loadNextBatch file-reader)
                (recur (f v root))
                (f v)))))))))

(defn with-metadata [path ^BufferAllocator allocator f]
  (->> (block-stream path allocator)
       (reduce (completing
                (fn [_ metadata-root]
                  (f metadata-root)))
               nil)))

(defn latest-metadata-object ^java.util.concurrent.CompletableFuture [^ObjectStore os]
  (-> (.listObjects os "metadata-*")

      (util/then-compose
        (fn [ks]
          (if-let [metadata-path (last (sort ks))]
            (let [tmp-path (doto (Files/createTempFile metadata-path "" (make-array FileAttribute 0))
                             (-> .toFile .deleteOnExit))]
              (.getObject os metadata-path tmp-path))
            (CompletableFuture/completedFuture nil))))))

(defn latest-row-id ^java.util.concurrent.CompletableFuture [^ObjectStore os, ^BufferAllocator allocator]
  (-> (latest-metadata-object os)

      (util/then-apply
        (fn [path]
          (with-metadata path allocator
            (fn [^VectorSchemaRoot metadata-root]
              (meta/max-value metadata-root "_tx-id" "_row-id")))))))

(defn latest-completed-tx ^java.util.concurrent.CompletableFuture [^ObjectStore os, ^BufferAllocator allocator]
  (-> (latest-metadata-object os)

      (util/then-apply
        (fn [path]
          (with-metadata path allocator
            (fn [^VectorSchemaRoot metadata-root]
              (when-let [max-tx-id (meta/max-value metadata-root "_tx-id")]
                (->> (util/local-date-time->date (meta/max-value metadata-root "_tx-time"))
                     (ingest/->TransactionInstant max-tx-id)))))))))

(definterface IIngestLoop
  (^void ingestLoop [])
  (^core2.ingest.TransactionInstant latestCompletedTx [])
  (^core2.ingest.TransactionInstant awaitTx [^core2.ingest.TransactionInstant tx])
  (^core2.ingest.TransactionInstant awaitTx [^core2.ingest.TransactionInstant tx, ^java.time.Duration timeout]))

(deftype IngestLoop [^LogReader log-reader
                     ^TransactionIngester ingester
                     ^:volatile-mutable ^TransactionInstant latest-completed-tx
                     ^ExecutorService pool
                     ^:unsynchronized-mutable ^Throwable ingest-error
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
                (try
                  (set! (.latest-completed-tx this)
                        (.indexTx ingester (log-record->tx-instant record) (.record record)))
                  (catch Throwable t
                    (set! (.ingest-error this) t)
                    (throw t)))))
            (Thread/sleep poll-sleep-ms)))
        (catch InterruptedException _))))

  (latestCompletedTx [_this] latest-completed-tx)

  (awaitTx [this tx] (.awaitTx this tx nil))

  (awaitTx [_this tx timeout]
    (if tx
      (let [{:keys [^Duration poll-sleep-duration],
             :or {poll-sleep-duration (Duration/ofMillis 100)}} ingest-opts
            end (.plus (Instant/now) timeout)
            tx-id (.tx-id tx)]
        (loop []
          (let [latest-completed-tx latest-completed-tx]
            (cond
              ingest-error (throw (ex-info "Error during ingestion" {} ingest-error))

              (and latest-completed-tx
                   (>= (.tx-id latest-completed-tx) tx-id))
              latest-completed-tx

              (or (nil? timeout)
                  (.isBefore (Instant/now) end))
              (do
                (Thread/sleep (.toMillis poll-sleep-duration))
                (recur))

              :else (throw (ex-info "await-tx timed out" {:requested tx, :available latest-completed-tx}))))))
      latest-completed-tx))

  Closeable
  (close [_]
    (doto pool
      (.shutdownNow)
      (.awaitTermination 5 TimeUnit/SECONDS))))

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

   (let [pool (Executors/newSingleThreadExecutor)
         ingest-loop (IngestLoop. log-reader ingester latest-completed-tx pool nil ingest-opts)]
     (.submit pool ^Runnable #(.ingestLoop ingest-loop))
     ingest-loop)))

(def ^:private ^{:tag 'long} arrow-magic-size (alength (.getBytes "ARROW1" StandardCharsets/UTF_8)))

(defn read-footer-position ^long [^SeekableByteChannel in]
  (let [footer-size-bb (.order (ByteBuffer/allocate Integer/BYTES) ByteOrder/LITTLE_ENDIAN)
        footer-size-offset (- (.size in) (+ (.capacity footer-size-bb) arrow-magic-size))]
    (.position in footer-size-offset)
    (while (pos? (.read in footer-size-bb)))
    (- footer-size-offset (.getInt footer-size-bb 0))))

(defn read-footer ^org.apache.arrow.vector.ipc.message.ArrowFooter [^SeekableByteChannel in]
  (let [footer-position (read-footer-position in)
        footer-size (- (.size in) footer-position)
        bb (ByteBuffer/allocate footer-size)]
    (.position in footer-position)
    (while (pos? (.read in bb)))
    (.flip bb)
    (ArrowFooter. (Footer/getRootAsFooter bb))))

(deftype Node [^BufferAllocator allocator
               ^LogReader log-reader
               ^ObjectStore object-store
               ^Ingester ingester
               ^IngestLoop ingest-loop]
  Closeable
  (close [_]
    (.close ingest-loop)
    (.close ingester)
    (.close ^Closeable object-store)
    (.close ^Closeable log-reader)
    (.close allocator)))

(defn ->local-node
  (^core2.core.Node [node-dir]
   (->local-node node-dir {}))
  (^core2.core.Node [node-dir opts]
   (let [object-dir (io/file node-dir "objects")
         log-dir (io/file node-dir "log")
         allocator (RootAllocator.)
         log-reader (log/->local-directory-log-reader log-dir)
         object-store (os/->file-system-object-store (.toPath object-dir) opts)
         ingester (ingest/->ingester allocator object-store @(latest-row-id object-store allocator) opts)
         ingest-loop (->ingest-loop log-reader ingester @(latest-completed-tx object-store allocator) opts)]
     (Node. allocator log-reader object-store ingester ingest-loop))))
