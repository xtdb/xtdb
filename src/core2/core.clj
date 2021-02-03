(ns core2.core
  (:require [core2.types :as t]
            [core2.ingest :as ingest]
            [core2.object-store :as os]
            [core2.log :as log]
            [clojure.string :as str]
            [core2.util :as util]
            [clojure.java.io :as io]
            [core2.metadata :as meta])
  (:import [java.io ByteArrayOutputStream Closeable]
           [java.nio ByteBuffer ByteOrder]
           [java.nio.channels Channels SeekableByteChannel]
           [java.nio.file Files OpenOption StandardOpenOption]
           java.nio.file.attribute.FileAttribute
           [java.time Duration Instant]
           [java.util.concurrent CompletableFuture Executors ExecutorService TimeUnit]
           [org.apache.arrow.flatbuf Footer]
           [org.apache.arrow.memory BufferAllocator RootAllocator]
           [org.apache.arrow.vector ValueVector VectorSchemaRoot]
           [org.apache.arrow.vector.complex DenseUnionVector StructVector]
           [org.apache.arrow.vector.ipc ArrowFileReader ArrowStreamWriter]
           [org.apache.arrow.vector.ipc.message ArrowFooter]
           [org.apache.arrow.vector.types Types$MinorType]
           [org.apache.arrow.vector.types.pojo Field Schema]
           [core2.ingest TransactionIngester TransactionInstant Ingester]
           [core2.object_store ObjectStore]
           [core2.log LogRecord LogReader LogWriter]))

(set! *unchecked-math* :warn-on-boxed)

(definterface TxProducer
  (^java.util.concurrent.CompletableFuture submitTx [_ ]))

(def ^:private tx-arrow-schema
  (Schema. [(t/->field "tx-ops" (.getType Types$MinorType/DENSEUNION) false
                       (t/->field "put" (.getType Types$MinorType/STRUCT) false
                                  (t/->field "document" (.getType Types$MinorType/DENSEUNION) false))
                       (t/->field "delete" (.getType Types$MinorType/STRUCT) true))]))

(defn- add-element-to-union! [^DenseUnionVector duv type-id ^long parent-offset child-offset]
  (while (< (.getValueCapacity duv) (inc parent-offset))
    (.reAlloc duv))

  (.setTypeId duv parent-offset type-id)
  (.setInt (.getOffsetBuffer duv)
           (* DenseUnionVector/OFFSET_WIDTH parent-offset)
           child-offset))

(defn serialize-tx-ops ^java.nio.ByteBuffer [tx-ops ^BufferAllocator allocator]
  (with-open [root (VectorSchemaRoot/create tx-arrow-schema allocator)]
    (let [^DenseUnionVector tx-op-vec (.getVector root "tx-ops")

          union-type-ids (into {} (map vector
                                       (->> (.getChildren (.getField tx-op-vec))
                                            (map #(keyword (.getName ^Field %))))
                                       (range)))]

      (->> (map-indexed vector tx-ops)
           (reduce (fn [acc [op-idx {:keys [op] :as tx-op}]]
                     (let [^int per-op-offset (get-in acc [:per-op-offsets op] 0)
                           ^int op-type-id (get union-type-ids op)]

                       (add-element-to-union! tx-op-vec op-type-id op-idx per-op-offset)

                       (case op
                         :put (let [{:keys [doc]} tx-op

                                    ^StructVector put-vec (.getStruct tx-op-vec op-type-id)
                                    ^DenseUnionVector put-doc-vec (.getChild put-vec "document" DenseUnionVector)]

                                (.setIndexDefined put-vec per-op-offset)

                                ;; TODO we could/should key this just by the field name, and have a promotable union in the value.
                                ;; but, for now, it's keyed by both field name and type.
                                (let [doc-fields (->> (for [[k v] (sort-by key doc)]
                                                        [k (t/->field (name k) (t/->arrow-type (type v)) true)])
                                                      (into (sorted-map)))
                                      field-k (format "%08x" (hash doc-fields))
                                      ^Field doc-field (apply t/->field field-k (.getType Types$MinorType/STRUCT) true (vals doc-fields))
                                      field-type-id (or (->> (map-indexed vector (keys (get-in acc [:put :per-struct-offsets])))
                                                             (some (fn [[idx ^Field field]]
                                                                     (when (= doc-field field)
                                                                       idx))))
                                                        (.registerNewTypeId put-doc-vec doc-field))
                                      struct-vec (.getStruct put-doc-vec field-type-id)
                                      per-struct-offset (long (get-in acc [:put :per-struct-offsets doc-field] 0))]

                                  (.setIndexDefined struct-vec per-struct-offset)

                                  (add-element-to-union! put-doc-vec field-type-id per-op-offset per-struct-offset)

                                  (doseq [[k v] doc
                                          :let [^Field field (get doc-fields k)
                                                field-vec (.addOrGet struct-vec (name k) (.getFieldType field) ValueVector)]]
                                    (if (some? v)
                                      (t/set-safe! field-vec per-struct-offset v)
                                      (t/set-null! field-vec per-struct-offset)))

                                  (-> acc
                                      (assoc-in [:per-op-offsets :put] (inc per-op-offset))
                                      (assoc-in [:put :per-struct-offsets doc-field] (inc per-struct-offset))))))))
                   {}))

      (.setRowCount root (count tx-ops))
      (.syncSchema root)

      (with-open [baos (ByteArrayOutputStream.)
                  sw (ArrowStreamWriter. root nil (Channels/newChannel baos))]
        (doto sw
          (.start)
          (.writeBatch)
          (.end))
        (ByteBuffer/wrap (.toByteArray baos))))))

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

(defn with-metadata [path ^BufferAllocator allocator f]
  (when path
    (with-open [file-ch (Files/newByteChannel path (into-array OpenOption #{StandardOpenOption/READ}))
                file-reader (ArrowFileReader. file-ch allocator)]
      (.loadNextBatch file-reader)
      (f (.getVectorSchemaRoot file-reader)))))

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
                (ingest/->TransactionInstant max-tx-id
                                             (-> (meta/max-value metadata-root "_tx-time")
                                                 util/local-date-time->date)))))))))

(defn index-all-available-transactions
  ([^LogReader log-reader
    ^TransactionIngester ingester
    ^TransactionInstant latest-completed-tx]
   (index-all-available-transactions log-reader ingester latest-completed-tx {}))

  ([^LogReader log-reader
    ^TransactionIngester ingester
    ^TransactionInstant latest-completed-tx
    {:keys [batch-size], :or {batch-size 100}}]

   (loop [latest-completed-tx latest-completed-tx]
     (if-let [log-records (seq (.readRecords log-reader (some-> latest-completed-tx .tx-id) batch-size))]
       (let [latest-completed-tx (reduce (fn [_ ^LogRecord record]
                                           (let [tx-instant (log-record->tx-instant record)]
                                             (.indexTx ingester tx-instant (.record record))

                                             (when (Thread/interrupted)
                                               (throw (InterruptedException.)))

                                             tx-instant))
                                         nil
                                         log-records)]

         (when (Thread/interrupted)
           (throw (InterruptedException.)))

         (recur latest-completed-tx))

       latest-completed-tx))))

(definterface IIngestLoop
  (^void ingestLoop [])
  (^core2.ingest.TransactionInstant latestCompletedTx [])
  (^core2.ingest.TransactionInstant awaitTx [^core2.ingest.TransactionInstant tx])
  (^core2.ingest.TransactionInstant awaitTx [^core2.ingest.TransactionInstant tx, ^java.time.Duration timeout]))

(deftype IngestLoop [^LogReader log-reader
                     ^TransactionIngester ingester
                     ^:unsynchronized-mutable ^TransactionInstant latest-completed-tx
                     ^ExecutorService pool
                     ingest-opts]
  IIngestLoop
  (ingestLoop [this]
    (let [{:keys [^Duration poll-sleep-duration],
           :or {poll-sleep-duration (Duration/ofMillis 100)}} ingest-opts]
      (try
        (while true
          (let [next-completed-tx (index-all-available-transactions log-reader ingester latest-completed-tx ingest-opts)]
            (if (= latest-completed-tx next-completed-tx)
              (Thread/sleep (.toMillis poll-sleep-duration))
              (set! (.latest-completed-tx this) next-completed-tx))))
        (catch InterruptedException _))))

  (latestCompletedTx [_this] latest-completed-tx)

  (awaitTx [this tx] (.awaitTx this tx nil))

  (awaitTx [_this tx timeout]
    (if tx
      (let [{:keys [^Duration poll-sleep-duration],
             :or {poll-sleep-duration (Duration/ofMillis 100)}} ingest-opts
            start (Instant/now)
            tx-id (.tx-id tx)]
        (loop []
          (let [latest-completed-tx latest-completed-tx]
            (if (and (or (nil? latest-completed-tx)
                         (< (.tx-id latest-completed-tx) tx-id))
                     (or (nil? timeout)
                         (.isBefore (Instant/now) (.plus start timeout))))
              (do
                (Thread/sleep (.toMillis poll-sleep-duration))
                (recur))
              latest-completed-tx))))
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
         ingest-loop (IngestLoop. log-reader ingester latest-completed-tx pool ingest-opts)]
     (.submit pool ^Runnable #(.ingestLoop ingest-loop))
     ingest-loop)))

(def ^:private ^{:tag 'long} arrow-magic-size (alength (.getBytes "ARROW1" "UTF-8")))

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

(defn ->local-node ^core2.core.Node [node-dir]
  (let [object-dir (io/file node-dir "objects")
        log-dir (io/file node-dir "log")
        allocator (RootAllocator.)
        log-reader (log/->local-directory-log-reader log-dir)
        object-store (os/->file-system-object-store (.toPath object-dir))
        ingester (ingest/->ingester allocator object-store @(latest-row-id object-store allocator))
        ingest-loop (->ingest-loop log-reader ingester @(latest-completed-tx object-store allocator))]
    (Node. allocator log-reader object-store ingester ingest-loop)))
