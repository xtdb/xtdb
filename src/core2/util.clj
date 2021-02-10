(ns core2.util
  (:require [clojure.java.io :as io]
            [clojure.tools.logging :as log])
  (:import [org.apache.arrow.vector ValueVector VectorSchemaRoot VectorLoader]
           [org.apache.arrow.vector.complex DenseUnionVector]
           [org.apache.arrow.flatbuf Footer Message RecordBatch]
           [org.apache.arrow.memory ArrowBuf BufferAllocator ReferenceManager OwnershipTransferResult]
           org.apache.arrow.memory.util.MemoryUtil
           [org.apache.arrow.vector.ipc.message ArrowBlock ArrowFooter ArrowRecordBatch MessageSerializer]
           [org.apache.arrow.vector.ipc ArrowStreamWriter]
           [java.io ByteArrayOutputStream File]
           java.lang.AutoCloseable
           [java.nio ByteBuffer ByteOrder]
           [java.nio.channels Channels FileChannel FileChannel$MapMode SeekableByteChannel]
           java.nio.charset.StandardCharsets
           [java.nio.file Files FileVisitResult LinkOption OpenOption Path StandardOpenOption SimpleFileVisitor]
           java.nio.file.attribute.FileAttribute
           java.util.Date
           [java.util.function Supplier Function]
           [java.util.concurrent CompletableFuture Executors ExecutorService ThreadFactory TimeUnit]
           java.util.concurrent.atomic.AtomicInteger
           [java.time LocalDateTime ZoneId]))

;;; IO

(defn ->seekable-byte-channel ^java.nio.channels.SeekableByteChannel [^ByteBuffer buffer]
  (let [buffer (.duplicate buffer)]
    (proxy [SeekableByteChannel] []
      (isOpen []
        true)

      (close [])

      (read [^ByteBuffer dst]
        (let [^ByteBuffer src (-> buffer (.slice) (.limit (.remaining dst)))]
          (.put dst src)
          (let [bytes-read (.position src)]
            (.position buffer (+ (.position buffer) bytes-read))
            bytes-read)))

      (position
        ([]
         (.position buffer))
        ([^long new-position]
         (.position buffer new-position)
         this))

      (size []
        (.capacity buffer))

      (write [src]
        (throw (UnsupportedOperationException.)))

      (truncate [size]
        (throw (UnsupportedOperationException.))))))

(defn ->file-channel
  (^java.nio.channels.FileChannel [^Path path]
   (->file-channel path #{StandardOpenOption/READ}))
  (^java.nio.channels.FileChannel [^Path path options]
   (FileChannel/open path (into-array OpenOption options))))

(defn ->mmap-path ^java.nio.MappedByteBuffer [^Path path]
  (with-open [in (->file-channel path)]
    (.map in FileChannel$MapMode/READ_ONLY 0 (.size in))))

(def ^:private file-deletion-visitor
  (proxy [SimpleFileVisitor] []
    (visitFile [file _]
      (Files/delete file)
      FileVisitResult/CONTINUE)

    (postVisitDirectory [dir _]
      (Files/delete dir)
      FileVisitResult/CONTINUE)))

(defn path-exists [^Path path]
  (Files/exists path (make-array LinkOption 0)))

(defn delete-dir [^Path dir]
  (when (path-exists dir)
    (Files/walkFileTree dir file-deletion-visitor)))

(defn mkdirs [^Path path]
  (Files/createDirectories path (make-array FileAttribute 0)))

(defn ->path ^Path [^String path]
  (.toPath (io/file path)))

(def ^:private ^ZoneId utc (ZoneId/of "UTC"))

(defn local-date-time->date ^java.util.Date [^LocalDateTime ldt]
  (Date/from (.toInstant (.atZone ldt utc))))

(defn ->supplier {:style/indent :defn} ^java.util.function.Supplier [f]
  (reify Supplier
    (get [_]
      (f))))

(defn ->jfn {:style/indent :defn} ^java.util.function.Function [f]
  (reify Function
    (apply [_ v]
      (f v))))

(defn then-apply {:style/indent :defn} [^CompletableFuture fut f]
  (.thenApply fut (->jfn f)))

(defn then-compose {:style/indent :defn} [^CompletableFuture fut f]
  (.thenCompose fut (->jfn f)))

(defmacro completable-future {:style/indent 1} [pool & body]
  `(CompletableFuture/supplyAsync (->supplier (fn [] ~@body)) ~pool))

(defn ->prefix-thread-factory ^java.util.concurrent.ThreadFactory [^String prefix]
  (let [default-thread-factory (Executors/defaultThreadFactory)]
    (reify ThreadFactory
      (newThread [_ r]
        (let [t (.newThread default-thread-factory r)]
          (.setName t (str prefix (.getName t)))
          t)))))

(defn try-close [c]
  (try
    (when (instance? AutoCloseable c)
      (.close ^AutoCloseable c))
    (catch Exception e
      (log/warn e "could not close"))))

(defn shutdown-pool
  ([^ExecutorService pool]
   (shutdown-pool pool 60))
  ([^ExecutorService pool ^long timeout-seconds]
   (try
     (.shutdown pool)
     (when-not (.awaitTermination pool timeout-seconds TimeUnit/SECONDS)
       (.shutdownNow pool)
       (when-not (.awaitTermination pool timeout-seconds TimeUnit/SECONDS)
         (log/warn "pool did not terminate" pool)))
     (catch InterruptedException _
       (.shutdownNow pool)
       (.interrupt (Thread/currentThread))))))

;;; Arrow

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

(defn ->dense-union-writer ^core2.util.DenseUnionWriter [^DenseUnionVector duv]
  (->DenseUnionWriterImpl duv
                          (.getValueCount duv)
                          (int-array (for [^ValueVector child-vec (.getChildrenFromFields duv)]
                                       (.getValueCount child-vec)))))

(defn root->arrow-ipc-byte-buffer ^java.nio.ByteBuffer [^VectorSchemaRoot root]
  (with-open [baos (ByteArrayOutputStream.)
              sw (ArrowStreamWriter. root nil (Channels/newChannel baos))]
    (doto sw
      (.start)
      (.writeBatch)
      (.end))
    (ByteBuffer/wrap (.toByteArray baos))))

(def ^:private ^{:tag 'bytes} arrow-magic (.getBytes "ARROW1" StandardCharsets/UTF_8))

(defn- validate-arrow-magic [^ArrowBuf ipc-file-format-buffer]
  (dotimes [n (alength arrow-magic)]
    (when-not (= (.getByte ipc-file-format-buffer
                           (dec (- (.capacity ipc-file-format-buffer) n)))
                 (aget arrow-magic (dec (- (alength arrow-magic) n))))
      (throw (IllegalArgumentException. "invalid Arrow IPC file format")))))

(defn read-arrow-footer ^org.apache.arrow.vector.ipc.message.ArrowFooter [^ArrowBuf ipc-file-format-buffer]
  (validate-arrow-magic ipc-file-format-buffer)
  (let [footer-size-offset (- (.capacity ipc-file-format-buffer) (+ Integer/BYTES (alength arrow-magic)))
        footer-size (.getInt ipc-file-format-buffer footer-size-offset)
        footer-position (- footer-size-offset footer-size)
        footer-bb (.nioBuffer (.slice ipc-file-format-buffer footer-position footer-size))]
    (ArrowFooter. (Footer/getRootAsFooter footer-bb))))

(def ^:private try-free-direct-buffer
  (try
    (eval
     '(fn free-direct-buffer [nio-buffer]
        (io.netty.util.internal.PlatformDependent/freeDirectBuffer nio-buffer)))
    (catch Exception e
      (fn free-dircect-buffer-nop [_]))))

(deftype NioViewReferenceManager [^BufferAllocator allocator ^:volatile-mutable ^ByteBuffer nio-buffer ^AtomicInteger ref-count]
  ReferenceManager
  (deriveBuffer [this source-buffer index length]
    (ArrowBuf. this
               nil
               length
               (+ (.memoryAddress source-buffer) index)))

  (getAccountedSize [this]
    (.getSize this))

  (getAllocator [this]
    allocator)

  (getRefCount [this]
    (.get ref-count))

  (getSize [this]
    (if-let [nio-buffer nio-buffer]
      (.capacity nio-buffer)
      0))

  (release [this]
    (.release this 1))

  (release [this decrement]
    (when-not (pos? decrement)
      (throw (IllegalArgumentException. "decrement must be positive")))
    (let [ref-count (.addAndGet ref-count (- decrement))])
    (cond
      (zero? ref-count)
      (let [nio-buffer nio-buffer]
        (set! (.nio-buffer this) nil)
        (try-free-direct-buffer nio-buffer)
        true)

      (neg? ref-count)
      (throw (IllegalStateException. "ref count has gone negative"))

      :else
      false))

  (retain [this]
    (.retain this 1))

  (retain [this src-buffer allocator]
    (when-not (identical? allocator (.getAllocator this))
      (throw (IllegalStateException. "cannot retain nio buffer in other allocator")))
    (doto (.slice src-buffer)
      (.readerIndex (.readerIndex src-buffer))
      (.writerIndex (.writerIndex src-buffer))
      (-> (.getReferenceManager) (.retain))))

  (retain [this increment]
    (when-not (pos? increment)
      (throw (IllegalArgumentException. "increment must be positive")))
    (let [ref-count (.getAndAdd ref-count increment)]
      (when-not (pos? ref-count)
        (throw (IllegalStateException. "ref count was at zero")))))

  (transferOwnership [this source-buffer target-allocator]
    (let [source-buffer (.retain this source-buffer target-allocator)]
      (reify OwnershipTransferResult
        (getAllocationFit [this]
          true)

        (getTransferredBuffer [this]
          source-buffer)))))

(defn ->arrow-buf-view ^org.apache.arrow.memory.ArrowBuf [^BufferAllocator allocator ^ByteBuffer nio-buffer]
  (when-not (.isDirect nio-buffer)
    (throw (IllegalArgumentException. (str "not a direct buffer: " nio-buffer))))
  (ArrowBuf. (->NioViewReferenceManager allocator nio-buffer (AtomicInteger. 1))
             nil
             (.capacity nio-buffer)
             (MemoryUtil/getByteBufferAddress nio-buffer)))

(defn ->arrow-record-batch-view ^org.apache.arrow.vector.ipc.message.ArrowRecordBatch [^ArrowBlock block ^ArrowBuf buffer]
  (let [prefix-size (if (= (.getInt buffer (.getOffset block)) MessageSerializer/IPC_CONTINUATION_TOKEN)
                      8
                      4)
        ^RecordBatch batch (.header (Message/getRootAsMessage
                                     (.nioBuffer buffer
                                                 (+ (.getOffset block) prefix-size)
                                                 (- (.getMetadataLength block) prefix-size)))
                                    (RecordBatch.))
        body-buffer (doto (.slice buffer
                                  (+ (.getOffset block)
                                     (.getMetadataLength block))
                                  (.getBodyLength block))
                      (.retain))]
    (MessageSerializer/deserializeRecordBatch batch body-buffer)))

(defn block-stream [^ArrowBuf ipc-file-format-buffer ^BufferAllocator allocator]
  (when ipc-file-format-buffer
    ;; `Stream`, when we go to Java
    (reify
      clojure.lang.IReduceInit
      (reduce [_ f init]
        (let [footer (read-arrow-footer ipc-file-format-buffer)]
          (with-open [root (VectorSchemaRoot/create (.getSchema footer) allocator)]
            (let [loader (VectorLoader. root)]
              (f (reduce
                  (fn [acc record-batch]
                    (with-open [batch (->arrow-record-batch-view record-batch ipc-file-format-buffer)]
                      (.load loader batch)
                      (f acc root)))
                  init
                  (.getRecordBatches footer))))))))))
