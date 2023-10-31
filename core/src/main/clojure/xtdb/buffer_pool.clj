(ns xtdb.buffer-pool
  (:require [clojure.string :as str]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.object-store]
            [xtdb.util :as util]
            [xtdb.object-store :as os])
  (:import java.io.Closeable
           (clojure.lang PersistentQueue)
           (java.nio ByteBuffer)
           (java.nio.channels FileChannel WritableByteChannel)
           [java.nio.file FileVisitOption Files LinkOption Path StandardOpenOption]
           (java.nio.file.attribute FileAttribute)
           [java.util Map NavigableMap TreeMap]
           [java.util.concurrent CompletableFuture]
           (java.util.concurrent.atomic AtomicLong)
           java.util.NavigableMap
           [org.apache.arrow.memory ArrowBuf BufferAllocator]
           (org.apache.arrow.vector VectorSchemaRoot)
           (org.apache.arrow.vector.ipc ArrowFileWriter)
           (org.apache.arrow.vector.ipc.message ArrowBlock ArrowFooter ArrowRecordBatch)
           xtdb.object_store.ObjectStore
           xtdb.util.ArrowBufLRU
           (xtdb IBufferPool)
           (xtdb.object_store IMultipartUpload SupportsMultipart)))

(set! *unchecked-math* :warn-on-boxed)

(def ^:private min-multipart-part-size (* 5 1024 1024))
(def ^:private max-multipart-per-upload-concurrency 4)

(defn- free-memory [^Map memory-store]
  (locking memory-store
    (run! util/close (.values memory-store))
    (.clear memory-store)))

(defn- retain [^ArrowBuf buf] (.retain (.getReferenceManager buf)) buf)

(defn- cache-get ^ArrowBuf [^Map memory-store k]
  (locking memory-store
    (some-> (.get memory-store k) retain)))

(def ^AtomicLong cache-miss-byte-counter (AtomicLong.))
(def ^AtomicLong cache-hit-byte-counter (AtomicLong.))
(def io-wait-nanos-counter (atom 0N))

(defn clear-cache-counters []
  (.set cache-miss-byte-counter 0)
  (.set cache-hit-byte-counter 0)
  (reset! io-wait-nanos-counter 0N))

(defn- record-cache-miss [^ArrowBuf arrow-buf]
  (.addAndGet cache-miss-byte-counter (.capacity arrow-buf)))

(defn- record-cache-hit [^ArrowBuf arrow-buf]
  (.addAndGet cache-hit-byte-counter (.capacity arrow-buf)))

(defn- record-io-wait [^long start-ns]
  (swap! io-wait-nanos-counter +' (- (System/nanoTime) start-ns)))

(defn- cache-compute
  "Returns a pair [hit-or-miss, buf] computing the cached ArrowBuf from (f) if needed.
  `hit-or-miss` is true if the buffer was found, false if the object was added as part of this call."
  [^Map memory-store k f]
  (locking memory-store
    (let [hit (.containsKey memory-store k)
          arrow-buf (if hit (.get memory-store k) (let [buf (f)] (.put memory-store k buf) buf))]
      (if hit (record-cache-hit arrow-buf) (record-cache-miss arrow-buf))
      [hit (retain arrow-buf)])))

(defn- upload-multipart-buffers [object-store k nio-buffers]
  (let [^IMultipartUpload upload @(.startMultipart ^SupportsMultipart object-store k)]
    (try

      (loop [part-queue (into PersistentQueue/EMPTY nio-buffers)
             waiting-parts []]
        (cond
          (empty? part-queue) @(CompletableFuture/allOf (into-array CompletableFuture waiting-parts))

          (< (count waiting-parts) (int max-multipart-per-upload-concurrency))
          (recur (pop part-queue) (conj waiting-parts (.uploadPart upload ^ByteBuffer (peek part-queue))))

          :else
          (do @(CompletableFuture/anyOf (into-array CompletableFuture waiting-parts))
              (recur part-queue (vec (remove future-done? waiting-parts))))))

      @(.complete upload)

      (catch Throwable upload-loop-t
        (try
          @(.abort upload)
          (catch Throwable abort-t
            (.addSuppressed upload-loop-t abort-t)))
        (throw upload-loop-t)))))

(defn- put-object [^IBufferPool bp k ^ByteBuffer buffer]
  (try
    (let [buffer (.duplicate buffer)]
      (with-open [ch (.openChannel bp k)]
        (while (.hasRemaining buffer)
          (.write ch buffer)))
      (CompletableFuture/completedFuture nil))
    (catch Throwable t
      (CompletableFuture/failedFuture t))))

(deftype InMemoryWritableChannel
  [allocator
   memory-store
   ^:volatile-mutable ^ByteBuffer buffer
   k
   ^:volatile-mutable open]
  WritableByteChannel
  (isOpen [_] open)

  (close [this]
    (let [create-arrow-buf #(util/->arrow-buf-view allocator (.flip buffer))
          [_ arrow-buf] (cache-compute memory-store k create-arrow-buf)]
      ;; cache-compute returns the buffer, so must increment its reference count
      ;; we are not interested in doing work with the buffer (just to putting it in the cache), hence util/close.
      (util/close arrow-buf)
      (set! (.-open this) false)))

  (write [this src]
    (let [remaining (.remaining src)]
      (loop [src src]
        (let [^ByteBuffer buffer (.-buffer this)]
          (if (<= (.remaining src) (.remaining buffer))
            (.put buffer src)
            (let [new-buffer (ByteBuffer/allocateDirect (* 2 (.capacity buffer)))]
              (.put new-buffer (.flip buffer))
              (set! (.-buffer this) new-buffer)
              (recur src)))))
      remaining)))

(defrecord InMemoryBufferPool
  [allocator
   ^NavigableMap memory-store]
  Closeable
  (close [_]
    (free-memory memory-store)
    (util/close allocator))
  IBufferPool
  (getBuffer [_ k]
    (let [cached-buffer (cache-get memory-store k)]
      (cond
        (nil? k)
        (CompletableFuture/completedFuture nil)

        cached-buffer
        (do (record-cache-hit cached-buffer)
            (CompletableFuture/completedFuture cached-buffer))

        :else
        (CompletableFuture/failedFuture (os/obj-missing-exception k)))))

  (listObjects [_]
    (locking memory-store (vec (.keySet ^NavigableMap memory-store))))

  (listObjects [_ dir]
    (locking memory-store
      (->> (.keySet (.tailMap ^NavigableMap memory-store dir))
           (into [] (take-while #(str/starts-with? % dir)))
           (vec))))

  (openChannel [this k] (->InMemoryWritableChannel allocator memory-store (ByteBuffer/allocateDirect 32) k true))

  (openArrowFileWriter [this k vsr] (ArrowFileWriter. vsr nil (.openChannel this k)))

  (putObject [this k buffer] (put-object this k buffer)))

(defn- create-tmp-path ^Path [^Path disk-store]
  (doto (.resolve disk-store (str ".tmp/" (random-uuid)))
    (util/create-parents)))

(defn- open-tmp-file ^FileChannel [^Path tmp-path]
  (util/->file-channel tmp-path [StandardOpenOption/CREATE_NEW, StandardOpenOption/WRITE]))

(deftype LocalWritableChannel
  [^IBufferPool bp
   allocator
   memory-store
   ^Path disk-store
   ^FileChannel file-channel
   tmp-path
   ^String k]
  WritableByteChannel
  (isOpen [_] (.isOpen file-channel))

  (close [this]
    (.close file-channel)
    (let [file-path (.resolve disk-store k)]
      (util/create-parents file-path)
      ;; see #2847
      (util/atomic-move tmp-path file-path)
      (util/then-apply (.getBuffer bp k) util/close)))

  (write [_ src] (.write file-channel src)))

(defrecord LocalBufferPool
  [allocator
   ^ArrowBufLRU memory-store
   ^Path disk-store]
  Closeable
  (close [_]
    (free-memory memory-store)
    (util/close allocator))
  IBufferPool
  (getBuffer [_ k]
    (let [cached-buffer (cache-get memory-store k)]
      (cond
        (nil? k)
        (CompletableFuture/completedFuture nil)

        cached-buffer
        (do (record-cache-hit cached-buffer)
            (CompletableFuture/completedFuture cached-buffer))

        :else
        (let [buffer-cache-path (.resolve disk-store (str k))]
          (-> (if (util/path-exists buffer-cache-path)
                ;; todo could this not race with eviction? e.g exists for this cond, but is evicted before we can map the file into the cache?
                (CompletableFuture/completedFuture buffer-cache-path)
                (CompletableFuture/failedFuture (os/obj-missing-exception k)))
              (util/then-apply
                (fn [path]
                  (let [nio-buffer (util/->mmap-path path)
                        create-arrow-buf #(util/->arrow-buf-view allocator nio-buffer)
                        [_ buf] (cache-compute memory-store k create-arrow-buf)]
                    buf))))))))

  (listObjects [_]
    (with-open [dir-stream (Files/walk disk-store (make-array FileVisitOption 0))]
      (vec (sort (for [^Path path (iterator-seq (.iterator dir-stream))
                       :when (Files/isRegularFile path (make-array LinkOption 0))]
                   (str (.relativize disk-store path)))))))

  (listObjects [_ dir]
    (let [dir (.resolve disk-store dir)]
      (when (Files/exists dir (make-array LinkOption 0))
        (with-open [dir-stream (Files/newDirectoryStream dir)]
          (vec (sort (for [^Path path dir-stream]
                       (str (.relativize disk-store path)))))))))

  (openChannel [this k]
    (let [tmp-path (create-tmp-path disk-store)]
      (->LocalWritableChannel this allocator memory-store disk-store (open-tmp-file tmp-path) tmp-path k)))

  (openArrowFileWriter [this k vsr]
    (ArrowFileWriter. vsr nil (.openChannel this k)))

  (putObject [this k buffer] (put-object this k buffer)))

(deftype RemoteWritableChannel
  [^IBufferPool bp
   allocator
   memory-store
   ^Path disk-store
   ^ObjectStore remote-store
   ^FileChannel file-channel
   tmp-path
   ^String k]
  WritableByteChannel
  (isOpen [_] (.isOpen file-channel))
  (close [_]
    (.close file-channel)
    (let [mmap-buffer (util/->mmap-path tmp-path)]
      (cond
        (not (instance? SupportsMultipart remote-store))
        @(.putObject remote-store k mmap-buffer)

        (<= (.remaining mmap-buffer) (int min-multipart-part-size))
        @(.putObject remote-store k mmap-buffer)

        :else
        (->> (range (.position mmap-buffer) (.limit mmap-buffer) min-multipart-part-size)
             (map (fn [n] (.slice mmap-buffer (int n) (min (int min-multipart-part-size) (- (.limit mmap-buffer) (int n))))))
             (upload-multipart-buffers remote-store k))))
    (let [file-path (.resolve disk-store k)]
      (util/create-parents file-path)
      ;; see #2847
      (util/atomic-move tmp-path file-path)
      (util/then-apply (.getBuffer bp k) util/close)))
  (write [_ src] (.write file-channel src)))

(deftype RemoteArrowFileChannel
  [^IBufferPool bp
   allocator
   memory-store
   ^Path disk-store
   ^ObjectStore remote-store
   ^FileChannel file-channel
   tmp-path
   ^String k]
  WritableByteChannel
  (isOpen [_] (.isOpen file-channel))
  (close [_]
    (.close file-channel)
    (let [mmap-buffer (util/->mmap-path tmp-path)]
      (cond
        (not (instance? SupportsMultipart remote-store))
        @(.putObject remote-store k mmap-buffer)

        (<= (.remaining mmap-buffer) (int min-multipart-part-size))
        @(.putObject remote-store k mmap-buffer)

        :else
        (with-open [arrow-buf (util/->arrow-buf-view allocator mmap-buffer)]
          (let [cuts
                (loop [cuts []
                       prev-cut (int 0)
                       cut (int 0)
                       blocks (.getRecordBatches (util/read-arrow-footer arrow-buf))]
                  (if-some [[^ArrowBlock block & blocks] (seq blocks)]
                    (let [offset (.getOffset block)
                          offset-delta (- offset cut)
                          metadata-length (.getMetadataLength block)
                          body-length (.getBodyLength block)
                          total-length (+ offset-delta metadata-length body-length)
                          new-cut (+ cut total-length)
                          cut-len (- new-cut prev-cut)]
                      (if (<= (int min-multipart-part-size) cut-len)
                        (recur (conj cuts new-cut) cut new-cut blocks)
                        (recur cuts prev-cut new-cut blocks)))
                    cuts))

                part-buffers
                (loop [part-buffers []
                       prev-cut (int 0)
                       cuts cuts]
                  (if-some [[cut & cuts] (seq cuts)]
                    (recur (conj part-buffers (.nioBuffer arrow-buf prev-cut (- (int cut) prev-cut))) (int cut) cuts)
                    (let [final-part (.nioBuffer arrow-buf prev-cut (- (.capacity arrow-buf) prev-cut))]
                      (conj part-buffers final-part))))]

            (upload-multipart-buffers remote-store k part-buffers)

            nil))))
    (let [file-path (.resolve disk-store k)]
      (util/create-parents file-path)
      ;; see #2847
      (util/atomic-move tmp-path file-path)
      (util/then-apply (.getBuffer bp k) util/close)))
  (write [_ src] (.write file-channel src)))

(defrecord RemoteBufferPool
  [allocator
   ^ArrowBufLRU memory-store
   ^Path disk-store
   ^ObjectStore remote-store]
  Closeable
  (close [_]
    (free-memory memory-store)
    (util/close allocator))
  IBufferPool
  (getBuffer [_ k]
    (let [cached-buffer (cache-get memory-store k)]
      (cond
        (nil? k)
        (CompletableFuture/completedFuture nil)

        cached-buffer
        (do (record-cache-hit cached-buffer)
            (CompletableFuture/completedFuture cached-buffer))

        :else
        (let [buffer-cache-path (.resolve disk-store (str k))
              start-ns (System/nanoTime)]
          (-> (if (util/path-exists buffer-cache-path)
                ;; todo could this not race with eviction? e.g exists for this cond, but is evicted before we can map the file into the cache?
                (CompletableFuture/completedFuture buffer-cache-path)
                (do (util/create-parents buffer-cache-path)
                    (-> (.getObject remote-store k buffer-cache-path)
                        (util/then-apply (fn [path] (record-io-wait start-ns) path)))))
              (util/then-apply
                (fn [path]
                  (let [nio-buffer (util/->mmap-path path)
                        close-fn (fn [] (util/delete-file path))
                        create-arrow-buf #(util/->arrow-buf-view allocator nio-buffer close-fn)
                        [_ buf] (cache-compute memory-store k create-arrow-buf)]
                    buf))))))))

  (listObjects [_] (.listObjects remote-store))

  (listObjects [_ dir] (.listObjects remote-store dir))

  (openChannel [this k]
    (let [tmp-path (create-tmp-path disk-store)
          file-channel (open-tmp-file tmp-path)]
      (->RemoteWritableChannel
        this
        allocator
        memory-store
        disk-store
        remote-store
        file-channel
        tmp-path
        k)))

  (openArrowFileWriter [this k vsr]
    (let [tmp-path (create-tmp-path disk-store)
          file-channel (open-tmp-file tmp-path)
          ch (->RemoteArrowFileChannel
               this
               allocator
               memory-store
               disk-store
               remote-store
               file-channel
               tmp-path
               k)]
      (ArrowFileWriter. vsr nil ch)))

  (putObject [this k buffer] (put-object this k buffer)))

(set! *unchecked-math* :warn-on-boxed)

(defn ->remote [{:keys [^BufferAllocator allocator
                        object-store
                        data-dir
                        max-cache-bytes
                        max-cache-entries]
                 :or {max-cache-entries 1024
                      max-cache-bytes 536870912}}]
  (map->RemoteBufferPool
    {:allocator (.newChildAllocator allocator "buffer-pool" 0 Long/MAX_VALUE)
     :memory-store (ArrowBufLRU. 16 max-cache-entries max-cache-bytes)
     :disk-store data-dir
     :remote-store object-store}))

(defmethod ig/prep-key ::remote [_ opts]
  (-> (merge {:allocator (ig/ref :xtdb/allocator)
              :object-store (ig/ref :xtdb/object-store)}
             opts)
      (util/maybe-update :disk-store util/->path)))

(defmethod ig/init-key ::remote [_ opts] (->remote opts))

(defmethod ig/halt-key! ::remote [_ bp] (util/close bp))

(derive ::remote :xtdb/buffer-pool)

(defn ->local [{:keys [^BufferAllocator allocator,
                       ^Path data-dir
                       max-cache-bytes
                       max-cache-entries]
                :or {max-cache-entries 1024
                     max-cache-bytes 536870912}}]
  (map->LocalBufferPool
    {:allocator (.newChildAllocator allocator "buffer-pool" 0 Long/MAX_VALUE)
     :memory-store (ArrowBufLRU. 16 max-cache-entries max-cache-bytes)
     :disk-store data-dir}))

(defmethod ig/prep-key ::local [_ opts]
  (-> (merge {:allocator (ig/ref :xtdb/allocator)} opts)
      (util/maybe-update :data-dir util/->path)))

(defmethod ig/init-key ::local [_ opts] (->local opts))

(defmethod ig/halt-key! ::local [_ bp] (util/close bp))

(derive ::local :xtdb/buffer-pool)

(defn ->in-memory [{:keys [^BufferAllocator allocator]}]
  (map->InMemoryBufferPool
    {:allocator (.newChildAllocator allocator "buffer-pool" 0 Long/MAX_VALUE)
     :memory-store (TreeMap.)}))

(defmethod ig/prep-key ::in-memory [_ opts] (merge {:allocator (ig/ref :xtdb/allocator)} opts))

(defmethod ig/init-key ::in-memory [_ opts] (->in-memory opts))

(defmethod ig/halt-key! ::in-memory [_ bp] (util/close bp))

(derive ::in-memory :xtdb/buffer-pool)

(defn get-footer ^ArrowFooter [^IBufferPool bp k]
  (with-open [^ArrowBuf arrow-buf @(.getBuffer bp (str k))]
    (util/read-arrow-footer arrow-buf)))

(defn open-record-batch ^ArrowRecordBatch [^IBufferPool bp k block-idx]
  (with-open [^ArrowBuf arrow-buf @(.getBuffer bp (str k))]
    (let [footer (util/read-arrow-footer arrow-buf)
          blocks (.getRecordBatches footer)
          block (nth blocks block-idx nil)]
      (if-not block
        (throw (IndexOutOfBoundsException. "Record batch index out of bounds of arrow file"))
        (util/->arrow-record-batch-view block arrow-buf)))))

(defn open-vsr ^VectorSchemaRoot [bp k allocator]
  (let [footer (get-footer bp k)
        schema (.getSchema footer)]
    (VectorSchemaRoot/create schema allocator)))
