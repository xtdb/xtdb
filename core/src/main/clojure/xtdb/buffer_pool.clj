(ns xtdb.buffer-pool
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
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
           (xtdb.object_store IMultipartUpload SupportsMultipart)))

(set! *unchecked-math* :warn-on-boxed)

(defn- free-memory [^Map memory-store]
  (locking memory-store
    (run! util/close (.values memory-store))
    (.clear memory-store)))

(declare get-buffer put-object list-objects)

(defrecord BufferPool [^BufferAllocator allocator,
                       ^Map memory-store,
                       ^Path disk-store,
                       ^ObjectStore remote-store,
                       allow-file-deletion]
  Closeable
  (close [_]
    (free-memory memory-store)
    (.close allocator)))

(defn ->remote [{:keys [^BufferAllocator allocator
                        object-store
                        data-dir
                        allow-file-deletion
                        max-cache-bytes
                        max-cache-entries]
                 :or {max-cache-entries 1024
                      max-cache-bytes 536870912
                      allow-file-deletion false}}]
  (map->BufferPool
    {:allocator (.newChildAllocator allocator "buffer-pool" 0 Long/MAX_VALUE)
     :memory-store (ArrowBufLRU. 16 max-cache-entries max-cache-bytes)
     :disk-store data-dir
     :allow-file-deletion allow-file-deletion
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
  (map->BufferPool
    {:allocator (.newChildAllocator allocator "buffer-pool" 0 Long/MAX_VALUE)
     :memory-store (ArrowBufLRU. 16 max-cache-entries max-cache-bytes)
     :disk-store data-dir
     :allow-file-deletion false}))

(defmethod ig/prep-key ::local [_ opts]
  (-> (merge {:allocator (ig/ref :xtdb/allocator)} opts)
      (util/maybe-update :data-dir util/->path)))

(defmethod ig/init-key ::local [_ opts] (->local opts))

(defmethod ig/halt-key! ::local [_ bp] (util/close bp))

(derive ::local :xtdb/buffer-pool)

(defn ->in-memory [{:keys [^BufferAllocator allocator]}]
  (map->BufferPool
    {:allocator (.newChildAllocator allocator "buffer-pool" 0 Long/MAX_VALUE)
     :memory-store (TreeMap.)}))

(defmethod ig/prep-key ::in-memory [_ opts] (merge {:allocator (ig/ref :xtdb/allocator)} opts))

(defmethod ig/init-key ::in-memory [_ opts] (->in-memory opts))

(defmethod ig/halt-key! ::in-memory [_ bp] (util/close bp))

(derive ::in-memory :xtdb/buffer-pool)

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

(defn- list-path
  ([^Path root-path]
   (with-open [dir-stream (Files/walk root-path (make-array FileVisitOption 0))]
     (vec (sort (for [^Path path (iterator-seq (.iterator dir-stream))
                      :when (Files/isRegularFile path (make-array LinkOption 0))]
                  (str (.relativize root-path path)))))))

  ([^Path root-path, ^String dir]
   (let [dir (.resolve root-path dir)]
     (when (Files/exists dir (make-array LinkOption 0))
       (with-open [dir-stream (Files/newDirectoryStream dir)]
         (vec (sort (for [^Path path dir-stream]
                      (str (.relativize root-path path))))))))))

(defn- cache-buffer-only [bp k nio-buffer on-close]
  (let [{:keys [allocator, memory-store]} bp
        create-arrow-buf #(util/->arrow-buf-view allocator nio-buffer on-close)
        [_ buf] (cache-compute memory-store k create-arrow-buf)]
    buf))

(defn- cache-file [bp k path]
  (let [{:keys [allow-file-deletion]} bp]
    (try
      (let [nio-buffer (util/->mmap-path path)]
        (cache-buffer-only bp k nio-buffer (if allow-file-deletion #(util/delete-file path) (constantly nil))))
      (catch Throwable t
        (try
          (when allow-file-deletion (util/delete-file path))
          (catch Throwable t1
            (.addSuppressed t t1)
            (log/error t1 "Error caught cleaning up file during exception handling")))
        (throw t)))))

(defn- cache-miss [bp k]
  (let [{:keys [^Path disk-store, ^ObjectStore remote-store]} bp
        start-ns (System/nanoTime)
        buffer-cache-path (.resolve disk-store (str k))]
    (-> (cond
          (util/path-exists buffer-cache-path)
          ;; todo could this not race with eviction? e.g exists for this cond, but is evicted before we can map the file into the cache?
          (CompletableFuture/completedFuture buffer-cache-path)

          (nil? remote-store)
          (CompletableFuture/failedFuture (os/obj-missing-exception k))

          :else
          (do (util/create-parents buffer-cache-path)
              (-> (.getObject remote-store k buffer-cache-path)
                  (util/then-apply (fn [path] (record-io-wait start-ns) path)))))
        (util/then-apply
          (fn [path]
            (cache-file bp k path))))))

(defn- cache-miss-no-disk [bp k]
  (let [{:keys [^ObjectStore remote-store]} bp
        start-ns (System/nanoTime)]
    (-> (.getObject remote-store k)
        (util/then-apply
          (fn [nio-buffer]
            (record-io-wait start-ns)
            (cache-buffer-only bp k nio-buffer (constantly nil)))))))

(defn get-buffer ^ArrowBuf [bp k]
  (let [{:keys [disk-store, remote-store, memory-store]} bp
        cached-buffer (cache-get memory-store k)]
    (cond
      (nil? k)
      (CompletableFuture/completedFuture nil)

      cached-buffer
      (do (record-cache-hit cached-buffer)
          (CompletableFuture/completedFuture cached-buffer))

      disk-store
      (cache-miss bp k)

      (nil? remote-store)
      (CompletableFuture/failedFuture (os/obj-missing-exception k))

      :else
      (cache-miss-no-disk bp k))))

(def ^:private min-multipart-part-size (* 5 1024 1024))
(def ^:private max-multipart-per-upload-concurrency 4)

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

(defn upload-arrow-ipc-file-multipart [object-store allocator k mmap-buffer]
  (assert (instance? SupportsMultipart object-store) "object-store must implement multipart")
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

      (upload-multipart-buffers object-store k part-buffers)

      nil)))

(defn upload-file-multipart [object-store k ^ByteBuffer mmap-buffer]
  (->> (range (.position mmap-buffer) (.limit mmap-buffer) min-multipart-part-size)
       (map (fn [n] (.slice mmap-buffer (int n) (min (int min-multipart-part-size) (- (.limit mmap-buffer) (int n))))))
       (upload-multipart-buffers object-store k)))

(defn upload-file [^ObjectStore object-store allocator k ^ByteBuffer mmap-buffer arrow-ipc-format]
  (cond
    (not (instance? SupportsMultipart object-store))
    @(.putObject object-store k mmap-buffer)

    (<= (.remaining mmap-buffer) (int min-multipart-part-size))
    @(.putObject object-store k mmap-buffer)

    arrow-ipc-format
    (upload-arrow-ipc-file-multipart object-store allocator k mmap-buffer)

    :else
    (upload-file-multipart object-store k mmap-buffer)))

(defn close-bp-file-channel [channel]
  (let [{:keys [bp, k, tmp-path, ^FileChannel file-channel, arrow-ipc-format]} channel
        {:keys [allocator, ^Path disk-store, remote-store]} bp
        file-path (.resolve disk-store (str k))]
    (.close file-channel)
    (when remote-store (upload-file remote-store allocator k (util/->mmap-path tmp-path) arrow-ipc-format))
    (util/create-parents file-path)
    ;; see #2847
    (util/atomic-move tmp-path file-path)
    ;; cache-file returns the buffer, so must increment its reference count
    ;; we are not interested in doing work with the buffer (just to putting it in the cache), hence util/close.
    (util/close (cache-file bp k file-path))))

(defrecord BufferPoolFileWritableChannel [bp k tmp-path ^FileChannel file-channel arrow-ipc-format]
  WritableByteChannel
  (isOpen [_] (.isOpen file-channel))
  (close [this] (close-bp-file-channel this))
  (write [_ src] (.write file-channel src)))

(defn close-bp-memory-channel [bp k state]
  (let [{:keys [^ByteBuffer buffer]} @state]
    ;; cache-buffer-only returns the buffer, so must increment its reference count
    ;; we are not interested in doing work with the buffer (just to putting it in the cache), hence util/close.
    (util/close (cache-buffer-only bp k (.flip buffer) (constantly nil)))
    (swap! state assoc :open false)))

(defn write-to-bp-memory-channel [state ^ByteBuffer src]
  (let [{:keys [^ByteBuffer buffer]} @state]
    (if (<= (.remaining src) (.remaining buffer))
      (.put buffer src)
      (let [new-buffer (ByteBuffer/allocateDirect (* 2 (.capacity buffer)))]
        (.put new-buffer (.flip buffer))
        (swap! state assoc :buffer new-buffer)
        (recur state src)))))

(defrecord BufferPoolMemoryWritableChannel [bp k state]
  WritableByteChannel
  (isOpen [_] (:open @state))
  (close [_] (close-bp-memory-channel bp k state))
  (write [_ src]
    (let [remaining (.remaining src)]
      (write-to-bp-memory-channel state src)
      remaining)))

(defn open-channel ^WritableByteChannel [bp k & {:keys [arrow-ipc-format]}]
  (if-not (:disk-store bp)
    (->BufferPoolMemoryWritableChannel bp k (atom {:open true, :buffer (ByteBuffer/allocateDirect 128)}))
    (let [tmp-path (Files/createTempFile "bp-channel" ".arrow" (make-array FileAttribute 0))]
      (->BufferPoolFileWritableChannel bp k tmp-path (util/->file-channel tmp-path [StandardOpenOption/APPEND]) arrow-ipc-format))))

(defn open-arrow-file-writer ^ArrowFileWriter [bp k vsr]
  (ArrowFileWriter. vsr nil (open-channel bp k :arrow-ipc-format true)))

(defn list-objects
  ([bp]
   (let [{:keys [^ObjectStore remote-store, disk-store, ^Map memory-store]} bp]
     (cond
       remote-store (.listObjects remote-store)
       disk-store (list-path disk-store)
       :else (locking memory-store (vec (.keySet ^NavigableMap memory-store))))))
  ([bp dir]
   (let [{:keys [^ObjectStore remote-store, disk-store, ^Map memory-store]} bp]
     (cond
       remote-store (.listObjects remote-store dir)
       disk-store (list-path disk-store dir)
       :else
       (locking memory-store
         (->> (.keySet (.tailMap ^NavigableMap memory-store dir))
              (into [] (take-while #(str/starts-with? % dir)))
              (vec)))))))

(defn get-footer ^ArrowFooter [bp k]
  (with-open [^ArrowBuf arrow-buf @(get-buffer bp (str k))]
    (util/read-arrow-footer arrow-buf)))

(defn open-record-batch ^ArrowRecordBatch [bp k block-idx]
  (with-open [^ArrowBuf arrow-buf @(get-buffer bp (str k))]
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

;; temporary compatibility shim, should use writers
(defn put-object [bp k ^ByteBuffer nio-buffer]
  (let [nio-buffer (.duplicate nio-buffer)]
    (try
      (with-open [channel (open-channel bp k)]
        (while (.hasRemaining nio-buffer)
          (.write channel nio-buffer)))
      (CompletableFuture/completedFuture nil)
      (catch Throwable t
        (CompletableFuture/failedFuture t)))))
