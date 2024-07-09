(ns xtdb.buffer-pool
  (:require [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.node :as xtn]
            [xtdb.object-store :as os]
            [xtdb.util :as util])
  (:import (clojure.lang PersistentQueue)
           [java.io ByteArrayOutputStream Closeable]
           (java.nio ByteBuffer)
           (java.nio.channels Channels ClosedByInterruptException)
           [java.nio.file FileVisitOption Files LinkOption Path]
           [java.nio.file.attribute FileAttribute]
           [java.util Map NavigableMap TreeMap]
           [java.util.concurrent CompletableFuture]
           (java.util.concurrent.atomic AtomicLong)
           java.util.NavigableMap
           [org.apache.arrow.memory ArrowBuf BufferAllocator]
           (org.apache.arrow.vector VectorSchemaRoot)
           (org.apache.arrow.vector.ipc ArrowFileWriter)
           (org.apache.arrow.vector.ipc.message ArrowBlock ArrowFooter ArrowRecordBatch)
           (xtdb IArrowWriter IBufferPool)
           (xtdb.api.storage ObjectStore Storage Storage$Factory Storage$LocalStorageFactory Storage$RemoteStorageFactory)
           xtdb.api.Xtdb$Config
           (xtdb.multipart IMultipartUpload SupportsMultipart)
           xtdb.util.ArrowBufLRU))

(set! *unchecked-math* :warn-on-boxed)

(defprotocol EvictBufferTest
  (evict-cached-buffer! [bp ^Path k]))

(def ^:private min-multipart-part-size (* 5 1024 1024))
(def ^:private max-multipart-per-upload-concurrency 4)

(def ^java.nio.file.Path storage-root
  ;; bump this if the storage format changes in a backwards-incompatible way
  (let [version 2]
    (util/->path (str "v" (util/->lex-hex-string version)))))

(defn- free-memory [^Map memory-store]
  (locking memory-store
    (run! util/close (.values memory-store))
    (.clear memory-store)))

(defn- retain [^ArrowBuf buf] (.retain (.getReferenceManager buf)) buf)

(defn- cache-get ^ArrowBuf [^Map memory-store k]
  (locking memory-store
    (some-> (.get memory-store k) retain)))

(defn- cache-compute
  "Returns a pair [hit-or-miss, buf] computing the cached ArrowBuf from (f) if needed.
  `hit-or-miss` is true if the buffer was found, false if the object was added as part of this call."
  [^Map memory-store k f]
  (locking memory-store
    (let [hit (.containsKey memory-store k)
          arrow-buf (if hit (.get memory-store k) (let [buf (f)] (.put memory-store k buf) buf))]
      [hit (retain arrow-buf)])))

(defn- close-arrow-writer [^ArrowFileWriter aw]
  ;; arrow wraps InterruptExceptions
  ;; https://github.com/apache/arrow/blob/d10f468b0603da41a285c60a38b095a30b91e2c1/java/vector/src/main/java/org/apache/arrow/vector/ipc/ArrowWriter.java#L217-L222
  (try
    (util/close aw)
    (catch Throwable t
      (throw (util/unroll-interrupt-ex t)))))

(defrecord MemoryBufferPool [allocator, ^NavigableMap memory-store]
  IBufferPool
  (getBuffer [_ k]
    (let [cached-buffer (cache-get memory-store k)]
      (cond
        (nil? k)
        (CompletableFuture/completedFuture nil)

        cached-buffer
        (CompletableFuture/completedFuture cached-buffer)

        :else
        (CompletableFuture/failedFuture (os/obj-missing-exception k)))))

  (putObject [_ k buffer]
    (CompletableFuture/completedFuture
     (locking memory-store
       (.put memory-store k (util/->arrow-buf-view allocator buffer)))))

  (listAllObjects [_]
    (locking memory-store (vec (.keySet ^NavigableMap memory-store))))

  (listObjects [_ dir]
    (locking memory-store
      (let [dir-depth (.getNameCount dir)]
        (->> (.keySet (.tailMap ^NavigableMap memory-store dir))
             (take-while #(.startsWith ^Path % dir))
             (keep (fn [^Path path]
                     (when (> (.getNameCount path) dir-depth)
                       (.subpath path 0 (inc dir-depth)))))
             (distinct)
             (vec)))))

  (openArrowWriter [this k vsr]
    (let [baos (ByteArrayOutputStream.)]
      (util/with-close-on-catch [write-ch (Channels/newChannel baos)
                                 aw (ArrowFileWriter. vsr nil write-ch)]

        (try
          (.start aw)
          (catch ClosedByInterruptException e
            (throw (InterruptedException.))))

        (reify IArrowWriter
          (writeBatch [_] (.writeBatch aw))

          (end [_]
            (.end aw)
            (.close write-ch)
            (.putObject this k (ByteBuffer/wrap (.toByteArray baos))))

          (close [_]
            (close-arrow-writer aw)
            (when (.isOpen write-ch)
              (.close write-ch)))))))

  EvictBufferTest
  (evict-cached-buffer! [_ _k]
    ;; no-op,
    )

  Closeable
  (close [_]
    (free-memory memory-store)
    (util/close allocator)))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn open-in-memory-storage [^BufferAllocator allocator]
  (->MemoryBufferPool (.newChildAllocator allocator "buffer-pool" 0 Long/MAX_VALUE)
                      (TreeMap.)))

(defn- create-tmp-path ^Path [^Path disk-store]
  (Files/createTempFile (doto (.resolve disk-store ".tmp") util/mkdirs)
                        "upload" ".arrow"
                        (make-array FileAttribute 0)))

(defrecord LocalBufferPool [allocator, ^ArrowBufLRU memory-store, ^Path disk-store]
  IBufferPool
  (getBuffer [_ k]
    (let [cached-buffer (cache-get memory-store k)]
      (cond
        (nil? k)
        (CompletableFuture/completedFuture nil)

        cached-buffer
        (CompletableFuture/completedFuture cached-buffer)

        :else
        (let [buffer-cache-path (.resolve disk-store k)]
          (-> (if (util/path-exists buffer-cache-path)
                ;; todo could this not race with eviction? e.g exists for this cond, but is evicted before we can map the file into the cache?
                (CompletableFuture/completedFuture buffer-cache-path)
                (CompletableFuture/failedFuture (os/obj-missing-exception k)))
              (util/then-apply
                (fn [path]
                  (try
                    (let [nio-buffer (util/->mmap-path path)
                          create-arrow-buf #(util/->arrow-buf-view allocator nio-buffer)
                          [_ buf] (cache-compute memory-store k create-arrow-buf)]
                      buf)
                    (catch ClosedByInterruptException _
                      (throw (InterruptedException.)))))))))))

  (putObject [_ k buffer]
    (CompletableFuture/completedFuture
     (try
       (let [tmp-path (create-tmp-path disk-store)]
         (util/write-buffer-to-path buffer tmp-path)

         (let [file-path (.resolve disk-store k)]
           (util/create-parents file-path)
           (util/atomic-move tmp-path file-path)))
       (catch ClosedByInterruptException _
         (throw (InterruptedException.))))))

  (listAllObjects [_]
    (util/with-open [dir-stream (Files/walk disk-store (make-array FileVisitOption 0))]
      (vec (sort (for [^Path path (iterator-seq (.iterator dir-stream))
                       :when (Files/isRegularFile path (make-array LinkOption 0))]
                   (.relativize disk-store path))))))

  (listObjects [_ dir]
    (let [dir (.resolve disk-store dir)]
      (when (Files/exists dir (make-array LinkOption 0))
        (util/with-open [dir-stream (Files/newDirectoryStream dir)]
          (vec (sort (for [^Path path dir-stream]
                       (.relativize disk-store path))))))))

  (openArrowWriter [_ k vsr]
    (let [tmp-path (create-tmp-path disk-store)]
      (util/with-close-on-catch [file-ch (util/->file-channel tmp-path util/write-truncate-open-opts)
                                 aw (ArrowFileWriter. vsr nil file-ch)]
        (try
          (.start aw)
          (catch ClosedByInterruptException e
            (throw (InterruptedException.))))

        (reify IArrowWriter
          (writeBatch [_]
            (try
              (.writeBatch aw)
              (catch ClosedByInterruptException e
                (throw (InterruptedException.)))))

          (end [_]
            (.end aw)
            (.close file-ch)

            (let [file-path (.resolve disk-store k)]
              (util/create-parents file-path)
              (util/atomic-move tmp-path file-path)))

          (close [_]
            (close-arrow-writer aw)
            (when (.isOpen file-ch)
              (.close file-ch)))))))

  EvictBufferTest
  (evict-cached-buffer! [_ k]
    (doto (.remove memory-store k)
      util/close))

  Closeable
  (close [_]
    (free-memory memory-store)
    (util/close allocator)))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn open-local-storage ^xtdb.IBufferPool [^BufferAllocator allocator, ^Storage$LocalStorageFactory factory]
  (->LocalBufferPool (.newChildAllocator allocator "buffer-pool" 0 Long/MAX_VALUE)
                     (ArrowBufLRU. 16 (.getMaxCacheEntries factory) (.getMaxCacheBytes factory))
                     (-> (.getPath factory)
                         (.resolve storage-root))))

(defn dir->buffer-pool
  "Creates a local storage buffer pool from the given directory."
  ^xtdb.IBufferPool [^BufferAllocator allocator, ^Path dir]
  (let [bp-path (util/tmp-dir "tmp-buffer-pool")
        storage-root (.resolve bp-path storage-root)]
    (util/copy-dir dir storage-root)
    (.openStorage (Storage/localStorage bp-path) allocator)))

(defmethod xtn/apply-config! ::local [^Xtdb$Config config _ {:keys [path max-cache-bytes max-cache-entries]}]
  (.storage config (cond-> (Storage/localStorage (util/->path path))
                     max-cache-bytes (.maxCacheBytes max-cache-bytes)
                     max-cache-entries (.maxCacheEntries max-cache-entries))))

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

(defn- arrow-buf-cuts [^ArrowBuf arrow-buf]
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
          (recur (conj cuts new-cut) new-cut new-cut blocks)
          (recur cuts prev-cut new-cut blocks)))
      cuts)))

(defn- arrow-buf->parts [^ArrowBuf arrow-buf]
  (loop [part-buffers []
         prev-cut (int 0)
         cuts (arrow-buf-cuts arrow-buf)]
    (if-some [[cut & cuts] (seq cuts)]
      (recur (conj part-buffers (.nioBuffer arrow-buf prev-cut (- (int cut) prev-cut))) (int cut) cuts)
      (let [final-part (.nioBuffer arrow-buf prev-cut (- (.capacity arrow-buf) prev-cut))]
        (conj part-buffers final-part)))))

(defn- upload-arrow-file [^BufferAllocator allocator, ^ObjectStore object-store, ^Path k, ^Path tmp-path]
  (let [mmap-buffer (util/->mmap-path tmp-path)]
    (if (or (not (instance? SupportsMultipart object-store))
            (<= (.remaining mmap-buffer) (int min-multipart-part-size)))
      @(.putObject object-store k mmap-buffer)

      (util/with-open [arrow-buf (util/->arrow-buf-view allocator mmap-buffer)]
        (upload-multipart-buffers object-store k (arrow-buf->parts arrow-buf))
        nil))))

(defrecord RemoteBufferPool [allocator
                             ^ArrowBufLRU memory-store
                             ^Path local-disk-cache
                             ^ObjectStore object-store]
  IBufferPool
  (getBuffer [_ k]
    (let [cached-buffer (cache-get memory-store k)]
      (cond
        (nil? k)
        (CompletableFuture/completedFuture nil)

        cached-buffer
        (CompletableFuture/completedFuture cached-buffer)

        :else
        (let [buffer-cache-path (.resolve local-disk-cache k)]
          (-> (if (util/path-exists buffer-cache-path)
                ;; todo could this not race with eviction? e.g exists for this cond, but is evicted before we can map the file into the cache?
                (CompletableFuture/completedFuture buffer-cache-path)
                (do (util/create-parents buffer-cache-path)
                    (.getObject object-store k buffer-cache-path)))
              (util/then-apply
                (fn [path]
                  (let [nio-buffer (util/->mmap-path path)
                        create-arrow-buf #(util/->arrow-buf-view allocator nio-buffer)
                        [_ buf] (cache-compute memory-store k create-arrow-buf)]

                    buf))))))))

  (listAllObjects [_] (.listAllObjects object-store))

  (listObjects [_ dir] (.listObjects object-store dir))

  (openArrowWriter [_ k vsr]
    (let [tmp-path (create-tmp-path local-disk-cache)]
      (util/with-close-on-catch [file-ch (util/->file-channel tmp-path util/write-truncate-open-opts)
                                 aw (ArrowFileWriter. vsr nil file-ch)]

        (try
          (.start aw)
          (catch ClosedByInterruptException _e
            (throw (InterruptedException.))))

        (reify IArrowWriter
          (writeBatch [_] (.writeBatch aw))

          (end [_]
            (.end aw)
            (.close file-ch)

            (upload-arrow-file allocator object-store k tmp-path)

            (let [file-path (.resolve local-disk-cache k)]
              (util/create-parents file-path)
              ;; see #2847
              (util/atomic-move tmp-path file-path)))

          (close [_]
            (close-arrow-writer aw)
            (when (.isOpen file-ch)
              (.close file-ch)))))))

  (putObject [_ k buffer]
    (if (or (not (instance? SupportsMultipart object-store))
            (<= (.remaining buffer) (int min-multipart-part-size)))
      (.putObject object-store k buffer)

      (let [buffers (->> (range (.position buffer) (.limit buffer) min-multipart-part-size)
                         (map (fn [n] (.slice buffer
                                              (int n)
                                              (min (int min-multipart-part-size)
                                                   (- (.limit buffer) (int n)))))))]
        (-> (CompletableFuture/runAsync
             (fn []
               (upload-multipart-buffers object-store k buffers)))

            (.thenRun (fn []
                        (let [tmp-path (create-tmp-path local-disk-cache)]
                          (util/with-open [file-ch (util/->file-channel tmp-path util/write-truncate-open-opts)]
                            (.write file-ch buffer))

                          (let [file-path (.resolve local-disk-cache k)]
                            (util/create-parents file-path)
                            ;; see #2847
                            (util/atomic-move tmp-path file-path)))))))))

  EvictBufferTest
  (evict-cached-buffer! [_ k]
    (doto (.remove memory-store k)
      (util/close)))

  Closeable
  (close [_]
    (free-memory memory-store)
    (util/close object-store)
    (util/close allocator)))

(set! *unchecked-math* :warn-on-boxed)

(defn open-remote-storage ^xtdb.IBufferPool [^BufferAllocator allocator, ^Storage$RemoteStorageFactory factory]
  (util/with-close-on-catch [object-store (.openObjectStore (.getObjectStore factory))]
    (->RemoteBufferPool (.newChildAllocator allocator "buffer-pool" 0 Long/MAX_VALUE)
                        (ArrowBufLRU. 16 (.getMaxCacheEntries factory) (.getMaxCacheBytes factory))
                        (.getLocalDiskCache factory)
                        object-store)))

(defmulti ->object-store-factory
  #_{:clj-kondo/ignore [:unused-binding]}
  (fn [tag opts]
    (when-let [ns (namespace tag)]
      (doseq [k [(symbol ns)
                 (symbol (str ns "." (name tag)))]]

        (try
          (require k)
          (catch Throwable _))))

    tag))

(defmethod ->object-store-factory :in-memory [_ opts] (->object-store-factory :xtdb.object-store-test/memory-object-store opts))
(defmethod ->object-store-factory :s3 [_ opts] (->object-store-factory :xtdb.aws.s3/object-store opts))
(defmethod ->object-store-factory :google-cloud [_ opts] (->object-store-factory :xtdb.google-cloud/object-store opts))
(defmethod ->object-store-factory :azure [_ opts] (->object-store-factory :xtdb.azure/object-store opts))

(defmethod xtn/apply-config! ::remote [^Xtdb$Config config _ {:keys [object-store local-disk-cache max-cache-bytes max-cache-entries]}]
  (.storage config (cond-> (Storage/remoteStorage (let [[tag opts] object-store]
                                                    (->object-store-factory tag opts))
                                                  (util/->path local-disk-cache))
                     max-cache-bytes (.maxCacheBytes max-cache-bytes)
                     max-cache-entries (.maxCacheEntries max-cache-entries))))

(defn get-footer ^ArrowFooter [^IBufferPool bp ^Path path]
  (util/with-open [^ArrowBuf arrow-buf @(.getBuffer bp path)]
    (util/read-arrow-footer arrow-buf)))

(defn open-record-batch ^ArrowRecordBatch [^IBufferPool bp ^Path path block-idx]
  (util/with-open [^ArrowBuf arrow-buf @(.getBuffer bp path)]
    (try
      (let [footer (util/read-arrow-footer arrow-buf)
            blocks (.getRecordBatches footer)
            block (nth blocks block-idx nil)]
        (if-not block
          (throw (IndexOutOfBoundsException. "Record batch index out of bounds of arrow file"))
          (util/->arrow-record-batch-view block arrow-buf)))
      (catch Exception e
        (throw (ex-info (format "Failed opening record batch '%s'" path) {:path path :block-idx block-idx} e))))))

(defn open-vsr ^VectorSchemaRoot [bp ^Path path allocator]
  (let [footer (get-footer bp path)
        schema (.getSchema footer)]
    (VectorSchemaRoot/create schema allocator)))

(defmethod xtn/apply-config! ::storage [config _ [tag opts]]
  (xtn/apply-config! config
                     (case tag
                       :in-memory ::in-memory
                       :local ::local
                       :remote ::remote)
                     opts))

(defmethod ig/prep-key :xtdb/buffer-pool [_ factory]
  {:allocator (ig/ref :xtdb/allocator)
   :factory factory})

(defmethod ig/init-key :xtdb/buffer-pool [_ {:keys [allocator ^Storage$Factory factory]}]
  (.openStorage factory allocator))

(defmethod ig/halt-key! :xtdb/buffer-pool [_ ^IBufferPool buffer-pool]
  (util/close buffer-pool))
