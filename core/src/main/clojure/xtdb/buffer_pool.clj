(ns xtdb.buffer-pool
  (:require [clojure.tools.logging :as log]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.file-list-cache :as flc]
            [xtdb.metrics :as metrics]
            [xtdb.node :as xtn]
            [xtdb.object-store :as os]
            [xtdb.util :as util])
  (:import (clojure.lang PersistentQueue)
           [io.micrometer.core.instrument MeterRegistry]
           [io.micrometer.core.instrument.simple SimpleMeterRegistry]
           [java.io ByteArrayOutputStream Closeable]
           [java.lang AutoCloseable]
           (java.nio ByteBuffer)
           (java.nio.channels Channels ClosedByInterruptException)
           [java.nio.file FileVisitOption Files LinkOption Path]
           [java.nio.file.attribute FileAttribute]
           [java.util NavigableMap SortedSet TreeMap]
           (java.util NavigableMap)
           [java.util.concurrent CompletableFuture ConcurrentSkipListSet]
           kotlin.Pair
           [org.apache.arrow.memory ArrowBuf BufferAllocator]
           (org.apache.arrow.vector VectorSchemaRoot)
           (org.apache.arrow.vector.ipc.message ArrowBlock ArrowFooter ArrowRecordBatch)
           (xtdb ArrowWriter IBufferPool)
           (xtdb.api.log FileListCache FileListCache$Subscriber)
           (xtdb.api.storage ObjectStore Storage Storage$Factory Storage$LocalStorageFactory Storage$RemoteStorageFactory)
           xtdb.api.Xtdb$Config
           (xtdb.cache DiskCache DiskCache$Entry MemoryCache)
           (xtdb.multipart IMultipartUpload SupportsMultipart)))

(set! *unchecked-math* :warn-on-boxed)

(defprotocol EvictBufferTest
  (evict-cached-buffer! [bp ^Path k]))

(def ^:private min-multipart-part-size (* 5 1024 1024))
(def ^:private max-multipart-per-upload-concurrency 4)

;; bump this if the storage format changes in a backwards-incompatible way
(def version (let [version 5] (str "v" (util/->lex-hex-string version))))

(def ^java.nio.file.Path storage-root (util/->path version))

(defn- retain [^ArrowBuf buf] (.retain (.getReferenceManager buf)) buf)

(defn ->buffer-pool-child-allocator [^BufferAllocator allocator, ^MeterRegistry metrics-registry]
  (let [child-allocator (.newChildAllocator allocator "buffer-pool" 0 Long/MAX_VALUE)]
    (when metrics-registry
      (metrics/add-allocator-gauge metrics-registry "buffer-pool.allocator.allocated_memory" child-allocator))
    child-allocator))

(defrecord MemoryBufferPool [allocator, ^NavigableMap memory-store]
  IBufferPool
  (getBuffer [_ k]
    (when k
      (or (locking memory-store
            (some-> (.get memory-store k) retain))
          (throw (os/obj-missing-exception k)))))

  (putObject [_ k buffer]
    (locking memory-store
      (.put memory-store k (util/->arrow-buf-view allocator buffer))))

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

  (openArrowWriter [this k rel]
    (let [baos (ByteArrayOutputStream.)]
      (util/with-close-on-catch [write-ch (Channels/newChannel baos)
                                 unl (.startUnload rel write-ch)]

        (reify ArrowWriter
          (writeBatch [_] (.writeBatch unl))

          (end [_]
            (.endFile unl)
            (.close write-ch)
            (.putObject this k (ByteBuffer/wrap (.toByteArray baos))))

          (close [_]
            (util/close unl)
            (when (.isOpen write-ch)
              (.close write-ch)))))))

  EvictBufferTest
  (evict-cached-buffer! [_ _k])

  Closeable
  (close [_]
    (locking memory-store
      (run! util/close (.values memory-store))
      (.clear memory-store))
    (util/close allocator)))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn open-in-memory-storage [^BufferAllocator allocator, ^MeterRegistry metrics-registry]
  (->MemoryBufferPool (->buffer-pool-child-allocator allocator metrics-registry) (TreeMap.)))

(defn- create-tmp-path ^Path [^Path disk-store]
  (Files/createTempFile (doto (.resolve disk-store ".tmp") util/mkdirs)
                        "upload" ".arrow"
                        (make-array FileAttribute 0)))

(defrecord LocalBufferPool [allocator, ^MemoryCache memory-cache, ^Path disk-store]
  IBufferPool
  (getBuffer [_ k]
    (when k
      (.get memory-cache k
            (fn [^Path k]
              (let [buffer-cache-path (.resolve disk-store k)]
                (when-not (util/path-exists buffer-cache-path)
                  (throw (os/obj-missing-exception k)))

                (CompletableFuture/completedFuture
                 (Pair. buffer-cache-path nil)))))))

  (putObject [_ k buffer]
    (try
      (let [tmp-path (create-tmp-path disk-store)]
        (util/write-buffer-to-path buffer tmp-path)

        (let [file-path (.resolve disk-store k)]
          (util/create-parents file-path)
          (util/atomic-move tmp-path file-path)))
      (catch ClosedByInterruptException _
        (throw (InterruptedException.)))))

  (listAllObjects [_]
    (util/with-open [dir-stream (Files/walk disk-store (make-array FileVisitOption 0))]
      (vec (sort (for [^Path path (iterator-seq (.iterator dir-stream))
                       :let [relativized (.relativize disk-store path)]
                       :when (and (Files/isRegularFile path (make-array LinkOption 0))
                                  (not (.startsWith relativized ".tmp")))]
                   relativized)))))

  (listObjects [_ dir]
    (let [dir (.resolve disk-store dir)]
      (if (Files/exists dir (make-array LinkOption 0))
        (util/with-open [dir-stream (Files/newDirectoryStream dir)]
          (vec (sort (for [^Path path dir-stream]
                       (.relativize disk-store path)))))
        [])))

  (openArrowWriter [_ k rel]
    (let [tmp-path (create-tmp-path disk-store)]
      (util/with-close-on-catch [file-ch (util/->file-channel tmp-path util/write-truncate-open-opts)
                                 unl (.startUnload rel file-ch)]
        (reify ArrowWriter
          (writeBatch [_]
            (try
              (.writeBatch unl)
              (catch ClosedByInterruptException _
                (throw (InterruptedException.)))))

          (end [_]
            (.endFile unl)
            (.close file-ch)

            (let [file-path (.resolve disk-store k)]
              (util/create-parents file-path)
              (util/atomic-move tmp-path file-path)))

          (close [_]
            (util/close unl)
            (when (.isOpen file-ch)
              (.close file-ch)))))))

  EvictBufferTest
  (evict-cached-buffer! [_ k]
    (.invalidate memory-cache k))

  Closeable
  (close [_]
    (util/close memory-cache)
    (util/close allocator)))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn open-local-storage ^xtdb.IBufferPool [^BufferAllocator allocator, ^Storage$LocalStorageFactory factory, ^MeterRegistry metrics-registry]
  (let [memory-cache (MemoryCache. allocator (.getMaxCacheBytes factory))]
    (metrics/add-cache-gauges metrics-registry "memorycache" (.getStats memory-cache))
    (->LocalBufferPool (->buffer-pool-child-allocator allocator metrics-registry)
                       memory-cache
                       (doto (-> (.getPath factory) (.resolve storage-root)) util/mkdirs))))

(defn dir->buffer-pool
  "Creates a local storage buffer pool from the given directory."
  ^xtdb.IBufferPool [^BufferAllocator allocator, ^Path dir]
  (let [bp-path (util/tmp-dir "tmp-buffer-pool")
        storage-root (.resolve bp-path storage-root)]
    (util/copy-dir dir storage-root)
    (open-local-storage allocator (Storage/localStorage bp-path) (SimpleMeterRegistry.))))

(defmethod xtn/apply-config! ::local [^Xtdb$Config config _ {:keys [path max-cache-bytes max-cache-entries]}]
  (.storage config (cond-> (Storage/localStorage (util/->path path))
                     max-cache-bytes (.maxCacheBytes max-cache-bytes)
                     max-cache-entries (.maxCacheEntries max-cache-entries))))

(defn- list-files-under-prefix [^SortedSet !os-file-names ^Path prefix]
  (let [prefix-depth (.getNameCount prefix)]
    (->> (.tailSet ^SortedSet !os-file-names prefix)
         (take-while #(.startsWith ^Path % prefix))
         (keep (fn [^Path path]
                 (when (> (.getNameCount path) prefix-depth)
                   (.subpath path 0 (inc prefix-depth)))))
         (distinct))))

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
          (log/infof "Error caught in upload-multipart-buffers - aborting multipart upload of %s" k)
          @(.abort upload)
          (catch Throwable abort-t
            (log/info "Throwable caught when aborting upload-multipart-buffers - " abort-t)
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
                             ^MemoryCache memory-cache
                             ^DiskCache disk-cache
                             ^FileListCache file-list-cache
                             ^ObjectStore object-store
                             ^SortedSet !os-file-names
                             ^AutoCloseable !os-file-name-subscription]
  IBufferPool
  (getBuffer [_ k]
    (when k
      (.get memory-cache k
            (fn [k]
              (-> (.get disk-cache k
                        (fn [^Path k, ^Path tmp-file]
                          (.getObject object-store k tmp-file)))

                  (.thenApply (fn [^DiskCache$Entry entry]
                                ;; cleanup action - when the entry is evicted from the memory cache,
                                ;; release one count on the disk-cache entry.
                                (Pair. (.getPath entry) entry))))))))

  (listAllObjects [_]
    (vec !os-file-names))

  (listObjects [_ dir]
    (list-files-under-prefix !os-file-names dir))

  (openArrowWriter [_ k rel]
    (let [tmp-path (.createTempPath disk-cache)]
      (util/with-close-on-catch [file-ch (util/->file-channel tmp-path util/write-truncate-open-opts)
                                 unl (.startUnload rel file-ch)]

        (reify ArrowWriter
          (writeBatch [_] (.writeBatch unl))

          (end [_]
            (.endFile unl)
            (.close file-ch)

            (upload-arrow-file allocator object-store k tmp-path)

            (.appendFileNotification file-list-cache (flc/map->FileNotification {:added [k]}))

            (.put disk-cache k tmp-path))

          (close [_]
            (util/close unl)
            (when (.isOpen file-ch)
              (.close file-ch)))))))

  (putObject [_ k buffer]
    @(-> (.putObject object-store k buffer)
         (.thenApply (fn [_]
                       (.appendFileNotification file-list-cache (flc/map->FileNotification {:added [k]}))))))

  EvictBufferTest
  (evict-cached-buffer! [_ k]
    (.invalidate memory-cache k))

  (close [_] 
    (util/close memory-cache)
    (util/close @!os-file-name-subscription)
    (util/close object-store)
    (util/close allocator)))

(set! *unchecked-math* :warn-on-boxed)

(defn calculate-limit-from-percentage-of-disk [^Path local-disk-cache ^long percentage]
  ;; Creating the empty directory if it doesn't exist
  (when-not (util/path-exists local-disk-cache)
    (util/mkdirs local-disk-cache))
  
  (let [file-store (Files/getFileStore local-disk-cache)
        total-disk-space-bytes (.getTotalSpace file-store)
        disk-size-limit (long (* total-disk-space-bytes (/ percentage 100.0)))]
    (log/debugf "%s%% of total disk space on filestore %s is %s bytes" percentage (.name file-store) disk-size-limit)
    disk-size-limit))

(defn open-remote-storage ^xtdb.IBufferPool [^BufferAllocator allocator, ^Storage$RemoteStorageFactory factory, ^FileListCache file-list-cache, ^MeterRegistry metrics-registry]
  (util/with-close-on-catch [object-store (.openObjectStore (.getObjectStore factory))]
    (let [!os-file-names (ConcurrentSkipListSet.)
          !os-file-name-subscription (promise)
          ^Path disk-cache-root (.getLocalDiskCache factory)]

      (.subscribeFileNotifications file-list-cache
                                   (reify FileListCache$Subscriber
                                     (accept [_ {:keys [added]}]
                                       (.addAll !os-file-names added))

                                     (onSubscribe [_ subscription]
                                       (deliver !os-file-name-subscription subscription))))

      (.addAll !os-file-names (.listAllObjects object-store))

      (let [memory-cache (MemoryCache. allocator (.getMaxCacheBytes factory))
            disk-cache (DiskCache. disk-cache-root
                                   (or (.getMaxDiskCacheBytes factory)
                                       (calculate-limit-from-percentage-of-disk disk-cache-root (.getMaxDiskCachePercentage factory))))]
        (metrics/add-cache-gauges metrics-registry "memorycache" (.getStats memory-cache))
        (metrics/add-cache-gauges metrics-registry "diskcache" (.getStats disk-cache))
        (->RemoteBufferPool (->buffer-pool-child-allocator allocator metrics-registry)
                            memory-cache
                            disk-cache
                            file-list-cache object-store !os-file-names !os-file-name-subscription)))))

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

(defmethod xtn/apply-config! ::remote [^Xtdb$Config config _ {:keys [object-store local-disk-cache max-cache-bytes max-cache-entries max-disk-cache-bytes max-disk-cache-percentage]}]
  (.storage config (cond-> (Storage/remoteStorage (let [[tag opts] object-store]
                                                    (->object-store-factory tag opts))
                                                  (util/->path local-disk-cache))
                     max-cache-bytes (.maxCacheBytes max-cache-bytes)
                     max-cache-entries (.maxCacheEntries max-cache-entries)
                     max-disk-cache-bytes (.maxDiskCacheBytes max-disk-cache-bytes)
                     max-disk-cache-percentage (.maxDiskCachePercentage max-disk-cache-percentage))))

(defn get-footer ^ArrowFooter [^IBufferPool bp ^Path path]
  (util/with-open [arrow-buf (.getBuffer bp path)]
    (util/read-arrow-footer arrow-buf)))

(defn open-record-batch ^ArrowRecordBatch [^IBufferPool bp ^Path path block-idx]
  (util/with-open [arrow-buf (.getBuffer bp path)]
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
   :factory factory
   :file-list-cache (ig/ref :xtdb/log)
   :metrics-registry (ig/ref :xtdb.metrics/registry)})

(defmethod ig/init-key :xtdb/buffer-pool [_ {:keys [allocator ^Storage$Factory factory, ^FileListCache file-list-cache metrics-registry]}]
  (.openStorage factory allocator file-list-cache metrics-registry))

(defmethod ig/halt-key! :xtdb/buffer-pool [_ ^IBufferPool buffer-pool]
  (util/close buffer-pool))
