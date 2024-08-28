(ns xtdb.buffer-pool
  (:require [clojure.tools.logging :as log]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.node :as xtn]
            [xtdb.object-store :as os]
            [xtdb.util :as util])
  (:import (clojure.lang PersistentQueue)
           (com.github.benmanes.caffeine.cache AsyncCache Cache Caffeine RemovalListener Weigher Policy$Eviction)
           [java.io ByteArrayOutputStream Closeable File]
           (java.nio ByteBuffer)
           (java.nio.channels Channels ClosedByInterruptException)
           [java.nio.file FileVisitOption Files LinkOption Path]
           [java.nio.file.attribute FileAttribute BasicFileAttributes]
           [java.util NavigableMap TreeMap]
           (java.util NavigableMap)
           [java.util.concurrent CompletableFuture ForkJoinPool TimeUnit]
           [java.util.function BiFunction]
           [org.apache.arrow.memory ArrowBuf BufferAllocator]
           (org.apache.arrow.vector VectorSchemaRoot)
           (org.apache.arrow.vector.ipc ArrowFileWriter)
           (org.apache.arrow.vector.ipc.message ArrowBlock ArrowFooter ArrowRecordBatch)
           (xtdb ArrowWriter IBufferPool)
           (xtdb.api.storage ObjectStore Storage Storage$Factory Storage$LocalStorageFactory Storage$RemoteStorageFactory)
           xtdb.api.Xtdb$Config
           (xtdb.multipart IMultipartUpload SupportsMultipart)))

(set! *unchecked-math* :warn-on-boxed)

(defprotocol EvictBufferTest
  (evict-cached-buffer! [bp ^Path k]))

(def ^:private min-multipart-part-size (* 5 1024 1024))
(def ^:private max-multipart-per-upload-concurrency 4)

;; bump this if the storage format changes in a backwards-incompatible way
(def version (let [version 4] (str "v" (util/->lex-hex-string version))))

(def ^java.nio.file.Path storage-root (util/->path version))

(defn- free-memory [^Cache memory-store]
  ;; closing should happen via the remove method
  (.invalidateAll memory-store)
  ;; HACK - invalidate is all async; investigate whether we can hook into caffeine
  (.awaitQuiescence (ForkJoinPool/commonPool) 100 TimeUnit/MILLISECONDS))

(defn- retain [^ArrowBuf buf] (.retain (.getReferenceManager buf)) buf)

(defn- cache-get ^ArrowBuf [^Cache memory-store k]
  (.. memory-store asMap (computeIfPresent k (reify BiFunction
                                               (apply [_ _k v]
                                                 (retain v))))))

(defn- cache-compute
  "Computing the cached ArrowBuf from (f) if needed."
  [^Cache memory-store k f]
  (.. memory-store asMap (compute k (reify BiFunction
                                      (apply [_ _k v]
                                        (retain (or v (f))))))))

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
  (evict-cached-buffer! [_ _k]
    ;; no-op,
    )

  Closeable
  (close [_]
    (locking memory-store
      (run! util/close (.values memory-store))
      (.clear memory-store))
    (util/close allocator)))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn open-in-memory-storage [^BufferAllocator allocator]
  (->MemoryBufferPool (.newChildAllocator allocator "buffer-pool" 0 Long/MAX_VALUE)
                      (TreeMap.)))

(defn- create-tmp-path ^Path [^Path disk-store]
  (Files/createTempFile (doto (.resolve disk-store ".tmp") util/mkdirs)
                        "upload" ".arrow"
                        (make-array FileAttribute 0)))


(defrecord LocalBufferPool [allocator, ^Cache memory-store, ^Path disk-store, ^long max-size]
  IBufferPool
  (getBuffer [_ k]
    (when k
      (or (cache-get memory-store k)
          (locking disk-store
            (let [buffer-cache-path (.resolve disk-store k)]
              ;; TODO could this not race with eviction? e.g exists for this cond, but is evicted before we can map the file into the cache?
              (when-not (util/path-exists buffer-cache-path)
                (throw (os/obj-missing-exception k)))

              (try
                (let [nio-buffer (util/->mmap-path buffer-cache-path)]
                  (cache-compute memory-store k #(util/->arrow-buf-view allocator nio-buffer)))
                (catch ClosedByInterruptException _
                  (throw (InterruptedException.)))))))))

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
              (catch ClosedByInterruptException e
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
    (.invalidate memory-store k)
    (.awaitQuiescence (ForkJoinPool/commonPool) 100 TimeUnit/MILLISECONDS))

  Closeable
  (close [_]
    (free-memory memory-store)
    (util/close allocator)))

(defn ->memory-buffer-cache ^Cache [^long max-cache-bytes]
  (-> (Caffeine/newBuilder)
      (.maximumWeight max-cache-bytes)
      (.weigher (reify Weigher
                  (weigh [_ _k v]
                    (.capacity ^ArrowBuf v))))
      (.removalListener (reify RemovalListener
                          (onRemoval [_ _k v _]
                            (util/close v))))
      (.build)))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn open-local-storage ^xtdb.IBufferPool [^BufferAllocator allocator, ^Storage$LocalStorageFactory factory]
  (->LocalBufferPool (.newChildAllocator allocator "buffer-pool" 0 Long/MAX_VALUE)
                     (->memory-buffer-cache (.getMaxCacheBytes factory))
                     (doto (-> (.getPath factory) (.resolve storage-root)) util/mkdirs)
                     (.getMaxCacheBytes factory)))

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
          (log/infof "Error caught in upload-multipart-buffers - aborting multipart upload of %s" k)
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

(defn update-evictor-key {:style/indent 2} [^AsyncCache local-disk-cache-evictor ^Path k update-fn]
  (-> (.asMap local-disk-cache-evictor)
      (.compute k (reify BiFunction
                    (apply [_ _k fut] (update-fn fut))))))

(defrecord RemoteBufferPool [allocator
                             ^Cache memory-store
                             ^Path local-disk-cache
                             ^AsyncCache local-disk-cache-evictor
                             ^ObjectStore object-store
                             !evictor-max-weight]
  IBufferPool
  (getBuffer [_ k]
    (when k
      (or (cache-get memory-store k)
          (let [buffer-cache-path (.resolve local-disk-cache k)
                {:keys [file-size ctx]} @(update-evictor-key local-disk-cache-evictor k
                                                             (fn [^CompletableFuture fut]
                                                               (-> (if (util/path-exists buffer-cache-path)
                                                                     (-> (or fut (CompletableFuture/completedFuture {:pinned? false}))
                                                                         (.thenApply (fn [{:keys [pinned?]}]
                                                                                       (log/tracef "Key %s found in local-disk-cache - returning buffer" k)
                                                                                       {:path buffer-cache-path :previously-pinned? pinned?})))
                                                                     (do (util/create-parents buffer-cache-path)
                                                                         (log/debugf "Key %s not found in local-disk-cache, loading from object store" k)
                                                                         (-> (.getObject object-store k buffer-cache-path)
                                                                             (.thenApply (fn [path]
                                                                                           {:path path :previously-pinned? false})))))
                                                                   (.thenApply (fn [{:keys [path previously-pinned?]}]
                                                                                 (let [file-size (util/size-on-disk path)
                                                                                       nio-buffer (util/->mmap-path path)
                                                                                       buffer-release-fn (fn []
                                                                                                           (update-evictor-key local-disk-cache-evictor k
                                                                                                             (fn [^CompletableFuture fut]
                                                                                                               (some-> fut
                                                                                                                       (.thenApply (fn [{:keys [pinned? file-size]}]
                                                                                                                                     (when pinned?
                                                                                                                                       (swap! !evictor-max-weight + file-size))
                                                                                                                                     {:pinned? false :file-size file-size}))))))
                                                                                       create-arrow-buf #(util/->arrow-buf-view allocator nio-buffer buffer-release-fn)
                                                                                       buf (cache-compute memory-store k create-arrow-buf)]
                                                                                   {:pinned? true :file-size file-size :ctx {:buf buf :previously-pinned? previously-pinned?}}))))))
                {:keys [previously-pinned? buf]} ctx]

            (when-not previously-pinned?
              (swap! !evictor-max-weight - file-size))

            buf))))

  (listAllObjects [_] (.listAllObjects object-store))

  (listObjects [_ dir] (.listObjects object-store dir))

  (openArrowWriter [_ k rel]
    (let [tmp-path (create-tmp-path local-disk-cache)]
      (util/with-close-on-catch [file-ch (util/->file-channel tmp-path util/write-truncate-open-opts)
                                 unl (.startUnload rel file-ch)]

        (reify ArrowWriter
          (writeBatch [_] (.writeBatch unl))

          (end [_]
            (.endFile unl)
            (.close file-ch)

            (upload-arrow-file allocator object-store k tmp-path)

            (let [buffer-cache-path (.resolve local-disk-cache k)]
              (update-evictor-key local-disk-cache-evictor k
                                  (fn [^CompletableFuture fut]
                                    (-> (or fut (CompletableFuture/completedFuture {:pinned? false}))
                                        (.thenApply (fn [{:keys [pinned?]}]
                                                      (log/tracef "Writing arrow file, %s, to local disk cache" k)
                                                      (util/create-parents buffer-cache-path)
                                                      ;; see #2847
                                                      (util/atomic-move tmp-path buffer-cache-path)
                                                      {:pinned? pinned? :file-size (util/size-on-disk buffer-cache-path)})))))))

          (close [_]
            (util/close unl)
            (when (.isOpen file-ch)
              (.close file-ch)))))))

  (putObject [_ k buffer]
    @(.putObject object-store k buffer))

  EvictBufferTest
  (evict-cached-buffer! [_ k]
    (.invalidate memory-store k)
    (.awaitQuiescence (ForkJoinPool/commonPool) 100 TimeUnit/MILLISECONDS))

  Closeable
  (close [_]
    (free-memory memory-store)
    (util/close object-store)
    (util/close allocator)))

(set! *unchecked-math* :warn-on-boxed)

(defn ->local-disk-cache-evictor ^com.github.benmanes.caffeine.cache.AsyncCache [size ^Path local-disk-cache]
  (log/debugf "Creating local-disk-cache with size limit of %s bytes" size)
  (let [cache (-> (Caffeine/newBuilder)
                  (.maximumWeight size)
                  (.weigher (reify Weigher
                              (weigh [_ _k {:keys [pinned? file-size]}]
                                (if pinned? 0 file-size))))
                  (.evictionListener (reify RemovalListener
                                       (onRemoval [_ k {:keys [file-size]} _]
                                         (log/debugf "Removing file %s from local-disk-cache - exceeded size limit (freed %s bytes)" k file-size)
                                         (util/delete-file (.resolve local-disk-cache ^Path k)))))
                  (.buildAsync))
        synced-cache (.synchronous cache)]
    
    ;; Load local disk cache into cache
    (let [files (filter #(.isFile ^File %) (file-seq (.toFile local-disk-cache)))
          file-infos (map (fn [^File file]
                            (let [^BasicFileAttributes attrs (Files/readAttributes (.toPath file) BasicFileAttributes  ^"[Ljava.nio.file.LinkOption;" (make-array LinkOption 0))
                                  last-accessed-ms (.toMillis (.lastAccessTime attrs))
                                  last-modified-ms (.toMillis (.lastModifiedTime attrs))]
                              {:file-path (util/->path file)
                               :last-access-time (max last-accessed-ms last-modified-ms)}))
                          files)
          ordered-files (sort-by #(:last-access-time %) file-infos)]
      (doseq [{:keys [file-path]} ordered-files]
        (let [k (.relativize local-disk-cache file-path)]
          (.put synced-cache k {:pinned? false
                                :file-size (util/size-on-disk file-path)}))))
    cache))

(defn calculate-limit-from-percentage-of-disk [^Path local-disk-cache ^long percentage]
  ;; Creating the empty directory if it doesn't exist
  (when-not (util/path-exists local-disk-cache)
    (util/mkdirs local-disk-cache))
  
  (let [file-store (Files/getFileStore local-disk-cache)
        total-disk-space-bytes (.getTotalSpace file-store)
        disk-size-limit (long (* total-disk-space-bytes (/ percentage 100.0)))]
    (log/debugf "%s%% of total disk space on filestore %s is %s bytes" percentage (.name file-store) disk-size-limit)
    disk-size-limit))

(defn open-remote-storage ^xtdb.IBufferPool [^BufferAllocator allocator, ^Storage$RemoteStorageFactory factory]
  (util/with-close-on-catch [object-store (.openObjectStore (.getObjectStore factory))]
    (let [^Path local-disk-cache (.getLocalDiskCache factory)
          local-disk-size-limit (or (.getMaxDiskCacheBytes factory)
                                    (calculate-limit-from-percentage-of-disk local-disk-cache (.getMaxDiskCachePercentage factory)))
          ^AsyncCache local-disk-cache-evictor (->local-disk-cache-evictor local-disk-size-limit local-disk-cache)
          !evictor-max-weight (atom local-disk-size-limit)]

      ;; Add watcher to max-weight atom - when it changes, update the cache max weight
      (add-watch !evictor-max-weight :update-cache-max-weight
                 (fn [_ _ _ ^long new-size]
                   (let [sync-cache (.synchronous local-disk-cache-evictor)
                         ^Policy$Eviction eviction-policy (.get (.eviction (.policy sync-cache)))]
                     (.setMaximum eviction-policy (max 0 new-size)))))

      
      (->RemoteBufferPool (.newChildAllocator allocator "buffer-pool" 0 Long/MAX_VALUE)
                          (->memory-buffer-cache (.getMaxCacheBytes factory))
                          local-disk-cache
                          local-disk-cache-evictor
                          object-store
                          !evictor-max-weight))))

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
   :factory factory})

(defmethod ig/init-key :xtdb/buffer-pool [_ {:keys [allocator ^Storage$Factory factory]}]
  (.openStorage factory allocator))

(defmethod ig/halt-key! :xtdb/buffer-pool [_ ^IBufferPool buffer-pool]
  (util/close buffer-pool))
