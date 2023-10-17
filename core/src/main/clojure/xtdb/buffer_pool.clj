(ns xtdb.buffer-pool
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.object-store :as object-store]
            [xtdb.util :as util]
            [xtdb.object-store :as os])
  (:import java.io.Closeable
           (java.nio ByteBuffer)
           (java.nio.channels FileChannel$MapMode)
           [java.nio.file FileSystems FileVisitOption Files LinkOption Path]
           [java.util Map NavigableMap]
           [java.util.concurrent CompletableFuture ConcurrentSkipListMap]
           (java.util.concurrent.atomic AtomicLong)
           java.util.NavigableMap
           [org.apache.arrow.memory ArrowBuf BufferAllocator]
           (org.apache.arrow.vector VectorSchemaRoot)
           (org.apache.arrow.vector.ipc.message ArrowFooter ArrowRecordBatch)
           xtdb.IBufferPool
           xtdb.object_store.ObjectStore
           xtdb.util.ArrowBufLRU))

(set! *unchecked-math* :warn-on-boxed)

(defn- retain [^ArrowBuf buf] (.retain (.getReferenceManager buf)) buf)

(defn- cache-get ^ArrowBuf [^Map buffers k]
  (locking buffers
    (some-> (.get buffers k) retain)))

(def ^AtomicLong cache-miss-byte-counter (AtomicLong.))
(def ^AtomicLong cache-hit-byte-counter (AtomicLong.))
(def io-wait-nanos-counter (atom 0N))

(defn clear-cache-counters []
  (.set cache-miss-byte-counter 0)
  (.set cache-hit-byte-counter 0)
  (reset! io-wait-nanos-counter 0N))

(defn record-cache-miss [^ArrowBuf arrow-buf]
  (.addAndGet cache-miss-byte-counter (.capacity arrow-buf)))

(defn record-cache-hit [^ArrowBuf arrow-buf]
  (.addAndGet cache-hit-byte-counter (.capacity arrow-buf)))

(defn record-io-wait [^long start-ns]
  (swap! io-wait-nanos-counter +' (- (System/nanoTime) start-ns)))

(defn- cache-compute
  "Returns a pair [hit-or-miss, buf] computing the cached ArrowBuf from (f) if needed.
  `hit-or-miss` is true if the buffer was found, false if the object was added as part of this call."
  [^Map buffers k f]
  (locking buffers
    (let [hit (.containsKey ^Map buffers k)
          arrow-buf (if hit (.get buffers k) (let [buf (f)] (.put buffers k buf) buf))]
      (if hit (record-cache-hit arrow-buf) (record-cache-miss arrow-buf))
      [hit (retain arrow-buf)])))

(deftype MemoryBufferPool [^BufferAllocator allocator ^NavigableMap buffers]
  IBufferPool
  (getBuffer [_ k]
    (CompletableFuture/completedFuture
     (when k
       (when-let [cached-buffer (cache-get buffers k)]
         (record-cache-hit cached-buffer)
         cached-buffer))))

  (getRangeBuffer [_ k start len]
    (object-store/ensure-shared-range-oob-behaviour start len)
    (CompletableFuture/completedFuture
     (when k
       (when-let [buffer (when-let [^ArrowBuf full-buffer (cache-get buffers k)]
                           (.slice full-buffer start len))]
         (record-cache-hit buffer)
         buffer))))

  (putObject [_ k buf]
    (.put buffers k (util/->arrow-buf-view allocator buf))
    (CompletableFuture/completedFuture nil))

  (listObjects [_] (vec (.keySet buffers)))

  ;; TODO #2740
  (listObjects [_ prefix]
    (->> (.keySet (.tailMap buffers prefix))
         (into [] (take-while #(str/starts-with? % prefix)))))

  Closeable
  (close [_]
    (locking buffers
      (let [i (.iterator (.values buffers))]
        (while (.hasNext i)
          (util/close (.next i))
          (.remove i)))
      (util/close allocator))))

(defmethod ig/prep-key ::in-memory [_ opts]
  (into {:allocator (ig/ref :xtdb/allocator)}
        opts))

(defmethod ig/init-key ::in-memory [_ {:keys [^BufferAllocator allocator]}]
  (util/with-close-on-catch [allocator (util/->child-allocator allocator "buffer-pool")]
    (->MemoryBufferPool allocator (ConcurrentSkipListMap.))))

(defmethod ig/halt-key! ::in-memory [_ buffer-pool]
  (util/close buffer-pool))

(derive ::in-memory :xtdb/buffer-pool)

(defn get-path ^ByteBuffer [^Path root-path, ^String k]
  (let [from-path (.resolve root-path k)]
    (when-not (util/path-exists from-path)
      (throw (os/obj-missing-exception k)))

    (util/->mmap-path from-path)))

(defn get-path-range ^ByteBuffer [^Path root-path, ^String k, ^long start, ^long len]
  (os/ensure-shared-range-oob-behaviour start len)

  (let [from-path (.resolve root-path k)]
    (when-not (util/path-exists from-path)
      (throw (os/obj-missing-exception k)))

    (with-open [in (util/->file-channel from-path #{:read})]
      (.map in FileChannel$MapMode/READ_ONLY start (max 1 (min (- (.size in) start) len))))))

(defn put-path [^Path root-path, ^String k, ^ByteBuffer buf]
  (let [buf (.duplicate buf)
        to-path (.resolve root-path k)]
    (util/mkdirs (.getParent to-path))

    (if (identical? (FileSystems/getDefault) (.getFileSystem to-path))
      (if (util/path-exists to-path)
        to-path
        (util/write-buffer-to-path-atomically buf root-path to-path))

      (util/write-buffer-to-path buf to-path))))

(defn list-path
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

(deftype LocalBufferPool [^BufferAllocator allocator, ^Map buffers, ^Path root-path]
  IBufferPool
  (getBuffer [_ k]
    (CompletableFuture/completedFuture
     (when k
       (if-let [cached-buffer (cache-get buffers k)]
         (do
           (record-cache-hit cached-buffer)
           cached-buffer)

         (let [nio-buffer (get-path root-path k)
               create-arrow-buf #(util/->arrow-buf-view allocator nio-buffer)
               [_ buf] (cache-compute buffers k create-arrow-buf)]
           buf)))))

  (getRangeBuffer [_ k start len]
    (object-store/ensure-shared-range-oob-behaviour start len)

    (CompletableFuture/completedFuture
     (when k
       (if-let [cached-buffer (or (cache-get buffers [k start len])
                                  (when-let [^ArrowBuf cached-full-buffer (cache-get buffers k)]
                                    (.slice cached-full-buffer start len)))]
         (do
           (record-cache-hit cached-buffer)
           cached-buffer)

         (let [nio-buffer (get-path-range root-path k start len)
               create-arrow-buf #(util/->arrow-buf-view allocator nio-buffer)
               [_ buf] (cache-compute buffers [k start len] create-arrow-buf)]
           buf)))))

  (putObject [_ k buf] (CompletableFuture/completedFuture (put-path root-path k buf)))
  (listObjects [_this] (list-path root-path))
  (listObjects [_this dir] (list-path root-path dir))

  Closeable
  (close [_]
    (locking buffers
      (let [i (.iterator (.values buffers))]
        (while (.hasNext i)
          (util/close (.next i))
          (.remove i)))
      (util/close allocator))))

(defn- ->buffer-cache [^long cache-entries-size ^long cache-bytes-size]
  (ArrowBufLRU. 16 cache-entries-size cache-bytes-size))

(defmethod ig/prep-key ::local [_ opts]
  (-> (merge {:cache-entries-size 1024
              :cache-bytes-size 536870912
              :allocator (ig/ref :xtdb/allocator)}
             opts)
      (util/maybe-update :path util/->path)))

(defmethod ig/init-key ::local [_ {:keys [^Path path, ^BufferAllocator allocator,
                                           ^long cache-entries-size, ^long cache-bytes-size]}]
  (when-not (util/path-exists path)
    (util/mkdirs path))

  (util/with-close-on-catch [allocator (util/->child-allocator allocator "buffer-pool")]
    (->LocalBufferPool allocator (->buffer-cache cache-entries-size cache-bytes-size) path)))

(defmethod ig/halt-key! ::local [_ buffer-pool]
  (util/close buffer-pool))

(derive ::local :xtdb/buffer-pool)

(deftype RemoteBufferPool [^BufferAllocator allocator ^ObjectStore object-store ^Map buffers ^Path cache-path]
  IBufferPool
  (getBuffer [_ k]
    (if (nil? k)
      (CompletableFuture/completedFuture nil)
      (let [cached-buffer (cache-get buffers k)]
        (cond
          cached-buffer
          (do
            (record-cache-hit cached-buffer)
            (CompletableFuture/completedFuture cached-buffer))

          cache-path
          (let [start-ns (System/nanoTime)
                buffer-cache-path (.resolve cache-path k)]
            (-> (if (util/path-exists buffer-cache-path)
                  (CompletableFuture/completedFuture buffer-cache-path)
                  (do (util/create-parents buffer-cache-path)
                      (.getObject object-store k buffer-cache-path)))
                (util/then-apply
                  (fn [buffer-path]
                    (record-io-wait start-ns)
                    (let [cleanup-file #(util/delete-file buffer-path)]
                      (try
                        (let [nio-buffer (util/->mmap-path buffer-path)
                              create-arrow-buf #(util/->arrow-buf-view allocator nio-buffer cleanup-file)
                              [_ buf] (cache-compute buffers k create-arrow-buf)]
                          buf)
                        (catch Throwable t
                          (try (cleanup-file) (catch Throwable t1 (log/error t1 "Error caught cleaning up file during exception handling")))
                          (throw t))))))))

          :else
          (let [start-ns (System/nanoTime)]
            (-> (.getObject object-store k)
                (util/then-apply
                  (fn [nio-buffer]
                    (record-io-wait start-ns)
                    (let [create-arrow-buf #(util/->arrow-buf-view allocator nio-buffer)
                          [_ buf] (cache-compute buffers k create-arrow-buf)]
                      buf)))))))))

  (getRangeBuffer [_ k start len]
    (object-store/ensure-shared-range-oob-behaviour start len)
    (if (nil? k)
      (CompletableFuture/completedFuture nil)
      (let [cached-full-buffer (cache-get buffers k)

            cached-buffer
            (or (cache-get buffers [k start len])
                (when ^ArrowBuf cached-full-buffer
                  (.slice cached-full-buffer start len)))]

        (if cached-buffer
          (do
            (record-cache-hit cached-buffer)
            (CompletableFuture/completedFuture cached-buffer))
          (let [start-ns (System/nanoTime)]
            (-> (.getObjectRange object-store k start len)
                (util/then-apply
                  (fn [nio-buffer]
                    (record-io-wait start-ns)
                    (let [create-arrow-buf #(util/->arrow-buf-view allocator nio-buffer)
                          [_ buf] (cache-compute buffers [k start len] create-arrow-buf)]
                      buf)))))))))

  (putObject [_ k buf] (.putObject object-store k buf))
  (listObjects [_] (.listObjects object-store))
  (listObjects [_ dir] (.listObjects object-store dir))

  Closeable
  (close [_]
    (locking buffers
      (let [i (.iterator (.values buffers))]
        (while (.hasNext i)
          (util/close (.next i))
          (.remove i)))
      (util/close allocator))))

(defmethod ig/prep-key ::remote [_ opts]
  (-> (merge {:cache-entries-size 1024
              :cache-bytes-size 536870912
              :allocator (ig/ref :xtdb/allocator)
              :object-store (ig/ref :xtdb/object-store)}
             opts)
      (util/maybe-update :cache-path util/->path)))

(defmethod ig/init-key ::remote
  [_ {:keys [^Path cache-path ^BufferAllocator allocator ^ObjectStore object-store ^long cache-entries-size ^long cache-bytes-size]}]
  (when (and cache-path (not (util/path-exists cache-path)))
    (util/mkdirs cache-path))
  (util/with-close-on-catch [allocator (util/->child-allocator allocator "buffer-pool")]
    (->RemoteBufferPool allocator object-store (->buffer-cache cache-entries-size cache-bytes-size) cache-path)))

(defmethod ig/halt-key! ::remote [_ buffer-pool]
  (util/close buffer-pool))

(derive ::remote :xtdb/buffer-pool)

(defn get-footer ^ArrowFooter [^IBufferPool bp path]
  (with-open [^ArrowBuf arrow-buf @(.getBuffer bp (str path))]
    (util/read-arrow-footer arrow-buf)))

(defn open-record-batch ^ArrowRecordBatch [^IBufferPool bp path block-idx]
  (with-open [^ArrowBuf arrow-buf @(.getBuffer bp (str path))]
    (let [footer (util/read-arrow-footer arrow-buf)
          blocks (.getRecordBatches footer)
          block (nth blocks block-idx nil)]
      (if-not block
        (throw (IndexOutOfBoundsException. "Record batch index out of bounds of arrow file"))
        (util/->arrow-record-batch-view block arrow-buf)))))

(defn open-vsr ^VectorSchemaRoot [bp path allocator]
  (let [footer (get-footer bp path)
        schema (.getSchema footer)]
    (VectorSchemaRoot/create schema allocator)))
