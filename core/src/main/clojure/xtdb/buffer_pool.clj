(ns xtdb.buffer-pool
  (:require [xtdb.object-store :as object-store]
            [xtdb.util :as util]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [clojure.tools.logging :as log])
  (:import xtdb.util.ArrowBufLRU
           xtdb.object_store.ObjectStore
           java.io.Closeable
           java.nio.file.Path
           [java.util Map UUID]
           java.util.concurrent.CompletableFuture
           java.util.concurrent.locks.StampedLock
           (java.util.concurrent.atomic AtomicLong)
           [org.apache.arrow.memory ArrowBuf BufferAllocator]
           (org.apache.arrow.vector VectorSchemaRoot)
           (org.apache.arrow.vector.ipc.message ArrowFooter ArrowRecordBatch)))

(set! *unchecked-math* :warn-on-boxed)

(definterface IBufferPool
  (^java.util.concurrent.CompletableFuture getBuffer [^String k])
  (^java.util.concurrent.CompletableFuture getRangeBuffer [^String k ^int start ^int len])
  (^boolean evictBuffer [^String k]))

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

(deftype BufferPool [^BufferAllocator allocator ^ObjectStore object-store ^Map buffers ^Path cache-path]
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
                              [hit buf] (cache-compute buffers k create-arrow-buf)]
                          (when hit (cleanup-file))
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

  (evictBuffer [_ k]
    (if-let [buffer (locking buffers
                      (.remove buffers k))]
      (do (util/close buffer)
          true)
      false))

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

(defmethod ig/prep-key ::buffer-pool [_ opts]
  (-> (merge {:cache-entries-size 1024
              :cache-bytes-size 536870912
              :allocator (ig/ref :xtdb/allocator)
              :object-store (ig/ref :xtdb/object-store)}
             opts)
      (util/maybe-update :cache-path util/->path)))

(defmethod ig/init-key ::buffer-pool
  [_ {:keys [^Path cache-path ^BufferAllocator allocator ^ObjectStore object-store ^long cache-entries-size ^long cache-bytes-size]}]
  (when (and cache-path (not (util/path-exists cache-path)))
    (util/mkdirs cache-path))
  (util/with-close-on-catch [allocator (util/->child-allocator allocator "buffer-pool")]
    (->BufferPool  allocator object-store (->buffer-cache cache-entries-size cache-bytes-size) cache-path)))

(defmethod ig/halt-key! ::buffer-pool [_ ^BufferPool buffer-pool]
  (.close buffer-pool))

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
