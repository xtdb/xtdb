(ns xtdb.buffer-pool
  (:require [xtdb.object-store :as object-store]
            [xtdb.util :as util]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [clojure.tools.logging :as log])
  (:import (xtdb.util ArrowBufLRU)
           (xtdb.object_store ObjectStore)
           java.io.Closeable
           java.nio.file.Path
           [java.util Map UUID]
           java.util.concurrent.CompletableFuture
           java.util.concurrent.locks.StampedLock
           [org.apache.arrow.memory ArrowBuf BufferAllocator]
           (org.apache.arrow.vector VectorSchemaRoot)
           (org.apache.arrow.vector.ipc.message ArrowFooter ArrowRecordBatch)))

(set! *unchecked-math* :warn-on-boxed)

(definterface IBufferPool
  (^java.util.concurrent.CompletableFuture getBuffer [^String k])
  (^java.util.concurrent.CompletableFuture getRangeBuffer [^String k ^int start ^int len])
  (^boolean evictBuffer [^String k]))

(defn- cache-get ^ArrowBuf [^Map buffers ^StampedLock buffers-lock k]
  (let [stamp (.readLock buffers-lock)]
    (try
      (.get buffers k)
      (finally
        (.unlock buffers-lock stamp)))))

(defn- cache-compute
  "Returns a pair [hit-or-miss, buf] computing the cached ArrowBuf from (f) if needed.
  `hit-or-miss` is true if the buffer was found, false if the object was added as part of this call."
  [^Map buffers ^StampedLock buffers-lock k f]
  (let [stamp (.writeLock buffers-lock)
        hit (.containsKey ^Map buffers k)]
    (try
      [hit (.computeIfAbsent buffers k (util/->jfn (fn [_] (f))))]
      (finally
        (.unlock buffers-lock stamp)))))

(defn- retain [^ArrowBuf buf] (.retain (.getReferenceManager buf)) buf)

(deftype BufferPool [^BufferAllocator allocator ^ObjectStore object-store ^Map buffers ^StampedLock buffers-lock
                     ^Path cache-path]
  IBufferPool
  (getBuffer [_ k]
    (if (nil? k)
      (CompletableFuture/completedFuture nil)
      (let [cached-buffer (cache-get buffers buffers-lock k)]
        (cond
          cached-buffer (CompletableFuture/completedFuture (retain cached-buffer))

          cache-path
          (-> (.getObject object-store k (.resolve cache-path (str (UUID/randomUUID))))
              (util/then-apply
                (fn [buffer-path]
                  (let [cleanup-file #(util/delete-file buffer-path)]
                    (try
                      (let [nio-buffer (util/->mmap-path buffer-path)
                            create-arrow-buf #(util/->arrow-buf-view allocator nio-buffer cleanup-file)
                            [hit buf] (cache-compute buffers buffers-lock k create-arrow-buf)]
                        (when hit (cleanup-file))
                        (retain buf)
                        buf)
                      (catch Throwable t
                        (try (cleanup-file) (catch Throwable t1 (log/error t1 "Error caught cleaning up file during exception handling")))
                        (throw t)))))))

          :else
          (-> (.getObject object-store k)
              (util/then-apply
                (fn [nio-buffer]
                  (let [create-arrow-buf #(util/->arrow-buf-view allocator nio-buffer)
                        [_ buf] (cache-compute buffers buffers-lock k create-arrow-buf)]
                    (retain buf)))))))))

  (getRangeBuffer [_ k start len]
    (object-store/ensure-shared-range-oob-behaviour start len)
    (if (nil? k)
      (CompletableFuture/completedFuture nil)
      (let [cached-full-buffer (cache-get buffers buffers-lock k)

            cached-buffer
            (or (cache-get buffers buffers-lock [k start len])
                (when ^ArrowBuf cached-full-buffer
                  (.slice cached-full-buffer start len)))]

        (if cached-buffer
          (CompletableFuture/completedFuture (retain cached-buffer))
          (-> (.getObjectRange object-store k start len)
              (util/then-apply
                (fn [nio-buffer]
                  (let [create-arrow-buf #(util/->arrow-buf-view allocator nio-buffer)
                        [_ buf] (cache-compute buffers buffers-lock [k start len] create-arrow-buf)]
                    (retain buf)))))))))

  (evictBuffer [_ k]
    (if-let [buffer (let [stamp (.writeLock buffers-lock)]
                      (try
                        (.remove buffers k)
                        (finally
                          (.unlock buffers-lock stamp))))]
      (do (util/close buffer)
          true)
      false))

  Closeable
  (close [_]
    (let [stamp (.writeLock buffers-lock)]
      (try
        (let [i (.iterator (.values buffers))]
          (while (.hasNext i)
            (util/close (.next i))
            (.remove i)))
        (finally
          (.unlock buffers-lock stamp)))
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
  (when cache-path
    (util/delete-dir cache-path)
    (util/mkdirs cache-path))
  (util/with-close-on-catch [allocator (util/->child-allocator allocator "buffer-pool")]
    (->BufferPool  allocator object-store
                   (->buffer-cache cache-entries-size cache-bytes-size) (StampedLock.) cache-path)))

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
