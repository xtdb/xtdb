(ns xtdb.buffer-pool
  (:require [xtdb.object-store :as object-store]
            [xtdb.util :as util]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [clojure.tools.logging :as log])
  (:import xtdb.util.LRU
           xtdb.object_store.ObjectStore
           java.io.Closeable
           java.nio.file.Path
           [java.util Map Map$Entry UUID]
           java.util.concurrent.CompletableFuture
           java.util.concurrent.locks.StampedLock
           java.util.function.BiPredicate
           [org.apache.arrow.memory ArrowBuf BufferAllocator]))

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
      (do (util/try-close buffer)
          true)
      false))

  Closeable
  (close [_]
    (let [stamp (.writeLock buffers-lock)]
      (try
        (let [i (.iterator (.values buffers))]
          (while (.hasNext i)
            (util/try-close (.next i))
            (.remove i)))
        (finally
          (.unlock buffers-lock stamp))))))

(defn- buffer-cache-bytes-size ^long [^Map buffers]
  (long (reduce + (for [^ArrowBuf buffer (vals buffers)]
                    (.capacity buffer)))))

(defn- ->buffer-cache [^long cache-entries-size ^long cache-bytes-size]
  (LRU. 16 (reify BiPredicate
             (test [_ map entry]
               (let [entries-size (.size ^Map map)]
                 (if (or (> entries-size cache-entries-size)
                         (and (> entries-size 1) (> (buffer-cache-bytes-size map) cache-bytes-size)))
                   (do (util/try-close (.getValue ^Map$Entry entry))
                       true)
                   false))))))

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
  (->BufferPool allocator object-store (->buffer-cache cache-entries-size cache-bytes-size) (StampedLock.) cache-path))

(defmethod ig/halt-key! ::buffer-pool [_ ^BufferPool buffer-pool]
  (.close buffer-pool))
