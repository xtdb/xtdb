(ns xtdb.buffer-pool
  (:require xtdb.object-store
            [xtdb.util :as util]
            [juxt.clojars-mirrors.integrant.core :as ig])
  (:import clojure.lang.MapEntry
           xtdb.LRU
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
  (^boolean evictBuffer [^String k]))

(deftype BufferPool [^BufferAllocator allocator ^ObjectStore object-store ^Map buffers ^StampedLock buffers-lock
                     ^Path cache-path]
  IBufferPool
  (getBuffer [_ k]
    (if (nil? k)
      (CompletableFuture/completedFuture nil)
      (let [v (let [stamp (.readLock buffers-lock)]
                (try
                  (.getOrDefault buffers k ::not-found)
                  (finally
                    (.unlock buffers-lock stamp))))]
        (if-not (= ::not-found v)
          (CompletableFuture/completedFuture (doto ^ArrowBuf v
                                               (-> (.getReferenceManager) (.retain))))
          (-> (if cache-path
                (-> (.getObject object-store k (.resolve cache-path (str (UUID/randomUUID))))
                    (util/then-apply (fn [buffer-path]
                                       (MapEntry/create (util/->mmap-path buffer-path) buffer-path))))

                (-> (.getObject object-store k)
                    (util/then-apply (fn [nio-buffer]
                                       (MapEntry/create nio-buffer nil)))))

              (util/then-apply
                (fn [[nio-buffer path]]
                  (let [stamp (.writeLock buffers-lock)
                        key-exists? (.containsKey ^Map buffers k)]
                    (try
                      (doto ^ArrowBuf (.computeIfAbsent buffers k (util/->jfn (fn [_]
                                                                                (util/->arrow-buf-view allocator
                                                                                                       nio-buffer
                                                                                                       (when path
                                                                                                         #(util/delete-file path))))))
                        (-> (.getReferenceManager) (.retain)))
                      (finally
                        (.unlock buffers-lock stamp)
                        (when (and key-exists? path)
                          (util/delete-file path))))))))))))

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
                    (.capacity buffer)))) )

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
