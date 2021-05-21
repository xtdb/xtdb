(ns core2.buffer-pool
  (:require core2.object-store
            [core2.system :as sys]
            [core2.util :as util])
  (:import core2.object_store.ObjectStore
           core2.LRU
           clojure.lang.MapEntry
           java.io.Closeable
           [java.nio.file Files Path]
           java.util.concurrent.CompletableFuture
           java.util.concurrent.locks.StampedLock
           java.util.function.BiPredicate
           [java.util Map Map$Entry LinkedHashMap UUID]
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
                                               (.retain)))
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
                        (.retain))
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

(def default-buffer-cache-entries-size 1024)
(def default-buffer-cache-bytes-size (* 512 1024 1024))

(defn- buffer-cache-bytes-size ^long [^Map buffers]
  (long (reduce + (for [^ArrowBuf buffer (vals buffers)]
                    (.capacity buffer)))) )

(defn- ->buffer-cache [^long cache-entries-size ^long cache-bytes-size ^Path cache-path]
  (LRU. 16 (reify BiPredicate
             (test [_ map entry]
               (let [entries-size (.size ^Map map)]
                 (if (or (> entries-size cache-entries-size)
                         (and (> entries-size 1) (> (buffer-cache-bytes-size map) cache-bytes-size)))
                   (do (util/try-close (.getValue ^Map$Entry entry))
                       true)
                   false))))))

(defn ->buffer-pool {::sys/deps {:allocator :core2/allocator
                                 :object-store :core2/object-store}
                     ::sys/args {:cache-path {:spec ::sys/path, :required? false}
                                 :cache-entries-size {:spec ::sys/int :default default-buffer-cache-entries-size}
                                 :cache-bytes-size {:spec ::sys/int :default default-buffer-cache-bytes-size}}}
  [{:keys [^Path cache-path ^BufferAllocator allocator ^ObjectStore object-store ^long cache-entries-size ^long cache-bytes-size]}]
  (when cache-path
    (util/delete-dir cache-path)
    (util/mkdirs cache-path))
  (->BufferPool allocator object-store (->buffer-cache cache-entries-size cache-bytes-size cache-path) (StampedLock.) cache-path))
