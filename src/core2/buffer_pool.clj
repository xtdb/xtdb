(ns core2.buffer-pool
  (:require core2.object-store
            [core2.system :as sys]
            [core2.util :as util])
  (:import core2.object_store.ObjectStore
           java.io.Closeable
           [java.nio.file Files Path]
           [java.util.concurrent CompletableFuture ConcurrentHashMap]
           java.util.Map
           [org.apache.arrow.memory ArrowBuf BufferAllocator]))

(definterface IBufferPool
  (^java.util.concurrent.CompletableFuture getBuffer [^String k])
  (^boolean evictBuffer [^String k]))

(deftype BufferPool [^BufferAllocator allocator ^ObjectStore object-store ^Map buffers
                     ^Path cache-path]
  IBufferPool
  (getBuffer [_ k]
    (if (nil? k)
      (CompletableFuture/completedFuture nil)
      (let [v (.getOrDefault buffers k ::not-found)]
        (if-not (= ::not-found v)
          (CompletableFuture/completedFuture (doto ^ArrowBuf v
                                               (.retain)))
          (let [buffer-path (when cache-path
                              (.resolve cache-path k))]
            (-> (if (and buffer-path (util/path-exists buffer-path))
                  (CompletableFuture/completedFuture (util/->mmap-path buffer-path))
                  (-> (.getObject object-store k)
                      (util/then-apply (fn [buf]
                                         (if buffer-path
                                           (do
                                             (util/write-buffer-to-path buf buffer-path)
                                             (util/->mmap-path buffer-path))
                                           buf)))))
                (util/then-apply
                  (fn [buf]
                    (doto ^ArrowBuf (.computeIfAbsent buffers k (util/->jfn (fn [_]
                                                                              (util/->arrow-buf-view allocator buf))))
                      (.retain))))))))))

  (evictBuffer [_ k]
    (if-let [^ArrowBuf buffer (.remove buffers k)]
      (do (.release buffer)
          (when cache-path
            (Files/deleteIfExists (.resolve cache-path k)))
          true)
      false))

  Closeable
  (close [_]
    (let [i (.iterator (.values buffers))]
      (while (.hasNext i)
        (.release ^ArrowBuf (.next i))
        (.remove i)))))

(defn ->buffer-pool {::sys/deps {:allocator :core2/allocator
                                 :object-store :core2/object-store}
                     ::sys/args {:cache-path {:spec ::sys/path, :required? false}}}
  [{:keys [^Path cache-path ^BufferAllocator allocator ^ObjectStore object-store]}]
  (when cache-path
    (util/mkdirs cache-path))
  (->BufferPool allocator object-store (ConcurrentHashMap.) cache-path))
