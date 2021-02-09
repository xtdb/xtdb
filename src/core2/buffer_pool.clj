(ns core2.buffer-pool
  (:require [core2.object-store :as os]
            [core2.util :as util])
  (:import [core2.object_store ObjectStore]
           [java.io Closeable]
           [java.nio.file Files Path]
           [java.util Map]
           [java.util.concurrent CompletableFuture ConcurrentHashMap]
           [org.apache.arrow.memory ArrowBuf BufferAllocator]))

(definterface BufferPool
  (^java.util.concurrent.CompletableFuture getBuffer [^String k])
  (^boolean evictBuffer [^String k]))

(deftype MemoryMappedBufferPool [^Path root-path ^BufferAllocator allocator ^ObjectStore object-store ^Map buffers]
  BufferPool
  (getBuffer [_ k]
    (let [v (.getOrDefault buffers k ::not-found)]
      (if-not (= ::not-found v)
        (CompletableFuture/completedFuture (doto ^ArrowBuf v
                                             (.retain)))
        (util/then-apply
          (.getObject object-store k (.resolve root-path k))
          (fn [^Path path]
            (doto ^ArrowBuf (.computeIfAbsent buffers k (util/->jfn (fn [_]
                                                                      (util/->arrow-buf-view allocator (util/->mmap-path path)))))
              (.retain)))))))

  (evictBuffer [_ k]
    (when-let [^ArrowBuf buffer (.remove buffers k)]
      (.release buffer)
      (Files/deleteIfExists (.resolve root-path k))))

  Closeable
  (close [_]
    (let [i (.iterator (.values buffers))]
      (while (.hasNext i)
        (.release ^ArrowBuf (.next i))
        (.remove i)))))

(defn ->memory-mapped-buffer-pool [^Path root-path ^BufferAllocator allocator^ObjectStore object-store]
  (util/mkdirs root-path)
  (->MemoryMappedBufferPool root-path allocator object-store (ConcurrentHashMap.)))
