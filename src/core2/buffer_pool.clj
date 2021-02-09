(ns core2.buffer-pool
  (:require [core2.object-store :as os]
            [core2.util :as util])
  (:import [core2.object_store ObjectStore]
           [java.io Closeable]
           [java.nio.file Files Path]
           [java.util Map]
           [java.util.concurrent CompletableFuture ConcurrentHashMap]))

(definterface BufferPool
  (^java.util.concurrent.CompletableFuture getBuffer [^String k])
  (^boolean evictBuffer [^String k]))

(deftype MemoryMappedBufferPool [^Path root-path ^Map buffers ^ObjectStore object-store]
  BufferPool
  (getBuffer [_ k]
    (let [v (.getOrDefault buffers k ::not-found)]
      (if-not (= ::not-found v)
        (CompletableFuture/completedFuture v)
        (util/then-apply
          (.getObject object-store k (.resolve root-path k))
          (fn [^Path path]
            (.computeIfAbsent buffers k (util/->jfn (fn [_]
                                                      (util/->mmap-path path)))))))))

  (evictBuffer [_ k]
    (when (.remove buffers k)
      (Files/deleteIfExists (.resolve root-path k))))

  Closeable
  (close [_]
    (.clear buffers)))

(defn ->memory-mapped-buffer-pool [^Path root-path ^ObjectStore object-store]
  (util/mkdirs root-path)
  (->MemoryMappedBufferPool root-path (ConcurrentHashMap.) object-store))
