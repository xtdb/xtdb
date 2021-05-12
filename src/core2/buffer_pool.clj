(ns core2.buffer-pool
  (:require core2.object-store
            [core2.system :as sys]
            [core2.util :as util])
  (:import core2.object_store.ObjectStore
           clojure.lang.MapEntry
           java.io.Closeable
           [java.nio.file Files Path]
           java.util.concurrent.CompletableFuture
           java.util.concurrent.locks.StampedLock
           [java.util Map Map$Entry LinkedHashMap UUID]
           [org.apache.arrow.memory ArrowBuf BufferAllocator]))

(set! *unchecked-math* :warn-on-boxed)

(definterface IBufferPool
  (^java.util.concurrent.CompletableFuture getBuffer [^String k])
  (^boolean evictBuffer [^String k]))

(defn- evict-internal [^ArrowBuf buffer ^Path buffer-path]
  (util/try-close buffer)
  (when buffer-path
    (util/delete-file buffer-path)))

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
          (CompletableFuture/completedFuture (doto ^ArrowBuf (first v)
                                               (.retain)))
          (-> (.getObject object-store k)
              (util/then-apply (fn [nio-buffer]
                                 (if cache-path
                                   (let [buffer-path (.resolve cache-path (str (UUID/randomUUID)))]
                                     (util/write-buffer-to-path nio-buffer buffer-path)
                                     (MapEntry/create (util/->mmap-path buffer-path) buffer-path))
                                   (MapEntry/create nio-buffer nil))))
              (util/then-apply
                (fn [[nio-buffer path]]
                  (let [stamp (.writeLock buffers-lock)]
                    (try
                      (let [[^ArrowBuf buf stored-path] (.computeIfAbsent buffers k (util/->jfn (fn [_]
                                                                                                  (MapEntry/create
                                                                                                   (util/->arrow-buf-view allocator nio-buffer) path))))]
                        (when-not (= path stored-path)
                          (util/delete-file path))
                        (doto buf
                          (.retain)))
                      (finally
                        (.unlock buffers-lock stamp)))))))))))

  (evictBuffer [_ k]
    (if-let [[buffer path] (let [stamp (.writeLock buffers-lock)]
                             (try
                               (.remove buffers k)
                               (finally
                                 (.unlock buffers-lock stamp))))]
      (do (evict-internal buffer path)
          true)
      false))

  Closeable
  (close [_]
    (let [stamp (.writeLock buffers-lock)]
      (try
        (let [i (.iterator (.values buffers))]
          (while (.hasNext i)
            (let [[buffer path] (.next i)]
              (evict-internal buffer path))
            (.remove i)))
        (finally
          (.unlock buffers-lock stamp))))))

(def default-buffer-cache-entries-size 1024)
(def default-buffer-cache-bytes-size (* 512 1024 1024))

(defn- buffer-cache-bytes-size ^long [^Map buffers]
  (long (reduce + (for [[^ArrowBuf buffer] (vals buffers)]
                    (.capacity buffer)))) )

(defn- ->buffer-cache [^long cache-entries-size ^long cache-bytes-size ^Path cache-path]
  (proxy [LinkedHashMap] [16 0.75 true]
    (removeEldestEntry [entry]
      (let [entries-size (.size ^Map this)]
        (if (or (> entries-size cache-entries-size)
                (and (> entries-size 1) (> (buffer-cache-bytes-size this) cache-bytes-size)))
          (let [[k [buffer path]] entry]
            (evict-internal buffer path)
            true)
          false)))))

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
