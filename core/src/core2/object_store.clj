(ns core2.object-store
  (:require [clojure.string :as str]
            [core2.util :as util]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [clojure.spec.alpha :as s])
  (:import java.io.Closeable
           java.nio.ByteBuffer
           [java.nio.file CopyOption Files FileSystems OpenOption Path StandardOpenOption]
           [java.util.concurrent CompletableFuture ConcurrentSkipListMap Executors ExecutorService]
           java.util.function.Supplier
           java.util.NavigableMap))

(set! *unchecked-math* :warn-on-boxed)

#_{:clj-kondo/ignore [:unused-binding]}
(definterface ObjectStore
  (^java.util.concurrent.CompletableFuture #_<ByteBuffer> getObject [^String k]
   "Asynchonously returns the given object in a ByteBuffer
    If the object doesn't exist, the CF completes with an IllegalStateException.")

  (^java.util.concurrent.CompletableFuture #_<Path> getObject [^String k, ^java.nio.file.Path out-path]
   "Asynchronously writes the object to the given path.
    If the object doesn't exist, the CF completes with an IllegalStateException.")

  (^java.util.concurrent.CompletableFuture #_<?> putObject [^String k, ^java.nio.ByteBuffer buf])
  (^java.lang.Iterable #_<String> listObjects [])
  (^java.lang.Iterable #_<String> listObjects [^String prefix])
  (^java.util.concurrent.CompletableFuture #_<?> deleteObject [^String k]))

(defn obj-missing-exception [k]
  (IllegalStateException. (format "Object '%s' doesn't exist." k)))

(deftype InMemoryObjectStore [^NavigableMap os]
  ObjectStore
  (getObject [_this k]
    (CompletableFuture/completedFuture
     (let [^ByteBuffer buf (or (.get os k)
                               (throw (obj-missing-exception k)))]
       (.slice buf))))

  (getObject [_this k out-path]

    (CompletableFuture/supplyAsync
     (reify Supplier
       (get [_]
         (let [buf (or (.get os k)
                       (throw (obj-missing-exception k)))]
           (with-open [ch (Files/newByteChannel out-path (into-array OpenOption #{StandardOpenOption/WRITE
                                                                                  StandardOpenOption/CREATE
                                                                                  StandardOpenOption/TRUNCATE_EXISTING}))]
             (.write ch buf)
             out-path))))))

  (putObject [_this k buf]
    (.putIfAbsent os k (.slice buf))
    (CompletableFuture/completedFuture nil))

  (listObjects [_this]
    (vec (.keySet os)))

  (listObjects [_this prefix]
    (->> (.keySet (.tailMap os prefix))
         (into [] (take-while #(str/starts-with? % prefix)))))

  (deleteObject [_this k]
    (.remove os k)
    (CompletableFuture/completedFuture nil))

  Closeable
  (close [_]
    (.clear os)))

(defmethod ig/init-key ::memory-object-store [_ _]
  (->InMemoryObjectStore (ConcurrentSkipListMap.)))

(defmethod ig/halt-key! ::memory-object-store [_ ^InMemoryObjectStore os]
  (.close os))

(derive ::memory-object-store :core2/object-store)

(deftype FileSystemObjectStore [^Path root-path, ^ExecutorService pool]
  ObjectStore
  (getObject [_this k]
    (CompletableFuture/completedFuture
     (let [from-path (.resolve root-path k)]
       (when-not (util/path-exists from-path)
         (throw (obj-missing-exception k)))

       (util/->mmap-path from-path))))

  (getObject [_this k out-path]
    (CompletableFuture/supplyAsync
     (reify Supplier
       (get [_]
         (let [from-path (.resolve root-path k)]
           (when-not (util/path-exists from-path)
             (throw (obj-missing-exception k)))

           (Files/copy from-path out-path
                       ^"[Ljava.nio.file.CopyOption;" (make-array CopyOption 0))
           out-path)))))

  (putObject [_this k buf]
    (let [buf (.duplicate buf)]
      (util/completable-future pool
        (let [to-path (.resolve root-path k)]
          (util/mkdirs (.getParent to-path))
          (if (identical? (FileSystems/getDefault) (.getFileSystem to-path))
            (if (util/path-exists to-path)
              to-path
              (util/write-buffer-to-path-atomically buf to-path))

            (util/write-buffer-to-path buf to-path))))))

  (listObjects [_this]
    (vec (sort (for [^Path path (iterator-seq (.iterator (Files/list root-path)))]
                 (str (.relativize root-path path))))))

  (listObjects [_this prefix]
    (with-open [dir-stream (Files/newDirectoryStream root-path (str prefix "*"))]
      (vec (sort (for [^Path path dir-stream]
                   (str (.relativize root-path path)))))))

  (deleteObject [_this k]
    (util/completable-future pool
      (util/delete-file (.resolve root-path k))))

  Closeable
  (close [_this]
    (util/shutdown-pool pool)))

(derive ::file-system-object-store :core2/object-store)

(s/def ::root-path ::util/path)
(s/def ::pool-size pos-int?)

(defmethod ig/prep-key ::file-system-object-store [_ opts]
  (-> (merge {:pool-size 4} opts)
      (util/maybe-update :root-path util/->path)))

(defmethod ig/pre-init-spec ::file-system-object-store [_]
  (s/keys :req-un [::root-path ::pool-size]))

(defmethod ig/init-key ::file-system-object-store [_ {:keys [root-path pool-size]}]
  (util/mkdirs root-path)
  (let [pool (Executors/newFixedThreadPool pool-size (util/->prefix-thread-factory "file-system-object-store-"))]
    (->FileSystemObjectStore root-path pool)))

(defmethod ig/halt-key! ::file-system-object-store [_ ^FileSystemObjectStore os]
  (.close os))
