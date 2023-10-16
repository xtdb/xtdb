(ns xtdb.object-store
  (:require [clojure.spec.alpha :as s]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.util :as util])
  (:import java.io.Closeable
           java.nio.ByteBuffer
           (java.nio.channels FileChannel$MapMode)
           [java.nio.file FileSystems FileVisitOption Files LinkOption Path]
           [java.util.concurrent CompletableFuture ExecutorService Executors]
           java.util.function.Supplier))

(set! *unchecked-math* :warn-on-boxed)

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface IMultipartUpload
  (^java.util.concurrent.CompletableFuture #_<Void> uploadPart [^java.nio.ByteBuffer buf] 
   "Asynchonously uploads a part to the multipart request - adds to the internal completed parts list")
  (^java.util.concurrent.CompletableFuture #_<Void> complete []
   "Asynchonously completes the multipart-request")
  (^java.util.concurrent.CompletableFuture #_<?> abort []
   "Asynchonously cancels the multipart-request - useful/necessary to cleanup any parts of the multipart upload on an error"))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface ObjectStore
  (^java.util.concurrent.CompletableFuture #_<ByteBuffer> getObject [^String k]
                                                                    "Asynchonously returns the given object in a ByteBuffer
    If the object doesn't exist, the CF completes with an IllegalStateException.")

  (^java.util.concurrent.CompletableFuture #_<ByteBuffer> getObjectRange
   [^String k ^long start ^long len]
   "Asynchonously returns the given len bytes starting from start (inclusive) of the object in a ByteBuffer
    If the object doesn't exist, the CF completes with an IllegalStateException.

    Out of bounds `start` cause the returned future to complete with an Exception, the type of which is implementation dependent.
    If you supply a len that exceeds the number of bytes remaining (from start) then you will receive less bytes than len.

    Exceptions are thrown immediately if the start is negative, or the len is zero or below. This is
    to ensure consistent boundary behaviour between different object store implementations. You should check for these conditions and deal with them
    before calling getObjectRange.

    Behaviour for a start position at or exceeding the byte length of the object is undefined. You may or may not receive an exception.")

  (^java.util.concurrent.CompletableFuture #_<Path> getObject [^String k, ^java.nio.file.Path out-path]
                                                              "Asynchronously writes the object to the given path.
    If the object doesn't exist, the CF completes with an IllegalStateException.")

  (^java.util.concurrent.CompletableFuture #_<?> putObject [^String k, ^java.nio.ByteBuffer buf])
  (^java.util.concurrent.CompletableFuture #_<IMultipartUpload> startMultipart [^String k])
  (^java.lang.Iterable #_<String> listObjects [])
  (^java.lang.Iterable #_<String> listObjects [^String dir])
  (^java.util.concurrent.CompletableFuture #_<?> deleteObject [^String k]))

(defn ensure-shared-range-oob-behaviour [^long i ^long len]
  (when (< i 0)
    (throw (IndexOutOfBoundsException. "Negative range indexes are not permitted")))
  (when (< len 1)
    (throw (IllegalArgumentException. "Negative or zero range requests are not permitted"))))

(defn obj-missing-exception [k]
  (IllegalStateException. (format "Object '%s' doesn't exist." k)))

(deftype FileSystemObjectStore [^Path root-path, ^ExecutorService pool]
  ObjectStore
  (getObject [_this k]
    (CompletableFuture/completedFuture
     (let [from-path (.resolve root-path k)]
       (when-not (util/path-exists from-path)
         (throw (obj-missing-exception k)))

       (util/->mmap-path from-path))))

  (getObjectRange [_this k start len]
    (ensure-shared-range-oob-behaviour start len)
    (CompletableFuture/completedFuture
      (let [from-path (.resolve root-path k)]
        (when-not (util/path-exists from-path)
          (throw (obj-missing-exception k)))

        (with-open [in (util/->file-channel from-path #{:read})]
          (.map in FileChannel$MapMode/READ_ONLY start (max 1 (min (- (.size in) start) len)))))))

  (getObject [_this k out-path]
    (CompletableFuture/supplyAsync
     (reify Supplier
       (get [_]
         (let [from-path (.resolve root-path k)]
           (when-not (util/path-exists from-path)
             (throw (obj-missing-exception k)))

           (when-not (util/path-exists out-path)
             (util/copy-file-atomically root-path from-path out-path))
           out-path)))))

  (putObject [_this k buf]
    (let [buf (.duplicate buf)]
      (util/completable-future pool
        (let [to-path (.resolve root-path k)]
          (util/mkdirs (.getParent to-path))
          (if (identical? (FileSystems/getDefault) (.getFileSystem to-path))
            (if (util/path-exists to-path)
              to-path
              (util/write-buffer-to-path-atomically buf root-path to-path))

            (util/write-buffer-to-path buf to-path))))))

  (listObjects [_this]
    (with-open [dir-stream (Files/walk root-path (make-array FileVisitOption 0))]
      (vec (sort (for [^Path path (iterator-seq (.iterator dir-stream))
                       :when (Files/isRegularFile path (make-array LinkOption 0))]
                   (str (.relativize root-path path)))))))

  (listObjects [_this dir]
    (let [dir (.resolve root-path dir)]
      (when (Files/exists dir (make-array LinkOption 0))
        (with-open [dir-stream (Files/newDirectoryStream dir)]
          (vec (sort (for [^Path path dir-stream]
                       (str (.relativize root-path path)))))))))

  (deleteObject [_this k]
    (util/completable-future pool
      (util/delete-file (.resolve root-path k))))

  Closeable
  (close [_this]
    (util/shutdown-pool pool)))

(derive ::file-system-object-store :xtdb/object-store)

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

(comment

  (def fos
    (->> (ig/prep-key ::file-system-object-store {:root-path "tmp/fos"})
         (ig/init-key ::file-system-object-store)))

  (.close fos)

  @(.putObject fos "foo.txt" (ByteBuffer/wrap (.getBytes "hello, world!")))

  (let [buf @(.getObject fos "foo.txt")
        arr (byte-array (.remaining buf))]
    (.get buf arr)
    (String. arr))

  (let [buf @(.getObjectRange fos "foo.txt" 2 5)
        arr (byte-array (.remaining buf))]
    (.get buf arr)
    (String. arr))

  )
