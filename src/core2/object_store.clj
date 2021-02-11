(ns core2.object-store
  (:require [core2.util :as util])
  (:import java.io.Closeable
           [java.nio.file CopyOption Files FileSystems Path StandardCopyOption]
           [java.util.concurrent CompletableFuture Executors ExecutorService]
           java.util.UUID))

(set! *unchecked-math* :warn-on-boxed)

(definterface ObjectStore
  (^java.util.concurrent.CompletableFuture getObject [^String k, ^java.nio.file.Path to-file])
  (^java.util.concurrent.CompletableFuture putObject [^String k, ^java.nio.ByteBuffer buf])
  (^java.util.concurrent.CompletableFuture listObjects [])
  (^java.util.concurrent.CompletableFuture listObjects [^String glob]
   "glob as defined by https://docs.oracle.com/javase/8/docs/api/java/nio/file/FileSystem.html#getPathMatcher-java.lang.String-")
  (^java.util.concurrent.CompletableFuture deleteObject [^String k]))

(deftype FileSystemObjectStore [^Path root-path, ^ExecutorService pool]
  ObjectStore
  (getObject [_this k to-path]
    (util/completable-future pool
      (let [from-path (.resolve root-path k)]
        (when (util/path-exists from-path)
          (Files/copy from-path to-path ^"[Ljava.nio.file.CopyOption;" (into-array CopyOption #{StandardCopyOption/REPLACE_EXISTING}))
          to-path))))

  (putObject [_this k buf]
    (letfn [(write-buf [^Path path]
              (with-open [file-ch (util/->file-channel path util/write-new-file-opts)]
                (.write file-ch buf)))]
      (util/completable-future pool
        (let [to-path (.resolve root-path k)]
          (util/mkdirs (.getParent to-path))
          (if (identical? (FileSystems/getDefault) (.getFileSystem to-path))
            (if (util/path-exists to-path)
              to-path

              (let [to-path-temp (.resolveSibling to-path (str "." (UUID/randomUUID)))]
                (try
                  (write-buf to-path-temp)
                  (Files/move to-path-temp to-path (into-array CopyOption [StandardCopyOption/ATOMIC_MOVE]))
                  (finally
                    (Files/deleteIfExists to-path-temp)))))

            (write-buf to-path))))))

  (listObjects [_this]
    (util/completable-future pool
      (vec (sort (for [^Path path (iterator-seq (.iterator (Files/list root-path)))]
                   (str (.relativize root-path path)))))))

  (listObjects [_this glob]
    (util/completable-future pool
      (with-open [dir-stream (Files/newDirectoryStream root-path glob)]
        (vec (sort (for [^Path path dir-stream]
                     (str (.relativize root-path path))))))))

  (deleteObject [_this k]
    (util/completable-future pool
      (Files/deleteIfExists (.resolve root-path k))))

  Closeable
  (close [_this]
    (util/shutdown-pool pool)))

(defn ->file-system-object-store
  (^core2.object_store.FileSystemObjectStore
   [^Path root-path]
   (->file-system-object-store root-path {}))
  (^core2.object_store.FileSystemObjectStore
   [^Path root-path {:keys [pool-size], :or {pool-size 4}}]
   (util/mkdirs root-path)
   (->FileSystemObjectStore root-path (Executors/newFixedThreadPool pool-size (util/->prefix-thread-factory "file-system-object-store-")))))

;; ok, so where we at?

;;; storing everything at block level
;; need to consider dictionaries
;; if we want everything in the object store as Arrow IPC files, need to faff about with layout ourselves

;;; dictionaries
;; looks like the Java version only writes dictionaries at the start, otherwise you're on your own
