(ns core2.object-store
  (:require [clojure.java.io :as io]
            [core2.util :as util])
  (:import java.io.Closeable
           [java.nio.file CopyOption StandardCopyOption Files LinkOption Path]
           java.nio.file.attribute.FileAttribute
           [java.util.concurrent CompletableFuture Executors ExecutorService TimeUnit]
           java.util.function.Supplier))

(set! *unchecked-math* :warn-on-boxed)

(definterface ObjectStore
  (^java.util.concurrent.CompletableFuture getObject [^String k, ^java.nio.file.Path to-file])
  (^java.util.concurrent.CompletableFuture putObject [^String k, ^java.nio.file.Path from-path])
  (^java.util.concurrent.CompletableFuture listObjects [])
  (^java.util.concurrent.CompletableFuture listObjects [^String glob]
   "glob as defined by https://docs.oracle.com/javase/8/docs/api/java/nio/file/FileSystem.html#getPathMatcher-java.lang.String-")
  (^java.util.concurrent.CompletableFuture deleteObject [^String k]))

(deftype FileSystemObjectStore [^Path root-path, ^ExecutorService pool]
  ObjectStore
  (getObject [_this k to-path]
    (util/completable-future pool
      (let [from-path (.resolve root-path k)]
        (when (Files/exists from-path (make-array LinkOption 0))
          (Files/copy from-path to-path ^"[Ljava.nio.file.CopyOption;" (into-array CopyOption #{StandardCopyOption/REPLACE_EXISTING}))
          to-path))))

  (putObject [_this k from-path]
    (util/completable-future pool
      (let [to-path (.resolve root-path k)]
        (util/mkdirs (.getParent to-path))
        (Files/copy from-path to-path ^"[Ljava.nio.file.CopyOption;" (make-array CopyOption 0)))))

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
    (doto pool
      (.shutdownNow)
      (.awaitTermination 5 TimeUnit/SECONDS))))

(defn ->file-system-object-store
  (^core2.object_store.FileSystemObjectStore
   [^Path root-path]
   (->file-system-object-store root-path {}))
  (^core2.object_store.FileSystemObjectStore
   [^Path root-path {:keys [pool-size], :or {pool-size 4}}]
   (util/mkdirs root-path)
   (->FileSystemObjectStore root-path (Executors/newFixedThreadPool pool-size))))

;; ok, so where we at?

;;; storing everything at block level
;; need to consider dictionaries
;; if we want everything in the object store as Arrow IPC files, need to faff about with layout ourselves

;;; dictionaries
;; looks like the Java version only writes dictionaries at the start, otherwise you're on your own
