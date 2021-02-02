(ns core2.object-store
  (:require [clojure.java.io :as io])
  (:import java.io.Closeable
           [java.nio.file CopyOption Files LinkOption Path]
           java.nio.file.attribute.FileAttribute
           [java.util.concurrent CompletableFuture Executors ExecutorService TimeUnit]
           java.util.function.Supplier))

(set! *unchecked-math* :warn-on-boxed)

(definterface ObjectStore
  (^java.util.concurrent.CompletableFuture getObject [^String k, ^java.nio.file.Path to-file])
  (^java.util.concurrent.CompletableFuture putObject [^String k, ^java.nio.file.Path from-path])
  (^java.util.concurrent.CompletableFuture listObjects [])
  (^java.util.concurrent.CompletableFuture deleteObject [^String k]))

(defn- ->supplier ^java.util.function.Supplier [f]
  (reify Supplier
    (get [_]
      (f))))

(defmacro completable-future {:style/indent 1} [pool & body]
  `(CompletableFuture/supplyAsync
    (->supplier (fn [] ~@body))
    ~pool))

(defn- mkdirs [^Path path]
  (Files/createDirectories path (make-array FileAttribute 0)))

(deftype FileSystemObjectStore [^Path root-path, ^ExecutorService pool]
  ObjectStore
  (getObject [_this k to-path]
    (completable-future pool
      (let [from-path (.resolve root-path k)]
        (when (Files/exists from-path (make-array LinkOption 0))
          (Files/copy from-path to-path ^"[Ljava.nio.file.CopyOption;" (make-array CopyOption 0))
          to-path))))

  (putObject [_this k from-path]
    (completable-future pool
      (let [to-path (.resolve root-path k)]
        (mkdirs (.getParent to-path))
        (Files/copy from-path to-path ^"[Ljava.nio.file.CopyOption;" (make-array CopyOption 0)))))

  (listObjects [_this]
    (completable-future pool
      (vec (sort (for [^Path path (iterator-seq (.iterator (Files/list root-path)))]
                   (str (.relativize root-path path)))))))

  (deleteObject [_this k]
    (completable-future pool
      (Files/deleteIfExists (.resolve root-path k))))

  Closeable
  (close [_this]
    (doto pool
      (.shutdownNow)
      (.awaitTermination 5 TimeUnit/SECONDS))))

(defn ->local-directory-object-store
  (^core2.object_store.FileSystemObjectStore
   [^Path root-path]
   (->local-directory-object-store root-path {}))
  (^core2.object_store.FileSystemObjectStore
   [^Path root-path {:keys [pool-size], :or {pool-size 4}}]
   (mkdirs root-path)
   (->FileSystemObjectStore root-path (Executors/newFixedThreadPool pool-size))))

;; ok, so where we at?

;;; storing everything at block level
;; need to consider dictionaries
;; if we want everything in the object store as Arrow IPC files, need to faff about with layout ourselves

;;; dictionaries
;; looks like the Java version only writes dictionaries at the start, otherwise you're on your own
