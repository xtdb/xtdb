(ns core2.object-store
  (:require [clojure.java.io :as io])
  (:import [java.io Closeable File]
           [java.util.concurrent CompletableFuture Executors ExecutorService TimeUnit]
           java.util.function.Supplier))

(set! *unchecked-math* :warn-on-boxed)

(definterface ObjectStore
  (^java.util.concurrent.CompletableFuture getObject [^String k, ^java.io.File to-file])
  (^java.util.concurrent.CompletableFuture putObject [^String k, ^java.io.File from-file])
  (^java.util.concurrent.Future listObjects [])
  (^java.util.concurrent.Future deleteObject [^String k]))

(defn- ->supplier ^java.util.function.Supplier [f]
  (reify Supplier
    (get [_]
      (f))))

(defmacro completable-future {:style/indent 1} [pool & body]
  `(CompletableFuture/supplyAsync
    (->supplier (fn [] ~@body))
    ~pool))

(deftype LocalDirectoryObjectStore [^File dir, ^ExecutorService pool]
  ObjectStore
  (getObject [_this k to-file]
    (completable-future pool
      (let [from-file (io/file dir k)]
        (when (.isFile from-file)
          (io/copy from-file to-file)
          to-file))))

  (putObject [_this k from-file]
    (completable-future pool
      (doto (io/file dir k)
        (io/make-parents)
        (->> (io/copy from-file)))))

  (listObjects [_this]
    (completable-future pool
      (let [dir-path (.toPath dir)]
        (vec (for [^File f (file-seq dir)
                   :when (.isFile f)]
               (str (.relativize dir-path (.toPath f))))))))

  (deleteObject [_this k]
    (completable-future pool
      (let [f (io/file dir k)]
        (if (.isFile f)
          (.delete f)
          (or (let [parent (.getParentFile f)]
                (when (empty? (.listFiles parent))
                  (.delete parent)))
              false)))))

  Closeable
  (close [_this]
    (doto pool
      (.shutdownNow)
      (.awaitTermination 5 TimeUnit/SECONDS))))

(defn ->local-directory-object-store
  (^core2.object_store.LocalDirectoryObjectStore
   [dir]
   (->local-directory-object-store dir {}))
  (^core2.object_store.LocalDirectoryObjectStore
   [dir {:keys [pool-size], :or {pool-size 4}}]
   (->LocalDirectoryObjectStore (io/file dir) (Executors/newFixedThreadPool pool-size))))

;; ok, so where we at?

;;; storing everything at block level
;; need to consider dictionaries
;; if we want everything in the object store as Arrow IPC files, need to faff about with layout ourselves

;;; dictionaries
;; looks like the Java version only writes dictionaries at the start, otherwise you're on your own
