(ns core2.object-store
  (:require [core2.util :as util]
            [core2.system :as sys])
  (:import java.io.Closeable
           [java.nio.file CopyOption Files FileSystems Path StandardCopyOption]
           [java.util.concurrent Executors ExecutorService]
           java.util.UUID))

(set! *unchecked-math* :warn-on-boxed)

(definterface ObjectStore
  (^java.util.concurrent.CompletableFuture getObject [^String k, ^java.nio.file.Path to-file])
  (^java.util.concurrent.CompletableFuture putObject [^String k, ^java.nio.ByteBuffer buf])
  (^java.lang.Iterable listObjects [])
  (^java.lang.Iterable listObjects [^String prefix])
  (^java.util.concurrent.CompletableFuture deleteObject [^String k]))

(deftype FileSystemObjectStore [^Path root-path, ^ExecutorService pool]
  ObjectStore
  (getObject [_this k to-path]
    (util/completable-future pool
      (let [from-path (.resolve root-path k)]
        (when (util/path-exists from-path)
          (let [to-path-temp (.resolveSibling to-path (str "." (UUID/randomUUID)))]
            (try
              (Files/copy from-path to-path-temp ^"[Ljava.nio.file.CopyOption;" (into-array CopyOption #{StandardCopyOption/REPLACE_EXISTING}))
              (Files/move to-path-temp to-path (into-array CopyOption [StandardCopyOption/ATOMIC_MOVE StandardCopyOption/REPLACE_EXISTING]))
              to-path
              (finally
                (Files/deleteIfExists to-path-temp))))
          to-path))))

  (putObject [_this k buf]
    (util/completable-future pool
      (let [to-path (.resolve root-path k)]
        (util/mkdirs (.getParent to-path))
        (if (identical? (FileSystems/getDefault) (.getFileSystem to-path))
          (if (util/path-exists to-path)
            to-path

            (let [to-path-temp (.resolveSibling to-path (str "." (UUID/randomUUID)))]
              (try
                (util/write-buffer-to-path buf to-path-temp)
                (Files/move to-path-temp to-path (into-array CopyOption [StandardCopyOption/ATOMIC_MOVE]))
                (finally
                  (Files/deleteIfExists to-path-temp)))))

          (util/write-buffer-to-path buf to-path)))))

  (listObjects [_this]
    (vec (sort (for [^Path path (iterator-seq (.iterator (Files/list root-path)))]
                 (str (.relativize root-path path))))))

  (listObjects [_this prefix]
    (with-open [dir-stream (Files/newDirectoryStream root-path (str prefix "*"))]
      (vec (sort (for [^Path path dir-stream]
                   (str (.relativize root-path path)))))))

  (deleteObject [_this k]
    (util/completable-future pool
      (Files/deleteIfExists (.resolve root-path k))))

  Closeable
  (close [_this]
    (util/shutdown-pool pool)))

(defn ->file-system-object-store {::sys/args {:root-path {:spec ::sys/path, :required? true}
                                              :pool-size {:spec ::sys/pos-int, :default 4}}}
  [{:keys [root-path pool-size]}]
  (util/mkdirs root-path)
  (let [pool (Executors/newFixedThreadPool pool-size (util/->prefix-thread-factory "file-system-object-store-"))]
    (->FileSystemObjectStore root-path pool)))
