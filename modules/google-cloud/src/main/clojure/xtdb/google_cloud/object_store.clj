(ns xtdb.google-cloud.object-store
  (:require [xtdb.file-list :as file-list]
            [xtdb.object-store :as os]
            [xtdb.util :as util])
  (:import [com.google.cloud.storage BlobId BlobInfo Storage Blob$BlobSourceOption
            Storage$BlobSourceOption Storage$BlobWriteOption StorageException]
           [java.io Closeable]
           [java.lang AutoCloseable]
           [java.nio ByteBuffer]
           [java.nio.file Path]
           [java.util NavigableSet]
           [java.util.concurrent CompletableFuture]
           [java.util.function Supplier]
           [xtdb.object_store ObjectStore]))

(def blob-source-opts (into-array Blob$BlobSourceOption []))

(defn get-blob [{:keys [^Storage storage-service bucket-name ^Path prefix]} ^Path blob-name]
  (let [prefixed-key (util/prefix-key prefix blob-name)
        blob-id (BlobId/of bucket-name (str prefixed-key))
        blob-byte-buffer (some-> (.get storage-service blob-id)
                                 (.getContent blob-source-opts)
                                 (ByteBuffer/wrap))]
    (if blob-byte-buffer
      blob-byte-buffer
      (throw (os/obj-missing-exception blob-name)))))

(def read-options (into-array Storage$BlobSourceOption []))

(defn- get-blob-range [{:keys [^Storage storage-service bucket-name ^Path prefix]} ^Path blob-name start len]
  (os/ensure-shared-range-oob-behaviour start len)
  (let [prefixed-key (util/prefix-key prefix blob-name)
        blob-id (BlobId/of bucket-name (str prefixed-key))
        blob (.get storage-service blob-id)
        out (ByteBuffer/allocate len)]
    (if blob
      ;; Read contents of reader into the allocated ByteBuffer
      (with-open [reader (.reader blob blob-source-opts)]
        (.seek reader start)
        ;; limit on google cloud readchannel starts from the beginning, so we need to add the start to the
        ;; specified length for desired behaviour - see:
        ;; https://cloud.google.com/java/docs/reference/google-cloud-core/latest/com.google.cloud.ReadChannel
        (.limit reader (+ start len))
        (.read reader out)
        (.flip out))
      (throw (os/obj-missing-exception blob-name)))))

;; Want to only put blob if a version doesn't already exist
(def write-options (into-array Storage$BlobWriteOption [(Storage$BlobWriteOption/doesNotExist)]))

(defn put-blob
  ([{:keys [^Storage storage-service bucket-name ^Path prefix]} ^Path blob-name byte-buf]
   (let [prefixed-key (util/prefix-key prefix blob-name)
         blob-id (BlobId/of bucket-name (str prefixed-key))
         blob-info (.build (BlobInfo/newBuilder blob-id))]

     (try
       (with-open [writer (.writer storage-service blob-info write-options)]
         (.write writer byte-buf))
       (catch StorageException e
         (when-not (= 412 (.getCode e))
           (throw e)))))))

(defn delete-blob [{:keys [^Storage storage-service bucket-name ^Path prefix]} ^Path blob-name]
  (let [prefixed-key (util/prefix-key prefix blob-name)
        blob-id (BlobId/of bucket-name (str prefixed-key))]
    (.delete storage-service blob-id)))

(defrecord GoogleCloudStorageObjectStore [^Storage storage-service bucket-name prefix ^NavigableSet file-name-cache ^AutoCloseable file-list-watcher]
  ObjectStore
  (getObject [this k]
    (CompletableFuture/completedFuture
     (get-blob this k)))

  (getObject [this k out-path]
    (CompletableFuture/supplyAsync
     (reify Supplier
       (get [_]
         (let [blob-buffer (get-blob this k)]
           (util/write-buffer-to-path blob-buffer out-path)

           out-path)))))

  (getObjectRange [this k start len]
    (CompletableFuture/completedFuture
     (get-blob-range this k start len)))

  (putObject [this k buf]
    (CompletableFuture/completedFuture
     (do
       (put-blob this k buf)
       (.add file-name-cache k))))

  (listObjects [_this]
    (into [] file-name-cache))

  (listObjects [_this dir]
    (file-list/list-files-under-prefix file-name-cache dir))

  (deleteObject [this k]
    (CompletableFuture/completedFuture 
     (do
       (delete-blob this k)
       (.remove file-name-cache k))))

  Closeable
  (close [_]
    (.close file-list-watcher)
    (.clear file-name-cache)))
