(ns xtdb.google-cloud.object-store
  (:require [xtdb.object-store :as os]
            [xtdb.util :as util])
  (:import [com.google.cloud.storage Blob BlobId BlobInfo Storage Blob$BlobSourceOption Storage$BlobListOption
            Storage$BlobSourceOption Storage$BlobWriteOption StorageException]
           [com.google.common.io ByteStreams]
           [java.nio ByteBuffer]
           [java.util.concurrent CompletableFuture]
           [java.util.function Supplier]
           [xtdb.object_store ObjectStore]))

(def blob-source-opts (into-array Blob$BlobSourceOption []))

(defn get-blob [{:keys [^Storage storage-service bucket-name prefix]} blob-name]
  (let [blob-id (BlobId/of bucket-name (str prefix blob-name))
        blob-byte-buffer (some-> (.get storage-service blob-id)
                                 (.getContent blob-source-opts)
                                 (ByteBuffer/wrap))]
    (if blob-byte-buffer
      blob-byte-buffer
      (throw (os/obj-missing-exception blob-name)))))

(def read-options (into-array Storage$BlobSourceOption []))

(defn- get-blob-range [{:keys [^Storage storage-service bucket-name prefix]} blob-name start len]
  (os/ensure-shared-range-oob-behaviour start len)
  (let [blob-id (BlobId/of bucket-name (str prefix blob-name))
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
  ([{:keys [^Storage storage-service bucket-name prefix]} blob-name byte-buf]
   (let [blob-id (BlobId/of bucket-name (str prefix blob-name))
         blob-info (.build (BlobInfo/newBuilder blob-id))]

     (try
       (with-open [writer (.writer storage-service blob-info write-options)]
         (.write writer byte-buf))
       (catch StorageException e
         (when-not (= 412 (.getCode e))
           (throw e)))))))

(defn list-blobs
  [{:keys [^Storage storage-service bucket-name prefix]} obj-prefix]
  (let [list-prefix (str prefix obj-prefix)
        list-blob-opts (into-array Storage$BlobListOption
                                   (if (not-empty list-prefix)
                                     [(Storage$BlobListOption/prefix list-prefix)]
                                     []))]
    (->> (.list storage-service bucket-name list-blob-opts)
         (.iterateAll)
         (mapv (fn [^Blob blob]
                 (subs (.getName blob) (count prefix)))))))

(defn delete-blob [{:keys [^Storage storage-service bucket-name prefix]} blob-name]
  (let [blob-id (BlobId/of bucket-name (str prefix blob-name))]
    (.delete storage-service blob-id)))

(defrecord GoogleCloudStorageObjectStore [^Storage storage-service bucket-name prefix]
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
    (put-blob this k buf)
    (CompletableFuture/completedFuture nil))

  (listObjects [this]
    (.listObjects this nil))

  (listObjects [this obj-prefix]
    (list-blobs this obj-prefix))

  (deleteObject [this k]
    (delete-blob this k)
    (CompletableFuture/completedFuture nil)))
