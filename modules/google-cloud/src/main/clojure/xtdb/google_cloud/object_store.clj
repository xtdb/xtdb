(ns xtdb.google-cloud.object-store
  (:require [clojure.string :as string]
            [clojure.tools.logging :as log]
            [xtdb.object-store :as os]
            [xtdb.util :as util])
  (:import (com.google.cloud.storage Blob Blob$BlobSourceOption BlobId BlobInfo Storage Storage$BlobListOption Storage$BlobSourceOption Storage$BlobWriteOption StorageException)
           (java.nio ByteBuffer)
           (java.nio.file Path)
           (java.util.concurrent CompletableFuture)
           (java.util.function Supplier)
           xtdb.api.storage.ObjectStore))
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

(defn pre-condition-error? [^StorageException e]
  (and
   (= (.getCode e) 412)
   (string/includes? (.getMessage (.getCause e)) "At least one of the pre-conditions you specified did not hold")))

(defn interrupted-exception? [^StorageException e] 
  (instance? InterruptedException (.getCause e)))

(defn put-blob
  ([{:keys [^Storage storage-service bucket-name ^Path prefix]} ^Path blob-name byte-buf]
   (let [prefixed-key (util/prefix-key prefix blob-name)
         blob-id (BlobId/of bucket-name (str prefixed-key))
         blob-info (.build (BlobInfo/newBuilder blob-id))] 
     (try
       (with-open [writer (.writer storage-service blob-info write-options)]
         (.write writer byte-buf))
       (catch StorageException e 
         (cond
           (pre-condition-error? e) (log/warnf "Object %s already exists in bucket %s" prefixed-key bucket-name)
           (interrupted-exception? e) (throw (.getCause e))
           :else (throw (ex-info
                         (format "Error when writing object %s to bucket %s" prefixed-key bucket-name)
                         {:bucket-name bucket-name
                          :blob-name blob-name
                          :blob-id blob-id
                          :blob-info blob-info}
                         e))))))))

(defn delete-blob [{:keys [^Storage storage-service bucket-name ^Path prefix]} ^Path blob-name]
  (let [prefixed-key (util/prefix-key prefix blob-name)
        blob-id (BlobId/of bucket-name (str prefixed-key))]
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
    (CompletableFuture/completedFuture (put-blob this k buf)))

  (listAllObjects [_this]
    (let [list-blob-opts (into-array Storage$BlobListOption
                                     (if prefix
                                       [(Storage$BlobListOption/prefix (str prefix))]
                                       []))]
      (->> (.list storage-service bucket-name list-blob-opts)
           (.iterateAll)
           (mapv (fn [^Blob blob]
                   (cond->> (util/->path (.getName blob))
                     prefix (.relativize prefix)))))))

  (deleteObject [this k]
    (CompletableFuture/completedFuture 
     (delete-blob this k))))
