(ns xtdb.azure.object-store
  (:require [clojure.tools.logging :as log]
            [xtdb.object-store :as os]
            [xtdb.file-list :as file-list]
            [xtdb.util :as util])
  (:import [com.azure.core.util BinaryData]
           [com.azure.storage.blob BlobContainerClient]
           [com.azure.storage.blob.specialized BlockBlobClient]
           [com.azure.storage.blob.models BlobRange BlobStorageException DownloadRetryOptions]
           [java.io ByteArrayOutputStream Closeable]
           [java.lang AutoCloseable]
           [java.nio ByteBuffer]
           [java.nio.file Path]
           [java.util NavigableSet ArrayList List Base64 Base64$Encoder]
           [java.util.concurrent CompletableFuture]
           [java.util.function Supplier]
           [reactor.core Exceptions Exceptions$ReactiveException]
           xtdb.api.storage.ObjectStore
           [xtdb.multipart SupportsMultipart IMultipartUpload]))

(defn- get-blob [^BlobContainerClient blob-container-client blob-name]
  (try
    (-> (.getBlobClient blob-container-client blob-name)
        (.downloadContent)
        (.toByteBuffer))
    (catch BlobStorageException e
      (if (= 404 (.getStatusCode e))
        (throw (os/obj-missing-exception blob-name))
        (throw e)))))

(defn- get-blob-range [^BlobContainerClient blob-container-client blob-name start len]
  (os/ensure-shared-range-oob-behaviour start len)
  (try
    ;; todo use async-client, azure sdk then defines exception behaviour, no need to block before fut
    (let [cl (.getBlobClient blob-container-client blob-name)
          out (ByteArrayOutputStream.)
          res (.downloadStreamWithResponse cl out (BlobRange. start len) (DownloadRetryOptions.)  nil false nil nil)
          status-code (.getStatusCode res)]
      (cond
        (<= 200 status-code 299) nil
        (= 404 status-code) (throw (os/obj-missing-exception blob-name))
        :else (throw (ex-info "Blob range request failure" {:status status-code})))
      (ByteBuffer/wrap (.toByteArray out)))
    (catch BlobStorageException e
      (if (= 404 (.getStatusCode e))
        (throw (os/obj-missing-exception blob-name))
        (throw e)))))

(defn- put-blob [^BlobContainerClient blob-container-client blob-name blob-buffer]
  (try
    (-> (.getBlobClient blob-container-client blob-name)
        (.upload (BinaryData/fromByteBuffer blob-buffer)))
    (catch BlobStorageException e
      (if (= 409 (.getStatusCode e))
        (log/infof "Blob already exists for %s - terminating put operation" blob-name)
        (throw e)))
    (catch Throwable e
      (let [unwrapped-e (Exceptions/unwrap e)]
        (when (instance? InterruptedException unwrapped-e)
          (log/infof "Blob upload interrupted for %s - terminating put operation" blob-name))
        (throw unwrapped-e)))))

(defn- delete-blob [^BlobContainerClient blob-container-client blob-name]
  (-> (.getBlobClient blob-container-client blob-name)
      (.deleteIfExists)))

(def ^Base64$Encoder base-64-encoder (Base64/getEncoder))

(defn random-block-id []
  (.encodeToString base-64-encoder (.getBytes (str (random-uuid)))))

(defrecord MultipartUpload [^BlockBlobClient block-blob-client on-complete ^List !staged-block-ids]
  IMultipartUpload
  (uploadPart [_  buf]
    (CompletableFuture/completedFuture
     (try
       (let [block-id (random-block-id)
             binary-data (BinaryData/fromByteBuffer buf)]
         (.stageBlock block-blob-client block-id binary-data)
         (.add !staged-block-ids block-id))
       (catch Throwable e
         (let [unwrapped-e (Exceptions/unwrap e)]
           (when (instance? InterruptedException unwrapped-e)
             (log/infof "Multipart upload interrupted for %s - aborting multipart operation" (.getBlobUrl block-blob-client)))
           (throw unwrapped-e))))))

  (complete [_]
    (CompletableFuture/completedFuture
     (do
       ;; Commit the staged blocks - if already exists, abort the upload and complete
       (try
         (.commitBlockList block-blob-client !staged-block-ids)
         (catch BlobStorageException e
           (if (= 409 (.getStatusCode e))
             (log/infof "Blob already exists for %s - aborting multipart upload" (.getBlobUrl block-blob-client))
             (throw e)))
         (catch Throwable e
           (let [unwrapped-e (Exceptions/unwrap e)]
             (when (instance? InterruptedException unwrapped-e)
               (log/infof "Multipart upload completion interrupted for %s - aborting multipart operation" (.getBlobUrl block-blob-client)))
             (throw unwrapped-e))))
       ;; Run passed in on-complete function (adds the key to the filename cache)
       (on-complete))))

  (abort [_]
    (CompletableFuture/completedFuture
     (try
       ;; Commit an empty blocklist removes all staged & uncomitted files
       (.commitBlockList block-blob-client [])
       ;; Delete the empty blob
       (.deleteIfExists block-blob-client)
       (catch BlobStorageException e
         (log/infof "Blob already exists for %s - nothing further to abort/commit" (.getBlobUrl block-blob-client))
         (if (= 409 (.getStatusCode e)) nil (throw e)))))))

(defn- start-multipart [^BlobContainerClient blob-container-client blob-name on-complete-fn]
  (let [block-blob-client (-> blob-container-client 
                              (.getBlobClient blob-name) 
                              (.getBlockBlobClient))]
    (->MultipartUpload block-blob-client on-complete-fn (ArrayList.))))

(defrecord AzureBlobObjectStore [^BlobContainerClient blob-container-client ^Path prefix multipart-minimum-part-size ^NavigableSet file-name-cache ^AutoCloseable file-list-watcher]
  ObjectStore
  (getObject [_ k]
    (let [prefixed-key (util/prefix-key prefix k)]
      (CompletableFuture/completedFuture
       (get-blob blob-container-client (str prefixed-key)))))

  (getObject [_ k out-path]
    (let [prefixed-key (util/prefix-key prefix k)]
      (CompletableFuture/supplyAsync
       (reify Supplier
         (get [_]
           (let [blob-buffer (get-blob blob-container-client (str prefixed-key))]
             (util/write-buffer-to-path blob-buffer out-path)

             out-path))))))

  (getObjectRange [_ k start len]
    (let [prefixed-key (util/prefix-key prefix k)]
      (CompletableFuture/completedFuture
       (get-blob-range blob-container-client (str prefixed-key) start len))))

  (putObject [_ k buf]
    (let [prefixed-key (util/prefix-key prefix k)]
      (CompletableFuture/completedFuture 
       (do
         (put-blob blob-container-client (str prefixed-key) buf)
         (.add file-name-cache k)))))

  (listAllObjects [_this]
    (into [] file-name-cache))

  (listObjects [_this dir]
    (file-list/list-files-under-prefix file-name-cache dir))

  (deleteObject [_ k]
    (let [prefixed-key (util/prefix-key prefix k)]
      (CompletableFuture/completedFuture
       (do 
         (delete-blob blob-container-client (str prefixed-key))
         (.remove file-name-cache k)))))
  
  SupportsMultipart
  (startMultipart [_ k]
    (let [prefixed-key (util/prefix-key prefix k)]
      (CompletableFuture/completedFuture
       (start-multipart blob-container-client
                        (str prefixed-key)
                        (fn [] (.add file-name-cache k))))))

  Closeable
  (close [_]
    (.close file-list-watcher)
    (.clear file-name-cache)))
