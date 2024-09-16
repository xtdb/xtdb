(ns xtdb.azure.object-store
  (:require [clojure.tools.logging :as log]
            [xtdb.object-store :as os]
            [xtdb.util :as util])
  (:import [com.azure.core.util BinaryData]
           [com.azure.storage.blob BlobContainerClient]
           [com.azure.storage.blob.models BlobItem BlobRange BlobStorageException DownloadRetryOptions ListBlobsOptions]
           [com.azure.storage.blob.specialized BlockBlobClient]
           [java.io ByteArrayOutputStream Closeable UncheckedIOException]
           [java.nio ByteBuffer]
           [java.nio.file FileAlreadyExistsException Path]
           [java.util ArrayList Base64 Base64$Encoder List]
           [java.util.concurrent CompletableFuture]
           [reactor.core Exceptions]
           xtdb.api.storage.ObjectStore
           [xtdb.multipart IMultipartUpload SupportsMultipart]))

(defmacro rethrowing-reactor-cause [form]
  `(try
     ~form
     (catch Throwable e#
       (throw (Exceptions/unwrap e#)))))

(defn- get-blob 
  ([^BlobContainerClient blob-container-client blob-name]
   (try
     (rethrowing-reactor-cause
      (-> (.getBlobClient blob-container-client blob-name)
          (.downloadContent)
          (.toByteBuffer)))
     (catch BlobStorageException e
       (if (= 404 (.getStatusCode e))
         (throw (os/obj-missing-exception blob-name))
         (throw e)))
     (catch InterruptedException e
       (log/infof "Blob download to byte buffer interrupted for %s - terminating get operation" blob-name)
        ;; Reactor (used under the hood by the Azure SDK) leaves the thread interrupted - we need to clear this 
       (Thread/interrupted)
       (throw e))
     (catch Exception e
       (log/errorf "Exception thrown when downloading blob %s to byte buffer - %s" blob-name e)
       (throw e))))
  ([^BlobContainerClient blob-container-client blob-name ^Path out-file]
   (try
     (rethrowing-reactor-cause
      (-> (.getBlobClient blob-container-client blob-name)
          (.downloadToFile (str out-file) true)))
     (catch BlobStorageException e
       (if (= 404 (.getStatusCode e))
         (throw (os/obj-missing-exception blob-name))
         (throw e)))
     (catch UncheckedIOException e
       (let [cause (.getCause e)]
         (if (instance? FileAlreadyExistsException cause)
           (do (log/debugf "File at getBlob already exists at output path %s, returning file path" out-file)
               out-file)
           (throw e))))
     (catch InterruptedException e
       (log/infof "Blob download to file interrupted for %s - terminating get operation" blob-name)
        ;; Reactor (used under the hood by the Azure SDK) leaves the thread interrupted - we need to clear this 
       (Thread/interrupted)
       (throw e))
     (catch Exception e
       (log/errorf "Exception thrown when downloading blob %s to file - %s" blob-name e)
       (throw e)))))

(defn- get-blob-range [^BlobContainerClient blob-container-client blob-name start len]
  (os/ensure-shared-range-oob-behaviour start len)
  (try
    ;; todo use async-client, azure sdk then defines exception behaviour, no need to block before fut
    (rethrowing-reactor-cause
     (let [cl (.getBlobClient blob-container-client blob-name)
           out (ByteArrayOutputStream.)
           res (.downloadStreamWithResponse cl out (BlobRange. start len) (DownloadRetryOptions.)  nil false nil nil)
           status-code (.getStatusCode res)]
       (cond
         (<= 200 status-code 299) nil
         (= 404 status-code) (throw (os/obj-missing-exception blob-name))
         :else (throw (ex-info "Blob range request failure" {:status status-code})))
       (ByteBuffer/wrap (.toByteArray out))))
    (catch BlobStorageException e
      (if (= 404 (.getStatusCode e))
        (throw (os/obj-missing-exception blob-name))
        (throw e)))
    (catch InterruptedException e
      (log/infof "Downloading blob range interrupted for %s - terminating get operation" blob-name)
      ;; Reactor (used under the hood by the Azure SDK) leaves the thread interrupted - we need to clear this 
      (Thread/interrupted)
      (throw e))
    (catch Exception e
      (log/errorf "Exception thrown when downloading blob range for %s - %s" blob-name e)
      (throw e))))

(defn- put-blob [^BlobContainerClient blob-container-client blob-name blob-buffer]
  (try
    (rethrowing-reactor-cause
     (-> (.getBlobClient blob-container-client blob-name)
         (.upload (BinaryData/fromByteBuffer blob-buffer))))
    (catch BlobStorageException e
      (if (= 409 (.getStatusCode e))
        (log/infof "Blob already exists for %s - terminating put operation" blob-name)
        (throw e)))
    (catch InterruptedException e
      (log/infof "Blob upload interrupted for %s - terminating put operation" blob-name)
      ;; Reactor (used under the hood by the Azure SDK) leaves the thread interrupted - we need to clear this 
      (Thread/interrupted)
      (throw e))
    (catch Exception e
      (log/errorf "Exception thrown when uploading blob %s - %s" blob-name e)
      (throw e))))

(defn- delete-blob [^BlobContainerClient blob-container-client blob-name]
  (try
    (rethrowing-reactor-cause
     (-> (.getBlobClient blob-container-client blob-name)
         (.deleteIfExists)))
    (catch InterruptedException e
      (log/infof "Blob delete interrupted for %s - terminating delete operation" blob-name)
      ;; Reactor (used under the hood by the Azure SDK) leaves the thread interrupted - we need to clear this 
      (Thread/interrupted)
      (throw e))
    (catch Exception e
      (log/errorf "Exception thrown when deleting blob %s - %s" blob-name e)
      (throw e))))

(def ^Base64$Encoder base-64-encoder (Base64/getEncoder))

(defn random-block-id []
  (.encodeToString base-64-encoder (.getBytes (str (random-uuid)))))

(defrecord MultipartUpload [^BlockBlobClient block-blob-client ^List !staged-block-ids]
  IMultipartUpload
  (uploadPart [_  buf]
    (CompletableFuture/completedFuture
     (try
       (rethrowing-reactor-cause
        (let [block-id (random-block-id)
              binary-data (BinaryData/fromByteBuffer buf)]
          (.stageBlock block-blob-client block-id binary-data)
          (.add !staged-block-ids block-id)))
       (catch InterruptedException e
         (log/infof "Staging block interrupted for %s - aborting multipart operation" (.getBlobUrl block-blob-client))
         ;; Reactor (used under the hood by the Azure SDK) leaves the thread interrupted - we need to clear this 
         (Thread/interrupted)
         (throw e))
       (catch Exception e
         (log/errorf "Exception thrown when staging block in multipart upload for %s - %s" (.getBlobUrl block-blob-client) e)
         (throw e)))))

  (complete [_]
    (CompletableFuture/completedFuture
     ;; Commit the staged blocks - if already exists, abort the upload and complete
     (try
       (rethrowing-reactor-cause
        (.commitBlockList block-blob-client !staged-block-ids))
       (catch BlobStorageException e
         (if (= 409 (.getStatusCode e))
           (log/infof "Blob already exists for %s - aborting multipart upload" (.getBlobUrl block-blob-client))
           (throw e)))
       (catch InterruptedException e
         (log/infof "Multipart upload completion interrupted for %s - aborting multipart operation" (.getBlobUrl block-blob-client))
          ;; Reactor (used under the hood by the Azure SDK) leaves the thread interrupted - we need to clear this 
         (Thread/interrupted)
         (throw e))
       (catch Exception e
         (log/errorf "Exception thrown when completing mulipart upload for %s - %s" (.getBlobUrl block-blob-client) e)
         (throw e)))))

  (abort [_]
    (CompletableFuture/completedFuture nil)))

(defn- start-multipart [^BlobContainerClient blob-container-client blob-name]
  (let [block-blob-client (-> blob-container-client
                              (.getBlobClient blob-name)
                              (.getBlockBlobClient))]
    (->MultipartUpload block-blob-client (ArrayList.))))

(defrecord AzureBlobObjectStore [^BlobContainerClient blob-container-client ^Path prefix multipart-minimum-part-size]
  ObjectStore
  (getObject [_ k]
    (let [prefixed-key (util/prefix-key prefix k)]
      (CompletableFuture/completedFuture
       (get-blob blob-container-client (str prefixed-key)))))

  (getObject [_ k out-path]
    (let [prefixed-key (util/prefix-key prefix k)]
      (CompletableFuture/supplyAsync
       (fn []
         (get-blob blob-container-client (str prefixed-key) out-path)
         out-path))))

  (getObjectRange [_ k start len]
    (let [prefixed-key (util/prefix-key prefix k)]
      (CompletableFuture/completedFuture
       (get-blob-range blob-container-client (str prefixed-key) start len))))

  (putObject [_ k buf]
    (let [prefixed-key (util/prefix-key prefix k)]
      (CompletableFuture/completedFuture
       (put-blob blob-container-client (str prefixed-key) buf))))

  (listAllObjects [_this]
    (let [list-blob-opts (cond-> (ListBlobsOptions.)
                           prefix (.setPrefix (str prefix)))]
      (->> (.listBlobs blob-container-client list-blob-opts nil)
           (.iterator)
           (iterator-seq)
           (mapv (fn [^BlobItem blob-item]
                   (cond->> (util/->path (.getName blob-item))
                     prefix (.relativize prefix)))))))

  (deleteObject [_ k]
    (let [prefixed-key (util/prefix-key prefix k)]
      (CompletableFuture/completedFuture
       (delete-blob blob-container-client (str prefixed-key)))))

  SupportsMultipart
  (startMultipart [_ k]
    (let [prefixed-key (util/prefix-key prefix k)]
      (CompletableFuture/completedFuture
       (start-multipart blob-container-client (str prefixed-key)))))

  Closeable
  (close [_]))
