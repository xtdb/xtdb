(ns xtdb.azure.object-store
  (:require [xtdb.object-store :as os]
            [xtdb.file-list :as file-list]
            [xtdb.azure.file-watch :as azure-file-watch]
            [xtdb.util :as util])
  (:import xtdb.object_store.ObjectStore
           java.util.concurrent.CompletableFuture
           java.util.function.Supplier
           java.util.NavigableSet
           com.azure.core.util.BinaryData
           [com.azure.storage.blob.models BlobRange BlobStorageException DownloadRetryOptions ListBlobsOptions BlobItem]
           [com.azure.storage.blob BlobContainerClient]
           (java.io ByteArrayOutputStream Closeable)
           (java.nio ByteBuffer)))

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
      (ByteBuffer/wrap (.toByteArray out)))))

(defn- put-blob [^BlobContainerClient blob-container-client blob-name blob-buffer]
  (try
    (-> (.getBlobClient blob-container-client blob-name)
        (.upload (BinaryData/fromByteBuffer blob-buffer)))
    (catch BlobStorageException e
      (if (= 409 (.getStatusCode e))
        nil
        (throw e)))))

(defn- delete-blob [^BlobContainerClient blob-container-client blob-name]
  (-> (.getBlobClient blob-container-client blob-name)
      (.deleteIfExists)))
(defrecord AzureBlobObjectStore [^BlobContainerClient blob-container-client prefix ^NavigableSet file-name-cache azure-watch-info]
  ObjectStore
  (getObject [_ k]
    (CompletableFuture/completedFuture
     (get-blob blob-container-client (str prefix k))))

  (getObject [_ k out-path]
    (CompletableFuture/supplyAsync
     (reify Supplier
       (get [_]
         (let [blob-buffer (get-blob blob-container-client (str prefix k))]
           (util/write-buffer-to-path blob-buffer out-path)

           out-path)))))

  (getObjectRange [_ k start len]
    (CompletableFuture/completedFuture
     (get-blob-range blob-container-client (str prefix k) start len)))

  (putObject [_ k buf]
    (put-blob blob-container-client (str prefix k) buf)
    (CompletableFuture/completedFuture nil))

  (listObjects [_this]
    (into [] file-name-cache))

  (listObjects [_this dir]
    (file-list/list-files-under-prefix file-name-cache dir))

  (deleteObject [_ k]
    (delete-blob blob-container-client (str prefix k))
    (CompletableFuture/completedFuture nil))

  Closeable
  (close [_]
    (azure-file-watch/watcher-close-fn azure-watch-info)
    (.clear file-name-cache)))
