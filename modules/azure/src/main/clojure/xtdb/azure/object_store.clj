(ns xtdb.azure.object-store
  (:require [xtdb.object-store :as os]
            [xtdb.file-list :as file-list]
            [xtdb.util :as util])
  (:import [com.azure.core.util BinaryData]
           [com.azure.storage.blob BlobContainerClient]
           [com.azure.storage.blob.specialized BlockBlobClient]
           [com.azure.storage.blob.models BlobRange BlobStorageException DownloadRetryOptions]
           [java.io ByteArrayOutputStream Closeable]
           [java.lang AutoCloseable]
           [java.nio ByteBuffer]
           [java.util NavigableSet ArrayList List Base64 Base64$Encoder]
           [java.util.concurrent CompletableFuture]
           [java.util.function Supplier]
           [xtdb.object_store ObjectStore SupportsMultipart IMultipartUpload]))

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
        nil
        (throw e)))))

(defn- delete-blob [^BlobContainerClient blob-container-client blob-name]
  (-> (.getBlobClient blob-container-client blob-name)
      (.deleteIfExists)))

(def ^Base64$Encoder base-64-encoder (Base64/getEncoder))

(defn block-number->base64-block-id [block-number]
  (.encodeToString base-64-encoder (.getBytes (str block-number))))

(defrecord MultipartUpload [^BlockBlobClient block-blob-client on-complete ^List !staged-block-ids]
  IMultipartUpload
  (uploadPart [_  buf]
    (CompletableFuture/completedFuture
     (let [block-number (inc (count !staged-block-ids))
           block-id (block-number->base64-block-id block-number)
           binary-data (BinaryData/fromByteBuffer buf)]
       (.stageBlock block-blob-client block-id binary-data)
       (.add !staged-block-ids block-id))))

  (complete [_]
    (CompletableFuture/completedFuture
     (do
       (.commitBlockList block-blob-client !staged-block-ids)
       ;; Run passed in on-complete function (adds the key to the filename cache)
       (on-complete))))
  
  (abort [_]
    (CompletableFuture/completedFuture
     (do
       ;; Commit an empty blocklist removes all staged & uncomitted files
       (.commitBlockList block-blob-client [])
       ;; Delete the empty blob
       (.deleteIfExists block-blob-client)))))

(defn- start-multipart [^BlobContainerClient blob-container-client blob-name on-complete-fn]
  (let [block-blob-client (-> blob-container-client 
                              (.getBlobClient blob-name) 
                              (.getBlockBlobClient))]
    (->MultipartUpload block-blob-client on-complete-fn (ArrayList.))))

(defrecord AzureBlobObjectStore [^BlobContainerClient blob-container-client prefix minimum-part-size ^NavigableSet file-name-cache ^AutoCloseable file-list-watcher]
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
    (.add file-name-cache k)
    (put-blob blob-container-client (str prefix k) buf)
    (CompletableFuture/completedFuture nil))

  (listObjects [_this]
    (into [] file-name-cache))

  (listObjects [_this dir]
    (file-list/list-files-under-prefix file-name-cache dir))

  (deleteObject [_ k]
    (.remove file-name-cache k)
    (delete-blob blob-container-client (str prefix k))
    (CompletableFuture/completedFuture nil))
  
  SupportsMultipart
  (startMultipart [_ k]
    (CompletableFuture/completedFuture
     (start-multipart blob-container-client
                      (str prefix k)
                      (fn [] (.add file-name-cache k)))))

  Closeable
  (close [_]
    (.close file-list-watcher)
    (.clear file-name-cache)))