(ns xtdb.azure
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as string]
            [xtdb.object-store :as os]
            [xtdb.util :as util]
            [juxt.clojars-mirrors.integrant.core :as ig])
  (:import xtdb.object_store.ObjectStore
           java.util.concurrent.CompletableFuture
           java.util.function.Supplier
           com.azure.core.util.BinaryData
           [com.azure.storage.blob.models BlobStorageException ListBlobsOptions BlobItem]
           [com.azure.storage.blob BlobServiceClientBuilder BlobContainerClient BlobClient]
           [com.azure.identity DefaultAzureCredentialBuilder]))

(defn- get-blob [^BlobContainerClient blob-container-client blob-name]
  (try
    (-> (.getBlobClient blob-container-client blob-name)
        (.downloadContent)
        (.toByteBuffer))
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

(defn- list-blobs [^BlobContainerClient blob-container-client prefix obj-prefix]
  (let [list-blob-opts (cond-> (ListBlobsOptions.)
                         obj-prefix (.setPrefix obj-prefix))]
    (->> (.listBlobs blob-container-client list-blob-opts nil)
         (.iterator)
         (iterator-seq)
         (mapv (fn [^BlobItem blob-item]
                 (subs (.getName blob-item) (count prefix)))))))
(defrecord AzureBlobObjectStore [^BlobContainerClient blob-container-client prefix]
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

  (putObject [_ k buf]
    (put-blob blob-container-client (str prefix k) buf)
    (CompletableFuture/completedFuture nil))

  (listObjects [this]
    (.listObjects this nil))

  (listObjects [_ obj-prefix]
    (list-blobs blob-container-client prefix (str prefix obj-prefix)))

  (deleteObject [_ k]
    (delete-blob blob-container-client (str prefix k))
    (CompletableFuture/completedFuture nil)))

(derive ::blob-object-store :xtdb/object-store)

(defn- parse-prefix [prefix]
  (cond
    (string/blank? prefix) ""
    (string/ends-with? prefix "/") prefix
    :else (str prefix "/")))

(s/def ::storage-account string?)
(s/def ::container string?)
(s/def ::prefix string?)

(defmethod ig/prep-key ::blob-object-store [_ opts]
  (-> opts
      (util/maybe-update :prefix parse-prefix)))

(defmethod ig/pre-init-spec ::blob-object-store [_]
  (s/keys :req-un [::storage-account ::container]
          :opt-un [::prefix]))

(defmethod ig/init-key ::blob-object-store [_ {:keys [storage-account container prefix]}]
  (let [blob-service-client (cond-> (-> (BlobServiceClientBuilder.)
                                        (.endpoint (str "https://" storage-account ".blob.core.windows.net"))
                                        (.credential (.build (DefaultAzureCredentialBuilder.)))
                                        (.buildClient)))
        blob-client (.getBlobContainerClient blob-service-client container)]
    (->AzureBlobObjectStore blob-client prefix)))
