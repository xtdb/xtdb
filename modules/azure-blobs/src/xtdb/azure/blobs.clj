(ns xtdb.azure.blobs
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as string]
            [juxt.clojars-mirrors.nippy.v3v1v1.taoensso.nippy :as nippy]
            [xtdb.db :as db]
            [xtdb.io :as xio]
            [xtdb.system :as sys]
            [xtdb.document-store :as ds])
  (:import com.azure.core.util.BinaryData
           [com.azure.storage.blob BlobServiceClientBuilder]
           [com.azure.identity DefaultAzureCredentialBuilder]
           [com.azure.storage.blob.models BlobStorageException ListBlobsOptions BlobItem]
           [com.azure.storage.blob BlobContainerClient]))

(defn- get-blob [{:keys [^BlobContainerClient blob-container-client]} blob-name]
  (try
    (-> (.getBlobClient blob-container-client blob-name)
        (.downloadContent)
        (.toBytes))
    (catch BlobStorageException e
      (if (= 404 (.getStatusCode e))
        nil
        (throw e)))))

(defn- put-blob 
  ([opts blob-name blob-bytes]
   (put-blob opts blob-name blob-bytes false))
  ([{:keys [^BlobContainerClient blob-container-client]} blob-name blob-bytes overwrite?]
   (try
     (-> (.getBlobClient blob-container-client blob-name)
         (.upload (BinaryData/fromBytes blob-bytes) overwrite?))
     (catch BlobStorageException e
       (if (= 409 (.getStatusCode e))
         nil
         (throw e))))))

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
(defrecord AzureBlobsDocumentStore [^BlobContainerClient blob-container-client prefix]
  db/DocumentStore
  (submit-docs [this docs]
    (->> (for [[id doc] docs]
           (future
             ;; Put blob - key set to prefix + id, blob is nippy compressed document, overwrite set to true
             (put-blob this (str prefix id) (nippy/freeze doc) true)))
         vec
         (run! deref)))

  (fetch-docs [this docs]
    (xio/with-nippy-thaw-all
      (reduce
       #(if-let [doc (get-blob this (str prefix %2))]
          (assoc %1 %2 (nippy/thaw doc))
          %1)
       {}
       docs))))

(s/def ::prefix (s/and ::sys/string
                       (s/conformer (fn [prefix]
                                      (cond
                                        (string/blank? prefix) ""
                                        (string/ends-with? prefix "/") prefix
                                        :else (str prefix "/"))))))

(defn ->document-store {::sys/deps {:document-cache 'xtdb.cache/->cache}
                        ::sys/args {:storage-account {:required? true
                                                      :spec ::sys/string
                                                      :doc "Azure Storage Account Name"}
                                    :container {:required? true,
                                                :spec ::sys/string
                                                :doc "Azure Blob Storage Container"}
                                    :prefix {:required? false
                                             :spec ::prefix
                                             :doc "A string to prefix all of your files with (ie, if 'foo' is provided all xtdb files will be located under a 'foo' directory)"}}}
  [{:keys [storage-account container prefix document-cache] :as opts}]
  (let [blob-service-client (cond-> (-> (BlobServiceClientBuilder.)
                                        (.endpoint (str "https://" storage-account ".blob.core.windows.net"))
                                        (.credential (.build (DefaultAzureCredentialBuilder.)))
                                        (.buildClient)))
        blob-client (.getBlobContainerClient blob-service-client container)]
    (ds/->cached-document-store
     (assoc opts
            :document-cache document-cache
            :document-store
            (->AzureBlobsDocumentStore blob-client prefix)))))
