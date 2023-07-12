(ns xtdb.azure.blobs
  (:require [clojure.edn :as edn]
            [clojure.spec.alpha :as s]
            [clojure.string :as string]
            [juxt.clojars-mirrors.nippy.v3v1v1.taoensso.nippy :as nippy]
            [xtdb.checkpoint :as cp]
            [xtdb.api :as xt]
            [xtdb.db :as db]
            [xtdb.io :as xio]
            [xtdb.system :as sys]
            [xtdb.document-store :as ds]
            [clojure.java.io :as io])
  (:import com.azure.core.util.BinaryData
           java.io.File
           java.nio.file.Path
           [com.azure.storage.blob BlobServiceClientBuilder]
           [com.azure.identity DefaultAzureCredentialBuilder]
           [com.azure.storage.blob.models BlobStorageException ListBlobsOptions BlobItem]
           [com.azure.storage.blob BlobContainerClient]))

(defn get-blob [{:keys [^BlobContainerClient blob-container-client prefix]} blob-name]
  (try
    (-> (.getBlobClient blob-container-client (str prefix blob-name))
        (.downloadContent))
    (catch BlobStorageException e
      (if (= 404 (.getStatusCode e))
        nil
        (throw e)))))

(defn get-blob-to-file [{:keys [^BlobContainerClient blob-container-client prefix]} blob-name to-file]
  (try
    (-> (.getBlobClient blob-container-client (str prefix blob-name))
        (.downloadToFile to-file))
    (catch BlobStorageException e
      (if (= 404 (.getStatusCode e))
        nil
        (throw e)))))

(defn put-blob
  ([this blob-name data]
   (put-blob this blob-name data false))
  ([{:keys [^BlobContainerClient blob-container-client prefix]} blob-name ^BinaryData data overwrite?]
   (try
     (-> (.getBlobClient blob-container-client (str prefix blob-name))
         (.upload data overwrite?))
     (catch BlobStorageException e
       (if (= 409 (.getStatusCode e))
         nil
         (throw e))))))

(defn delete-blob [{:keys [^BlobContainerClient blob-container-client prefix]} blob-name]
  (-> (.getBlobClient blob-container-client (str prefix blob-name))
      (.deleteIfExists)))

(defn list-blobs [{:keys [^BlobContainerClient blob-container-client prefix]} obj-prefix]
  (let [list-blob-opts (.setPrefix (ListBlobsOptions.) (str prefix obj-prefix))]
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
             ;; Put blob - key set to id, blob is nippy compressed document 
             ;; converted to BinaryData, overwrite? set to true
             (put-blob this
                       id
                       (BinaryData/fromBytes (nippy/freeze doc))
                       true)))
         vec
         (run! deref)))

  (fetch-docs [this ids]
    (xio/with-nippy-thaw-all
      (reduce
       (fn [docs id]
         (if-let [doc (get-blob this id)]
           (assoc docs id (nippy/thaw (.toBytes doc)))
           docs))
       {}
       ids))))
(defrecord AzureBlobsCheckpointStore [^BlobContainerClient blob-container-client prefix]
  cp/CheckpointStore
  (available-checkpoints [this {::cp/keys [cp-format]}]
    (->> (list-blobs this "metadata-")
         (sort-by (fn [path]
                    (let [[_ tx-id checkpoint-at] (re-matches #"metadata-checkpoint-(\d+)-(.+).edn" path)]
                      [(Long/parseLong tx-id) checkpoint-at]))
                  #(compare %2 %1))
         (keep (fn [cp-metadata-path]
                 (let [resp (some-> (get-blob this cp-metadata-path)
                                    (.toString)
                                    (edn/read-string))]
                   (when (= (::cp/cp-format resp) cp-format)
                     resp))))))

  (download-checkpoint [this {::keys [azure-dir] :as checkpoint} dir]
    (when-not (empty? (.listFiles ^File dir))
      (throw (IllegalArgumentException. (str "non-empty checkpoint restore dir: " dir))))

    (let [azure-paths (list-blobs this azure-dir)
          blob-files (->> azure-paths
                          (mapv (fn [azure-path]
                                  (let [output-file (io/file dir (subs azure-path (count azure-dir)))
                                        _ (io/make-parents output-file)
                                        blob-properties (get-blob-to-file this azure-path (str output-file))]
                                    (when (.exists output-file)
                                      [azure-path blob-properties]))))
                          (into {}))]

      (when-not (= (set (keys blob-files)) (set azure-paths))
        (xio/delete-dir dir)
        (throw (ex-info "incomplete checkpoint restore" {:expected azure-paths
                                                         :actual (keys blob-files)})))
      checkpoint))

  (upload-checkpoint [this dir {:keys [tx cp-at ::cp/cp-format]}]
    (let [dir-path (.toPath ^File dir)
          azure-dir (format "checkpoint-%s-%s" (::xt/tx-id tx) (xio/format-rfc3339-date cp-at))]
      
      (->> (file-seq dir)
           (mapv (fn [^File file]
                   (when (.isFile file)
                     (let [file-path (.toPath file)
                           blob-name (str azure-dir "/" (.relativize dir-path (.toPath file)))]
                       (put-blob this blob-name (BinaryData/fromFile file-path)))))))

      (let [cp {::cp/cp-format cp-format,
                :tx tx
                ::azure-dir (str azure-dir "/")
                ::cp/checkpoint-at cp-at}]
        (put-blob this
                  (str "metadata-" azure-dir ".edn")
                  (BinaryData/fromString (pr-str cp)))
        
        cp)))

  (cleanup-checkpoint [this {:keys [tx cp-at]}]
    (let [azure-dir (format "checkpoint-%s-%s" (::xt/tx-id tx) (xio/format-rfc3339-date cp-at))]

      ;; Delete EDN file first
      (delete-blob this (str "metadata-" azure-dir ".edn"))

      ;; List & delete directory contents
      (let [files (list-blobs this azure-dir)]
        (mapv #(delete-blob this %) files)))))

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

(defn ->cp-store {::sys/args {:storage-account {:required? true
                                                :spec ::sys/string
                                                :doc "Azure Storage Account Name"}
                              :container {:required? true,
                                          :spec ::sys/string
                                          :doc "Azure Blob Storage Container"}
                              :prefix {:required? false
                                       :spec ::prefix
                                       :doc "A string to prefix all of your files with (ie, if 'foo' is provided all xtdb files will be located under a 'foo' directory)"}}}
  [{:keys [storage-account container prefix]}]
  (let [blob-service-client (cond-> (-> (BlobServiceClientBuilder.)
                                        (.endpoint (str "https://" storage-account ".blob.core.windows.net"))
                                        (.credential (.build (DefaultAzureCredentialBuilder.)))
                                        (.buildClient)))
        blob-client (.getBlobContainerClient blob-service-client container)]
    (->AzureBlobsCheckpointStore blob-client prefix)))
