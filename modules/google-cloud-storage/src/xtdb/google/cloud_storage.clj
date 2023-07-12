(ns xtdb.google.cloud-storage
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [clojure.string :as string]
            [juxt.clojars-mirrors.nippy.v3v1v1.taoensso.nippy :as nippy]
            [xtdb.api :as xt]
            [xtdb.checkpoint :as cp]
            [xtdb.document-store :as ds]
            [xtdb.db :as db]
            [xtdb.io :as xio]
            [xtdb.system :as sys])
  (:import [com.google.cloud.storage Blob BlobId BlobInfo Storage StorageOptions
            Blob$BlobSourceOption Storage$BlobListOption Storage$BlobWriteOption Storage$BlobTargetOption]
           java.io.File
           java.nio.charset.StandardCharsets))

(defn get-blob [{:keys [^Storage storage-service bucket-name prefix]} blob-name]
  (let [blob-id (BlobId/of bucket-name (str prefix blob-name))]
    (some-> (.get storage-service blob-id)
            (.getContent (into-array Blob$BlobSourceOption [])))))

(defn get-blob-to-file [{:keys [^Storage storage-service bucket-name prefix]} blob-name file-path]
  (let [blob-id (BlobId/of bucket-name (str prefix blob-name))]
    (some-> (.get storage-service blob-id)
            (.downloadTo file-path (into-array Blob$BlobSourceOption [])))))

(defn put-blob-bytes
  ([{:keys [^Storage storage-service bucket-name prefix]} blob-name data]
   (let [blob-id (BlobId/of bucket-name (str prefix blob-name))
         blob-info (.build (BlobInfo/newBuilder blob-id))]
     (.create storage-service blob-info data (into-array Storage$BlobTargetOption [])))))

(defn put-blob-from-file
  ([{:keys [^Storage storage-service bucket-name prefix]} blob-name file-path]
   (let [blob-id (BlobId/of bucket-name (str prefix blob-name))
         blob-info (.build (BlobInfo/newBuilder blob-id))]
     (.createFrom storage-service blob-info file-path (into-array Storage$BlobWriteOption [])))))

(defn list-blobs 
  [{:keys [^Storage storage-service bucket-name prefix]} obj-prefix]
   (let [list-prefix (str prefix obj-prefix)
         list-blob-opts (into-array Storage$BlobListOption
                                    (if (not-empty list-prefix)
                                      [(Storage$BlobListOption/prefix (str prefix obj-prefix))]
                                      []))]
     (->> (.list storage-service bucket-name list-blob-opts)
          (.iterateAll)
          (mapv (fn [^Blob blob]
                  (subs (.getName blob) (count prefix)))))))

(defn delete-blob [{:keys [^Storage storage-service bucket-name prefix]} blob-name]
  (let [blob-id (BlobId/of bucket-name (str prefix blob-name))]
    (.delete storage-service blob-id)))
(defrecord GoogleCloudStorageDocumentStore [^Storage storage-service bucket-name prefix]
  db/DocumentStore
  (submit-docs [this docs]
    (->> (for [[id doc] docs]
           (future
             ;; Put blob - key set to id, blob is nippy compressed document byte array
             (put-blob-bytes this id (nippy/freeze doc))))
         vec
         (run! deref)))

  (fetch-docs [this ids]
    (xio/with-nippy-thaw-all
      (reduce
       (fn [docs id]
         (if-let [doc (get-blob this id)]
           (assoc docs id (nippy/thaw doc))
           docs))
       {}
       ids))))

(defrecord GoogleCloudStorageCheckpointStore [^Storage storage-service bucket-name prefix]
  cp/CheckpointStore
  (available-checkpoints [this {::cp/keys [cp-format]}]
    (->> (list-blobs this "metadata-")
         (sort-by (fn [path]
                    (let [[_ tx-id checkpoint-at] (re-matches #"metadata-checkpoint-(\d+)-(.+).edn" path)]
                      [(Long/parseLong tx-id) checkpoint-at]))
                  #(compare %2 %1))
         (keep (fn [cp-metadata-path]
                 (let [resp (some-> (get-blob this cp-metadata-path)
                                    (String. StandardCharsets/UTF_8)
                                    (edn/read-string))]
                   (when (= (::cp/cp-format resp) cp-format)
                     resp))))))

  (download-checkpoint [this {::keys [gcs-dir] :as checkpoint} dir]
    (when-not (empty? (.listFiles ^File dir))
      (throw (IllegalArgumentException. (str "non-empty checkpoint restore dir: " dir))))

    (let [gcs-paths (list-blobs this gcs-dir)
          blob-files (->> gcs-paths
                          (mapv (fn [gcs-path]
                                  (let [output-file (io/file dir (subs gcs-path (count gcs-dir)))
                                        _ (io/make-parents output-file)
                                        blob (get-blob-to-file this gcs-path (.toPath output-file))]
                                    (when (.exists output-file)
                                      [gcs-path blob]))))
                          (into {}))]

      (when-not (= (set (keys blob-files)) (set gcs-paths))
        (xio/delete-dir dir)
        (throw (ex-info "incomplete checkpoint restore" {:expected gcs-paths
                                                         :actual (keys blob-files)})))
      checkpoint))

  (upload-checkpoint [this dir {:keys [tx cp-at ::cp/cp-format]}]
    (let [dir-path (.toPath ^File dir)
          gcs-dir (format "checkpoint-%s-%s" (::xt/tx-id tx) (xio/format-rfc3339-date cp-at))]

      (->> (file-seq dir)
           (mapv (fn [^File file]
                   (when (.isFile file)
                     (let [file-path (.toPath file)
                           blob-name (str gcs-dir "/" (.relativize dir-path file-path))]
                       (put-blob-from-file this blob-name file-path))))))

      (let [cp {::cp/cp-format cp-format,
                :tx tx
                ::gcs-dir (str gcs-dir "/")
                ::cp/checkpoint-at cp-at}]
        (put-blob-bytes this
                        (str "metadata-" gcs-dir ".edn")
                        (.getBytes (pr-str cp) StandardCharsets/UTF_8))

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
                        ::sys/args {:project-id {:required? true
                                                 :spec ::sys/string
                                                 :doc "The name of your GCP project"}
                                    :bucket {:required? true,
                                             :spec ::sys/string
                                             :doc "The name of your GCS bucket"}
                                    :prefix {:required? false
                                             :spec ::prefix
                                             :doc "A string to prefix all of your files with (ie, if 'foo' is provided all xtdb documents will be located under a 'foo' directory)"}}}
  [{:keys [project-id bucket prefix document-cache] :as opts}]
  (let [storage-service (-> (StorageOptions/newBuilder)
                            (.setProjectId project-id)
                            (.build)
                            (.getService))]
    (ds/->cached-document-store
     (assoc opts
            :document-cache document-cache
            :document-store
            (->GoogleCloudStorageDocumentStore storage-service bucket prefix)))))

(defn ->checkpoint-store {::sys/args {:project-id {:required? true
                                                   :spec ::sys/string
                                                   :doc "The name of your GCP project"}
                                      :bucket {:required? true,
                                               :spec ::sys/string
                                               :doc "The name of your GCS bucket"}
                                      :prefix {:required? false
                                               :spec ::prefix
                                               :doc "A string to prefix all of your files with (ie, if 'foo' is provided all xtdb checkpoint files will be located under a 'foo' directory)"}}}
  [{:keys [project-id bucket prefix]}]
  (let [storage-service (-> (StorageOptions/newBuilder)
                            (.setProjectId project-id)
                            (.build)
                            (.getService))]
    (->GoogleCloudStorageCheckpointStore storage-service bucket prefix)))
