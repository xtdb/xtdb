(ns xtdb.google.cloud-storage
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as string]
            [juxt.clojars-mirrors.nippy.v3v1v1.taoensso.nippy :as nippy]
            [xtdb.checkpoint :as cp]
            [xtdb.document-store :as ds]
            [xtdb.db :as db]
            [xtdb.io :as xio]
            [xtdb.system :as sys])
  (:import [com.google.cloud.storage Blob$BlobSourceOption BlobId BlobInfo Storage StorageOptions Storage$BlobTargetOption]))

(defn get-blob [{:keys [^Storage storage-service bucket-name prefix]} blob-name]
  (let [blob-id (BlobId/of bucket-name (str prefix blob-name))]
    (some-> (.get storage-service blob-id)
            (.getContent (into-array Blob$BlobSourceOption [])))))

(defn put-blob-bytes
  ([{:keys [^Storage ^Storage storage-service bucket-name prefix]} blob-name data]
   (let [blob-id (BlobId/of bucket-name (str prefix blob-name))
         blob-info (.build (BlobInfo/newBuilder blob-id))]
     (.create storage-service blob-info data (into-array Storage$BlobTargetOption [])))))
(defrecord GoogleCloudStorageDocumentStore [^Storage storage-service bucket-name prefix]
  db/DocumentStore
  (submit-docs [this docs]
    (->> (for [[id doc] docs]
           (future
             ;; Put blob - key set to id, blob is nippy compressed document byte array
             (put-blob-bytes this id (nippy/freeze doc))))
         vec
         (run! deref)))

  (fetch-docs [this docs]
    (xio/with-nippy-thaw-all
      (reduce
       #(if-let [doc (get-blob this %2)]
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
                        ::sys/args {:project-id {:required? true
                                                 :spec ::sys/string
                                                 :doc "The name of your GCP project"}
                                    :bucket {:required? true,
                                             :spec ::sys/string
                                             :doc "The name of your GCS bucket"}
                                    :prefix {:required? false
                                             :spec ::prefix
                                             :doc "A string to prefix all of your files with (ie, if 'foo' is provided all xtdb files will be located under a 'foo' directory)"}}}
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

(defn ->checkpoint-store {::sys/deps (::sys/deps (meta #'cp/->filesystem-checkpoint-store))
                          ::sys/args (::sys/args (meta #'cp/->filesystem-checkpoint-store))}
  [opts]
  (cp/->filesystem-checkpoint-store opts))
