(ns xtdb.azure.blobs
  (:require [juxt.clojars-mirrors.clj-http.v3v12v2.clj-http.client :as http]
            [juxt.clojars-mirrors.nippy.v3v1v1.taoensso.nippy :as nippy]
            [xtdb.db :as db]
            [xtdb.io :as xio]
            [xtdb.system :as sys]
            [xtdb.document-store :as ds]))

(defn- get-blob [sas-token storage-account container blob-name]
  ;; TODO : ETag
  (try
    (-> (format "https://%s.blob.core.windows.net/%s/%s?%s" storage-account container blob-name sas-token)
        http/get
        :body
        ((fn [^String s] (.getBytes s))))
    (catch Exception e
      (if (= 404 (:status (ex-data e)))
        nil
        (throw e)))))

(defn- put-blob [sas-token storage-account container blob-name blob-bytes]
  ;; TODO ETag
  (-> (format "https://%s.blob.core.windows.net/%s/%s?%s" storage-account container blob-name sas-token)
      (http/put {:headers {"x-ms-blob-type" "BlockBlob"}
                 :body blob-bytes})))

(defrecord AzureBlobsDocumentStore [sas-token storage-account container]
  db/DocumentStore
  (submit-docs [_ docs]
    (->> (for [[id doc] docs]
           (future
             (put-blob sas-token storage-account container
                       (str id)
                       (nippy/freeze doc))))
         vec
         (run! deref)))

  (fetch-docs [_ docs]
    (xio/with-nippy-thaw-all
      (reduce
       #(if-let [doc (get-blob sas-token storage-account container (str %2))]
          (assoc %1 %2 (nippy/thaw doc))
          %1)
       {}
       docs))))

(defn ->document-store {::sys/deps {:document-cache 'xtdb.cache/->cache}
                        ::sys/args {:sas-token {:required? true
                                                :spec ::sys/string
                                                :doc "Azure Blob Storage SAS Token"}
                                    :storage-account {:required? true
                                                      :spec ::sys/string
                                                      :doc "Azure Storage Account Name"}
                                    :container {:required? true,
                                                :spec ::sys/string
                                                :doc "Azure Blob Storage Container"}}}
  [{:keys [sas-token storage-account container document-cache] :as opts}]
  (ds/->cached-document-store
   (assoc opts
          :document-cache document-cache
          :document-store
          (->AzureBlobsDocumentStore sas-token
                                     storage-account
                                     container))))
