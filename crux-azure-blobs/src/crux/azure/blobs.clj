(ns crux.azure.blobs
  (:require [clj-http.client :as http]
            [taoensso.nippy :as nippy]
            [crux.db :as db]
            [crux.system :as sys]
            [crux.document-store :as ds]
            [crux.lru :as lru]))

(defn- get-blob [sas-token storage-account container blob-name]
  ;; TODO : ETag
  (try
    (-> (format "https://%s.blob.core.windows.net/%s/%s?%s" storage-account container blob-name sas-token)
        http/get
        :body
        ((fn [^String s] (.getBytes s))))
    (catch Exception _ ;; TODO : Log "not found" etc.
      nil)))

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
    (reduce
     #(if-let [doc (get-blob sas-token storage-account container (str %2))]
        (assoc %1 %2 (nippy/thaw doc))
        %1)
     {}
     docs)))

(defn ->document-store {::sys/args {:sas-token {:required? true
                                                :spec ::sys/string
                                                :doc "Azure Blob Storage SAS Token"}
                                    :storage-account {:required? true
                                                      :spec ::sys/string
                                                      :doc "Azure Storage Account Name"}
                                    :container {:required? true,
                                                :spec ::sys/string
                                                :doc "Azure Blob Storage Container"}
                                    :doc-cache-size ds/doc-cache-size-opt}}
  [{:keys [sas-token storage-account container doc-cache-size] :as opts}]
  (ds/->cached-document-store
   (assoc opts
          :document-store
          (->AzureBlobsDocumentStore sas-token
                                     storage-account
                                     container))))
