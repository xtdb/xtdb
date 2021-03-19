(ns crux.azure.blobs
  (:require [clj-http.client :as http]
            [crux.db :as db]
            [crux.document-store :as ds]
            [crux.io :as cio]
            [crux.system :as sys]
            [taoensso.nippy :as nippy])
  (:import clojure.lang.MapEntry
           java.util.concurrent.CompletableFuture
           [java.util.function Function Supplier]))

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
  (submit-docs-async [_ docs]
    (CompletableFuture/allOf
     (into-array CompletableFuture
                 (for [[id doc] docs]
                   (CompletableFuture/supplyAsync
                    (reify Supplier
                      (get [_]
                        (put-blob sas-token storage-account container
                                  (str id)
                                  (nippy/freeze doc)))))))))

  (fetch-docs-async [_ ids]
    (let [futs (for [id ids]
                 (-> (CompletableFuture/supplyAsync
                      (reify Supplier
                        (get [_]
                          (get-blob sas-token storage-account container (str id)))))
                     (.thenApply
                      (reify Function
                        (apply [_ doc]
                          (cio/with-nippy-thaw-all
                            (MapEntry/create id (nippy/thaw doc))))))))]
      (-> (CompletableFuture/allOf (into-array CompletableFuture futs))
          (.thenApply (reify Function
                        (apply [_ _]
                          (into {} (map deref) futs))))))))

(defn ->document-store {::sys/deps {:document-cache 'crux.cache/->cache}
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
