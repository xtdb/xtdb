(ns xtdb.azure
  (:require [xtdb.azure.object-store :as os]
            [xtdb.buffer-pool :as bp]
            [xtdb.error :as err]
            [xtdb.util :as util])
  (:import [com.azure.identity DefaultAzureCredentialBuilder]
           [com.azure.storage.blob BlobServiceClientBuilder]
           [xtdb.api.storage AzureBlobStorage AzureBlobStorage$Factory Storage]))

(defmethod bp/->object-store-factory ::object-store [_ {:keys [storage-account container
                                                               prefix user-managed-identity-client-id
                                                               storage-account-endpoint]}]
  (cond-> (AzureBlobStorage/azureBlobStorage storage-account container)
    prefix (.prefix (util/->path prefix))
    user-managed-identity-client-id (.userManagedIdentityClientId user-managed-identity-client-id)
    storage-account-endpoint (.storageAccountEndpoint storage-account-endpoint)))

;; No minimum block size in azure
(def minimum-part-size 0)

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn open-object-store [^AzureBlobStorage$Factory factory] 
  (let [^String storage-account-endpoint (cond
                                           (.getStorageAccountEndpoint factory) (.getStorageAccountEndpoint factory)
                                           (.getStorageAccount factory) (format "https://%s.blob.core.windows.net" (.getStorageAccount factory))
                                           :else (throw (err/illegal-arg :xtdb/missing-storage-account {::err/message "At least one of storageAccount or storageAccountEndpoint must be provided."})))
        user-managed-identity-id (.getUserManagedIdentityClientId factory)
        credential (.build (cond-> (DefaultAzureCredentialBuilder.)
                             user-managed-identity-id (.managedIdentityClientId user-managed-identity-id))) 
        container (.getContainer factory)
        prefix (.getPrefix factory)
        prefix-with-version (if prefix (.resolve prefix Storage/storageRoot) Storage/storageRoot)
        blob-service-client (cond-> (-> (BlobServiceClientBuilder.)
                                        (.endpoint storage-account-endpoint)
                                        (.credential credential)
                                        (.buildClient)))
        blob-client (.getBlobContainerClient blob-service-client container)]
    (os/->AzureBlobObjectStore blob-client prefix-with-version minimum-part-size)))
