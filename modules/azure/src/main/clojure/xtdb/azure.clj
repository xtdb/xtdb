(ns xtdb.azure
  (:require [xtdb.azure.file-watch :as azure-file-watch]
            [xtdb.azure.object-store :as os]
            [xtdb.buffer-pool :as bp]
            [xtdb.error :as err]
            [xtdb.util :as util])
  (:import [com.azure.identity DefaultAzureCredentialBuilder]
           [com.azure.storage.blob BlobServiceClientBuilder]
           [java.util.concurrent ConcurrentSkipListSet]
           [xtdb.api.storage AzureBlobStorage AzureBlobStorage$Factory]))

(defmethod bp/->object-store-factory ::object-store [_ {:keys [storage-account container servicebus-namespace servicebus-topic-name 
                                                               prefix user-managed-identity-client-id
                                                               storage-account-endpoint servicebus-namespace-fqdn]}]
  (cond-> (AzureBlobStorage/azureBlobStorage storage-account container servicebus-namespace servicebus-topic-name)
    prefix (.prefix (util/->path prefix))
    user-managed-identity-client-id (.userManagedIdentityClientId user-managed-identity-client-id)
    storage-account-endpoint (.storageAccountEndpoint storage-account-endpoint)
    servicebus-namespace-fqdn (.serviceBusNamespaceFQDN servicebus-namespace-fqdn)))

;; No minimum block size in azure
(def minimum-part-size 0)

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn open-object-store [^AzureBlobStorage$Factory factory] 
  (let [storage-account-endpoint (cond
                                   (.getStorageAccountEndpoint factory) (.getStorageAccountEndpoint factory)
                                   (.getStorageAccount factory) (format "https://%s.blob.core.windows.net" (.getStorageAccount factory))
                                   :else (throw (err/illegal-arg :xtdb/missing-storage-account {::err/message "At least one of storageAccount or storageAccountEndpoint must be provided."})))
        servicebus-namespace-fqdn (cond
                                    (.getServiceBusNamespaceFQDN factory) (.getServiceBusNamespaceFQDN factory)
                                    (.getServiceBusNamespace factory) (format "%s.servicebus.windows.net" (.getServiceBusNamespace factory))
                                    :else (throw (err/illegal-arg :xtdb/missing-servicebus-namespace {::err/message "At least one of serviceBusNamespace or serviceBusNamespaceEndpoint must be provided."})))
        user-managed-identity-id (.getUserManagedIdentityClientId factory)
        credential (.build (cond-> (DefaultAzureCredentialBuilder.)
                             user-managed-identity-id (.managedIdentityClientId user-managed-identity-id))) 
        container (.getContainer factory)
        servicebus-topic-name (.getServiceBusTopicName factory)
        prefix (.getPrefix factory)
        prefix-with-version (if prefix (.resolve prefix bp/storage-root) bp/storage-root)
        blob-service-client (cond-> (-> (BlobServiceClientBuilder.)
                                        (.endpoint storage-account-endpoint)
                                        (.credential credential)
                                        (.buildClient)))
        blob-client (.getBlobContainerClient blob-service-client container)
        file-name-cache (ConcurrentSkipListSet.)
        ;; Watch azure container for changes
        file-list-watcher (azure-file-watch/open-file-list-watcher {:container container
                                                                    :servicebus-namespace-fqdn servicebus-namespace-fqdn
                                                                    :servicebus-topic-name servicebus-topic-name
                                                                    :prefix prefix-with-version
                                                                    :blob-container-client blob-client
                                                                    :azure-credential credential}
                                                                   file-name-cache)]
    (os/->AzureBlobObjectStore blob-client
                               prefix-with-version
                               minimum-part-size
                               file-name-cache
                               file-list-watcher)))

