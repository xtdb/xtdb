(ns xtdb.azure
  (:require [clojure.tools.logging :as log]
            [xtdb.azure.file-watch :as azure-file-watch]
            [xtdb.azure.log :as tx-log]
            [xtdb.azure.object-store :as os]
            [xtdb.buffer-pool :as bp]
            [xtdb.error :as err]
            [xtdb.log :as xtdb-log]
            [xtdb.node :as xtn]
            [xtdb.time :as time]
            [xtdb.util :as util])
  (:import [com.azure.core.credential TokenCredential]
           [com.azure.core.management AzureEnvironment]
           [com.azure.core.management.profile AzureProfile]
           [com.azure.identity DefaultAzureCredentialBuilder]
           [com.azure.messaging.eventhubs EventHubClientBuilder]
           [com.azure.resourcemanager.eventhubs EventHubsManager]
           [com.azure.resourcemanager.eventhubs.models EventHub EventHub$Definition EventHubs]
           [com.azure.storage.blob BlobServiceClientBuilder]
           [java.util.concurrent ConcurrentSkipListSet]
           [xtdb.api Xtdb$Config]
           [xtdb.api.log AzureEventHub AzureEventHub$Factory]
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

(defmethod xtn/apply-config! ::event-hub-log
  [^Xtdb$Config config _ {:keys [namespace event-hub-name max-wait-time poll-sleep-duration
                                 create-event-hub? retention-period-in-days resource-group-name
                                 user-managed-identity-client-id]}]
  (doto config
    (.setTxLog (cond-> (AzureEventHub/azureEventHub namespace event-hub-name)
                 max-wait-time (.maxWaitTime (time/->duration max-wait-time))
                 poll-sleep-duration (.pollSleepDuration (time/->duration poll-sleep-duration))
                 create-event-hub? (.autoCreateEventHub create-event-hub?)
                 retention-period-in-days (.retentionPeriodInDays retention-period-in-days)
                 resource-group-name (.resourceGroupName resource-group-name)
                 user-managed-identity-client-id (.userManagedIdentityClientId user-managed-identity-client-id)))))

(defn resource-group-present? [resource-group-name]
  (when-not resource-group-name
    (throw (IllegalArgumentException. "Must provide :resource-group-name when creating an eventhub automatically."))))

(defn create-event-hub-if-not-exists [^TokenCredential azure-credential resource-group-name namespace event-hub-name retention-period-in-days]
  (let [event-hub-manager (EventHubsManager/authenticate azure-credential (AzureProfile. (AzureEnvironment/AZURE)))
        ^EventHubs event-hubs (.eventHubs event-hub-manager)
        event-hub-exists? (some
                           #(= event-hub-name (.name ^EventHub %))
                           (.listByNamespace event-hubs resource-group-name namespace))]
    (try
      (when-not event-hub-exists?
        (-> event-hubs
            ^EventHub$Definition (.define event-hub-name)
            (.withExistingNamespace resource-group-name namespace)
            (.withPartitionCount 1)
            (.withRetentionPeriodInDays retention-period-in-days)
            (.create)))
      (catch Exception e
        (log/error "Error when creating event hub - " (.getMessage e))
        (throw e)))))

;; Used within tests
(defn delete-event-hub-if-exists [^TokenCredential azure-credential resource-group-name namespace event-hub-name]
  (let [event-hub-manager (EventHubsManager/authenticate azure-credential (AzureProfile. (AzureEnvironment/AZURE)))
        ^EventHubs event-hubs (.eventHubs event-hub-manager)
        event-hub-exists? (some
                           #(= event-hub-name (.name ^EventHub %))
                           (.listByNamespace event-hubs resource-group-name namespace))]
    (when event-hub-exists?
      (-> event-hubs
          (.deleteByName resource-group-name namespace event-hub-name)))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn open-log [^AzureEventHub$Factory factory]
  (let [namespace (.getNamespace factory)
        event-hub-name (.getEventHubName factory)
        max-wait-time (.getMaxWaitTime factory)
        poll-sleep-duration (.getPollSleepDuration factory)
        create-event-hub? (.getAutoCreateEventHub factory)
        retention-period-in-days (.getRetentionPeriodInDays factory)
        resource-group-name (.getResourceGroupName factory)
        fully-qualified-namespace (format "%s.servicebus.windows.net" namespace)
        user-managed-identity-id (.getUserManagedIdentityClientId factory)
        credential (.build (cond-> (DefaultAzureCredentialBuilder.)
                             user-managed-identity-id (.managedIdentityClientId user-managed-identity-id)))
        event-hub-client-builder (-> (EventHubClientBuilder.)
                                     (.consumerGroup "$DEFAULT")
                                     (.credential credential) 
                                     (.fullyQualifiedNamespace fully-qualified-namespace)
                                     (.eventHubName event-hub-name))
        subscriber-handler (xtdb-log/->notifying-subscriber-handler nil)]

    (when create-event-hub?
      (resource-group-present? resource-group-name)
      (create-event-hub-if-not-exists credential resource-group-name namespace event-hub-name retention-period-in-days))

    (tx-log/->EventHubLog subscriber-handler
                          (.buildAsyncProducerClient event-hub-client-builder)
                          (.buildConsumerClient event-hub-client-builder)
                          max-wait-time
                          poll-sleep-duration)))
