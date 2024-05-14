(ns xtdb.azure
  (:require [clojure.tools.logging :as log]
            [xtdb.azure.file-watch :as azure-file-watch]
            [xtdb.azure.log :as tx-log]
            [xtdb.azure.object-store :as os]
            [xtdb.buffer-pool :as bp]
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

(defmethod bp/->object-store-factory ::object-store [_ {:keys [storage-account container servicebus-namespace servicebus-topic-name prefix]}]
  (cond-> (AzureBlobStorage/azureBlobStorage storage-account container servicebus-namespace servicebus-topic-name)
    prefix (.prefix (util/->path prefix))))

;; No minimum block size in azure
(def minimum-part-size 0)

(defn open-object-store [^AzureBlobStorage$Factory factory]
  (let [credential (.build (DefaultAzureCredentialBuilder.))
        storage-account (.getStorageAccount factory)
        container (.getContainer factory)
        servicebus-namespace (.getServiceBusNamespace factory)
        servicebus-topic-name (.getServiceBusTopicName factory)
        prefix (.getPrefix factory)
        prefix-with-version (if prefix (.resolve prefix bp/storage-root) bp/storage-root)
        blob-service-client (cond-> (-> (BlobServiceClientBuilder.)
                                        (.endpoint (str "https://" storage-account ".blob.core.windows.net"))
                                        (.credential credential)
                                        (.buildClient)))
        blob-client (.getBlobContainerClient blob-service-client container)
        file-name-cache (ConcurrentSkipListSet.)
        ;; Watch azure container for changes
        file-list-watcher (azure-file-watch/open-file-list-watcher {:storage-account storage-account
                                                                    :container container
                                                                    :servicebus-namespace servicebus-namespace
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
                                 create-event-hub? retention-period-in-days resource-group-name]}]
  (doto config
    (.setTxLog (cond-> (AzureEventHub/azureEventHub namespace event-hub-name)
                 max-wait-time (.maxWaitTime (time/->duration max-wait-time))
                 poll-sleep-duration (.pollSleepDuration (time/->duration poll-sleep-duration))
                 create-event-hub? (.autoCreateEventHub create-event-hub?)
                 retention-period-in-days (.retentionPeriodInDays retention-period-in-days)
                 resource-group-name (.resourceGroupName resource-group-name)))))

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
        credential (.build (DefaultAzureCredentialBuilder.))
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
