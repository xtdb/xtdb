(ns xtdb.azure
  (:require [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [clojure.string :as string]
            [xtdb.util :as util]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.log :as xtdb-log]
            [xtdb.azure
             [object-store :as os]
             [log :as tx-log]])
  (:import [com.azure.storage.blob BlobServiceClientBuilder]
           [com.azure.identity DefaultAzureCredentialBuilder]
           [com.azure.messaging.eventhubs EventHubClientBuilder EventProcessorClientBuilder]
           [com.azure.resourcemanager.eventhubs EventHubsManager]
           [com.azure.core.management AzureEnvironment]
           [com.azure.core.management.profile AzureProfile]))

(derive ::blob-object-store :xtdb/object-store)

(defn- parse-prefix [prefix]
  (cond
    (string/blank? prefix) ""
    (string/ends-with? prefix "/") prefix
    :else (str prefix "/")))

(s/def ::storage-account string?)
(s/def ::container string?)
(s/def ::prefix string?)

(defmethod ig/prep-key ::blob-object-store [_ opts]
  (-> opts
      (util/maybe-update :prefix parse-prefix)))

(defmethod ig/pre-init-spec ::blob-object-store [_]
  (s/keys :req-un [::storage-account ::container]
          :opt-un [::prefix]))

(defmethod ig/init-key ::blob-object-store [_ {:keys [storage-account container prefix]}]
  (let [blob-service-client (cond-> (-> (BlobServiceClientBuilder.)
                                        (.endpoint (str "https://" storage-account ".blob.core.windows.net"))
                                        (.credential (.build (DefaultAzureCredentialBuilder.)))
                                        (.buildClient)))
        blob-client (.getBlobContainerClient blob-service-client container)]
    (os/->AzureBlobObjectStore blob-client prefix)))

(s/def ::namespace string?)
(s/def ::event-hub-name string?)
(s/def ::create-event-hub? boolean?)
(s/def ::resource-group-name string?)
(s/def ::retention-period-in-days number?)
(s/def ::max-wait-time ::util/duration)
(s/def ::poll-sleep-duration ::util/duration)


(derive ::event-hub-log :xtdb/log)

(defmethod ig/prep-key ::event-hub-log [_ opts]
  (-> (merge {:create-event-hub? false
              :max-wait-time "PT1S"
              :poll-sleep-duration "PT1S"
              :retention-period-in-days 7} opts)
      (util/maybe-update :max-wait-time util/->duration)))

(defmethod ig/pre-init-spec ::event-hub-log [_]
  (s/keys :req-un [::namespace ::event-hub-name ::max-wait-time ::create-event-hub? ::retention-period-in-days ::poll-sleep-duration]
          :opt-un [::resource-group-name]))

(defn resource-group-present? [{:keys [resource-group-name]}]
  (when-not resource-group-name
    (throw (IllegalArgumentException. "Must provide :resource-group-name when creating an eventhub automatically."))))

(defn create-event-hub-if-not-exists [azure-credential {:keys [resource-group-name namespace event-hub-name retention-period-in-days]}]
  (let [event-hub-manager (EventHubsManager/authenticate azure-credential (AzureProfile. (AzureEnvironment/AZURE)))
        event-hubs (.eventHubs event-hub-manager)
        event-hub-exists? (some
                           #(= event-hub-name (.name %))
                           (.listByNamespace event-hubs resource-group-name namespace))]
    (try
      (when-not event-hub-exists?
        (-> event-hubs
            (.define event-hub-name)
            (.withExistingNamespace resource-group-name namespace)
            (.withPartitionCount 1)
            (.withRetentionPeriodInDays retention-period-in-days)
            (.create)))
      (catch Exception e
        (log/error "Errror when creating event hub - " (.getMessage e))
        (throw e)))))

(defmethod ig/init-key ::event-hub-log [_ {:keys [create-event-hub? namespace event-hub-name max-wait-time poll-sleep-duration] :as opts}]
  (let [credential (.build (DefaultAzureCredentialBuilder.))
        fully-qualified-namespace (format "%s.servicebus.windows.net" namespace)
        event-hub-client-builder (-> (EventHubClientBuilder.)
                                     (.consumerGroup "$DEFAULT")
                                     (.credential credential)
                                     (.fullyQualifiedNamespace fully-qualified-namespace)
                                     (.eventHubName event-hub-name))
        subscriber-handler (xtdb-log/->notifying-subscriber-handler nil)]
    
    (when create-event-hub?
      (resource-group-present? opts)
      (create-event-hub-if-not-exists credential opts))
    
    (tx-log/->EventHubLog subscriber-handler
                          (.buildAsyncProducerClient event-hub-client-builder)
                          (.buildConsumerClient event-hub-client-builder)
                          max-wait-time
                          poll-sleep-duration)))

(defmethod ig/halt-key! ::event-hub-log [_ log]
  (util/try-close log))
