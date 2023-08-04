(ns xtdb.azure.file-watch
  (:require [clojure.data.json :as json]
            [clojure.string :as string]
            [clojure.tools.logging :as log])
  (:import [com.azure.core.credential TokenCredential]
           [com.azure.core.management AzureEnvironment]
           [com.azure.core.management.profile AzureProfile]
           [com.azure.messaging.servicebus ServiceBusClientBuilder]
           [com.azure.messaging.servicebus.administration ServiceBusAdministrationClientBuilder]
           [com.azure.messaging.servicebus.administration.models CreateQueueOptions]
           [com.azure.resourcemanager.eventgrid EventGridManager]
           [com.azure.resourcemanager.eventgrid.models ServiceBusQueueEventSubscriptionDestination EventSubscriptionProvisioningState]
           [com.azure.resourcemanager.eventgrid.fluent.models EventSubscriptionInner]
           [com.azure.resourcemanager.servicebus ServiceBusManager]
           [com.azure.resourcemanager.servicebus.models EntityStatus]
           [com.azure.storage.blob.models ListBlobsOptions BlobItem]
           [com.azure.storage.blob BlobContainerClient]
           [java.util NavigableSet UUID]
           java.util.function.Consumer
           java.time.Duration))

(defn file-list-init [{:keys [^BlobContainerClient blob-container-client prefix]}  ^NavigableSet file-name-cache]
  (let [list-blob-opts (cond-> (ListBlobsOptions.)
                         prefix (.setPrefix prefix))
        filename-list (->> (.listBlobs blob-container-client list-blob-opts nil)
                           (.iterator)
                           (iterator-seq)
                           (mapv (fn [^BlobItem blob-item]
                                   (subs (.getName blob-item) (count prefix)))))]
    (.addAll file-name-cache filename-list)))

(defn mk-short-uuid []
  (subs (str (UUID/randomUUID)) 0 8))

(defn successuflly-created? [subscription-info]
  (= (.provisioningState subscription-info) EventSubscriptionProvisioningState/SUCCEEDED))

(defn failed-creation? [subscription-info]
  (= (.provisioningState subscription-info) EventSubscriptionProvisioningState/FAILED))

(defn setup-notif-queue [{:keys [^TokenCredential azure-credential resource-group-name servicebus-namespace eventgrid-topic]}]
  (let [servicebus-manager (ServiceBusManager/authenticate azure-credential (AzureProfile. (AzureEnvironment/AZURE)))
        servicebus-admin-client (-> (ServiceBusAdministrationClientBuilder.)
                                    (.credential (format "%s.servicebus.windows.net" servicebus-namespace)
                                                 azure-credential)
                                    (.buildClient))
        eventgrid-manager (EventGridManager/authenticate azure-credential (AzureProfile. (AzureEnvironment/AZURE)))
        queue-name (format "xtdb-object-store-container-notifs-%s" (mk-short-uuid))

        _ (log/info "Creating Service Bus Queue " queue-name)
        _ (-> servicebus-admin-client
              (.createQueue queue-name (-> (CreateQueueOptions.)
                                           (.setAutoDeleteOnIdle nil)
                                           (.setDefaultMessageTimeToLive (Duration/ofHours 1)))))

        _ (log/info "Fetching resource info for Service Bus Queue " queue-name)
        queue-info (-> servicebus-manager
                       (.serviceClient)
                       (.getQueues)
                       (.get resource-group-name servicebus-namespace queue-name))

        _ (log/info (format "Creating subscription on %s for queue %s" eventgrid-topic queue-name))
        subscription-info (-> eventgrid-manager
                              (.systemTopicEventSubscriptions)
                              (.createOrUpdate resource-group-name
                                               eventgrid-topic
                                               (format "%s-subscription" queue-name)
                                               (-> (EventSubscriptionInner.)
                                                   (.withDestination
                                                    (-> (ServiceBusQueueEventSubscriptionDestination.)
                                                        (.withResourceId (.id queue-info)))))))]

    

    ;; await successful creation of subscription
    (while (not (successuflly-created? subscription-info))
      (when (failed-creation? subscription-info)
        (throw (Exception. (format "Service bus queue %s failed to be created, closing node." queue-name))))
      (Thread/sleep 10))

    {:servicebus-admin-client servicebus-admin-client
     :eventgrid-manager eventgrid-manager
     :subscription-name (.name subscription-info)
     :queue-name queue-name
     :resource-group-name resource-group-name
     :eventgrid-topic eventgrid-topic}))

(defn file-list-watch [{:keys [^BlobContainerClient blob-container-client ^TokenCredential azure-credential servicebus-namespace container prefix] :as opts} ^NavigableSet file-name-cache]
  (let [;; Create queue that will subscribe to sns topic for notifications
        {:keys [queue-name subscription-name] :as closable-opts} (setup-notif-queue opts)

        _ (log/info "Initializing filename list from container " container)
         ;; Init the filename cache with current files
        _ (file-list-init opts file-name-cache)
        
        url-suffix (if prefix (str "/" prefix) "/")
        base-file-url (str (.getBlobContainerUrl blob-container-client) url-suffix)
        processor-client (-> (ServiceBusClientBuilder.)
                             (.fullyQualifiedNamespace (format "%s.servicebus.windows.net" servicebus-namespace))
                             (.credential azure-credential)
                             (.processor)
                             (.queueName queue-name)
                             (.subscriptionName subscription-name)
                             (.disableAutoComplete)
                             (.processMessage (reify Consumer
                                                (accept [_ msg]
                                                  (let [parsed-msg (json/read-str (.. msg getMessage getBody toString) :key-fn keyword)
                                                        msg-data (:data parsed-msg)
                                                        event-type (get {"PutBlob" :create "DeleteBlob" :delete} (:api msg-data))
                                                        file-url (:url msg-data)
                                                        file (when (string/starts-with? file-url base-file-url)
                                                              (subs file-url (count base-file-url)))]
                                                    (log/info (format "Message received, performing %s on file %s" event-type file))
                                                    (when (and event-type file)
                                                      (cond
                                                        (= event-type :create) (.add file-name-cache file)
                                                        (= event-type :delete) (.remove file-name-cache file)))
                                                    
                                                    (.complete msg)))))
                             (.processError (reify Consumer
                                              (accept [_ msg]
                                                      (log/error "Error when processing message from service bus queue - " (.getException msg)))))
                             (.buildProcessorClient))]

      ;; Start processing messages from the queue
    (log/info "Watching for filechanges from container " container)
    (.start processor-client)

      ;; Return all closeable opts from the function
    (assoc closable-opts :processor-client processor-client)))

(defn watcher-close-fn [{:keys [resource-group-name eventgrid-topic servicebus-admin-client eventgrid-manager processor-client subscription-name queue-name]}]
  (log/info "Stopping & closing filechange processor client")
  (.close processor-client)

  (log/info "Awaiting AMQP connection close")
  (Thread/sleep 60000)

  (log/info "Removing queue subscription, subscription-id: " subscription-name)
  (-> eventgrid-manager
      (.systemTopicEventSubscriptions)
      (.delete resource-group-name
               eventgrid-topic
               subscription-name))
  
  (log/info "Removing Service Bus Queue, queue-name: " queue-name)
  (.deleteQueue servicebus-admin-client queue-name))
