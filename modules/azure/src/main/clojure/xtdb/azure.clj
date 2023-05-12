(ns xtdb.azure
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as string]
            [xtdb.util :as util]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.log :as xtdb-log]
            [xtdb.azure
             [object-store :as os]
             [log :as tx-log]])
  (:import [com.azure.storage.blob BlobServiceClientBuilder]
           [com.azure.identity DefaultAzureCredentialBuilder]
           [com.azure.messaging.eventhubs EventHubClientBuilder EventProcessorClientBuilder]))

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

(s/def ::fully-qualified-namespace string?)
(s/def ::event-hub-name string?)
(s/def ::max-wait-time ::util/duration)

(derive ::event-hub-log :xtdb/log)

(defmethod ig/prep-key ::event-hub-log [_ opts]
  (-> (merge {:max-wait-time "PT1S"} opts)
      (util/maybe-update :max-wait-time util/->duration)))

(defmethod ig/pre-init-spec ::event-hub-log [_]
  (s/keys :req-un [::fully-qualified-namespace ::event-hub-name ::max-wait-time]))

(defmethod ig/init-key ::event-hub-log [_ {:keys [fully-qualified-namespace event-hub-name max-wait-time]}]
  (let [event-hub-client-builder (-> (EventHubClientBuilder.)
                                     (.consumerGroup "$DEFAULT")
                                     (.credential fully-qualified-namespace
                                                  event-hub-name
                                                  (.build (DefaultAzureCredentialBuilder.))))
        subscriber-handler (xtdb-log/->notifying-subscriber-handler nil)]
    (tx-log/->EventHubLog subscriber-handler
                          (.buildAsyncProducerClient event-hub-client-builder)
                          (.buildConsumerClient event-hub-client-builder)
                          max-wait-time)))

(defmethod ig/halt-key! ::event-hub-log [_ log]
  (util/try-close log))
