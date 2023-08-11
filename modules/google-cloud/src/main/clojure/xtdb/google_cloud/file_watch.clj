(ns xtdb.google-cloud.file-watch
  (:require [clojure.string :as string]
            [clojure.tools.logging :as log])
  (:import [com.google.pubsub.v1 SubscriptionName TopicName PushConfig PubsubMessage]
           [com.google.cloud.pubsub.v1 SubscriptionAdminClient Subscriber MessageReceiver AckReplyConsumer]
           [com.google.cloud.storage Blob Storage Storage$BlobListOption]
           [java.lang AutoCloseable]
           [java.util NavigableSet UUID]))

(defn file-list-init [{:keys [^Storage storage-service bucket prefix]} ^NavigableSet file-name-cache]
  (let [list-blob-opts (into-array Storage$BlobListOption
                                   (if (not-empty prefix)
                                     [(Storage$BlobListOption/prefix prefix)]
                                     []))
        filename-list (->> (.list storage-service bucket list-blob-opts)
                           (.iterateAll)
                           (mapv (fn [^Blob blob]
                                   (subs (.getName blob) (count prefix)))))]
    (.addAll file-name-cache filename-list)))

(defn mk-short-uuid []
  (subs (str (UUID/randomUUID)) 0 8))

(defn setup-topic-subscription [{:keys [project-id pubsub-topic]}]
  (let [subcription-admin-client (SubscriptionAdminClient/create)
        topic (TopicName/of project-id pubsub-topic)
        subscription-name (format "xtdb-notifs-subscription-%s" (mk-short-uuid))
        subscription (SubscriptionName/of project-id subscription-name)

        _ (log/info (format "Creating new subscription on Pub/Sub topic %s, subscription name %s" pubsub-topic subscription-name))
        subscription-info (.createSubscription subcription-admin-client subscription topic (PushConfig/getDefaultInstance) 0)]
    {:subcription-admin-client subcription-admin-client
     :subscription-name subscription-name
     :subscription-resource-name (.getName subscription-info)}))

(defn open-file-list-watcher [{:keys [bucket prefix pubsub-topic] :as opts} ^NavigableSet file-name-cache]
  (let [;; Create queue that will subscribe to sns topic for notifications
        {:keys [^SubscriptionAdminClient subcription-admin-client 
                ^String subscription-name 
                ^String subscription-resource-name]} (setup-topic-subscription opts)

        ;; Init the filename cache with current files
        _ (log/info "Initializing filename list from bucket " bucket)
        _ (file-list-init opts file-name-cache)

        message-receiver (reify MessageReceiver
                           (receiveMessage [_ message consumer]
                             (let [{:strs [objectId eventType]} (.getAttributes ^PubsubMessage message)
                                   event-type (get {"OBJECT_FINALIZE" :create "OBJECT_DELETE" :delete} eventType)
                                   file (cond
                                          (string/ends-with? objectId "/")
                                          nil

                                          prefix
                                          (when (string/starts-with? objectId prefix)
                                            (subs objectId (count prefix)))

                                          :else objectId)]
                               (log/debug (format "Message received, performing %s on file %s" event-type file))
                               (when (and event-type file)
                                 (cond
                                   (= event-type :create) (.add file-name-cache file)
                                   (= event-type :delete) (.remove file-name-cache file)))

                               (.ack ^AckReplyConsumer consumer))))
        ^Subscriber subscriber (.build (Subscriber/newBuilder ^String subscription-resource-name ^MessageReceiver message-receiver))]

      ;; Start processing messages with the subscriber
    (log/info "Watching for filechanges from bucket " bucket)
    (.startAsync subscriber)
    (.awaitRunning subscriber)

    ;; Return an auto closeable object that clears up the processor and subscription
    (reify
      AutoCloseable
      (close [_]
        (log/info "Stopping & terminate filechange subscriber client")
        (.stopAsync subscriber)
        (.awaitTerminated subscriber)

        (log/info (format "Removing subscription %s on topic %s " subscription-name pubsub-topic))
        (.deleteSubscription subcription-admin-client subscription-resource-name)))))
