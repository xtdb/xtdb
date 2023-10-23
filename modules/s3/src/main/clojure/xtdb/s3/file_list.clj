(ns xtdb.s3.file-list
  (:require [clojure.data.json :as json]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [xtdb.file-list :as file-list])
  (:import [java.lang AutoCloseable]
           [java.util UUID NavigableSet]
           [software.amazon.awssdk.core.exception AbortedException]
           [software.amazon.awssdk.services.s3 S3AsyncClient]
           [software.amazon.awssdk.services.s3.model ListObjectsV2Request ListObjectsV2Response S3Object]
           [software.amazon.awssdk.services.sns SnsClient]
           [software.amazon.awssdk.services.sns.model SubscribeRequest SubscribeResponse UnsubscribeRequest]
           [software.amazon.awssdk.services.sqs SqsClient]
           [software.amazon.awssdk.services.sqs.model Message CreateQueueRequest CreateQueueResponse DeleteMessageRequest DeleteQueueRequest ReceiveMessageRequest
            SetQueueAttributesRequest GetQueueAttributesRequest GetQueueAttributesResponse QueueDoesNotExistException]))

(defn list-objects [{:keys [^S3AsyncClient s3-client bucket prefix] :as s3-opts} continuation-token]
  (vec
   (let [^ListObjectsV2Request
         req (-> (ListObjectsV2Request/builder)
                 (.bucket bucket)
                 (.prefix prefix)
                 (cond-> continuation-token (.continuationToken continuation-token))
                 (.build))

         ^ListObjectsV2Response
         resp (.get (.listObjectsV2 ^S3AsyncClient s3-client req))]

     (concat (for [^S3Object object (.contents resp)]
               (subs (.key object) (count prefix)))
             (when (.isTruncated resp)
               (list-objects s3-opts (.nextContinuationToken resp)))))))

(defn file-list-init [s3-opts ^NavigableSet file-name-cache]
  (let [filename-list (list-objects s3-opts nil)]
    (file-list/add-filename-list file-name-cache filename-list)))

(defn ->sqs-write-policy [sns-topic-arn queue-arn]
  (json/write-str
   {:Id "Queue_Policy"
    :Version "2012-10-17",
    :Statement [{:Sid "Queue_SnsTopic_SendMessage"
                 :Effect "Allow"
                 :Principal {:AWS "*"},
                 :Action "SQS:SendMessage"
                 :Resource queue-arn
                 :Condition {:ArnLike {"aws:SourceArn" sns-topic-arn}}}]}))

(defn setup-notif-queue [{:keys [^SqsClient sqs-client ^SnsClient sns-client sns-topic-arn]}]
  (let [queue-name (format "xtdb-object-store-notifs-queue-%s" (UUID/randomUUID))

        _ (log/info "Creating SQS queue " queue-name)
        ^CreateQueueResponse sqs-queue (.createQueue sqs-client (-> (CreateQueueRequest/builder)
                                                                    (.queueName queue-name)
                                                                    ^CreateQueueRequest (.build)))

        queue-url (.queueUrl sqs-queue)
        ^GetQueueAttributesResponse queue-attr (.getQueueAttributes sqs-client (-> (GetQueueAttributesRequest/builder)
                                                                                   (.queueUrl queue-url)
                                                                                   (.attributeNamesWithStrings ^"[Ljava.lang.String;" (into-array String ["QueueArn"]))
                                                                                   ^GetQueueAttributesRequest (.build)))
        queue-arn (get (.attributesAsStrings queue-attr) "QueueArn")

        _ (log/info (format "Adding relevant permissions to allow SNS topic with ARN %s to write to the queue %s" sns-topic-arn queue-name))
        _ (.setQueueAttributes sqs-client (-> (SetQueueAttributesRequest/builder)
                                              (.queueUrl queue-url)
                                              (.attributesWithStrings {"Policy" (->sqs-write-policy sns-topic-arn queue-arn)})
                                              ^SetQueueAttributesRequest (.build)))

        _ (log/info (format "Subscribing SQS queue %s to SNS topic with ARN %s" queue-name sns-topic-arn))
        ^SubscribeResponse subscription (.subscribe sns-client (-> (SubscribeRequest/builder)
                                                                   (.topicArn sns-topic-arn)
                                                                   (.endpoint queue-arn)
                                                                   (.protocol "sqs")
                                                                   ^SubscribeRequest (.build)))]
    {:queue-url queue-url
     :subscription-arn (.subscriptionArn subscription)}))

(defn parse-s3-event [message-body]
  (-> message-body
      (json/read-str)
      (get "Message")
      (json/read-str :key-fn keyword)
      (:Records)
      (first)))

(defn open-file-list-watcher [{:keys [bucket sns-topic-arn prefix] :as opts} ^NavigableSet file-name-cache]
  (let [^SqsClient sqs-client (SqsClient/create)
        ^SnsClient sns-client (SnsClient/create)
        _ (log/info "Creating AWS resources for watching files on bucket " bucket)
        ;; Create queue that will subscribe to sns topic for notifications
        {:keys [queue-url subscription-arn]} (setup-notif-queue {:sqs-client sqs-client
                                                                 :sns-client sns-client
                                                                 :sns-topic-arn sns-topic-arn})

        _ (log/info "Initializing filename list from bucket " bucket)
        ;; Init the filename cache with current files
        _ (file-list-init opts file-name-cache)

        ;; Setup watcher thread for handling messages from the queue 
        watcher-thread (Thread. (fn []
                                  (try
                                    (while true
                                      (when (Thread/interrupted)
                                        (throw (InterruptedException.)))

                                      (when-let [messages (-> (.receiveMessage sqs-client (-> (ReceiveMessageRequest/builder)
                                                                                              (.queueUrl queue-url)
                                                                                              ^ReceiveMessageRequest (.build)))
                                                              (.messages)
                                                              (not-empty))]
                                        (doseq [^Message message messages]
                                          (when (Thread/interrupted)
                                            (throw (InterruptedException.)))

                                          (let [receipt-handle (.receiptHandle message)
                                                s3-event (parse-s3-event (.body message))
                                                event-name (:eventName s3-event)
                                                event-type (cond
                                                             (string/starts-with? event-name "ObjectCreated") :create
                                                             (string/starts-with? event-name "ObjectRemoved") :delete)
                                                filename (get-in s3-event [:s3 :object :key])
                                                file (if prefix
                                                       (when (string/starts-with? filename prefix)
                                                         (subs filename (count prefix)))
                                                       filename)]
                                            (log/debug (format "Message received, performing %s on file %s" event-type file))
                                            (when file
                                              (cond
                                                (= event-type :create) (file-list/add-filename file-name-cache file)
                                                (= event-type :delete) (file-list/remove-filename file-name-cache file)))

                                            (.deleteMessage sqs-client (-> (DeleteMessageRequest/builder)
                                                                           (.queueUrl queue-url)
                                                                           (.receiptHandle receipt-handle)
                                                                           ^DeleteMessageRequest (.build))))))
                                      (Thread/sleep 10))
                                    (catch InterruptedException _)
                                    (catch QueueDoesNotExistException _)
                                    (catch AbortedException _))))]

    ;; Start processing messages from the queue
    (log/info "Watching for filechanges from bucket " bucket)
    (.start watcher-thread)

    ;; Return an auto closeable object that clears up the thread, SNS topic and SQS queue
    (reify
      AutoCloseable
      (close [_]
        (log/info "Interrupting s3 file watcher thread...")
        (.interrupt watcher-thread)
        (.join watcher-thread)

        (log/info "Unsubscribing from SNS topic, subscription arn: " subscription-arn)
        (.unsubscribe sns-client (-> (UnsubscribeRequest/builder)
                                     (.subscriptionArn subscription-arn)
                                     ^UnsubscribeRequest (.build)))
        
        (log/info "Removing SQS queue, queue url: " queue-url)
        (.deleteQueue sqs-client (-> (DeleteQueueRequest/builder)
                                     (.queueUrl queue-url)
                                     ^DeleteQueueRequest (.build)))))))

