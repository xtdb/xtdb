(ns xtdb.s3.file-list
  (:require [clojure.data.json :as json]
            [clojure.string :as string])
  (:import [java.util UUID NavigableSet]
           [software.amazon.awssdk.services.sqs.model Message CreateQueueRequest DeleteMessageRequest DeleteQueueRequest ReceiveMessageRequest SetQueueAttributesRequest GetQueueAttributesRequest QueueDoesNotExistException]
           [software.amazon.awssdk.services.sns.model SubscribeRequest UnsubscribeRequest]
           [software.amazon.awssdk.services.s3.model ListObjectsV2Request ListObjectsV2Response S3Object]
           software.amazon.awssdk.core.exception.AbortedException
           software.amazon.awssdk.services.s3.S3AsyncClient
           software.amazon.awssdk.services.sns.SnsClient
           software.amazon.awssdk.services.sqs.SqsClient))

;; Fns for file name init (grabbing all current files from s3 bucket)

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
    (.addAll file-name-cache filename-list)))

;; Fns for file name watching (listening to SNS notifications from the bucket to add/remove files on the file name cache)

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

(defn setup-notif-queue [{:keys [sqs-client sns-client sns-topic-arn]}]
  (let [queue-name (format "xtdb-object-store-notifs-queue-%s" (UUID/randomUUID))
        sqs-queue (.createQueue sqs-client (-> (CreateQueueRequest/builder)
                                               (.queueName queue-name)
                                               (.build)))
        queue-url (.queueUrl sqs-queue)
        queue-arn (-> (.getQueueAttributes sqs-client (-> (GetQueueAttributesRequest/builder)
                                                          (.queueUrl queue-url)
                                                          (.attributeNamesWithStrings (into-array String ["QueueArn"]))
                                                          (.build)))
                      (.attributesAsStrings)
                      (get "QueueArn"))
        ;; Add permissions to SQS to allow SNS to send messages
        _ (.setQueueAttributes sqs-client (-> (SetQueueAttributesRequest/builder)
                                              (.queueUrl queue-url)
                                              (.attributesWithStrings {"Policy" (->sqs-write-policy sns-topic-arn queue-arn)})
                                              (.build)))
        ;; Subscribe to the SNS topic
        subscription (.subscribe sns-client (-> (SubscribeRequest/builder)
                                                (.topicArn sns-topic-arn)
                                                (.endpoint queue-arn)
                                                (.protocol "sqs")
                                                (.build)))]
    {:queue-url queue-url
     :subscription-arn (.subscriptionArn subscription)}))

(defn parse-s3-event [message-body]
  (-> message-body
      (json/read-str)
      (get "Message")
      (json/read-str :key-fn keyword)
      (:Records)
      (first)))

(defn file-list-watch [{:keys [sns-topic-arn prefix] :as opts} ^NavigableSet file-name-cache]
  (let [^SqsClient sqs-client (SqsClient/create)
        ^SnsClient sns-client (SnsClient/create)

        ;; Create queue that will subscribe to sns topic for notifications
        {:keys [queue-url subscription-arn]} (setup-notif-queue {:sqs-client sqs-client
                                                                 :sns-client sns-client
                                                                 :sns-topic-arn sns-topic-arn})

        ;; Init the filename cache with current files
        _ (file-list-init opts file-name-cache)

        ;; Start processing messages from the queue
        watcher-thread (Thread. (fn []
                                  (try
                                    (while true
                                      (when (Thread/interrupted)
                                        (throw (InterruptedException.)))

                                      (when-let [messages (-> (.receiveMessage sqs-client (-> (ReceiveMessageRequest/builder)
                                                                                              (.queueUrl queue-url)
                                                                                              (.build)))
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
                                            (when file
                                              (cond
                                                (= event-type :create) (.add file-name-cache file)
                                                (= event-type :delete) (.remove file-name-cache file)))

                                            (.deleteMessage sqs-client (-> (DeleteMessageRequest/builder)
                                                                           (.queueUrl queue-url)
                                                                           (.receiptHandle receipt-handle)
                                                                           (.build))))))
                                      (Thread/sleep 10))
                                    (catch InterruptedException _)
                                    (catch QueueDoesNotExistException _)
                                    (catch AbortedException _))))]
    (.start watcher-thread)
    
    {:watcher-thread watcher-thread
     :sns-client sns-client
     :sqs-client sqs-client
     :queue-url queue-url
     :subscription-arn subscription-arn}))

(defn watcher-close-fn [{:keys [watcher-thread sns-client sqs-client queue-url subscription-arn]}]
  (.interrupt watcher-thread)
  (.join watcher-thread)
  (.unsubscribe sns-client (-> (UnsubscribeRequest/builder)
                               (.subscriptionArn subscription-arn)
                               (.build)))
  (.deleteQueue sqs-client (-> (DeleteQueueRequest/builder)
                               (.queueUrl queue-url)
                               (.build))))

