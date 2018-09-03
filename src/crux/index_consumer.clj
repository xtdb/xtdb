(ns crux.index-consumer
  (:require [clojure.tools.logging :as log]
            [crux.kafka :as k])
  (:import java.io.Closeable))

(defrecord IndexerConsumer [running? ^Thread worker-thread]
  Closeable
  (close [_]
    (reset! running? false)))

(defn thread-main-loop
  [{:keys [running? indexer consumer follower options]}]
  (with-open [consumer
              (k/create-consumer
                {"bootstrap.servers" (:bootstrap-servers options)
                 "group.id" (:group-id options)})]
    (k/subscribe-from-stored-offsets
      indexer consumer [(:tx-topic options) (:doc-topic options)])
    (while @running?
      (try
        (k/consume-and-index-entities
          {:indexer indexer
           :consumer consumer
           :follower follower
           :timeout 100
           :tx-topic (:tx-topic options)
           :doc-topic (:doc-topic options)})
        (catch Exception e
          (log/error e "Error while consuming and indexing from Kafka:")
          (Thread/sleep 500))))))

(defn ^Closeable create-index-consumer
  [admin-client follower indexer
   {:keys [tx-topic
           bootstrap-servers
           group-id
           replication-factor
           doc-partitions
           doc-topic] :as options}]
  (let [replication-factor (Long/parseLong replication-factor)]
    (k/create-topic admin-client tx-topic 1 replication-factor k/tx-topic-config)
    (k/create-topic admin-client doc-topic
                                (Long/parseLong doc-partitions)
                                replication-factor k/doc-topic-config)
    (let [index-follower (map->IndexerConsumer {:running? (atom true)
                                                :indexer indexer
                                                :follower follower
                                                :options options})]
      (assoc
        index-follower
        :worker-thread
        (doto (Thread. ^Runnable (partial thread-main-loop index-follower))
          (.start))))))
