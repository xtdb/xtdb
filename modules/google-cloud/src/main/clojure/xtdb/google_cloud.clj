(ns xtdb.google-cloud
  (:require [xtdb.buffer-pool :as bp]
            [xtdb.google-cloud.file-watch :as google-file-watch]
            [xtdb.google-cloud.object-store :as os]
            [xtdb.util :as util])
  (:import [com.google.cloud.storage StorageOptions StorageOptions$Builder]
           [java.util.concurrent ConcurrentSkipListSet]
           [xtdb.api.storage GoogleCloudStorage GoogleCloudStorage$Factory ObjectStore]))

(defmethod bp/->object-store-factory ::object-store [_ {:keys [project-id bucket pubsub-topic prefix]}]
  (cond-> (GoogleCloudStorage/googleCloudStorage project-id bucket pubsub-topic)
    prefix (.prefix (util/->path prefix))))

(defn open-object-store ^ObjectStore [^GoogleCloudStorage$Factory factory]
  (let [project-id (.getProjectId factory)
        bucket (.getBucket factory)
        pubsub-topic (.getPubsubTopic factory)
        prefix (.getPrefix factory)
        storage-service (-> (StorageOptions/newBuilder)
                            ^StorageOptions$Builder (.setProjectId project-id)
                            ^StorageOptions (.build)
                            (.getService))
        file-name-cache (ConcurrentSkipListSet.)
        ;; Watch cloud storage bucket for changes
        file-list-watcher (google-file-watch/open-file-list-watcher {:project-id project-id
                                                                     :bucket bucket
                                                                     :pubsub-topic pubsub-topic
                                                                     :prefix prefix
                                                                     :storage-service storage-service} 
                                                                    file-name-cache)]
    (os/->GoogleCloudStorageObjectStore storage-service bucket prefix file-name-cache file-list-watcher)))
