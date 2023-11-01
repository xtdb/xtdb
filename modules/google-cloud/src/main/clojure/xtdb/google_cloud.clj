(ns xtdb.google-cloud
  (:require [clojure.spec.alpha :as s] 
            [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.google-cloud.file-watch :as google-file-watch]
            [xtdb.google-cloud.object-store :as os]
            [xtdb.util :as util])
  (:import [com.google.cloud.storage StorageOptions StorageOptions$Builder]
           [java.nio.file Path]
           [java.util.concurrent ConcurrentSkipListSet]))

(derive ::blob-object-store :xtdb/object-store)

(s/def ::project-id string?)
(s/def ::pubsub-topic string?)
(s/def ::bucket string?)
(s/def ::prefix ::util/path)

(defmethod ig/prep-key ::blob-object-store [_ opts]
  (-> opts
      (util/maybe-update :prefix util/->path)))

(defmethod ig/pre-init-spec ::blob-object-store [_]
  (s/keys :req-un [::project-id ::bucket ::pubsub-topic]
          :opt-un [::prefix]))

(defmethod ig/init-key ::blob-object-store [_ {:keys [project-id bucket ^Path prefix] :as opts}]
  (let [storage-service (-> (StorageOptions/newBuilder)
                            ^StorageOptions$Builder (.setProjectId project-id)
                            ^StorageOptions (.build)
                            (.getService))
        file-name-cache (ConcurrentSkipListSet.)
        ;; Watch cloud storage bucket for changes
        file-list-watcher (google-file-watch/open-file-list-watcher (assoc opts :storage-service storage-service) file-name-cache)]
    (os/->GoogleCloudStorageObjectStore storage-service bucket prefix file-name-cache file-list-watcher)))
