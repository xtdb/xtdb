(ns xtdb.google-cloud
  (:require [xtdb.buffer-pool :as bp]
            [xtdb.google-cloud.object-store :as os]
            [xtdb.util :as util])
  (:import [com.google.cloud.storage StorageOptions StorageOptions$Builder]
           [xtdb.api.storage GoogleCloudStorage GoogleCloudStorage$Factory ObjectStore Storage]))

(defmethod bp/->object-store-factory ::object-store [_ {:keys [project-id bucket prefix]}]
  (cond-> (GoogleCloudStorage/googleCloudStorage project-id bucket)
    prefix (.prefix (util/->path prefix))))

(defn open-object-store ^ObjectStore [^GoogleCloudStorage$Factory factory]
  (let [project-id (.getProjectId factory)
        bucket (.getBucket factory)
        prefix (.getPrefix factory)
        prefix-with-version (if prefix (.resolve prefix Storage/storageRoot) Storage/storageRoot)
        storage-service (-> (StorageOptions/newBuilder)
                            ^StorageOptions$Builder (.setProjectId project-id)
                            ^StorageOptions (.build)
                            (.getService))]
    (os/->GoogleCloudStorageObjectStore storage-service bucket prefix-with-version)))
