(ns xtdb.google-cloud
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as string]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.google-cloud.object-store :as os]
            [xtdb.util :as util])
  (:import [com.google.cloud.storage StorageOptions StorageOptions$Builder]))

(derive ::blob-object-store :xtdb/object-store)

(defn- parse-prefix [prefix]
  (cond
    (string/blank? prefix) ""
    (string/ends-with? prefix "/") prefix
    :else (str prefix "/")))

(s/def ::project-id string?)
(s/def ::bucket string?)
(s/def ::prefix string?)

(defmethod ig/prep-key ::blob-object-store [_ opts]
  (-> opts
      (util/maybe-update :prefix parse-prefix)))

(defmethod ig/pre-init-spec ::blob-object-store [_]
  (s/keys :req-un [::project-id ::bucket]
          :opt-un [::prefix]))

(defmethod ig/init-key ::blob-object-store [_ {:keys [project-id bucket prefix]}]
  (let [storage-service (-> (StorageOptions/newBuilder)
                            ^StorageOptions$Builder (.setProjectId project-id)
                            ^StorageOptions (.build)
                            (.getService))]
    (os/->GoogleCloudStorageObjectStore storage-service bucket prefix)))
