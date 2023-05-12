(ns xtdb.azure
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as string]
            [xtdb.util :as util]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.azure
             [object-store :as os]])
  (:import  [com.azure.storage.blob BlobServiceClientBuilder]
            [com.azure.identity DefaultAzureCredentialBuilder]))

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
