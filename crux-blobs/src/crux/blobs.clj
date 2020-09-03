(ns crux.blobs
  (:require [clojure.edn :as edn]
            [clojure.spec.alpha :as s]
            [crux.db :as db]
            [crux.node :as n]
            [crux.document-store :as ds]
            [crux.lru :as lru]
            [clj-http.client :as http]))

(s/def ::storage-account string?)
(s/def ::container string?)

(defn get-blob [sas-token storage-account container blob-name]
  ;; TODO : ETag
  (-> (format "https://%s.blob.core.windows.net/%s/%s?%s" storage-account container blob-name sas-token)
      http/get
      :body
      edn/read-string))

(defn put-blob [sas-token storage-account container blob-name blob-bytes]
  ;; TODO ETag
  (-> (format "https://%s.blob.core.windows.net/%s/%s?%s" storage-account container blob-name sas-token)
      (http/put {:headers {"x-ms-blob-type" "BlockBlob"}
                 :body blob-bytes})))

(defn delete-blob [sas-token storage-account container blob-name]
  (-> (format "https://%s.blob.core.windows.net/%s/%s?%s" storage-account container blob-name sas-token)
      (http/delete {:headers {"x-ms-delete-snapshots" "include"}})))

(defrecord BlobsDocumentStore [sas-token storage-account container]
  db/DocumentStore

  (submit-docs [_ docs]
    (->> (for [[id doc] docs]
           (future (put-blob sas-token storage-account container
                             (str id)
                             (.getBytes (str doc)))))
         vec
         (run! deref)))

  (fetch-docs [_ docs]
    (println 'fetch-docs (count docs))
    (reduce
     #(assoc %1 %2 (get-blob sas-token storage-account container (str %2)))
     {}
     docs)))

(def blobs-doc-store
  {::configurator {:start-fn (fn [_ _]
                               {::sas-token (System/getenv "CRUX_BLOBS_SAS_TOKEN")})}
   
   ::n/document-store {:start-fn (fn [{{::keys [sas-token]} ::configurator} {:crux.document-store/keys [doc-cache-size]
                                                                             ::keys [storage-account container]}]
                                   (ds/->CachedDocumentStore (lru/new-cache doc-cache-size)
                                                             (->BlobsDocumentStore sas-token storage-account container)))
                       :args {::storage-account {:require? true
                                                 :crux.config/type ::storage-account
                                                 :doc "Azure Storage Account Name"}
                              ::container {:required? true,
                                           :crux.config/type ::container
                                           :doc "Azure Blob Storage Container"}
                              :crux.document-store/doc-cache-size ds/doc-cache-size-opt}
                       :deps #{::configurator}}})

