(ns crux.s3
  (:require [crux.db :as db]
            [crux.node :as n]
            [clojure.spec.alpha :as s]
            [taoensso.nippy :as nippy]
            [clojure.string :as string]
            [clojure.tools.logging :as log])
  (:import (java.util.concurrent CompletableFuture)
           (java.util.function BiFunction)
           (software.amazon.awssdk.core ResponseBytes)
           (software.amazon.awssdk.core.async AsyncRequestBody AsyncResponseTransformer)
           (software.amazon.awssdk.services.s3 S3AsyncClient)
           (software.amazon.awssdk.services.s3.model GetObjectRequest PutObjectRequest)))

(defrecord S3DocumentStore [^S3AsyncClient client bucket prefix]
  db/DocumentStore
  (submit-docs [_ docs]
    (->> (for [[id doc] docs]
           (.putObject client
                       (-> (PutObjectRequest/builder)
                           (.bucket bucket)
                           (.key (str prefix id))
                           ^PutObjectRequest (.build))
                       (AsyncRequestBody/fromBytes (nippy/fast-freeze doc))))
         vec
         (run! (fn [^CompletableFuture req]
                 (.get req)))))

  (fetch-docs [_ ids]
    (->> (for [id ids]
           [id (-> (.getObject client
                               (-> (GetObjectRequest/builder)
                                   (.bucket bucket)
                                   (.key (str prefix id))
                                   ^GetObjectRequest (.build))
                               (AsyncResponseTransformer/toBytes))

                   (.handle (reify BiFunction
                              (apply [_ resp e]
                                (if-not resp
                                  (log/warnf e "Error fetching S3 object: s3://%s/%s" bucket (str prefix id))
                                  (-> (.asByteArray ^ResponseBytes resp)
                                      (nippy/fast-thaw)))))))])

         (into {})
         (into {} (keep (fn [[id ^CompletableFuture resp]]
                          (when-let [doc (.get resp)]
                            [id doc])))))))

(s/def ::bucket string?)
(s/def ::prefix string?)

(def s3-doc-store
  {::n/document-store {:start-fn (fn [deps {::keys [bucket prefix]}]
                                   (->S3DocumentStore (S3AsyncClient/create)
                                                      bucket
                                                      (cond
                                                        (string/blank? prefix) ""
                                                        (string/ends-with? prefix "/") prefix
                                                        :else (str prefix "/"))))
                       :args {::bucket {:required? true,
                                        :crux.config/type ::bucket
                                        :doc "S3 bucket"}
                              ::prefix {:required? false,
                                        :crux.config/type ::prefix
                                        :doc "S3 prefix"}}}})
