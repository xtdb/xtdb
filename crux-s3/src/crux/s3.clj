(ns crux.s3
  (:require [crux.db :as db]
            [crux.document-store :as ds]
            [crux.lru :as lru]
            [crux.node :as n]
            [clojure.spec.alpha :as s]
            [taoensso.nippy :as nippy]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [crux.system :as sys])
  (:import (crux.s3 S3Configurator)
           (java.util.concurrent CompletableFuture)
           (java.util.function BiFunction)
           (software.amazon.awssdk.core ResponseBytes)
           (software.amazon.awssdk.core.async AsyncRequestBody AsyncResponseTransformer)
           (software.amazon.awssdk.services.s3 S3AsyncClient)
           (software.amazon.awssdk.services.s3.model GetObjectRequest PutObjectRequest NoSuchKeyException)))

(defrecord S3DocumentStore [^S3Configurator configurator ^S3AsyncClient client bucket prefix]
  db/DocumentStore
  (submit-docs [_ docs]
    (->> (for [[id doc] docs]
           (.putObject client
                       (-> (PutObjectRequest/builder)
                           (.bucket bucket)
                           (.key (str prefix id))
                           (->> (.configurePut configurator))
                           ^PutObjectRequest (.build))
                       (AsyncRequestBody/fromBytes (.freeze configurator doc))))
         vec
         (run! (fn [^CompletableFuture req]
                 (.get req)))))

  (fetch-docs [_ ids]
    (->> (for [id ids]
           (let [s3-key (str prefix id)]
             [id (-> (.getObject client
                                 (-> (GetObjectRequest/builder)
                                     (.bucket bucket)
                                     (.key s3-key)
                                     (->> (.configureGet configurator))
                                     ^GetObjectRequest (.build))
                                 (AsyncResponseTransformer/toBytes))

                     (.handle (reify BiFunction
                                (apply [_ resp e]
                                  (if e
                                    (try
                                      (throw (.getCause ^Throwable e))
                                      (catch NoSuchKeyException e
                                        (log/warn "S3 key not found: " s3-key))
                                      (catch Exception e
                                        (log/warnf e "Error fetching S3 object: s3://%s/%s" bucket (str prefix id))))

                                    (-> (.asByteArray ^ResponseBytes resp)
                                        (->> (.thaw configurator))))))))]))

         (into {})
         (into {} (keep (fn [[id ^CompletableFuture resp]]
                          (when-let [doc (.get resp)]
                            [id doc])))))))

(s/def ::bucket string?)
(s/def ::prefix string?)

(defn ->configurator [_]
  (reify S3Configurator))

(defn ->document-store {::sys/args {:bucket {:required? true,
                                             :spec ::bucket
                                             :doc "S3 bucket"}
                                    :prefix {:required? false,
                                             :spec ::prefix
                                             :doc "S3 prefix"}
                                    :doc-cache-size ds/doc-cache-size-opt}
                        ::sys/deps {:configurator `->configurator}}

  [{:keys [bucket prefix ^S3Configurator configurator doc-cache-size]}]
  (ds/->CachedDocumentStore (lru/new-cache doc-cache-size)
                            (->S3DocumentStore configurator
                                               (.makeClient configurator)
                                               bucket
                                               (cond
                                                 (string/blank? prefix) ""
                                                 (string/ends-with? prefix "/") prefix
                                                 :else (str prefix "/")))))
