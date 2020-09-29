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
           (clojure.lang MapEntry)
           (java.io Closeable)
           (java.util.concurrent CompletableFuture)
           (java.util.function BiFunction)
           (software.amazon.awssdk.core ResponseBytes)
           (software.amazon.awssdk.core.async AsyncRequestBody AsyncResponseTransformer)
           (software.amazon.awssdk.services.s3 S3AsyncClient)
           (software.amazon.awssdk.services.s3.model GetObjectRequest PutObjectRequest
                                                     ListObjectsV2Request ListObjectsV2Response
                                                     CommonPrefix S3Object
                                                     NoSuchKeyException)))

(defn ^:no-doc put-objects [{:keys [^S3Configurator configurator ^S3AsyncClient client bucket prefix]} objs]
  (->> (for [[path ^AsyncRequestBody request-body] objs]
         (.putObject client
                     (-> (PutObjectRequest/builder)
                         (.bucket bucket)
                         (.key (str prefix path))
                         (->> (.configurePut configurator))
                         ^PutObjectRequest (.build))
                     request-body))
       vec
       (run! (fn [^CompletableFuture req]
               (.get req)))))

(defn ^:no-doc get-objects [{:keys [^S3Configurator configurator ^S3AsyncClient client bucket prefix]} reqs]
  (->> (for [[path ^AsyncResponseTransformer response-transformer] reqs]
         (let [s3-key (str prefix path)]
           [path (-> (.getObject client
                                 (-> (GetObjectRequest/builder)
                                     (.bucket bucket)
                                     (.key s3-key)
                                     (->> (.configureGet configurator))
                                     ^GetObjectRequest (.build))
                                 response-transformer)

                     (.handle (reify BiFunction
                                (apply [_ resp e]
                                  (if e
                                    (try
                                      (throw (.getCause ^Throwable e))
                                      (catch NoSuchKeyException e
                                        (log/warn "S3 key not found: " s3-key))
                                      (catch Exception e
                                        (log/warnf e "Error fetching S3 object: s3://%s/%s" bucket (str prefix path))))

                                    resp)))))]))

         (into {})
         (into {} (keep (fn [[path ^CompletableFuture fut]]
                          (when-let [resp (.get fut)]
                            [path resp]))))))

(defn ^:no-doc list-objects [{:keys [^S3Configurator configurator ^S3AsyncClient client bucket prefix]}
                             {:keys [path recursive?]}]
  (letfn [(list-objects* [continuation-token]
            (lazy-seq
             (let [^ListObjectsV2Request
                   req (-> (ListObjectsV2Request/builder)
                           (.bucket bucket)
                           (.prefix (str prefix path))
                           (cond-> (not recursive?) (.delimiter "/"))
                           (cond-> continuation-token (.continuationToken continuation-token))
                           (.build))

                   ^ListObjectsV2Response
                   resp (.get (.listObjectsV2 client req))]

               (concat (for [^S3Object object (.contents resp)]
                         [:object (subs (.key object) (count prefix))])
                       (for [^CommonPrefix common-prefix (.commonPrefixes resp)]
                         [:common-prefix (subs (.prefix common-prefix) (count prefix))])
                       (when (.isTruncated resp)
                         (list-objects* (.nextContinuationToken resp)))))))]
    (list-objects* nil)))

(defrecord S3DocumentStore [^S3Configurator configurator ^S3AsyncClient client bucket prefix]
  db/DocumentStore
  (submit-docs [this docs]
    (put-objects this (for [[id doc] docs]
                        (MapEntry/create id (AsyncRequestBody/fromBytes (.freeze configurator doc))))))

  (fetch-docs [this ids]
    (->> (get-objects this (for [id ids]
                             (MapEntry/create id (AsyncResponseTransformer/toBytes))))

         (into {} (map (fn [[id ^ResponseBytes resp]]
                         [id (-> (.asByteArray ^ResponseBytes resp)
                                 (->> (.thaw configurator)))])))))

  Closeable
  (close [_]
    (.close client)))

(s/def ::bucket string?)
(s/def ::prefix (s/and string?
                       (s/conformer (fn [prefix]
                                      (cond
                                        (string/blank? prefix) ""
                                        (string/ends-with? prefix "/") prefix
                                        :else (str prefix "/"))))))

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
                                               prefix)))
