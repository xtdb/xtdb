(ns crux.s3
  (:require [crux.db :as db]
            [crux.node :as n]
            [clojure.spec.alpha :as s]
            [taoensso.nippy :as nippy]
            [clojure.string :as string]
            [clojure.tools.logging :as log])
  (:import (crux.s3 S3Configurator)
           (java.util.concurrent CompletableFuture)
           (java.util.function BiFunction)
           (software.amazon.awssdk.core ResponseBytes)
           (software.amazon.awssdk.core.async AsyncRequestBody AsyncResponseTransformer)
           (software.amazon.awssdk.services.s3 S3AsyncClient)
           (software.amazon.awssdk.services.s3.model GetObjectRequest PutObjectRequest)))

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
                                   (->> (.configureGet configurator))
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
  {::configurator {:start-fn (fn [deps args]
                               (reify S3Configurator))}

   ::n/document-store {:start-fn (fn [{::keys [^S3Configurator configurator]} {::keys [bucket prefix]}]
                                   (->S3DocumentStore configurator
                                                      (.makeClient configurator)
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
                                        :doc "S3 prefix"}}
                       :deps #{::configurator}}})
