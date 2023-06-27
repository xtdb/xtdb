(ns xtdb.s3
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as string]
            [xtdb.object-store :as os]
            [xtdb.util :as util]
            [juxt.clojars-mirrors.integrant.core :as ig])
  (:import xtdb.object_store.ObjectStore
           xtdb.s3.S3Configurator
           java.io.Closeable
           java.util.concurrent.CompletableFuture
           java.util.function.Function
           (java.nio ByteBuffer)
           [software.amazon.awssdk.core ResponseBytes]
           [software.amazon.awssdk.core.async AsyncRequestBody AsyncResponseTransformer]
           [software.amazon.awssdk.services.s3.model DeleteObjectRequest GetObjectRequest HeadObjectRequest ListObjectsV2Request ListObjectsV2Response NoSuchKeyException PutObjectRequest S3Object]
           software.amazon.awssdk.services.s3.S3AsyncClient))

(defn- get-obj-req
  ^GetObjectRequest [{:keys [^S3Configurator configurator bucket prefix]} k]
  (-> (GetObjectRequest/builder)
      (.bucket bucket)
      (.key (str prefix k))
      (->> (.configureGet configurator))
      ^GetObjectRequest (.build)))

(defn- get-obj-range-req
  ^GetObjectRequest [{:keys [^S3Configurator configurator bucket prefix]} k ^Long start ^long len]
  (let [end-byte (+ start (dec len))]
    (-> (GetObjectRequest/builder)
        (.bucket bucket)
        (.key (str prefix k))
        (.range (format "bytes=%d-%d" start end-byte))
        (->> (.configureGet configurator))
        ^GetObjectRequest (.build))))

(defn- with-exception-handler [^CompletableFuture fut k]
  (.exceptionally fut (reify Function
                        (apply [_ e]
                          (try
                            (throw (.getCause ^Exception e))
                            (catch NoSuchKeyException _
                              (throw (os/obj-missing-exception k))))))))

(defrecord S3ObjectStore [^S3Configurator configurator ^S3AsyncClient client bucket prefix]
  ObjectStore
  (getObject [this k]
    (-> (.getObject client (get-obj-req this k) (AsyncResponseTransformer/toBytes))
        (.thenApply (reify Function
                      (apply [_ bs]
                        (.asByteBuffer ^ResponseBytes bs))))
        (with-exception-handler k)))

  (getObject [this k out-path]
    (-> (.getObject client (get-obj-req this k) out-path)
        (.thenApply (reify Function
                      (apply [_ _]
                        out-path)))
        (with-exception-handler k)))

  (getObjectRange [this k start len]
    (os/ensure-shared-range-oob-behaviour start len)
    (try
      (-> (.getObject client ^GetObjectRequest (get-obj-range-req this k start len) (AsyncResponseTransformer/toBytes))
          (.thenApply (reify Function
                        (apply [_ bs]
                          (.asByteBuffer ^ResponseBytes bs))))
          (with-exception-handler k))
      (catch IndexOutOfBoundsException e
        (CompletableFuture/failedFuture e))))

  (putObject [_ k buf]
    (-> (.headObject client
                     (-> (HeadObjectRequest/builder)
                         (.bucket bucket)
                         (.key (str prefix k))
                         (->> (.configureHead configurator))
                         ^HeadObjectRequest (.build)))
        (util/then-apply (fn [_resp] true))
        (.exceptionally (reify Function
                          (apply [_ e]
                            (let [e (.getCause ^Exception e)]
                              (if (instance? NoSuchKeyException e)
                                false
                                (throw e))))))
        (util/then-compose (fn [exists?]
                             (if exists?
                               (CompletableFuture/completedFuture nil)
                               (.putObject client
                                           (-> (PutObjectRequest/builder)
                                               (.bucket bucket)
                                               (.key (str prefix k))
                                               (->> (.configurePut configurator))
                                               ^PutObjectRequest (.build))
                                           (AsyncRequestBody/fromByteBuffer buf)))))))

  (listObjects [this] (.listObjects this nil))

  (listObjects [_ dir]
    (letfn [(list-objects* [continuation-token]
              (lazy-seq
               (let [^ListObjectsV2Request
                     req (-> (ListObjectsV2Request/builder)
                             (.bucket bucket)
                             (.prefix (str prefix dir "/"))
                             (cond-> continuation-token (.continuationToken continuation-token))
                             (.build))

                     ^ListObjectsV2Response
                     resp (.get (.listObjectsV2 client req))]

                 (concat (for [^S3Object object (.contents resp)]
                           (subs (.key object) (count prefix)))
                         (when (.isTruncated resp)
                           (list-objects* (.nextContinuationToken resp)))))))]
      (list-objects* nil)))

  (deleteObject [_ k]
    (.deleteObject client
                   (-> (DeleteObjectRequest/builder)
                       (.bucket bucket)
                       (.key (str prefix k))
                       ^DeleteObjectRequest (.build))))

  Closeable
  (close [_]
    (.close client)))

(defn- parse-prefix [prefix]
  (cond
    (string/blank? prefix) ""
    (string/ends-with? prefix "/") prefix
    :else (str prefix "/")))

(s/def ::configurator #(instance? S3Configurator %))
(s/def ::bucket string?)
(s/def ::prefix string?)

(defmethod ig/prep-key ::object-store [_ opts]
  (-> (merge {:configurator (reify S3Configurator)}
             opts)
      (util/maybe-update :prefix parse-prefix)))

(defmethod ig/pre-init-spec ::object-store [_]
  (s/keys :req-un [::configurator ::bucket]
          :opt-un [::prefix]))

(defmethod ig/init-key ::object-store [_ {:keys [bucket prefix ^S3Configurator configurator]}]
  (->S3ObjectStore configurator
                   (.makeClient configurator)
                   bucket prefix))

(defmethod ig/halt-key! ::object-store [_ os]
  (util/try-close os))

(comment

  (def os
    (->> (ig/prep-key ::object-store {:bucket "xtdb-s3-test", :prefix "wot"})
         (ig/init-key ::object-store)))

  (.close os)

  (get-obj-req os "foo.txt")

  (get-obj-range-req os "foo.txt" 10 40)

  @(.getObject os "foo.txt")

  (String. (let [buf @(.getObjectRange os "foo.txt" 2 5)
                 a (byte-array (.remaining buf))]
             (.get buf a)
             a))

  @(.putObject os "foo.txt" (ByteBuffer/wrap (.getBytes "hello, world")))

  )
