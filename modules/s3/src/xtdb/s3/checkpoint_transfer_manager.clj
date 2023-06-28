(ns xtdb.s3.checkpoint-transfer-manager
  (:require [xtdb.s3 :as s3]
            [xtdb.api :as xt]
            [xtdb.checkpoint :as cp]
            [clojure.string :as string]
            [xtdb.io :as xio]
            [clojure.edn :as edn]
            [xtdb.system :as sys])
  (:import (xtdb.s3 S3Configurator)
           (java.io Closeable File)
           (java.nio.file Paths)
           (software.amazon.awssdk.transfer.s3 S3TransferManager)
           (software.amazon.awssdk.transfer.s3.model
            UploadDirectoryRequest DownloadDirectoryRequest
            DirectoryDownload DirectoryUpload
            CompletedDirectoryDownload CompletedDirectoryUpload)
           (software.amazon.awssdk.core ResponseBytes)
           (software.amazon.awssdk.core.async AsyncRequestBody AsyncResponseTransformer)
           (software.amazon.awssdk.services.s3 S3AsyncClient)
           (software.amazon.awssdk.services.s3.model ListObjectsV2Request$Builder)))

(defn as-consumer [f]
  (reify java.util.function.Consumer
    (accept [this arg]
       (f arg))))

(defrecord CheckpointStore [^S3Configurator configurator  ^S3AsyncClient client bucket prefix]
  cp/CheckpointStore
  (available-checkpoints [this {::cp/keys [cp-format]}]
    (->> (s3/list-objects this {})
         (keep (fn [[type arg]]
                 (when (= type :object)
                   arg)))
         (sort-by (fn [path]
                    (let [[_ tx-id checkpoint-at] (re-matches #"checkpoint-(\d+)-(.+).edn" path)]
                      [(Long/parseLong tx-id) checkpoint-at]))
                  #(compare %2 %1))
         (keep (fn [cp-metadata-path]
                 (let [resp (some-> (s3/get-objects this {cp-metadata-path (AsyncResponseTransformer/toBytes)})
                                    ^ResponseBytes (get cp-metadata-path)
                                    (.asUtf8String)
                                    (edn/read-string))]
                   (when (= (::cp/cp-format resp) cp-format)
                     resp))))))

  (download-checkpoint [{:keys [bucket ^S3AsyncClient client]}
                        {::keys [s3-dir] :as checkpoint} dir]

    (when-not (empty? (.listFiles ^File dir))
      (throw (IllegalArgumentException. (str "non-empty checkpoint restore dir: " dir))))

    (with-open [^S3TransferManager transfer-manager
                (-> (S3TransferManager/builder)
                    (.s3Client client)
                    (.build))]

      (let [^DownloadDirectoryRequest dir-download-request
            (-> (DownloadDirectoryRequest/builder)
                (.destination (Paths/get (.getPath ^File dir) (make-array String 0)))
                (.bucket bucket)
                (.listObjectsV2RequestTransformer
                 (as-consumer (fn [^ListObjectsV2Request$Builder x]
                                (.prefix x (str prefix s3-dir)))))
                (.build))
            ^DirectoryDownload dir-download-results
            (.downloadDirectory transfer-manager dir-download-request)
            ^CompletedDirectoryDownload completed-dir-download
            (-> dir-download-results
                (.completionFuture)
                (.join))
            failed (.failedTransfers completed-dir-download)]
        (when (seq failed)
          (throw (ex-info "incomplete checkpoint restore" {:failed-transfers (map str failed)})))))

    checkpoint)

  (upload-checkpoint [{:keys [bucket prefix ^S3AsyncClient client] :as this}
                      dir {:keys [tx ::cp/cp-format]}]

    (let [dir-path (.toPath ^File dir)
          cp-at (java.util.Date.)
          s3-dir (format "checkpoint-%s-%s" (::xt/tx-id tx) (xio/format-rfc3339-date cp-at))]

      (with-open [^S3TransferManager transfer-manager
                  (-> (S3TransferManager/builder)
                      (.s3Client client)
                      (.build))]
        (let [^UploadDirectoryRequest dir-upload-request
              (-> (UploadDirectoryRequest/builder)
                  (.source dir-path)
                  (.bucket bucket)
                  (.s3Prefix (str prefix s3-dir))
                  (.build))
              ^DirectoryUpload dir-upload-results
              (.uploadDirectory transfer-manager dir-upload-request)
              ^CompletedDirectoryUpload completed-dir-upload
              (-> dir-upload-results
                  (.completionFuture)
                  (.join))
              failed (.failedTransfers completed-dir-upload)]
          (when (seq failed)
            (throw (ex-info "failed checkpoint upload" {:failed-transfers (map str failed)})))))

      (let [cp {::cp/cp-format cp-format,
                :tx tx
                ::s3-dir (str s3-dir "/")
                ::cp/checkpoint-at cp-at}]
        (s3/put-objects this
                        {(str s3-dir ".edn")
                         (AsyncRequestBody/fromString (pr-str {::cp/cp-format cp-format,
                                                               :tx tx
                                                               ::s3-dir (str s3-dir "/")
                                                               ::cp/checkpoint-at cp-at}))})
        cp)))
  
  (cleanup-checkpoint [this {:keys [tx cp-at]}]
    (let [s3-dir (format "checkpoint-%s-%s" (::xt/tx-id tx) (xio/format-rfc3339-date cp-at))]

      ;; Delete EDN file first
      (s3/delete-objects this [(str s3-dir ".edn")])

      ;; List & delete directory contents
      (let [files (as-> (s3/list-objects this {:path s3-dir :recursive? true}) $
                      (keep (fn [[type arg]]
                              (when (= type :object)
                                arg)) $)
                      (vec $)
                      (conj $ s3-dir))]
          (s3/delete-objects this files))))

  Closeable
  (close [_]
    (.close client)))

(defn ->cp-store {::sys/deps {:configurator (fn [_] (reify S3Configurator))}
                  ::sys/args {:bucket {:required? true,
                                       :spec ::s3/bucket
                                       :doc "S3 bucket"}
                              :prefix {:required? false,
                                       :spec ::s3/prefix
                                       :doc "S3 prefix"}}}
  [{:keys [^S3Configurator configurator bucket prefix]}]
  (->CheckpointStore configurator
                     (.makeClient configurator)
                     bucket
                     (cond
                       (string/blank? prefix) ""
                       (string/ends-with? prefix "/") prefix
                       :else (str prefix "/"))))
