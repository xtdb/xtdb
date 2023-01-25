(ns xtdb.s3.checkpoint
  (:require [xtdb.s3 :as s3]
            [xtdb.api :as xt]
            [xtdb.checkpoint :as cp]
            [clojure.string :as string]
            [clojure.java.io :as io]
            [xtdb.io :as xio]
            [clojure.edn :as edn]
            [xtdb.system :as sys])
  (:import (xtdb.s3 S3Configurator)
           (clojure.lang MapEntry)
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

(defmacro as-consumer [f]
  `(reify java.util.function.Consumer
     (accept [this arg#]
       (~f arg#))))

(defrecord CheckpointStore [^S3Configurator configurator
                            ^S3AsyncClient client
                            transfer-manager? bucket prefix]
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

  (download-checkpoint [{:keys [bucket transfer-manager? ^S3AsyncClient client] :as this}
                        {::keys [s3-dir] :as checkpoint} dir]

    (when-not (empty? (.listFiles ^File dir))
      (throw (IllegalArgumentException. "non-empty checkpoint restore dir: " dir)))

    (if transfer-manager?
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
                                  (.prefix x s3-dir))))
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

      (let [s3-paths (->> (s3/list-objects this {:path s3-dir, :recursive? true})
                          (map second))
            get-objs-resp (s3/get-objects this
                                          (for [s3-path s3-paths]
                                            (let [file (io/file dir (str (.relativize (Paths/get s3-dir (make-array String 0))
                                                                                      (Paths/get s3-path (make-array String 0)))))]
                                              (MapEntry/create s3-path
                                                               (AsyncResponseTransformer/toFile (doto file (io/make-parents)))))))]

        (when-not (= (set (keys get-objs-resp)) (set s3-paths))
          (throw (ex-info "incomplete checkpoint restore" {:expected s3-paths
                                                           :actual (keys get-objs-resp)})))))

    checkpoint)

  (upload-checkpoint [{:keys [bucket prefix ^S3AsyncClient client transfer-manager?] :as this}
                      dir {:keys [tx ::cp/cp-format]}]

    (let [dir-path (.toPath ^File dir)
          cp-at (java.util.Date.)
          s3-dir (format "checkpoint-%s-%s" (::xt/tx-id tx) (xio/format-rfc3339-date cp-at))]

      (if transfer-manager?
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

        (->> (file-seq dir)
             (into {} (keep (fn [^File file]
                              (when (.isFile file)
                                (MapEntry/create (str s3-dir "/" (.relativize dir-path (.toPath file)))
                                                 (AsyncRequestBody/fromFile file))))))
             (s3/put-objects this)))

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

  Closeable
  (close [_]
    (.close client)))

(defn ->cp-store {::sys/deps {:configurator (fn [_] (reify S3Configurator))}
                  ::sys/args {:bucket {:required? true,
                                       :spec ::s3/bucket
                                       :doc "S3 bucket"}
                              :prefix {:required? false,
                                       :spec ::s3/prefix
                                       :doc "S3 prefix"}
                              :transfer-manager? {:required? false,
                                                  :spec boolean?
                                                  :doc "S3 Transfer Manager"}}}
  [{:keys [^S3Configurator configurator bucket prefix transfer-manager?]}]
  (->CheckpointStore configurator
                     (.makeClient configurator)
                     transfer-manager?
                     bucket
                     (cond
                       (string/blank? prefix) ""
                       (string/ends-with? prefix "/") prefix
                       :else (str prefix "/"))))
