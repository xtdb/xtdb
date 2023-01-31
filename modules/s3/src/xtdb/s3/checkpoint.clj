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
           (software.amazon.awssdk.core ResponseBytes)
           (software.amazon.awssdk.core.async AsyncRequestBody AsyncResponseTransformer)
           (software.amazon.awssdk.services.s3 S3AsyncClient)))

(defrecord CheckpointStore [^S3Configurator configurator ^S3AsyncClient client bucket prefix]
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

  (download-checkpoint [this {::keys [s3-dir] :as checkpoint} dir]
    (when-not (empty? (.listFiles ^File dir))
      (throw (IllegalArgumentException. "non-empty checkpoint restore dir: " dir)))

    (let [success (atom 0)]
      (try
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
                                                             :actual (keys get-objs-resp)})))
          (swap! success inc)
          checkpoint)
        (finally
          (when-not (pos? @success)
            (xio/delete-dir dir))))))

  (upload-checkpoint [this dir {:keys [tx cp-at ::cp/cp-format]}]
    (let [dir-path (.toPath ^File dir)
          s3-dir (format "checkpoint-%s-%s" (::xt/tx-id tx) (xio/format-rfc3339-date cp-at))]
      (->> (file-seq dir)
           (into {} (keep (fn [^File file]
                            (when (.isFile file)
                              (MapEntry/create (str s3-dir "/" (.relativize dir-path (.toPath file)))
                                               (AsyncRequestBody/fromFile file))))))
           (s3/put-objects this))

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

  (cleanup-checkpoint [this  {:keys [tx cp-at]}]
    (let [s3-dir (format "checkpoint-%s-%s" (::xt/tx-id tx) (xio/format-rfc3339-date cp-at))
          files (as-> (s3/list-objects this {:path s3-dir :recursive? true}) $
                  (keep (fn [[type arg]]
                          (when (= type :object)
                            arg)) $)
                  (vec $)
                  (conj $ s3-dir))]
      (s3/delete-objects this files)))

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
