(ns xtdb.bench.products
  (:require [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [cognitect.transit :as transit]
            [xtdb.api :as xt]
            [xtdb.bench :as b]
            [xtdb.serde :as serde])
  (:import [java.io File]
           [java.time Duration]
           java.util.zip.GZIPInputStream
           [software.amazon.awssdk.services.s3 S3Client]
           [software.amazon.awssdk.services.s3.model GetObjectRequest]))

(def ^File products-file
  (io/file "modules/bench/dataset-downloads/products.transit.msgpack.gz"))

(defn download-dataset []
  (when-not (.exists products-file)
    (log/info "downloading" (str products-file))
    (io/make-parents products-file)

    (.getObject (S3Client/create)
                (-> (GetObjectRequest/builder)
                    (.bucket "xtdb-datasets")
                    (.key "products.transit.msgpack.gz")
                    ^GetObjectRequest (.build))
                (.toPath products-file))))

(defn store-documents! [node docs]
  (dorun (->> docs
              (partition-all 500) ; reduced from 1000 - product docs are large
              (map-indexed (fn [idx docs]
                             (log/debug "batch" idx)
                             (xt/submit-tx node [(into [:put-docs :products] docs)]))))))

(defmethod b/cli-flags :products [_]
  [["-l" "--limit LIMIT"
    :parse-fn parse-long]

   ["-h" "--help"]])

(defmethod b/->benchmark :products [_ {:keys [no-load? limit]}]
  (log/info {:no-load? no-load? :limit limit})

  {:title "Products"
   :parameters {:no-load? no-load? :limit limit}
   :tasks [{:t :call, :stage :download
            :f (fn [_]
                 (download-dataset))}

           {:t :do, :stage :ingest
            :tasks (concat (when-not no-load?
                             [{:t :do
                               :stage :submit-docs
                               :tasks [{:t :call
                                        :f (fn [{:keys [node]}]
                                             (with-open [is (-> products-file
                                                                io/input-stream
                                                                (GZIPInputStream.))]
                                               (store-documents! node (cond->> (serde/transit-seq (transit/reader is :msgpack))
                                                                        limit (take limit)))))}]}])

                           [{:t :call, :stage :sync
                             :f (fn [{:keys [node]}]
                                  (b/sync-node node (Duration/ofHours 5)))}

                            {:t :call, :stage :finish-block
                             :f (fn [{:keys [node]}]
                                  (b/finish-block! node))}

                            {:t :call, :stage :compact
                             :f (fn [{:keys [node]}]
                                  (b/compact! node))}])}]})
