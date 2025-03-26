(ns xtdb.bench.products
  (:require [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [cognitect.transit :as transit]
            [xtdb.api :as xt]
            [xtdb.bench :as b])
  (:import [java.io File]
           [java.time Duration]
           java.util.zip.GZIPInputStream
           [software.amazon.awssdk.services.s3 S3Client]
           [software.amazon.awssdk.services.s3.model GetObjectRequest]))

(def ^File products-file
  (io/file "datasets/products.transit.msgpack.gz"))

(defn download-dataset []
  (when-not (.exists products-file)
    (log/info "downloading" (str products-file))
    (io/make-parents (io/file "datasets"))

    (.getObject (S3Client/create)
                (-> (GetObjectRequest/builder)
                    (.bucket "xtdb-datasets")
                    (.key "products.transit.msgpack.gz")
                    ^GetObjectRequest (.build))
                (.toPath products-file))))

(defn transit-seq [rdr]
  (lazy-seq
   (try
     (cons (transit/read rdr)
           (transit-seq rdr))
     (catch RuntimeException ex
       (when-not (instance? java.io.EOFException (.getCause ex))
         (throw ex))))))

(defn store-documents! [node docs]
  (dorun (->> docs
              (partition-all 1000)
              (map-indexed (fn [idx docs]
                             (log/debug "batch" idx)
                             (xt/submit-tx node [(into [:put-docs :products] docs)]))))))

(defmethod b/cli-flags :products [_]
  [["-l" "--limit LIMIT"
    :parse-fn parse-long]

   ["-h" "--help"]])

(defmethod b/->benchmark :products [_ {:keys [load-phase limit], :or {load-phase true}}]
  {:title "Products"
   :tasks [{:t :call, :stage :download
            :f (fn [_]
                 (download-dataset))}

           {:t :do, :stage :ingest
            :tasks (into (if load-phase
                           [{:t :do
                             :stage :submit-docs
                             :tasks [{:t :call
                                      :f (fn [{:keys [sut]}]
                                           (with-open [is (-> products-file
                                                              io/input-stream
                                                              (GZIPInputStream.))]
                                             (store-documents! sut (cond->> (transit-seq (transit/reader is :msgpack))
                                                                     limit (take limit)))))}]}]
                           [])

                         [{:t :call, :stage :sync
                           :f (fn [{:keys [sut]}]
                                (b/sync-node sut (Duration/ofHours 5)))}

                          {:t :call, :stage :finish-block
                           :f (fn [{:keys [sut]}]
                                (b/finish-block! sut))}

                          {:t :call, :stage :compact
                           :f (fn [{:keys [sut]}]
                                (b/compact! sut))}])}]})
