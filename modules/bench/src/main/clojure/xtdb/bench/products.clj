(ns xtdb.bench.products
  (:require [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [cognitect.transit :as transit]
            [xtdb.api :as xt]
            [xtdb.bench.xtdb2 :as bxt])
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

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn benchmark [{:keys [load-phase limit], :or {load-phase true}}]
  {:title "Products"
   :tasks
   [{:t :do
     :stage :download
     :tasks [{:t :do
              :stage :download
              :tasks [{:t :call :f (fn [_]
                                     (download-dataset))}]}]}
    {:t :do
     :stage :ingest
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
                  [{:t :do
                    :stage :sync
                    :tasks [{:t :call :f (fn [{:keys [sut]}] (bxt/sync-node sut (Duration/ofHours 5)))}]}
                   {:t :do
                    :stage :finish-block
                    :tasks [{:t :call :f (fn [{:keys [sut]}] (bxt/finish-block! sut))}]}
                   {:t :do
                    :stage :compact
                    :tasks [{:t :call :f (fn [{:keys [sut]}] (bxt/compact! sut))}]}])}]})
