(ns xtdb.bench
  (:require [clojure.tools.cli :as cli]
            [clojure.tools.logging :as log]
            [xtdb.indexer.live-index :as li]
            [xtdb.kafka :as k]
            [xtdb.node :as xtn]
            [xtdb.protocols :as xtp]
            [xtdb.s3 :as s3]
            [xtdb.util :as util])
  (:import [java.nio.file Files Path]
           java.nio.file.attribute.FileAttribute
           java.time.Duration
           java.util.UUID
           software.amazon.awssdk.services.s3.model.GetObjectRequest
           software.amazon.awssdk.services.s3.S3Client
           xtdb.indexer.IIndexer))

(defn parse-args [arg-spec args]
  (let [{:keys [options summary errors]}
        (cli/parse-opts args arg-spec)]
    (if errors
      (binding [*out* *err*]
        (run! println errors)
        (println summary))

      options)))

(defmacro with-timing [block-name & body]
  `(let [block-name# (name ~block-name)]
     (log/infof "Starting '%s'..." block-name#)
     (let [start-ns# (System/nanoTime)]
       (try
         ~@body
         (finally
           (log/infof "Done '%s', %fms"
                      block-name#
                      (/ (- (System/nanoTime) start-ns#) 1e6)))))))

(defn finish-chunk! [node]
  (li/finish-chunk! (util/component node :xtdb/indexer)))

(defn start-node
  (^java.lang.AutoCloseable [] (start-node {}))

  (^java.lang.AutoCloseable [{:keys [node-id node-type ^Path node-tmp-dir]
                      :or {node-id (str (UUID/randomUUID))
                           node-type :in-memory}}]
   (log/info "Starting node, id:" node-id)

   (xtn/start-node (case node-type
                      :in-memory {}
                      :local-fs {:log [:local {:path (.resolve node-tmp-dir "log")}]
                                 :storage [:local {:path (.resolve node-tmp-dir "objects")}]}
                      :kafka-s3 {:log [:kafka {:bootstrap-servers "localhost:9092"
                                               :topic-name (str "bench-log-" node-id)}]
                                 :storage [:remote {:object-store [:s3
                                                                   {:bucket "core2-bench"
                                                                    :prefix (str "node." node-id)}]}]}))))

(defn sync-node
  ([node]
   (sync-node node nil))

  ([node ^Duration timeout]
   @(.awaitTxAsync ^IIndexer (util/component node :xtdb/indexer)
                   (xtp/latest-submitted-tx node)
                   timeout)))

(defn tmp-file-path ^java.nio.file.Path [prefix suffix]
  (doto (Files/createTempFile prefix suffix (make-array FileAttribute 0))
    (Files/delete)))

(def !s3-client (delay (S3Client/create)))

(defn download-s3-dataset-file [s3-key ^Path tmp-path]
  (.getObject ^S3Client @!s3-client
              (-> (GetObjectRequest/builder)
                  (.bucket "xtdb-datasets")
                  (.key s3-key)
                  ^GetObjectRequest (.build))
              tmp-path)
  tmp-path)

(def node-type-arg
  [nil "--node-type (in-memory | local | kafka-s3)" nil
   :id :node-type
   :default :local-fs
   :parse-fn keyword])
