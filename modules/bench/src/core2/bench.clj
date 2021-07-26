(ns core2.bench
  (:require [clojure.tools.cli :as cli]
            [clojure.tools.logging :as log]
            [core2.core :as c2]
            [core2.kafka :as k]
            [core2.s3 :as s3])
  (:import core2.core.Node
           [java.nio.file Files Path]
           java.nio.file.attribute.FileAttribute
           java.util.UUID
           software.amazon.awssdk.services.s3.model.GetObjectRequest
           software.amazon.awssdk.services.s3.S3Client))

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

(defn finish-chunk [^Node node]
  (.finishChunk ^core2.indexer.Indexer (.indexer node)))

(defn ^core2.core.Node start-node
  ([] (start-node (str (UUID/randomUUID))))

  ([node-id]
   (log/info "Starting node, id:" node-id)
   (c2/start-node {::k/log {:bootstrap-servers "localhost:9092"
                            :topic-name (str "bench-log-" node-id)}
                   ::s3/object-store {:bucket "core2-bench"
                                      :prefix (str "node." node-id)}})))

(defn tmp-file-path ^java.nio.file.Path [prefix suffix]
  (doto (Files/createTempFile prefix suffix (make-array FileAttribute 0))
    (Files/delete)))

(def ^S3Client s3-client (S3Client/create))

(defn download-s3-dataset-file [s3-key ^Path tmp-path]
  (.getObject s3-client
              (-> (GetObjectRequest/builder)
                  (.bucket "crux-datasets")
                  (.key s3-key)
                  ^GetObjectRequest (.build))
              tmp-path)
  tmp-path)
