(ns core2.bench
  (:require [clojure.tools.cli :as cli]
            [clojure.tools.logging :as log]
            core2.indexer
            [core2.kafka :as k]
            [core2.log.local-directory-log :as ldl]
            [core2.node :as node]
            [core2.object-store :as os]
            [core2.s3 :as s3]
            [core2.util :as util])
  (:import core2.indexer.Indexer
           core2.node.Node
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
  (.finishChunk ^Indexer (util/component node :core2.indexer/indexer)))

(defn start-node
  (^core2.node.Node [] (start-node {}))

  (^core2.node.Node [{:keys [node-id node-type ^Path node-tmp-dir]
                      :or {node-id (str (UUID/randomUUID))
                           node-type :in-memory}}]
   (log/info "Starting node, id:" node-id)

   (node/start-node (case node-type
                      :in-memory {}
                      :local-fs {:core2.log/local-directory-log {:root-path (.resolve node-tmp-dir "log")}
                                 ::os/file-system-object-store {:root-path (.resolve node-tmp-dir "objects")}}
                      :kafka-s3 {::k/log {:bootstrap-servers "localhost:9092"
                                          :topic-name (str "bench-log-" node-id)}
                                 ::s3/object-store {:bucket "core2-bench"
                                                    :prefix (str "node." node-id)}}))))

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
