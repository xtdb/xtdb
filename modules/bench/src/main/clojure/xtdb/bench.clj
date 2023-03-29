(ns xtdb.bench
  (:require [clojure.tools.cli :as cli]
            [clojure.tools.logging :as log]
            [xtdb.api.impl :as xt.impl]
            [xtdb.indexer :as idx]
            xtdb.ingester
            [xtdb.kafka :as k]
            [xtdb.node :as node]
            [xtdb.object-store :as os]
            [xtdb.s3 :as s3]
            [xtdb.util :as util])
  (:import xtdb.ingester.Ingester
           [java.nio.file Files Path]
           java.nio.file.attribute.FileAttribute
           java.time.Duration
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

(defn finish-chunk! [node]
  (idx/finish-chunk! (util/component node :xtdb/indexer)))

(defn start-node
  (^xtdb.node.Node [] (start-node {}))

  (^xtdb.node.Node [{:keys [node-id node-type ^Path node-tmp-dir]
                      :or {node-id (str (UUID/randomUUID))
                           node-type :in-memory}}]
   (log/info "Starting node, id:" node-id)

   (node/start-node (case node-type
                      :in-memory {}
                      :local-fs {:xtdb.log/local-directory-log {:root-path (.resolve node-tmp-dir "log")}
                                 ::os/file-system-object-store {:root-path (.resolve node-tmp-dir "objects")}}
                      :kafka-s3 {::k/log {:bootstrap-servers "localhost:9092"
                                          :topic-name (str "bench-log-" node-id)}
                                 ::s3/object-store {:bucket "core2-bench"
                                                    :prefix (str "node." node-id)}}))))

(defn sync-node
  (^xtdb.api.TransactionInstant [node]
   (sync-node node nil))

  (^xtdb.api.TransactionInstant [node ^Duration timeout]
   @(.awaitTxAsync ^Ingester (util/component node :xtdb/ingester)
                   (xt.impl/latest-submitted-tx node)
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
