(ns core2.bench
  (:require [clojure.tools.cli :as cli]
            [clojure.tools.logging :as log]
            [core2.core :as c2])
  (:import core2.core.Node
           java.util.UUID))

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
   (c2/start-node {:core2/log {:core2/module 'core2.kafka/->log
                               :bootstrap-servers "localhost:9092"
                               :topic-name (str "bench-log-" node-id)}
                   :core2/object-store {:core2/module 'core2.s3/->object-store
                                        :bucket "core2-bench"
                                        :prefix (str "node." node-id)}})))
