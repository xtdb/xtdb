(ns core2.bench
  (:require [clojure.tools.cli :as cli]
            [clojure.tools.logging :as log]
            [core2.core :as c2]
            [core2.tpch :as tpch])
  (:import core2.core.Node
           java.time.Duration
           java.util.function.Consumer
           java.util.UUID))

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

(defn parse-args [args]
  (let [{:keys [options summary errors]}
        (cli/parse-opts args
                        [[nil "--scale-factor 0.01" "Scale factor for regular TPCH test"
                          :id :scale-factor
                          :default 0.01
                          :parse-fn #(Double/parseDouble %)]])]
    (if errors
      (binding [*out* *err*]
        (run! println errors)
        (println summary))

      options)))

(defn start-node ^core2.core.Node []
  (let [node-id (str (UUID/randomUUID))]
    (log/info "Starting node, id:" node-id)
    (c2/start-node {:core2/log {:core2/module 'core2.kafka/->log
                                :bootstrap-servers "localhost:9092"
                                :topic-name (str "bench-log-" node-id)}
                    :core2/object-store {:core2/module 'core2.s3/->object-store
                                         :bucket "core2-bench"
                                         :prefix (str "tpc-h." node-id)}})))

(defn ingest-tpch [^Node node {:keys [scale-factor]}]
  (let [tx (with-timing :submit-docs
             (tpch/submit-docs! node scale-factor))]
    (with-timing :await-tx
      (c2/await-tx node tx (Duration/ofHours 5)))

    (with-timing :finish-chunk
      (.finishChunk ^core2.indexer.Indexer (.indexer node)))))

(defn query-tpch [node watermark]
  (doseq [q tpch/queries]
    (with-timing (str "query " (:name (meta q)))
      (try
        (with-open [cursor (c2/open-q node watermark @q)]
          (.forEachRemaining cursor
                             (reify Consumer
                               (accept [_ root]))))
        (catch Exception e
          (.printStackTrace e))))))

(comment
  (with-open [node (start-node)]
    (with-timing :ingest
      (ingest-tpch node {:scale-factor 0.01}))

    (with-open [watermark (c2/open-watermark node)]
      (with-timing :cold-queries
        (query-tpch node watermark))

      (with-timing :hot-queries
        (query-tpch node watermark)))))

(defn -main [& args]
  (try
    (let [opts (or (parse-args args)
                   (System/exit 1))]
      (log/info "Opts: " (pr-str opts))
      (with-open [node (start-node)]
        (with-timing :ingest
          (ingest-tpch node opts))

        (with-open [watermark (c2/open-watermark node)]
          (with-timing :cold-queries
            (query-tpch node watermark))

          (with-timing :hot-queries
            (query-tpch node watermark)))))

    (catch Exception e
      (.printStackTrace e)
      (System/exit 1))

    (finally
      (shutdown-agents))))
