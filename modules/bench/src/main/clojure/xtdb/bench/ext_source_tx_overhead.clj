(ns xtdb.bench.ext-source-tx-overhead
  "`ingest-tx-overhead`, but through an external source rather than submits:
   one `IngestNode` per batch size, whose database's `GeneratedDocsSource` hands off
   `doc-count` puts in transactions of `batch-size` without awaiting each one.

   The benchmark's own node is unused - the ingest nodes are opened here, so that
   the replica log can be chosen with `--log`.
   `kafka` needs a local broker (`docker-compose up kafka`).

   Run: ./gradlew ext-source-tx-overhead -PdocCount=100000 -PbatchSizes=1000,1 -PreplicaLog=kafka"
  (:require [clojure.string :as s]
            [clojure.tools.logging :as log]
            [xtdb.bench :as b]
            [xtdb.util :as util])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]
           [xtdb.api IngestNode IngestNode$Config TransactionResult$Committed]
           [xtdb.api.log KafkaCluster$ClusterFactory KafkaCluster$LogFactory Log]
           [xtdb.bench GeneratedDocsSource]
           [xtdb.database Database$Config]))

(defn- ->log-factory [{:keys [log]} batch-size]
  (case log
    :memory (Log/getInMemoryLog)
    :local (Log/localLog (Files/createTempDirectory (str "xt-ext-source-" batch-size "-") (make-array FileAttribute 0)))
    :kafka (KafkaCluster$LogFactory. "kafkaCluster" (str "bench-ext-source-" (subs (str (random-uuid)) 0 8) "-" batch-size))))

(defn- open-ingest-node ^IngestNode [{:keys [bootstrap-servers] :as opts} batch-size ^GeneratedDocsSource source]
  (-> (IngestNode$Config.)
      (cond-> (= :kafka (:log opts)) (.logCluster "kafkaCluster" (KafkaCluster$ClusterFactory. bootstrap-servers)))
      (.database "bench" (-> (Database$Config.)
                             (.log (->log-factory opts batch-size))
                             (.externalSource source)))
      (.open)))

(defmethod b/cli-flags :ext-source-tx-overhead [_]
  [["-dc" "--doc-count DOCUMENT_COUNT" "Number of documents to ingest"
    :parse-fn parse-long
    :default 100000]

   ["-bs" "--batch-sizes BATCH_SIZES" "Batch sizes to use for ingestion, e.g. \"1000,100,10,1\""
    :parse-fn #(->> (s/split % #",") (map parse-long) (into #{}))
    :default #{1000 100 10 1}]

   [nil "--log LOG" "Replica log for the ingest databases: memory, local or kafka"
    :parse-fn keyword
    :validate [#{:memory :local :kafka} "must be one of memory, local, kafka"]
    :default :memory]

   [nil "--bootstrap-servers SERVERS" "Kafka bootstrap servers, for `--log kafka`"
    :default "localhost:9092"]

   ["-h" "--help"]])

(defn benchmark [{:keys [seed doc-count batch-sizes log bootstrap-servers],
                  :or {seed 0, doc-count 100000, batch-sizes #{1000 100 10 1}, log :memory, bootstrap-servers "localhost:9092"}}]
  (let [opts {:log log, :bootstrap-servers bootstrap-servers}]
    (log/info {:doc-count doc-count :batch-sizes batch-sizes :log log})

    {:title "Ingest batch vs individual, external source"
     :seed seed
     :parameters {:doc-count doc-count :batch-sizes (sort > batch-sizes) :log log}
     :tasks (for [batch-size (sort > batch-sizes)
                  :let [source (GeneratedDocsSource. (str "batched_" batch-size) doc-count batch-size)
                        !node (atom nil)]
                  task [{:t :call
                         :f (fn [_] (reset! !node (open-ingest-node opts batch-size source)))}

                        {:t :call
                         :batch-size batch-size
                         :stage (keyword (str "ingest-batch-" batch-size))
                         :f (fn [_]
                              (let [res (.ingest source)]
                                (assert (instance? TransactionResult$Committed res)
                                        (format "batch size %d: last tx didn't commit: %s" batch-size res))))}

                        {:t :call
                         :f (fn [_] (util/close @!node))}]]
              task)}))

(defmethod b/->benchmark :ext-source-tx-overhead [_ opts]
  (benchmark opts))

(comment
  (with-open [node (xtdb.node/start-node)]
    ((b/compile-benchmark (benchmark {:batch-sizes #{1000 1}, :doc-count 100000, :log :memory}))
     node)))
