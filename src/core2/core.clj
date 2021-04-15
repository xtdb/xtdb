(ns core2.core
  (:require [core2.indexer :as indexer]
            core2.ingest-loop
            [core2.logical-plan :as lp]
            [core2.system :as sys]
            core2.tx-producer
            [core2.util :as util])
  (:import [core2.indexer IChunkManager TransactionIndexer]
           core2.ingest_loop.IIngestLoop
           core2.operator.IOperatorFactory
           core2.tx_producer.ITxProducer
           java.io.Closeable
           java.lang.AutoCloseable
           [org.apache.arrow.memory RootAllocator]))

(set! *unchecked-math* :warn-on-boxed)

(defprotocol PNode
  (await-tx
    ^core2.tx.TransactionInstant [node tx]
    ^core2.tx.TransactionInstant [node tx timeout])

  (latest-completed-tx ^core2.tx.TransactionInstant [node])
  (open-watermark ^core2.tx.Watermark [node])

  (open-q
    ^java.lang.AutoCloseable [node watermark query]))

(defprotocol PSubmitNode
  (submit-tx
    ^java.util.concurrent.CompletableFuture [tx-producer tx-ops]))

(defrecord Node [^TransactionIndexer indexer
                 ^IIngestLoop ingest-loop
                 ^IOperatorFactory op-factory
                 ^ITxProducer tx-producer
                 !system
                 close-fn]
  PNode
  (await-tx [this tx] (await-tx this tx nil))
  (await-tx [_ tx timeout] (.awaitTx ingest-loop tx timeout))

  (latest-completed-tx [_] (.latestCompletedTx indexer))

  (open-watermark [_] (.getWatermark ^IChunkManager indexer))

  (open-q [_ watermark query] (lp/open-q op-factory watermark query))

  PSubmitNode
  (submit-tx [_ tx-ops]
    (.submitTx tx-producer tx-ops))

  Closeable
  (close [_]
    (when close-fn
      (close-fn))))

(defn ->node {::sys/deps {:indexer :core2/indexer
                          :ingest-loop :core2/ingest-loop
                          :op-factory :core2/op-factory
                          :tx-producer :core2/tx-producer}}
  [deps]
  (map->Node (assoc deps :!system (atom nil))))

(defn ->allocator [_]
  (RootAllocator.))

(defn start-node ^core2.core.Node [opts]
  (let [system (-> (sys/prep-system (into [{:core2/node `->node
                                            :core2/allocator `->allocator
                                            :core2/indexer 'core2.indexer/->indexer
                                            :core2/ingest-loop 'core2.ingest-loop/->ingest-loop
                                            :core2/log 'core2.log/->log
                                            :core2/tx-producer 'core2.tx-producer/->tx-producer
                                            :core2/metadata-manager 'core2.metadata/->metadata-manager
                                            :core2/temporal-manager 'core2.temporal/->temporal-manager
                                            :core2/object-store 'core2.object-store/->object-store
                                            :core2/buffer-pool 'core2.buffer-pool/->buffer-pool
                                            :core2/op-factory 'core2.operator/->operator-factory}]
                                          (cond-> opts (not (vector? opts)) vector)))
                   (sys/start-system))]

    (-> (:core2/node system)
        (doto (-> :!system (reset! system)))
        (assoc :close-fn #(.close ^AutoCloseable system)))))

(defrecord SubmitNode [^ITxProducer tx-producer, !system, close-fn]
  PSubmitNode
  (submit-tx [_ tx-ops]
    (.submitTx tx-producer tx-ops))

  AutoCloseable
  (close [_]
    (when close-fn
      (close-fn))))

(defn ->submit-node {::sys/deps {:tx-producer :core2/tx-producer}}
  [{:keys [tx-producer]}]
  (map->SubmitNode {:tx-producer tx-producer, :!system (atom nil)}))

(defn start-submit-node ^core2.core.SubmitNode [opts]
  (let [system (-> (sys/prep-system (into [{:core2/submit-node `->submit-node
                                            :core2/tx-producer 'core2.tx-producer/->tx-producer
                                            :core2/allocator `->allocator
                                            :core2/log 'core2.log/->local-directory-log-writer}]
                                          (cond-> opts (not (vector? opts)) vector)))
                   (sys/start-system))]

    (-> (:core2/submit-node system)
        (doto (-> :!system (reset! system)))
        (assoc :close-fn #(.close ^AutoCloseable system)))))

(defn -main [& [node-dir :as args]]
  (if node-dir
    (let [node-dir (util/->path node-dir)]
      (start-node {:core2/log {:root-path (.resolve node-dir "log")}
                   :core2/buffer-pool {:cache-path (.resolve node-dir "buffers")}
                   :core2/object-store {:root-path (.resolve node-dir "objects")}})
      (println "core2 started in" (str node-dir)))
    (binding [*out* *err*]
      (println "node directory argument required")
      (System/exit 1))))
