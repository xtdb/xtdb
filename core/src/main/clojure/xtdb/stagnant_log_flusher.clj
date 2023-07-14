(ns xtdb.stagnant-log-flusher
  (:require [clojure.tools.logging :as log]
            [xtdb.indexer]
            [xtdb.tx-producer]
            [juxt.clojars-mirrors.integrant.core :as ig])
  (:import (java.nio.channels ClosedByInterruptException)
           (java.time Duration)
           (java.util.concurrent ExecutorService Executors ThreadFactory TimeUnit)
           (xtdb.indexer IIndexer)
           (xtdb.tx_producer ITxProducer)))

;; see https://github.com/xtdb/xtdb/issues/2548

(defonce thread-counter (atom -1))

(defmethod ig/prep-key ::flusher
  [_ opts]
  (merge {:tx-producer (ig/ref :xtdb.tx-producer/tx-producer)
          :indexer (ig/ref :xtdb/indexer)}
         opts))

(defmethod ig/init-key ::flusher
  [_ {:keys [^ITxProducer tx-producer
             ^IIndexer indexer
             duration
             ;; callback hook used to control timing in tests
             ;; receives a map of the :last-flush, :last-seen tx-keys
             on-heartbeat]
      :or {duration #time/duration "PT4H"
           on-heartbeat (constantly nil)}}]
  (let [exr-tf (reify ThreadFactory
                 (newThread [_ r]
                   (doto (Thread. r)
                     (.setName (str "xtdb.stagnant-log-flusher" (swap! thread-counter inc))))))
        exr (Executors/newSingleThreadScheduledExecutor exr-tf)

        ;; the tx-key of the last seen completed-tx
        last-seen-tx-id (atom nil)
        ;; the tx-key of the msg to flush sent by me
        last-flush-tx-id (atom nil)

        ^Runnable f
        (bound-fn heartbeat []
          (on-heartbeat {:last-flush @last-flush-tx-id, :last-seen @last-seen-tx-id})
          (when-some [{latest-tx-id :tx-id} (.latestCompletedTx indexer)]
            (try
              (when (and (= @last-seen-tx-id latest-tx-id)
                         (or (nil? @last-flush-tx-id)
                             (< @last-flush-tx-id latest-tx-id)))
                (log/infof "log remains on tx-id %s, flushing any pending chunks" latest-tx-id)
                (let [{flush-tx-id :tx-id} @(.submitTx tx-producer [[:flush-index-if-log-not-moving latest-tx-id]] {})]
                  (log/debugf "flush tx = %s" flush-tx-id)
                  (reset! last-flush-tx-id flush-tx-id)))
              (catch InterruptedException _)
              (catch ClosedByInterruptException _)
              (catch Throwable e
                (log/error e "exception caught submitting :flush-index-if-log-not-moving transaction"))
              (finally
                (reset! last-seen-tx-id latest-tx-id)))))]
    {:executor exr
     :task (.scheduleAtFixedRate exr f 0 (.toMillis ^Duration duration) TimeUnit/MILLISECONDS)}))

(defmethod ig/halt-key! ::flusher [_ {:keys [^ExecutorService executor, task]}]
  (future-cancel task)
  (.shutdown executor)
  (let [timeout-secs 10]
    (when-not (.awaitTermination executor timeout-secs TimeUnit/SECONDS)
      (log/warnf "flusher did not shutdown within %d seconds"))))

(comment

  ((requiring-resolve 'xtdb.test-util/set-log-level!)
   'xtdb.indexer :debug)

  (require 'xtdb.node)

  (def sys
    (-> {:xtdb/allocator {}
         :xtdb/default-tz nil
         :xtdb/indexer {}
         :xtdb.operator.scan/scan-emitter {}
         :xtdb/live-chunk {}
         :xtdb.temporal/temporal-manager {}
         :xtdb.operator/ra-query-source {}
         :xtdb.metadata/metadata-manager {}
         :xtdb.indexer/internal-id-manager {}
         :xtdb.indexer/live-index {}
         :xtdb/ingester {}
         :xtdb.tx-producer/tx-producer {}
         :xtdb.buffer-pool/buffer-pool {}
         ::flusher {:duration #time/duration "PT5S"}}
        (doto ig/load-namespaces)
        (#'xtdb.node/with-default-impl :xtdb/log :xtdb.log/memory-log)
        (#'xtdb.node/with-default-impl :xtdb/object-store :xtdb.object-store/memory-object-store)
        (doto ig/load-namespaces)
        (ig/prep)
        (ig/init)))

  (ig/halt! sys)

  (::flusher sys)

  (.latestCompletedTx (:xtdb/ingester sys))
  (.latestCompletedTx (:xtdb/indexer sys))

  (defn submit [tx] (.submitTx (:xtdb.tx-producer/tx-producer sys) tx {}))

  @(submit [[:put :foo {:xt/id 42, :msg "Hello, world!"}]])


  )
