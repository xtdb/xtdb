(ns xtdb.stagnant-log-flusher
  (:require [clojure.tools.logging :as log]
            [xtdb.log :as xt-log]
            [xtdb.indexer]
            [xtdb.tx-producer]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.util :as util])
  (:import (java.nio ByteBuffer)
           (java.nio.channels ClosedByInterruptException)
           (java.time Duration)
           (java.util.concurrent ExecutorService Executors ThreadFactory TimeUnit)
           (xtdb.indexer IIndexer)
           (xtdb.log Log)))

;; see https://github.com/xtdb/xtdb/issues/2548

(defmethod ig/prep-key ::flusher
  [_ opts]
  (merge {:indexer (ig/ref :xtdb/indexer)
          :log (ig/ref :xtdb/log)}
         opts))

(defmethod ig/init-key ::flusher
  [_ {:keys [^IIndexer indexer
             ^Log log
             duration
             ;; callback hook used to control timing in tests
             ;; receives a map of the :last-flush, :last-seen tx-keys
             on-heartbeat]
      :or {duration #time/duration "PT4H"
           on-heartbeat (constantly nil)}}]
  (let [exr-tf (util/->prefix-thread-factory "xtdb.stagnant-log-flush")
        exr (Executors/newSingleThreadScheduledExecutor exr-tf)

        ;; the tx-key of the last seen chunk tx
        previously-seen-chunk-tx-id (atom nil)
        ;; the tx-key of the last flush msg sent by me
        !last-flush-tx-id (atom nil)

        ^Runnable f
        (bound-fn heartbeat []
          (on-heartbeat {:last-flush @!last-flush-tx-id, :last-seen @previously-seen-chunk-tx-id})
          (when-some [{latest-tx-id :tx-id} (.latestCompletedTx indexer)]
            (let [{latest-chunk-tx-id :tx-id} (.latestCompletedChunkTx indexer)]
              (try
                (when (and (= @previously-seen-chunk-tx-id latest-chunk-tx-id)
                           (or (nil? @!last-flush-tx-id)
                               (< @!last-flush-tx-id latest-tx-id)))
                  (log/infof "last chunk tx-id %s, flushing any pending writes" latest-chunk-tx-id)

                  (let [record-buf (-> (ByteBuffer/allocate 9)
                                       (.put (byte xt-log/hb-flush-chunk))
                                       (.putLong (or latest-chunk-tx-id -1))
                                       .flip)
                        record @(.appendRecord log record-buf)]
                   (reset! !last-flush-tx-id (:tx-id (:tx record)))))
                (catch InterruptedException _)
                (catch ClosedByInterruptException _)
                (catch Throwable e
                  (log/error e "exception caught submitting flush record"))
                (finally
                  (reset! previously-seen-chunk-tx-id latest-chunk-tx-id))))))]
    {:executor exr
     :task (.scheduleAtFixedRate exr f (.toMillis ^Duration duration) (.toMillis ^Duration duration) TimeUnit/MILLISECONDS)}))

(defmethod ig/halt-key! ::flusher [_ {:keys [^ExecutorService executor, task]}]
  (future-cancel task)
  (.shutdown executor)
  (let [timeout-secs 10]
    (when-not (.awaitTermination executor timeout-secs TimeUnit/SECONDS)
      (log/warnf "flusher did not shutdown within %d seconds" timeout-secs))))

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

  (.latestCompletedTx (:xtdb/indexer sys))

  (defn submit [tx] (.submitTx (:xtdb.tx-producer/tx-producer sys) tx {}))

  @(submit [[:put :foo {:xt/id 42, :msg "Hello, world!"}]])

  )
