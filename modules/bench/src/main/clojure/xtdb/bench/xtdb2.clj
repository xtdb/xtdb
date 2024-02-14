(ns xtdb.bench.xtdb2
  (:require [clojure.java.io :as io]
            [clojure.tools.cli :as cli]
            [clojure.tools.logging :as log]
            [xtdb.api :as xt]
            [xtdb.bench :as b]
            [xtdb.bench.measurement :as bm]
            [xtdb.indexer]
            [xtdb.indexer.live-index :as li]
            [xtdb.node :as xtn]
            [xtdb.protocols :as xtp]
            [xtdb.util :as util]
            [xtdb.query-ra :as ra]
            [xtdb.metrics :as metrics])
  (:import (io.micrometer.core.instrument MeterRegistry Timer)
           (java.io Closeable File)
           (java.nio.file Path)
           (java.time Duration InstantSource)
           (java.util.concurrent.atomic AtomicLong)
           (xtdb.api IXtdb TransactionKey)
           (xtdb.indexer IIndexer)))

(set! *warn-on-reflection* false)

(defn finish-chunk! [node]
  (li/finish-chunk! (util/component node :xtdb.indexer/live-index)))

(defn sync-node
  ([node]
   (sync-node node nil))

  ([node ^Duration timeout]
   @(.awaitTxAsync ^IIndexer (util/component node :xtdb/indexer)
                   (xtp/latest-submitted-tx node)
                   timeout)))

(defn ->local-node ^xtdb.api.IXtdb [{:keys [^Path node-dir ^String buffers-dir
                                            rows-per-chunk log-limit page-limit instant-src]
                                     :or {buffers-dir "objects"}}]
  (xtn/start-node {:log [:local {:path (.resolve node-dir "log"), :instant-src instant-src}]
                   :storage [:local {:path (.resolve node-dir buffers-dir)}]
                   :indexer (->> {:log-limit log-limit, :page-limit page-limit, :rows-per-chunk rows-per-chunk}
                                 (into {} (filter val)))}))

(defn install-tx-fns [worker fns]
  (->> (for [[id fn-def] fns]
         [:put-fn id fn-def])
       (xt/submit-tx (:sut worker))))

(defn generate
  ([worker table f n]
   (let [doc-seq (remove nil? (repeatedly (long n) (partial f worker)))
         partition-count 512]
     (doseq [chunk (partition-all partition-count doc-seq)]
       (xt/submit-tx (:sut worker) [(into [:put-docs table] chunk)])))))

(defn run-benchmark [{:keys [node-opts benchmark-type benchmark-opts]}]
  (let [benchmark (case benchmark-type
                    :auctionmark
                    ((requiring-resolve 'xtdb.bench.auctionmark/benchmark) benchmark-opts)
                    :tpch
                    ((requiring-resolve 'xtdb.bench.tpch/benchmark) benchmark-opts)
                    :ts-devices
                    ((requiring-resolve 'xtdb.bench.ts-devices/benchmark) benchmark-opts))
        benchmark-fn (b/compile-benchmark benchmark bm/wrap-task)]
    (with-open [node (->local-node node-opts)]
      (binding [bm/*registry* (:registry node)]
        (benchmark-fn node)))))

(defn node-dir->config [^File node-dir]
  (let [^Path path (.toPath node-dir)]
    {:log [:local {:path (.resolve path "log")}]
     :storage [:local {:path (.resolve path "objects")}]}))

(comment

  ;; ======
  ;; Running in process
  ;; ======

  (def run-duration "PT5S")
  (def run-duration "PT10S")
  (def run-duration "PT30S")
  (def run-duration "PT2M")
  (def run-duration "PT10M")

  (def node-dir (io/file "dev/dev-node"))
  (util/delete-dir (.toPath node-dir))

  ;; The load-phase is essentially required once to setup some initial data,
  ;; but can be ignored on subsequent runs.
  ;; run-benchmark clears up the node it creates (but not the data),
  ;; hence needing to create a new one to test single point queries

  (run-benchmark
   {:node-opts {:node-dir (.toPath node-dir)
                :instant-src (InstantSource/system)}
    :benchmark-type :auctionmark
    :benchmark-opts {:duration run-duration :load-phase true
                     :scale-factor 0.1 :threads 1}})


  ;;;;;;;;;;;;;
  ;; testing single point queries
  ;;;;;;;;;;;;;

  (def node (xtn/start-node (node-dir->config node-dir)))

  (def get-item-query '(from :item [{:xt/id i_id, :i_status :open}
                                    i_u_id i_initial_price i_current_price]) )
  ;; ra for the above
  (def ra-query
    '[:scan
      {:table item :for-valid-time [:at :now], :for-system-time nil}
      [{i_status (= i_status :open)}
       i_u_id
       i_current_price
       i_initial_price
       {xt/id (= xt/id ?i_id)}
       id]])

  (def open-ids (->> (xt/q node '(from :item [{:xt/id i :i_status :open}]))
                     (map :i)))

  (def q  (fn [open-id]
            (ra/query-ra ra-query {:node node
                                   :params {'?i_id open-id}})))
  ;; ra query
  (time
   #(doseq [id (take 1000 (shuffle open-ids))]
      (q id)))

  ;; datalog query
  (time
   (doseq [id (take 1000 (shuffle open-ids))]
     (xt/q node [get-item-query id]))))
