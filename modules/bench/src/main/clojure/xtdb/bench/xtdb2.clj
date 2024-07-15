(ns xtdb.bench.xtdb2
  (:require [clojure.java.io :as io]
            [clojure.tools.cli :as cli]
            [clojure.tools.logging :as log]
            [xtdb.api :as xt]
            [xtdb.bench :as b]
            [xtdb.bench.measurement :as bm]
            [xtdb.compactor :as c]
            [xtdb.indexer]
            [xtdb.indexer.live-index :as li]
            [xtdb.node :as xtn]
            [xtdb.protocols :as xtp]
            [xtdb.query-ra :as ra]
            [xtdb.util :as util])
  (:import (java.io File)
           (java.nio.file Path)
           (java.time Duration InstantSource)
           (xtdb.api.metrics Metrics)
           (xtdb.indexer IIndexer)))

(set! *warn-on-reflection* false)

(defn sync-node
  ([node]
   (sync-node node nil))

  ([node ^Duration timeout]
   @(.awaitTxAsync ^IIndexer (util/component node :xtdb/indexer)
                   (xtp/latest-submitted-tx node)
                   timeout)))

(defn finish-chunk! [node]
  (li/finish-chunk! (util/component node :xtdb.indexer/live-index)))

(defn compact! [node]
  (c/compact-all! node (Duration/ofMinutes 10)))

(defn ->local-node ^xtdb.api.IXtdb [{:keys [node-dir ^String buffers-dir
                                            rows-per-chunk log-limit page-limit instant-src]
                                     :or {buffers-dir "objects"}}]
  (let [node-dir (util/->path node-dir)]
    (xtn/start-node {:log [:local {:path (.resolve node-dir "log"), :instant-src instant-src}]
                     :storage [:local {:path (.resolve node-dir buffers-dir)}]
                     :metrics [:prometheus {:port 8080}]
                     :indexer (->> {:log-limit log-limit, :page-limit page-limit, :rows-per-chunk rows-per-chunk}
                                   (into {} (filter val)))})))

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
                    ((requiring-resolve 'xtdb.bench.ts-devices/benchmark) benchmark-opts)
                    :test-bm
                    ((requiring-resolve 'xtdb.bench.test-benchmark/benchmark) benchmark-opts)
                    :products
                    ((requiring-resolve 'xtdb.bench.products/benchmark) benchmark-opts))
        benchmark-fn (b/compile-benchmark benchmark bm/wrap-task)]
    (with-open [node (->local-node node-opts)]
      (let [^Metrics metrics (-> node :system :xtdb.metrics/registry)]
        (binding [bm/*registry* (.getRegistry metrics)]
          (benchmark-fn node))))))

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
    :benchmark-opts {:duration run-duration, :load-phase true, :scale-factor 0.1, :threads 1}})


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

(def ^:private tpch-cli-options
  [["-s" "--scale-factor SCALE_FACTOR" "TPC-H scale factor to use"
    :parse-fn parse-double
    :default 0.01]

   ["-h" "--help"]])

(def ^:private auctionmark-cli-options
  [["-d" "--duration DURATION"
    :parse-fn #(Duration/parse %)
    :default #xt.time/duration "PT30S"]

   ["-s" "--scale-factor SCALE_FACTOR"
    :parse-fn parse-double
    :default 0.01]

   ["-t" "--threads THREADS"
    :parse-fn parse-long
    :default (max 1 (/ (.availableProcessors (Runtime/getRuntime)) 2))]

   ["-h" "--help"]])

(def ^:private products-cli-options
  [["-l" "--limit LIMIT"
    :parse-fn parse-long]

   ["-h" "--help"]])

(defn -main [benchmark-type & args]
  (let [benchmark-type (keyword benchmark-type)
        {:keys [options errors summary]} (cli/parse-opts args (case benchmark-type
                                                                :tpch tpch-cli-options
                                                                :auctionmark auctionmark-cli-options
                                                                :products products-cli-options
                                                                {:errors [(str "Unknown benchmark: " (name benchmark-type))]}))]
    (cond
      (seq errors) (binding [*out* *err*]
                     (doseq [error errors]
                       (println error))
                     (System/exit 2))

      (:help options) (binding [*out* *err*]
                        (println summary)
                        (System/exit 0))

      :else (try
              (util/with-tmp-dirs #{node-tmp-dir}
                (run-benchmark
                 {:node-opts {:node-dir node-tmp-dir
                              :instant-src (InstantSource/system)}
                  :benchmark-type benchmark-type
                  :benchmark-opts options}))
              (System/exit 0)
              (catch Throwable t
                (log/error t "Error running benchmark")
                (System/exit 1)))))

  (shutdown-agents))
