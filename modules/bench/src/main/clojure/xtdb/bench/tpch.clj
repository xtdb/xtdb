(ns xtdb.bench.tpch
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [xtdb.bench :as bench]
            [xtdb.buffer-pool :as bp]
            [xtdb.datasets.tpch :as tpch]
            [xtdb.datasets.tpch.ra :as tpch-ra]
            [xtdb.node :as node]
            [xtdb.test-util :as tu])
  (:import xtdb.node.Node
           java.time.Duration))

(defn ingest-tpch [^Node node {:keys [scale-factor]}]
  (bench/with-timing :submit-docs
    (tpch/submit-docs! node scale-factor))

  (bench/with-timing :sync
    (bench/sync-node node (Duration/ofHours 5)))

  (bench/with-timing :finish-chunk
    (bench/finish-chunk! node)))

(defn query-tpch [node]
  (tu/with-allocator
    (fn []
      (doseq [q tpch-ra/queries]
        (bench/with-timing (str "query " (:name (meta q)))
          (let [q @q
                {::tpch-ra/keys [params table-args]} (meta q)]
            (try
              (count (tu/query-ra q {:node node
                                     :params params
                                     :table-args table-args}))
              (catch Exception e
                (.printStackTrace e)))))))))

(comment
  (with-open [node (bench/start-node)]
    (bench/with-timing :ingest
      (ingest-tpch node {:scale-factor 0.01}))

    (bench/with-timing :cold-queries
      (query-tpch node))

    (bench/with-timing :hot-queries
      (query-tpch node))))

(defn bp-stats
  "Returns string reflecting buffer pool stats over a given test run"
  [ms]
  (let [miss-bytes (.get bp/cache-miss-byte-counter)
        hit-bytes (.get bp/cache-hit-byte-counter)
        ;; thanks swn: https://sw1nn.com/blog/2012/03/26/human-readable-size/
        humanize-bytes
        (fn [bytes]
          (let [unit 1024]
            (if (< bytes unit)
              (str bytes " B")
              (let [exp (int (/ (Math/log bytes)
                                (Math/log unit)))
                    pre (str (nth "KMGTPE" (dec exp)) "i")]
                (format "%.1f %sB" (/ bytes (Math/pow unit exp)) pre)))))

        humanize-nanos
        (fn [nanos]
          (let [nanos (long nanos)
                unit 1000]
            (if (< nanos unit)
              (str nanos " ns")
              (let [exp (min (int (/ (Math/log nanos) (Math/log unit))) 2)
                    suf (case exp
                          1 "μs"
                          2 "s"
                          "s")]
                (format "%.1f %s" (/ nanos (Math/pow unit exp)) suf)))))

        cache-ratio (/ hit-bytes (max 1 (+ hit-bytes miss-bytes)))
        stall-nanos @bp/io-wait-nanos-counter]
    (->> ["hit" (format "%s (%.2f)" (humanize-bytes hit-bytes) (double cache-ratio))
          "miss" (humanize-bytes miss-bytes)
          "io" (format "%s/sec" (humanize-bytes (/ miss-bytes (max 1 (/ ms 1000)))))
          "io-wait" (humanize-nanos stall-nanos)]
         (partition 2)
         (map (fn [[label s]] (str label ": " s)))
         (str/join ", "))))

(defn run-bench [node]
  (let [start (System/currentTimeMillis)]
    (bp/clear-cache-counters)
    (bench/with-timing :cold-queries
      (query-tpch node))
    (log/info "cold buffer pool -" (bp-stats (- (System/currentTimeMillis) start))))

  (let [start (System/currentTimeMillis)]
    (bp/clear-cache-counters)
    (bench/with-timing :hot-queries
      (query-tpch node))
    (log/info "hot buffer pool -" (bp-stats (- (System/currentTimeMillis) start)))))

(defn bench [opts]
  (tu/with-tmp-dirs #{node-tmp-dir}
    (with-open [node (bench/start-node (into opts {:node-tmp-dir node-tmp-dir}))]
      (bench/with-timing :ingest (ingest-tpch node opts))
      (run-bench node))))

(comment

  (bench {:node-type :local-fs, :scale-factor 0.01})
  (bench {:node-type :local-fs, :scale-factor 0.05})
  (bench {:node-type :local-fs, :scale-factor 0.15})

  )

(defn -main [& args]
  (try
    (let [opts (or (bench/parse-args [[nil "--scale-factor 0.01" "Scale factor for regular TPCH test"
                                       :id :scale-factor
                                       :default 0.01
                                       :parse-fn #(Double/parseDouble %)]
                                      bench/node-type-arg]
                                     args)
                   (System/exit 1))]
      (log/info "Opts: " (pr-str opts))
      (bench opts))
    (catch Exception e
      (.printStackTrace e)
      (System/exit 1))

    (finally
      (shutdown-agents))))
