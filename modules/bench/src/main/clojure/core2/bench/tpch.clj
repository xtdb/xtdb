(ns core2.bench.tpch
  (:require [clojure.tools.logging :as log]
            [core2.bench :as bench]
            [core2.datasets.tpch :as tpch]
            [core2.datasets.tpch.ra :as tpch-ra]
            [core2.node :as node]
            [core2.test-util :as tu])
  (:import core2.node.Node
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
      (tu/with-tmp-dirs #{node-tmp-dir}
        (with-open [node (bench/start-node (into opts {:node-tmp-dir node-tmp-dir}))]
          (bench/with-timing :ingest
            (ingest-tpch node opts))

          (bench/with-timing :cold-queries
            (query-tpch node))

          (bench/with-timing :hot-queries
            (query-tpch node)))))

    (catch Exception e
      (.printStackTrace e)
      (System/exit 1))

    (finally
      (shutdown-agents))))
