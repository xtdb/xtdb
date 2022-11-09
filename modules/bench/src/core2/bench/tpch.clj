(ns core2.bench.tpch
  (:require [clojure.tools.logging :as log]
            [core2.bench :as bench]
            [core2.ingester :as ingest]
            [core2.node :as node]
            [core2.test-util :as tu]
            [core2.tpch :as tpch]
            [core2.util :as util])
  (:import core2.node.Node
           java.time.Duration))

(defn ingest-tpch [^Node node {:keys [scale-factor]}]
  (let [tx (bench/with-timing :submit-docs
             (tpch/submit-docs! node scale-factor))]
    (bench/with-timing :await-tx
      @(node/snapshot-async node tx (Duration/ofHours 5)))

    (bench/with-timing :finish-chunk
      (bench/finish-chunk node))))

(defn query-tpch [db]
  (doseq [q tpch/queries]
    (bench/with-timing (str "query " (:name (meta q)))
      (let [q @q]
        (try
          (count (tu/query-ra q (merge {'$ db} (::tpch/params (meta q)))))
          (catch Exception e
            (.printStackTrace e)))))))

(comment
  (with-open [node (bench/start-node)]
    (bench/with-timing :ingest
      (ingest-tpch node {:scale-factor 0.01}))

    (let [db (ingest/snapshot (util/component node :core2/ingester))]
      (bench/with-timing :cold-queries
        (query-tpch db))

      (bench/with-timing :hot-queries
        (query-tpch db)))))

(defn -main [& args]
  (try
    (let [opts (or (bench/parse-args [[nil "--scale-factor 0.01" "Scale factor for regular TPCH test"
                                       :id :scale-factor
                                       :default 0.01
                                       :parse-fn #(Double/parseDouble %)]]
                                     args)
                   (System/exit 1))]
      (log/info "Opts: " (pr-str opts))
      (with-open [node (bench/start-node)]
        (bench/with-timing :ingest
          (ingest-tpch node opts))

        (let [db (ingest/snapshot (util/component node :core2/ingester))]
          (bench/with-timing :cold-queries
            (query-tpch db))

          (bench/with-timing :hot-queries
            (query-tpch db)))))

    (catch Exception e
      (.printStackTrace e)
      (System/exit 1))

    (finally
      (shutdown-agents))))
