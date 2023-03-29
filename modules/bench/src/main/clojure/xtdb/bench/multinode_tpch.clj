(ns xtdb.bench.multinode-tpch
  (:require [clojure.tools.logging :as log]
            [xtdb.bench :as bench]
            [xtdb.ingester :as ingest]
            [xtdb.node :as node]
            [xtdb.datasets.tpch :as tpch]
            [xtdb.datasets.tpch.ra :as tpch.ra]
            [xtdb.temporal :as temporal]
            [xtdb.test-util :as tu]
            [xtdb.util :as util]
            [xtdb.indexer :as idx])
  (:import java.nio.file.attribute.FileAttribute
           java.nio.file.Files
           java.time.Duration
           java.util.UUID))

(defn run-multinode [{:keys [scale-factor sleep-ms]} start-node]
  (log/info "Starting primary node")
  (with-open [^xtdb.node.Node primary-node (start-node)]
    (let [!last-tx (future
                     (let [last-tx (tpch/submit-docs! primary-node scale-factor)]
                       (log/info "last submitted tx:" last-tx)
                       last-tx))]
      (try
        (Thread/sleep sleep-ms)

        (log/info "Starting secondary node 1")
        (with-open [^xtdb.node.Node secondary-node1 (start-node)]
          (Thread/sleep sleep-ms)

          (log/info "Starting secondary node 2")
          (with-open [^xtdb.node.Node secondary-node2 (start-node)]
            (Thread/sleep sleep-ms)

            (log/info "Starting secondary node 3")
            (with-open [^xtdb.node.Node secondary-node3 (start-node)]
              (let [last-tx @!last-tx
                    query tpch.ra/q1-pricing-summary-report]
                (letfn [(test-node [k ^xtdb.node.Node node]
                          (log/info "awaiting" k "node")
                          (tu/then-await-tx* last-tx node (Duration/ofHours 1))
                          (log/info "rows:"
                                    (count (tu/query-ra query {:node node, :params (::tpch/params (meta query))}))))]
                  (doseq [[k node] {:primary primary-node
                                    :secondary1 secondary-node1
                                    :secondary2 secondary-node2
                                    :secondary3 secondary-node3}]
                    (test-node k node))

                  (idx/finish-chunk! (util/component primary-node :xtdb/indexer))
                  (.awaitSnapshotBuild ^xtdb.temporal.TemporalManagerPrivate (::temporal/temporal-manager @(:!system primary-node)))

                  (log/info "Starting post finish-chunk node")
                  (with-open [^xtdb.node.Node secondary-node4 (start-node)]
                    (test-node :secondary4 secondary-node4)))))))

        (finally
          (future-cancel !last-tx))))))

(comment
  (let [node-dir (Files/createTempDirectory "multinode-0.1" (make-array FileAttribute 0))
        node-opts {:xtdb.log/local-directory-log {:root-path (.resolve node-dir "log")}
                   :xtdb.object-store/file-system-object-store {:root-path (.resolve node-dir "objects")}}]
    (run-multinode {:scale-factor 0.1, :sleep-ms 60000}
                   (fn []
                     (node/start-node node-opts)))))

(defn -main [& args]
  (try
    (let [opts (or (bench/parse-args [[nil "--scale-factor 0.01" "Scale factor for regular TPCH test"
                                       :id :scale-factor
                                       :default 0.01
                                       :parse-fn #(Double/parseDouble %)]
                                      [nil "--sleep-ms 5000" "time to sleep between starting nodes"
                                       :id :sleep-ms
                                       :default 5000
                                       :parse-fn #(Long/parseLong %)]]
                                     args)
                   (System/exit 1))
          node-id (str (UUID/randomUUID))]
      (log/info "starting cluster" node-id "with opts" (pr-str opts))
      (run-multinode opts
                     (fn []
                       (bench/start-node node-id))))
    (catch Exception e
      (.printStackTrace e)
      (System/exit 1))

    (finally
      (shutdown-agents))))
