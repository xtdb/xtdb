(ns core2.bench.multinode-tpch
  (:require [clojure.tools.logging :as log]
            [core2.bench :as bench]
            [core2.core :as c2]
            [core2.tpch :as tpch]
            [core2.temporal :as temporal])
  (:import java.nio.file.attribute.FileAttribute
           java.nio.file.Files
           java.time.Duration
           java.util.UUID))

(defn run-multinode [{:keys [scale-factor sleep-ms]} start-node]
  (log/info "Starting primary node")
  (with-open [^core2.core.Node primary-node (start-node)]
    (let [!last-tx (future
                     (let [last-tx (tpch/submit-docs! primary-node scale-factor)]
                       (log/info "last submitted tx:" last-tx)
                       last-tx))]
      (try
        (Thread/sleep sleep-ms)

        (log/info "Starting secondary node 1")
        (with-open [^core2.core.Node secondary-node1 (start-node)]
          (Thread/sleep sleep-ms)

          (log/info "Starting secondary node 2")
          (with-open [^core2.core.Node secondary-node2 (start-node)]
            (Thread/sleep sleep-ms)

            (log/info "Starting secondary node 3")
            (with-open [^core2.core.Node secondary-node3 (start-node)]
              (let [last-tx @!last-tx
                    query tpch/tpch-q1-pricing-summary-report]
                (letfn [(test-node [k ^core2.core.Node node]
                          (log/info "awaiting" k "node")
                          (c2/with-db [db node {:tx last-tx, :timeout (Duration/ofHours 1)}]
                            (log/info "rows:"
                                      (->> (c2/plan-ra query (merge {'$ db} (::tpch/params (meta query))))
                                           (sequence)
                                           (count)))))]
                  (doseq [[k node] {:primary primary-node
                                    :secondary1 secondary-node1
                                    :secondary2 secondary-node2
                                    :secondary3 secondary-node3}]
                    (test-node k node))

                  (.finishChunk ^core2.indexer.Indexer (.indexer primary-node))
                  (.awaitSnapshotBuild ^core2.temporal.TemporalManagerPrivate (::temporal/temporal-manager @(:!system primary-node)))

                  (log/info "Starting post finish-chunk node")
                  (with-open [^core2.core.Node secondary-node4 (start-node)]
                    (test-node :secondary4 secondary-node4)))))))

        (finally
          (future-cancel !last-tx))))))

(comment
  (let [node-dir (Files/createTempDirectory "multinode-0.1" (make-array FileAttribute 0))
        node-opts {:core2.log/local-directory-log {:root-path (.resolve node-dir "log")}
                   :core2.object-store/file-system-object-store {:root-path (.resolve node-dir "objects")}}]
    (run-multinode {:scale-factor 0.1, :sleep-ms 60000}
                   (fn []
                     (c2/start-node node-opts)))))

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
