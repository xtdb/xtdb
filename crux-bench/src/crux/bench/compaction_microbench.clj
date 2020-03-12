(ns crux.bench.compaction-microbench
  (:require [clojure.java.io :as io]
            [crux.api :as crux]
            [crux.bench :as bench]
            [crux.compaction :as cc]
            [crux.codec :as c])
  (:import java.util.Calendar))

(defn submit-batches [node]
  (for [doc-batch (->> (for [eid (map #(keyword (str "doc-" %)) (range 1000))
                             v (range 20)
                             :let [vt (.getTime
                                       (doto (Calendar/getInstance)
                                         (.set Calendar/YEAR 2000)
                                         (.add Calendar/YEAR v)))]]
                         [:crux.tx/put {:crux.db/id eid :v v} vt])
                       (partition-all 1000))]
    (crux/submit-tx node (vec doc-batch))))

(defn submit-batches-no-compaction [node]
  (for [doc-batch (->> (for [eid (map #(keyword (str "doc-" %)) (range 1000))
                             v (range 20)]
                         [:crux.tx/put {:crux.db/id eid :v v}])
                       (partition-all 1000))]
    (crux/submit-tx node (vec doc-batch))))

(defn run-benches [node submitter [submit-bench-type await-bench-type]]
  (bench/run-bench await-bench-type
    (let [submitted-tx (-> (bench/run-bench submit-bench-type
                             (-> {:success? true}
                                 (with-meta {:submitted-tx (last (submitter node))})))
                           meta
                           :submitted-tx)]

      (crux/await-tx node submitted-tx (java.time.Duration/ofSeconds 20))
      {:success? true})))

(comment
  (require '[crux.kafka.embedded :as ek]
           '[crux.fixtures :as f])

  (f/with-tmp-dir "crux" [tmp-dir]
    (with-open [ek (ek/start-embedded-kafka
                    #:crux.kafka.embedded{:zookeeper-data-dir (str (io/file tmp-dir "zk-data"))
                                          :kafka-dir (str (io/file tmp-dir "kafka-data"))
                                          :kafka-log-dir (str (io/file tmp-dir "kafka-log"))})
                node (crux/start-node {:crux.node/topology ['crux.kafka/topology ;cc/module
                                                            ]
                                       :crux.node/kv-store 'crux.kv.rocksdb/kv
                                       :crux.kv/db-dir (str (io/file tmp-dir "rocks"))})]
      (bench/with-bench-ns :compaction
        (run-benches node submit-batches [:initial-submits :initial-await]))

      ;; (assert (not (.document node (c/new-id {:crux.db/id :doc-0 :v 1}))))
      ;; (assert (not (.document node (c/new-id {:crux.db/id :doc-0 :v 18}))))
      ;; (assert (.document node (c/new-id {:crux.db/id :doc-0 :v 19})))

      (bench/with-bench-ns :no-compaction
        (run-benches node submit-batches-no-compaction [:initial-submits :initial-await])))))

;; 6 4 - no compaction module loaded
;; 8 7 - compaction module loaded

;; 2.5 / 6.5
;; next is to microbench around this specific fn? use the a
