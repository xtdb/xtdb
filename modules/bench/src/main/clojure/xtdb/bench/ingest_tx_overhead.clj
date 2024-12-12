(ns xtdb.bench.ingest-tx-overhead
  (:require [xtdb.api :as xt]
            [xtdb.bench :as bench]
            [xtdb.node :as xtn]))

(defn benchmark [{:keys [seed doc-count], :or {seed 0, doc-count 100000}}]
  (letfn [(do-ingest [node table ^long per-batch]
            (doseq [batch (partition-all per-batch (range doc-count))]
              (xt/submit-tx node
                            [(into [:put-docs table]
                                   (map (fn [idx]
                                          {:xt/id idx}))
                                   batch)]))

            (let [[{actual :doc-count}] (xt/q node (format "SELECT COUNT(*) doc_count FROM %s" (name table)))]
              (assert (= actual doc-count)
                      (format "failed for %s: expected: %d, got: %d" (name table) doc-count actual))))]

    {:title "Ingest batch vs individual"
     :seed seed
     :tasks [{:t :call
              :stage :ingest-batch-1000
              :f (fn [{node :sut}]
                   (do-ingest node :batched_1000 1000))}

             {:t :call
              :stage :ingest-batch-100
              :f (fn [{node :sut}]
                   (do-ingest node :batched_100 100))}

             {:t :call
              :stage :ingest-batch-10
              :f (fn [{node :sut}]
                   (do-ingest node :batched_10 10))}

             {:t :call
              :stage :ingest-batch-1
              :f (fn [{node :sut}]
                   (do-ingest node :batched_1 1))}]}))

(comment
  (let [f (bench/compile-benchmark (benchmark {})
                                   @(requiring-resolve `xtdb.bench.measurement/wrap-task))]
    (with-open [node (xtn/start-node {:server {:port 0}})]
      (f node))

    #_
    (f dev/node)))
