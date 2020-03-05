(ns crux.bench.sorted-maps-microbench
  (:require [crux.api :as crux]
            [crux.bench :as bench]))

(defn submit-batches [node]
  (for [doc-batch (->> (for [n (range 25000)]
                         [:crux.tx/put {:crux.db/id (keyword (str "doc-" n))
                                        :nested-map {:foo :bar
                                                     :baz :quux
                                                     :doc-idx n}}])
                       (partition-all 5000))]
    (crux/submit-tx node doc-batch)))

(defn run-benches [node [submit-bench-type await-bench-type]]
  (bench/run-bench await-bench-type
    (let [submitted-tx (-> (bench/run-bench submit-bench-type
                             (-> {:success? true}
                                 (with-meta {:submitted-tx (last (submit-batches node))})))
                           meta
                           :submitted-tx)]

      (crux/await-tx node submitted-tx (java.time.Duration/ofSeconds 20))
      {:success? true})))

(defn run-sorted-maps-microbench [node]
  (bench/with-bench-ns :sorted-maps
    (run-benches node [:initial-submits :initial-await])
    (run-benches node [:subsequent-submits :subsequent-await])))
