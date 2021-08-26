(ns crux.fixtures.lubm
  (:require [crux.fixtures :refer [*api*]]
            [crux.api :as xt]
            [xtdb.rdf :as rdf]))

(def ^:const lubm-triples-resource-8k "lubm/University0_0.ntriples")
(def ^:const lubm-triples-resource-100k "lubm/lubm10.ntriples")

(defn with-lubm-data [f]
  (let [last-tx (->> (concat (rdf/->tx-ops (rdf/ntriples "lubm/univ-bench.ntriples"))
                             (rdf/->tx-ops (rdf/ntriples lubm-triples-resource-8k)))
                     (rdf/->default-language)
                     (partition-all 1000)
                     (reduce (fn [_ tx-ops]
                               (xt/submit-tx *api* (vec tx-ops)))
                             nil))]

    (xt/await-tx *api* last-tx)
    (f)))
