(ns crux.fixtures.lubm
  (:require [crux.fixtures.api :refer [*api*]]
            [crux.api :as api]
            [crux.rdf :as rdf]))

(def ^:const lubm-triples-resource-8k "lubm/University0_0.ntriples")
(def ^:const lubm-triples-resource-100k "lubm/lubm10.ntriples")

(defn with-lubm-data [f]
  (let [tx-ops (->> (concat (rdf/->tx-ops (rdf/ntriples "lubm/univ-bench.ntriples"))
                            (rdf/->tx-ops (rdf/ntriples lubm-triples-resource-8k)))
                    (rdf/->default-language)
                    vec)

        last-tx (->> (partition-all 1000 tx-ops)
                     (reduce (fn [_ tx-ops]
                               (api/submit-tx *api* (vec tx-ops)))
                             nil))]

    (api/sync *api* (:crux.tx/tx-time last-tx) nil)
    (f)))
