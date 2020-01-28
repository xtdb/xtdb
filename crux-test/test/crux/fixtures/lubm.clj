(ns crux.fixtures.lubm
  (:require [crux.fixtures.api :refer [*node*]]
            [crux.api :as api]
            [crux.rdf :as rdf]))

(def ^:const lubm-triples-resource-8k "lubm/University0_0.ntriples")
(def ^:const lubm-triples-resource-100k "lubm/lubm10.ntriples")

(defn with-lubm-data [f]
  (let [last-tx (->> (concat (rdf/->tx-ops (rdf/ntriples "lubm/univ-bench.ntriples"))
                             (rdf/->tx-ops (rdf/ntriples lubm-triples-resource-8k)))
                     (rdf/->default-language)
                     (partition-all 1000)
                     (reduce (fn [_ tx-ops]
                               (api/submit-tx *node* (vec tx-ops)))
                             nil))]

    (api/await-tx *node* last-tx)
    (f)))
