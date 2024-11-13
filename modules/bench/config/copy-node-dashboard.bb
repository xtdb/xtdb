#!/usr/bin/env bb

(require '[clojure.string :as str])

(def src "../../../monitoring/grafana/dashboards/xtdb-node-debugging.json")
(def target "./dashboards/XTDB_node_metrics.json")

(->> (-> (slurp src) (str/replace "${DS_DS_XTDB}" "DS_XTDB"))
     (spit target))
