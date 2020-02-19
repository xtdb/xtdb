(ns crux.main.graal
  (:require [clojure.tools.logging :as log]
            [crux.standalone :as sa]
            [crux.node :as n]
            [crux.io :as cio]
            [crux.kv.memdb]
            [crux.kv.rocksdb])
  (:import crux.api.ICruxAPI)
  (:gen-class))

(defn -main [& args]
  (with-open [node ^ICruxAPI (n/start sa/topology {:crux.kv/db-dir "graal-data"
                                                   :crux.node/kv-store 'crux.kv.rocksdb/kv})]
    (log/info "Starting Crux native image" (cio/pr-edn-str (.status node)))))
