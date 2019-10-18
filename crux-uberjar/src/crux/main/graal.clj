(ns crux.main.graal
  (:require [clojure.tools.logging :as log]
            [crux.standalone :as sa]
            [crux.node :as n]
            [crux.kv.memdb]
            [crux.kv.rocksdb])
  (:gen-class))

(defn -main [& args]
  (with-open [node (n/start sa/topology {:crux.kv/db-dir "graal-data"
                                         :crux.standalone/event-log-dir "graal-event-log"
                                         :crux.node/kv-store "crux.kv.rocksdb/kv"})]
    (log/info "Starting Crux native image" (pr-str (.status node)))))
