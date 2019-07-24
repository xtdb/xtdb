(ns crux.main.graal
  (:require [clojure.tools.logging :as log]
            [crux.standalone :as sa]
            [crux.bootstrap :as b]
            [crux.kv.memdb]
            [crux.kv.rocksdb])
  (:gen-class))

(defn -main [& args]
  (with-open [node (b/start-node sa/node-config {:db-dir "graal-data"
                                                 :event-log-dir "graal-event-log"
                                                 :kv-backend "crux.kv.rocksdb.RocksKv"})]
    (log/info "Starting Crux native image" (pr-str (.status node)))))
