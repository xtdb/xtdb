(ns crux.main.graal
  (:require [crux.bootstrap.cli :as cli]
            [crux.bootstrap :as b]
            [crux.kv.memdb]
            [crux.kv.rocksdb]
            [clojure.tools.logging :as log])
  (:gen-class))

(defn -main [& args]
  (with-open [kv (b/start-kv-store {:db-dir "graal-data" :kv-backend "crux.kv.rocksdb.RocksKv"})]
    (log/info "Starting Crux native image" (pr-str kv))))
