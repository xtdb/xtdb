(ns restore
  (:require [crux.backup :as backup]
            [clojure.pprint :as pp]))

(defn -main []
  (println "restore script")
  (backup/restore
    {:db-dir "data/db-dir-1"
     :backup-dir "checkpoint"})
  (System/exit 0))
