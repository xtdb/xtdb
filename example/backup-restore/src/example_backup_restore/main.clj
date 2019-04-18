(ns example-backup-restore.main
  (:require [crux.api :as api]
            [crux.backup :as backup]))

(def crux-options
  {:kv-backend         "crux.kv.rocksdb.RocksKv"
   :event-log-dir      "data/eventlog-1"
   :db-dir             "data/db-dir-1"
   :backup-dir         "checkpoint"})

; (backup/restore crux-options)

(defonce syst (api/start-standalone-system crux-options))

; (println (api/entity (api/db syst) :id/bongo2))

(comment

  (backup/backup crux-options syst)

  (backup/check-and-restore crux-options)

  (api/valid-time (api/db syst #inst "2019-02-02"))

  (api/history syst :id/jeff)

  (api/entity (api/db syst) :id/jeff)

  (api/document syst "48ea7b320721dbdbd09c5e6be1486b0e76369b6a")

  (api/submit-tx
           syst
           [[:crux.tx/put :id/jeff
             {:crux.db/id :id/jeff
              :person/name "Jeff"}]
            [:crux.tx/put :id/lia
             {:crux.db/id :id/lia
              :person/name "Lia"}]])

  (api/transaction-time
    (api/db syst #inst "2019-02-02"
            #inst "2019-04-16T12:35:05.042-00:00"))

  (api/document
    (api/db syst #inst "2019-02-02"
            #inst "2019-04-16T12:35:05.042-00:00")))

