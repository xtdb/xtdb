(ns example-backup-restore.main
  (:require [crux.api :as api]
            [crux.backup :as backup]))

(def crux-options
  {:crux.node/topology :crux.standalone/topology
   :crux.node/kv-store "crux.kv.rocksdb/kv"
   :crux.standalone/event-log-dir "data/eventlog-1"
   :crux.kv/db-dir "data/db-dir-1"
   :backup-dir "checkpoint"})

(def syst (api/start-node crux-options))


(comment

  (backup/restore crux-options)

  (backup/backup crux-options syst)

  (backup/check-and-restore crux-options)

  (api/valid-time (api/db syst #inst "2019-02-02"))

  (api/history syst :id/jeff)

  (api/entity (api/db syst) :id/jeff)

  (api/document syst "48ea7b320721dbdbd09c5e6be1486b0e76369b6a")

  (api/submit-tx
           syst
           [[:crux.tx/put
             {:crux.db/id :id/jeff
              :person/name "Jeff"}]
            [:crux.tx/put
             {:crux.db/id :id/lia
              :person/name "Lia"}]])

  (api/transaction-time
    (api/db syst #inst "2019-02-02"
            #inst "2019-04-16T12:35:05.042-00:00"))

  (api/document
    (api/db syst #inst "2019-02-02"
            #inst "2019-04-16T12:35:05.042-00:00")))
