; load a repl with the latest Crux dependency, e.g. using clj:
; $ clj -Sdeps '{:deps {juxt/crux {:mvn/version "19.07-1.1.1-alpha"}}}'

(ns walkthrough.crux-standalone
  (:require [crux.api :as crux])
  (:import (crux.api ICruxAPI)))

; this standalone configuration is the easiest way to try Crux, no Kafka needed


(def crux-options
  {:kv-backend "crux.kv.memdb.MemKv" ; in-memory, see docs for LMDB/RocksDB storage
   :event-log-dir     "data/event-log-dir-1" ; :event-log-dir is ignored when using MemKv
   :db-dir     "data/db-dir-1"}) ; :db-dir is ignored when using MemKv


(def system (crux/start-standalone-system crux-options))


; transaction containing a `put` operation, optionally specifying a valid time
(crux/submit-tx
  system
  [[:crux.tx/put
    {:crux.db/id :dbpedia.resource/Pablo-Picasso ; id
     :name "Pablo"
     :last-name "Picasso"
     :location "Spain"}
    #inst "1881-10-25T09:20:27.966-00:00"]]) ; valid time, Picasso's birth


; transaction containing a `cas` (compare-and-swap) operation
(crux/submit-tx
  system
  [[:crux.tx/cas
    {:crux.db/id :dbpedia.resource/Pablo-Picasso ; old version
     :name "Pablo"
     :last-name "Picasso"
     :location "Spain"}
    {:crux.db/id :dbpedia.resource/Pablo-Picasso ; new version
     :name "Pablo"
     :last-name "Picasso"
     :height 1.63
     :location "France"}
    #inst "1973-04-08T09:20:27.966-00:00"]]) ; valid time, Picasso's death


; transaction containing a `delete` operation, historical versions remain
(crux/submit-tx
  system
  [[:crux.tx/delete :dbpedia.resource/Pablo-Picasso
    #inst "1973-04-08T09:20:27.966-00:00"]])


; transaction containing an `evict` operation, historical data is destroyed
(crux/submit-tx
  system
  [[:crux.tx/evict :dbpedia.resource/Pablo-Picasso
    #inst "1973-04-07T09:20:27.966-00:00" ; start-valid-time
    #inst "1973-04-09T09:20:27.966-00:00" ; end-valid-time (optional)
    false                                 ; keep-latest? (optional)
    true]])                               ; keep-earliest? (optional)


; query the system as-of now
(crux/q
  (crux/db system)
  '{:find [e]
    :where [[e :name "Pablo"]]
    :full-results? true}) ; using `:full-results?` is useful for manual queries


; query the system as-of now, as-at #inst "1973-04-07T09:20:27.966-00:00"
(crux/q
  (crux/db system #inst "1973-04-07T09:20:27.966-00:00")
  '{:find [e]
    :where [[e :name "Pablo"]]
    :full-results? true})


; `put` the new version of the document again
(crux/submit-tx
  system
  [[:crux.tx/put
    {:crux.db/id :dbpedia.resource/Pablo-Picasso
     :name "Pablo"
     :last-name "Picasso"
     :height 1.63
     :location "France"}
    #inst "1973-04-08T09:20:27.966-00:00"]])


; again, query the system as-of now
(crux/q
  (crux/db system)
  '{:find [e]
    :where [[e :name "Pablo"]]
    :full-results? true})


; again, query the system as-of now, as-at #inst "1973-04-07T09:20:27.966-00:00"
(crux/q
  (crux/db system #inst "1973-04-07T09:20:27.966-00:00")
  '{:find [e]
    :where [[e :name "Pablo"]]
    :full-results? true})


(comment
  ; use the following to help when not starting the system from the REPL 

  (defn run-system [{:keys [server-port] :as options} with-system-fn]
    (with-open [crux-system (crux/start-standalone-system options)]
      (with-system-fn crux-system)))

  (declare s system)

  ; run a system and return control to the REPL
  (def ^ICruxAPI s
    (future
      (run-system
        crux-options
        (fn [crux-system]
          (def system crux-system)
          (Thread/sleep Long/MAX_VALUE)))))

  ; close the system by cancelling the future
  (future-cancel s)

  ; ...or close the system directly
  (.close system)

)
