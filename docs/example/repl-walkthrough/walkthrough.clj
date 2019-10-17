; load a repl with the latest crux-core dependency, e.g. using clj:
; $ clj -Sdeps '{:deps {juxt/crux-core {:mvn/version "RELEASE"}}}'

(ns walkthrough.crux-standalone
  (:require [crux.api :as crux])
  (:import (crux.api ICruxAPI)))

; this standalone configuration is the easiest way to try Crux, no Kafka needed


(def crux-options
  {:crux.node/topology :crux.standalone/topology
   :crux.node/kv-store "crux.kv.memdb/kv" ; in-memory, see docs for LMDB/RocksDB storage
   :crux.standalone/event-log-kv-store "crux.kv.memdb/kv" ; same as above
   :crux.standalone/event-log-dir "data/event-log-dir-1" ; :event-log-dir is ignored when using MemKv
   :crux.kv/db-dir "data/db-dir-1"}) ; :db-dir is ignored when using MemKv


(def node (crux/start-node crux-options))


; transaction containing a `put` operation, optionally specifying a valid time
(crux/submit-tx
  node
  [[:crux.tx/put
    {:crux.db/id :dbpedia.resource/Pablo-Picasso ; id
     :name "Pablo"
     :last-name "Picasso"
     :location "Spain"}
    #inst "1881-10-25T09:20:27.966-00:00"]
   [:crux.tx/put
    {:crux.db/id :dbpedia.resource/Pablo-Picasso ; id
     :name "Pablo"
     :last-name "Picasso"
     :location "Sain2"}
    #inst "1881-10-25T09:20:27.966-00:00"]]) ; valid time, Picasso's birth


; transaction containing a `cas` (compare-and-swap) operation
(crux/submit-tx
  node
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
  node
  [[:crux.tx/delete :dbpedia.resource/Pablo-Picasso
    #inst "1973-04-08T09:20:27.966-00:00"]])


; transaction containing an `evict` operation, historical data is destroyed
(crux/submit-tx
  node
  [[:crux.tx/evict :dbpedia.resource/Pablo-Picasso
    #inst "1973-04-07T09:20:27.966-00:00" ; start-valid-time
    #inst "1973-04-09T09:20:27.966-00:00" ; end-valid-time (optional)
    false                                 ; keep-latest? (optional)
    true]])                               ; keep-earliest? (optional)


; query the node as-of now
(crux/q
  (crux/db node)
  '{:find [e]
    :where [[e :name "Pablo"]]
    :full-results? true}) ; using `:full-results?` is useful for manual queries


; query the node as-of now, as-at #inst "1973-04-07T09:20:27.966-00:00"
(crux/q
  (crux/db node #inst "1973-04-07T09:20:27.966-00:00")
  '{:find [e]
    :where [[e :name "Pablo"]]
    :full-results? true})


; `put` the new version of the document again
(crux/submit-tx
  node
  [[:crux.tx/put
    {:crux.db/id :dbpedia.resource/Pablo-Picasso
     :name "Pablo"
     :last-name "Picasso"
     :height 1.63
     :location "France"}
    #inst "1973-04-08T09:20:27.966-00:00"]])


; again, query the node as-of now
(crux/q
  (crux/db node)
  '{:find [e]
    :where [[e :name "Pablo"]]
    :full-results? true})


; again, query the node as-of now, as-at #inst "1973-04-07T09:20:27.966-00:00"
(crux/q
  (crux/db node #inst "1973-04-07T09:20:27.966-00:00")
  '{:find [e]
    :where [[e :name "Pablo"]]
    :full-results? true})


(comment
  ; use the following to help when not starting the node from the REPL

  (defn run-node [{:keys [server-port] :as options} with-node-fn]
    (with-open [crux-node (crux/start-node options)]
      (with-node-fn crux-node)))

  (declare s node)

  ; run a node and return control to the REPL
  (def ^ICruxAPI s
    (future
      (run-node
        crux-options
        (fn [crux-node]
          (def node crux-node)
          (Thread/sleep Long/MAX_VALUE)))))

  ; close the node by cancelling the future
  (future-cancel s)

  ; ...or close the node directly
  (.close node)

)
