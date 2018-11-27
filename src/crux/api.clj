(ns crux.api
  (:require [clojure.tools.logging :as log]
            [crux.bootstrap :as bootstrap]
            [crux.db :as db]
            [crux.kv-store :as ks]
            [crux.query :as q])
  (:import java.io.Closeable
           crux.query.QueryDatasource))

(defprotocol CruxDatasource
  (doc [this eid]
    "returns the document for an entity")
  (entity-tx [this eid]
    "returns the entity tx for an entity")
  (new-snapshot [this snapshot]
    "returns a new snapshot for q, allowing lazy results in a with-open block")
  (q [this q] [this snapshot q]
    "queries the db"))

(extend-protocol CruxDatasource
  QueryDatasource
  (doc [this eid]
    (q/doc this eid))

  (entity-tx [this eid]
    (q/entity-tx this eid))

  (new-snapshot [this]
    (ks/new-snapshot (:kv this)))

  (q [this q]
    (q/q this q))

  (q [this snapshot q]
    (q/q this snapshot q)))

(defprotocol CruxSystem
  (db [this] [this business-time] [this business-time transact-time]
    "returns a db for the system")
  (submit-tx [this data]
    "writes the transactions to the log for processing")
  (submitted-tx-updated-entity? [this submitted-tx eid]
    "checks if a submitted tx did update an entity"))

(defrecord ApiSystem [close-promise underlying]
  CruxSystem
  (db [_]
    (q/db (:kv-store @underlying)))

  (db [_ business-time]
    (q/db (:kv-store @underlying) business-time))

  (db [_ business-time transact-time]
    (q/db (:kv-store @underlying) business-time transact-time))

  (submit-tx [_ tx-ops]
    (db/submit-tx (:tx-log @underlying) tx-ops))

  (submitted-tx-updated-entity? [_ submitted-tx eid]
    (q/submitted-tx-updated-entity? (:kv-store @underlying) submitted-tx eid))

  Closeable
  (close [_] (deliver close-promise true)))

(defn q-maps
  [db query]
  (for [item (q/q db query)]
    (into
      {}
      (for [[k v] (meta item)] [(keyword k) (:value v)]))))

(defn ^Closeable start-system
  [options]
  (log/info "running crux in library mode")
  (let [underlying (atom nil)
        close-promise (promise)
        started-promise (promise)
        options (merge bootstrap/default-options options)
        running-future
        (future
          (log/info "crux thread intialized")
          (bootstrap/start-system
            options
            (fn with-system-callback [system]
              (deliver started-promise true)
              (log/info "crux system start completed")
              (reset! underlying system)
              @close-promise
              (log/info "starting teardown of crux system")))
          (log/info "crux system completed teardown"))]
    (while (not (or (deref started-promise 100 false)
                    (deref running-future 100 false))))
    (->ApiSystem close-promise underlying)))
