(ns crux.api
  "Public API of Crux. For documentation, see the JavaDoc."
  (:refer-clojure :exclude [sync])
  (:import [crux.api Crux ICruxAPI ICruxDatasource]
           java.io.Closeable
           java.util.Date
           java.time.Duration))

(defprotocol PCruxSystem
  (db
    [this]
    [this ^Date valid-time]
    [this ^Date valid-time ^Date transaction-time])

  (document [this content-hash])

  (history [this eid])

  (history-range [this eid ^Date valid-time-start ^Date transaction-time-start ^Date valid-time-end ^Date transaction-time-end])

  (status [this])

  (submit-tx [this tx-ops]
    "Writes transactions to the log for processing")

  (submitted-tx-updated-entity? [this submitted-tx eid])

  (submitted-tx-corrected-entity? [this submitted-tx ^Date valid-time eid])

  (sync [this ^Duraction duration])

  (new-tx-log-context ^java.io.Closeable [this])

  (tx-log [this tx-log-context from-tx-id with-documents?]))

(extend-protocol PCruxSystem
  ICruxAPI
  (db
    ([this]
     (.db this))
    ([this ^Date valid-time]
     (.db this valid-time))
    ([this ^Date valid-time ^Date transaction-time]
     (.db this valid-time transaction-time)))

  (document [this content-hash]
    (.document this content-hash))

  (history [this eid]
    (.history this eid))

  (history-range [this eid valid-time-start transaction-time-start valid-time-end transaction-time-end]
    (.historyRange this eid valid-time-start transaction-time-start valid-time-end transaction-time-end))

  (status [this]
    (.status this))

  (submit-tx [this tx-ops]
    (.submitTx this tx-ops))

  (submitted-tx-updated-entity? [this submitted-tx eid]
    (.hasSubmittedTxUpdatedEntity this submitted-tx eid))

  (submitted-tx-corrected-entity? [this submitted-tx ^Date valid-time eid]
    (.hasSubmittedTxCorrectedEntity this submitted-tx valid-time eid))

  (sync [this timeout]
    (.sync this timeout))

  (new-tx-log-context ^java.io.Closeable [this]
    (.newTxLogContext this))

  (tx-log [this tx-log-context from-tx-id with-documents?]
    (.txLog this tx-log-context from-tx-id with-documents?)))

(defprotocol PCruxDatasource
  (entity [this eid])

  (entity-tx [this eid])

  (new-snapshot ^java.io.Closeable [this])

  (q
    [this query]
    [this snapshot query])

  (history-ascending [this snapshot eid])

  (history-descending [this snapshot eid])

  (valid-time [this])

  (transaction-time [this]))

(extend-protocol PCruxDatasource
  ICruxDatasource
  (entity [this eid]
    (.entity this eid))

  (entity-tx [this eid]
    (.entityTx this eid))

  (new-snapshot [this]
    (.newSnapshot this))

  (q
    ([this query]
     (.q this query))
    ([this snapshot query]
     (.q this snapshot query)))

  (history-ascending [this snapshot eid]
    (.historyAscending this snapshot eid))

  (history-descending [this snapshot eid]
    (.historyDescending this snapshot eid))

  (valid-time [this]
    (.validTime this))

  (transaction-time [this]
    (.transactionTime this)))

(defn start-cluster-node ^ICruxAPI [options]
  (Crux/startClusterNode options))

(defn start-standalone-system ^ICruxAPI [options]
  (Crux/startStandaloneSystem options))

(defn new-api-client ^ICruxAPI [url]
  (Crux/newApiClient url))
