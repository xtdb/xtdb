(ns crux.api
  "Public API of Crux."
  (:refer-clojure :exclude [sync])
  (:require [clojure.spec.alpha :as s]
            [crux.codec :as c]
            [crux.query-state :as qs]
            [crux.system :as sys]
            [clojure.tools.logging :as log])
  (:import (crux.api Crux ICruxAPI ICruxIngestAPI
                     ICruxAsyncIngestAPI ICruxDatasource
                     HistoryOptions HistoryOptions$SortOrder RemoteClientOptions)
           java.lang.AutoCloseable
           (java.util Date)
           java.time.Duration
           [java.util.function Supplier Consumer]))

(s/def :crux.db/id c/valid-id?)
(s/def :crux.db/evicted? boolean?)
(s/def :crux.db.fn/args (s/coll-of any? :kind vector?))

(s/def :crux.db/fn
  (s/cat :fn #{'fn}
         :args (s/coll-of symbol? :kind vector? :min-count 1)
         :body (s/* any?)))

(defn- conform-tx-ops [tx-ops]
  (->> tx-ops
       (mapv
        (fn [tx-op]
          (map #(if (instance? java.util.Map %) (into {} %) %)
               tx-op)))
       (mapv vec)))

(defprotocol DBProvider
  (db
    ^crux.api.ICruxDatasource [node]
    ^crux.api.ICruxDatasource [node ^Date valid-time]
    ^crux.api.ICruxDatasource [node ^Date valid-time ^Date transaction-time]
    "When a valid time is specified then returned db value contains only those
  documents whose valid time is before the specified time.

  When both valid and transaction time are specified returns a db value as of
  the valid time and the latest transaction time indexed at or before the
  specified transaction time.

  If the node hasn't yet indexed a transaction at or past the given
  transaction-time, this throws NodeOutOfSyncException")

  (open-db
    ^crux.api.ICruxDatasource [node]
    ^crux.api.ICruxDatasource [node ^Date valid-time]
    ^crux.api.ICruxDatasource [node ^Date valid-time ^Date transaction-time]
    "When a valid time is specified then returned db value contains only those
  documents whose valid time is before the specified time.

  When both valid and transaction time are specified returns a db value as of
  the valid time and the latest transaction time indexed at or before the
  specified transaction time.

  If the node hasn't yet indexed a transaction at or past the given
  transaction-time, this throws NodeOutOfSyncException

  This DB opens up shared resources to make multiple requests faster - it must
  be `.close`d when you've finished using it (for example, in a `with-open`
  block)"))

(defprotocol TransactionFnContext
  (indexing-tx [tx-fn-ctx]))

(defprotocol PCruxNode
  "Provides API access to Crux."
  (status [node]
    "Returns the status of this node as a map.")

  (tx-committed? [node submitted-tx]
    "Checks if a submitted tx was successfully committed.
     submitted-tx must be a map returned from `submit-tx`.
     Returns true if the submitted transaction was committed,
     false if the transaction was not committed, and throws `NodeOutOfSyncException`
     if the node has not yet indexed the transaction.")

  (sync
    [node]
    [node ^Duration timeout]
    "Blocks until the node has caught up indexing to the latest tx available at
  the time this method is called. Will throw an exception on timeout. The
  returned date is the latest transaction time indexed by this node. This can be
  used as the second parameter in (db valid-time, transaction-time) for
  consistent reads.

  timeout – max time to wait, can be nil for the default.
  Returns the latest known transaction time.")

  (await-tx-time
    [node ^Date tx-time]
    [node ^Date tx-time ^Duration timeout]
    "Blocks until the node has indexed a transaction that is past the supplied
  txTime. Will throw on timeout. The returned date is the latest index time when
  this node has caught up as of this call.")

  (await-tx
    [node tx]
    [node tx ^Duration timeout]
    "Blocks until the node has indexed a transaction that is at or past the
  supplied tx. Will throw on timeout. Returns the most recent tx indexed by the
  node.")

  (listen ^java.lang.AutoCloseable [node event-opts f]
    "Attaches a listener to Crux's event bus.

  `event-opts` should contain `:crux/event-type`, along with any other options the event-type requires.

  We currently only support one public event-type: `:crux/indexed-tx`.
  Supplying `:with-tx-ops? true` will include the transaction's operations in the event passed to `f`.

  `(.close ...)` the return value to detach the listener.

  This is an experimental API, subject to change.")

  (latest-completed-tx [node]
    "Returns the latest transaction to have been indexed by this node.")

  (latest-submitted-tx [node]
    "Returns the latest transaction to have been submitted to this cluster")

  (attribute-stats [node]
    "Returns frequencies map for indexed attributes")

  (active-queries [node]
    "Returns a list of currently running queries")

  (recent-queries [node]
    "Returns a list of recently completed/failed queries")

  (slowest-queries [node]
    "Returns a list of slowest completed/failed queries ran on the node"))

(defprotocol PCruxIngestClient
  "Provides API access to Crux ingestion."
  (submit-tx [node tx-ops]
    "Writes transactions to the log for processing
  tx-ops datalog style transactions.
  Returns a map with details about the submitted transaction,
  including tx-time and tx-id.")

  (open-tx-log ^java.io.Closeable [this after-tx-id with-ops?]
    "Reads the transaction log. Optionally includes
  operations, which allow the contents under the :crux.api/tx-ops
  key to be piped into (submit-tx tx-ops) of another
  Crux instance.

  after-tx-id      optional transaction id to start after.
  with-ops?       should the operations with documents be included?

  Returns an iterator of the TxLog"))

(defn- ->HistoryOptions [sort-order
                         {:keys [with-corrections? with-docs? start end]
                          :or {with-corrections? false, with-docs false}}]
  (HistoryOptions. (case sort-order
                     :asc HistoryOptions$SortOrder/ASC
                     :desc HistoryOptions$SortOrder/DESC)
                   (boolean with-corrections?)
                   (boolean with-docs?)
                   (:crux.db/valid-time start)
                   (:crux.tx/tx-time start)
                   (:crux.db/valid-time end)
                   (:crux.tx/tx-time end)))

(extend-protocol DBProvider
  ICruxAPI
  (db
    ([this] (.db this))
    ([this ^Date valid-time] (.db this valid-time))
    ([this ^Date valid-time ^Date transaction-time] (.db this valid-time transaction-time)))

  (open-db
    ([this] (.openDB this))
    ([this ^Date valid-time] (.openDB this valid-time))
    ([this ^Date valid-time ^Date transaction-time] (.openDB this valid-time transaction-time))))

(extend-protocol PCruxNode
  ICruxAPI
  (status [this] (.status this))

  (tx-committed? [this submitted-tx] (.hasTxCommitted this submitted-tx))

  (sync
    ([this] (.sync this nil))
    ([this timeout] (.sync this timeout))

    ([this tx-time timeout]
     (defonce warn-on-deprecated-sync
       (log/warn "(sync tx-time <timeout?>) is deprecated, replace with either (await-tx-time tx-time <timeout?>) or, preferably, (await-tx tx <timeout?>)"))
     (.awaitTxTime this tx-time timeout)))

  (await-tx
    ([this submitted-tx] (await-tx this submitted-tx nil))
    ([this submitted-tx timeout] (.awaitTx this submitted-tx timeout)))

  (await-tx-time
    ([this tx-time] (await-tx-time this tx-time nil))
    ([this tx-time timeout] (.awaitTxTime this tx-time timeout)))

  (listen [this event-opts f]
    (.listen this event-opts (reify Consumer
                               (accept [_ evt]
                                 (f evt)))))

  (latest-completed-tx [node] (.latestCompletedTx node))
  (latest-submitted-tx [node] (.latestSubmittedTx node))

  (attribute-stats [this] (.attributeStats this))

  (active-queries [this]
    (map qs/<-QueryState (.activeQueries this)))

  (recent-queries [this]
    (map qs/<-QueryState (.recentQueries this)))

  (slowest-queries [this]
    (map qs/<-QueryState (.slowestQueries this))))

(extend-protocol PCruxIngestClient
  ICruxIngestAPI
  (submit-tx [this tx-ops]
    (.submitTx this (conform-tx-ops tx-ops)))

  (open-tx-log ^crux.api.ICursor [this after-tx-id with-ops?]
    (.openTxLog this after-tx-id (boolean with-ops?))))

(defprotocol PCruxDatasource
  "Represents the database as of a specific valid and
  transaction time."

  (entity [db eid]
    "queries a document map for an entity.
  eid is an object which can be coerced into an entity id.
  returns the entity document map.")

  (entity-tx [db eid]
    "returns the transaction details for an entity. Details
  include tx-id and tx-time.
  eid is an object that can be coerced into an entity id.")

  (q [db query]
    "q[uery] a Crux db.
  query param is a datalog query in map, vector or string form.
  This function will return a set of result tuples if you do not specify `:order-by`, `:limit` or `:offset`;
  otherwise, it will return a vector of result tuples.")

  (open-q ^crux.api.ICursor [db query]
    "lazily q[uery] a Crux db.
  query param is a datalog query in map, vector or string form.

  This function returns a Cursor of result tuples - once you've consumed
  as much of the sequence as you need to, you'll need to `.close` the sequence.
  A common way to do this is using `with-open`:

  (with-open [res (crux/open-q db '{:find [...]
                                    :where [...]})]
    (doseq [row (iterator-seq res)]
      ...))

  Once the sequence is closed, attempting to iterate it is undefined.")

  (entity-history
    [db eid sort-order]
    [db eid sort-order opts]
    "Eagerly retrieves entity history for the given entity.

  Options:
  * `sort-order` (parameter): `#{:asc :desc}`
  * `:with-docs?`: specifies whether to include documents in the entries
  * `:with-corrections?`: specifies whether to include bitemporal corrections in the sequence, sorted first by valid-time, then transaction-time.
  * `:start` (nested map, inclusive, optional): the `:crux.db/valid-time` and `:crux.tx/tx-time` to start at.
  * `:end` (nested map, exclusive, optional): the `:crux.db/valid-time` and `:crux.tx/tx-time` to stop at.

  No matter what `:start` and `:end` parameters you specify, you won't receive
  results later than the valid-time and transact-time of this DB value.

  Each entry in the result contains the following keys:
   * `:crux.db/valid-time`,
   * `:crux.db/tx-time`,
   * `:crux.tx/tx-id`,
   * `:crux.db/content-hash`
   * `:crux.db/doc` (see `with-docs?`).")

  (open-entity-history
    ^crux.api.ICursor [db eid sort-order]
    ^crux.api.ICursor [db eid sort-order opts]
    "Lazily retrieves entity history for the given entity.
  Don't forget to close the cursor when you've consumed enough history!
  See `entity-history` for all the options")

  (valid-time [db]
    "returns the valid time of the db.
  If valid time wasn't specified at the moment of the db value retrieval
  then valid time will be time of the latest transaction.")

  (transaction-time [db]
    "returns the time of the latest transaction applied to this db value.
  If a tx time was specified when db value was acquired then returns
  the specified time.")

  (with-tx [db tx-ops]
    "Returns a new db value with the tx-ops speculatively applied.
  The tx-ops will only be visible in the value returned from this function - they're not submitted to the cluster, nor are they visible to any other database value in your application.
  If the transaction doesn't commit (eg because of a failed 'match'), this function returns nil."))

(let [arglists '(^crux.api.ICursor
                 [db eid sort-order]
                 ^crux.api.ICursor
                 [db eid sort-order {:keys [with-docs? with-corrections?]
                                     {start-vt :crux.db/valid-time, start-tt :crux.tx/tx-time} :start
                                     {end-vt :crux.db/valid-time, end-tt :crux.tx/tx-time} :end}])]
  (alter-meta! #'entity-history assoc :arglists arglists)
  (alter-meta! #'open-entity-history assoc :arglists arglists))

(extend-protocol PCruxDatasource
  ICruxDatasource
  (entity [this eid] (.entity this eid))
  (entity-tx [this eid] (.entityTx this eid))

  (q [this query] (.query this query))
  (open-q [this query] (.openQuery this query))

  (entity-history
    ([this eid sort-order] (entity-history this eid sort-order {}))
    ([this eid sort-order opts] (.entityHistory this eid (->HistoryOptions sort-order opts))))

  (open-entity-history
    ([this eid sort-order] (open-entity-history this eid sort-order {}))
    ([this eid sort-order opts] (.openEntityHistory this eid (->HistoryOptions sort-order opts))))

  (valid-time [this] (.validTime this))
  (transaction-time [this] (.transactionTime this))

  (with-tx [this tx-ops] (.withTx this tx-ops)))

(defprotocol PCruxAsyncIngestClient
  "Provides API access to Crux async ingestion."
  (submit-tx-async [node tx-ops]
    "Writes transactions to the log for processing tx-ops datalog
  style transactions. Non-blocking.  Returns a deref with map with
  details about the submitted transaction, including tx-time and
  tx-id."))

(extend-protocol PCruxAsyncIngestClient
  ICruxAsyncIngestAPI
  (submit-tx-async [this tx-ops]
    (.submitTxAsync this (conform-tx-ops tx-ops))))

(defn start-node
  "NOTE: requires any dependencies on the classpath that the Crux modules may need.

  Accepts a map, or a JSON/EDN file or classpath resource.

  See https://opencrux.com/reference/configuration.html for details.

  Returns a node which implements ICruxAPI and java.io.Closeable.
  Latter allows the node to be stopped by calling `(.close node)`.

  Throws IndexVersionOutOfSyncException if the index needs rebuilding."
  ^crux.api.ICruxAPI [options]
  (let [system (-> (sys/prep-system (into [{:crux/node 'crux.node/->node
                                            :crux/indexer 'crux.kv.indexer/->kv-indexer
                                            :crux/bus 'crux.bus/->bus
                                            :crux/tx-ingester 'crux.tx/->tx-ingester
                                            :crux/document-store 'crux.kv.document-store/->document-store
                                            :crux/tx-log 'crux.kv.tx-log/->tx-log
                                            :crux/query-engine 'crux.query/->query-engine}]
                                          (cond-> options (not (vector? options)) vector)))
                   (sys/start-system))]
    (reset! (get-in system [:crux/node :!system]) system)
    (-> (:crux/node system)
        (assoc :close-fn #(.close ^AutoCloseable system)))))

(defn- ->RemoteClientOptions [{:keys [->jwt-token] :as opts}]
  (RemoteClientOptions. (when ->jwt-token
                          (reify Supplier
                            (get [_] (->jwt-token))))))

(defn new-api-client
  "Creates a new remote API client ICruxAPI.
  The remote client requires valid and transaction time to be specified for all calls to `db`.

  NOTE: Requires either clj-http or http-kit on the classpath,
  See https://opencrux.com/reference/http.html for more information.

  url the URL to a Crux HTTP end-point.
  (OPTIONAL) auth-supplier a supplier function which provides an auth token string for the Crux HTTP end-point.

  returns a remote API client."
  (^ICruxAPI [url]
   (Crux/newApiClient url))
  (^ICruxAPI [url opts]
   (Crux/newApiClient url (->RemoteClientOptions opts))))

(defn new-ingest-client
  "Starts an ingest client for transacting into Crux without running a full local node with index.

  Accepts a map, or a JSON/EDN file or classpath resource.

  See https://opencrux.com/reference/configuration.html for details.

  Returns a crux.api.ICruxIngestAPI component that implements java.io.Closeable.
  Latter allows the node to be stopped by calling `(.close node)`."
  ^ICruxAsyncIngestAPI [options]
  (Crux/newIngestClient options))
