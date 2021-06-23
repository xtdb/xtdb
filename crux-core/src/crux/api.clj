(ns crux.api
  "Public API of Crux."
  (:refer-clojure :exclude [sync])
  (:require [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [crux.codec :as c]
            [crux.io :as cio]
            [crux.system :as sys])
  (:import [crux.api Crux RemoteClientOptions]
           java.lang.AutoCloseable
           java.time.Duration
           [java.util Date Map]
           java.util.function.Supplier))

(s/def :crux.db/id c/valid-id?)
(s/def :crux.db/evicted? boolean?)
(s/def :crux.db.fn/args (s/coll-of any? :kind vector?))

(s/def :crux.db/fn
  (s/cat :fn #{'fn}
         :args (s/coll-of symbol? :kind vector? :min-count 1)
         :body (s/* any?)))

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
    [node tx-time ^Duration timeout]
    "Blocks until the node has caught up indexing to the latest tx available at
  the time this method is called. Will throw an exception on timeout. The
  returned date is the latest transaction time indexed by this node. This can be
  used as the second parameter in (db valid-time transaction-time) for
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

  (q* [db query args]
    "q[uery] a Crux db.
  query param is a datalog query in map, vector or string form.
  This function will return a set of result tuples if you do not specify `:order-by`, `:limit` or `:offset`;
  otherwise, it will return a vector of result tuples.")

  (open-q* ^crux.api.ICursor [db query args]
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

  (pull [db query eid]
    "Returns the requested data for the given entity ID, based on the projection spec

  e.g. `(pull db [:film/name :film/year] :spectre)`
    => `{:film/name \"Spectre\", :film/year 2015}`

  See https://opencrux.com/reference/queries.html#pull for details of the spec format.")

  (pull-many [db query eids]
    "Returns the requested data for the given entity IDs, based on the projection spec

  e.g. `(pull-many db [:film/name :film/year] #{:spectre :skyfall})`
    => `[{:film/name \"Spectre\", :film/year 2015}, {:film/name \"Skyfall\", :film/year 2012}]`

  See https://opencrux.com/reference/queries.html#pull for details of the spec format.")

  (entity-history
    [db eid sort-order]
    [db eid sort-order opts]
    "Eagerly retrieves entity history for the given entity.

  Options:
  * `sort-order`: `#{:asc :desc}`
  * `:with-docs?` (boolean, default false): specifies whether to include documents in the entries under the `:crux.db/doc` key
  * `:with-corrections?` (boolean, default false): specifies whether to include bitemporal corrections in the sequence, sorted first by valid-time, then tx-id.
  * `:start-valid-time`, `:start-tx-time`, `:start-tx-id` (inclusive, default unbounded): bitemporal co-ordinates to start at
  * `:end-valid-time`, `:end-tx-time`, `:end-tx-id` (exclusive, default unbounded): bitemporal co-ordinates to stop at

  No matter what `:start-*` and `:end-*` parameters you specify, you won't receive results later than the valid-time and tx-id of this DB value.

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
  then valid time will be time of db value retrieval.")

  (transaction-time [db]
    "returns the time of the latest transaction applied to this db value.
  If a tx time was specified when db value was acquired then returns
  the specified time.")

  (db-basis [db]
    "returns the basis of this db snapshot - a map containing `:crux.db/valid-time` and `:crux.tx/tx`")

  (^java.io.Closeable with-tx [db tx-ops]
   "Returns a new db value with the tx-ops speculatively applied.
  The tx-ops will only be visible in the value returned from this function - they're not submitted to the cluster, nor are they visible to any other database value in your application.
  If the transaction doesn't commit (eg because of a failed 'match'), this function returns nil."))

(defprotocol PCruxAsyncIngestClient
  "Provides API access to Crux async ingestion."
  (submit-tx-async [node tx-ops]
    "Writes transactions to the log for processing tx-ops datalog
  style transactions. Non-blocking.  Returns a deref with map with
  details about the submitted transaction, including tx-time and
  tx-id."))

(defn start-node
  "NOTE: requires any dependencies on the classpath that the Crux modules may need.

  Accepts a map, or a JSON/EDN file or classpath resource.

  See https://opencrux.com/reference/configuration.html for details.

  Returns a node which implements: DBProvider, PCruxNode, PCruxIngestClient, PCruxAsyncIngestClient and java.io.Closeable.

  Latter allows the node to be stopped by calling `(.close node)`.

  Throws IndexVersionOutOfSyncException if the index needs rebuilding."
  ^java.io.Closeable [options]
  (let [system (-> (sys/prep-system (into [{:crux/node 'crux.node/->node
                                            :crux/index-store 'crux.kv.index-store/->kv-index-store
                                            :crux/bus 'crux.bus/->bus
                                            :crux/tx-ingester 'crux.tx/->tx-ingester
                                            :crux/document-store 'crux.kv.document-store/->document-store
                                            :crux/tx-log 'crux.kv.tx-log/->tx-log
                                            :crux/query-engine 'crux.query/->query-engine}]
                                          (cond-> options (not (vector? options)) vector)))
                   (sys/start-system))]
    (when (and (nil? @cio/malloc-arena-max)
               (cio/glibc?))
      (defonce warn-on-malloc-arena-max
        (log/warn "MALLOC_ARENA_MAX not set, memory usage might be high, recommended setting for Crux is 2")))
    (reset! (get-in system [:crux/node :!system]) system)
    (-> (:crux/node system)
        (assoc :close-fn #(.close ^AutoCloseable system)))))

(defn- ->RemoteClientOptions [{:keys [->jwt-token] :as opts}]
  (RemoteClientOptions. (when ->jwt-token
                          (reify Supplier
                            (get [_] (->jwt-token))))))

(defn new-api-client
  "Creates a new remote API client.

  This implements: DBProvider, PCruxNode, PCruxIngestClient and java.io.Closeable.

  The remote client requires valid and transaction time to be specified for all calls to `db`.

  NOTE: Requires either clj-http or http-kit on the classpath,
  See https://opencrux.com/reference/http.html for more information.

  url the URL to a Crux HTTP end-point.
  (OPTIONAL) auth-supplier a supplier function which provides an auth token string for the Crux HTTP end-point.

  returns a remote API client."
  (^java.io.Closeable [url]
   (:node (Crux/newApiClient url)))
  (^java.io.Closeable [url opts]
   (:node (Crux/newApiClient url (->RemoteClientOptions opts)))))

(defn new-ingest-client
  "Starts an ingest client for transacting into Crux without running a full local node with index.
  Accepts a map, or a JSON/EDN file or classpath resource.
  See https://opencrux.com/reference/configuration.html for details.
  Returns a component that implements java.io.Closeable, PCruxIngestClient and PCruxAsyncIngestClient.
  Latter allows the node to be stopped by calling `(.close node)`."
  ^java.io.Closeable [options]
  (:client (Crux/newIngestClient ^Map options)))

(defn conform-tx-ops [tx-ops]
  (->> tx-ops
       (mapv
        (fn [tx-op]
          (map #(if (instance? Map %) (into {} %) %)
               tx-op)))
       (mapv vec)))

(defprotocol DBProvider
  (db
    [node]
    [node valid-time-or-basis]
    ^:deprecated [node valid-time tx-time]
    "Returns a DB snapshot at the given time.

  db-basis: (optional map, all keys optional)
    - `:crux.db/valid-time` (Date):
        If provided, DB won't return any data with a valid-time greater than the given time.
        Defaults to now.
    - `:crux.tx/tx` (Map):
        If provided, DB will be a snapshot as of the given transaction.
        Defaults to the latest completed transaction.
    - `:crux.tx/tx-time` (Date):
        Shorthand for `{:crux.tx/tx {:crux.tx/tx-time <>}}`

  Providing both `:crux.tx/tx` and `:crux.tx/tx-time` is undefined.
  Arities passing dates directly (`node vt` and `node vt tt`) are deprecated and will be removed in a later release.

  If the node hasn't yet indexed a transaction at or past the given transaction, this throws NodeOutOfSyncException")

  (open-db
    ^java.io.Closeable [node]
    ^java.io.Closeable [node valid-time-or-basis]
    ^java.io.Closeable ^:deprecated [node valid-time tx-time]
    "Opens a DB snapshot at the given time.

  db-basis: (optional map, all keys optional)
    - `:crux.db/valid-time` (Date):
        If provided, DB won't return any data with a valid-time greater than the given time.
        Defaults to now.
    - `:crux.tx/tx` (Map):
        If provided, DB will be a snapshot as of the given transaction.
        Defaults to the latest completed transaction.
    - `:crux.tx/tx-time` (Date):
        Shorthand for `{:crux.tx/tx {:crux.tx/tx-time <>}}`

  Providing both `:crux.tx/tx` and `:crux.tx/tx-time` is undefined.
  Arities passing dates directly (`node vt` and `node vt tt`) are deprecated and will be removed in a later release.

  If the node hasn't yet indexed a transaction at or past the given transaction, this throws NodeOutOfSyncException

  This DB opens up shared resources to make multiple requests faster - it must
  be `.close`d when you've finished using it (for example, in a `with-open`
  block)"))

(let [db-args '(^java.io.Closeable [node]
                ^java.io.Closeable [node db-basis]
                ^java.io.Closeable ^:deprecated [node valid-time]
                ^java.io.Closeable ^:deprecated [node valid-time tx-time])]
  (alter-meta! #'db assoc :arglists db-args)
  (alter-meta! #'open-db assoc :arglists db-args))

(defn q
  "q[uery] a Crux db.
  query param is a datalog query in map, vector or string form.
  This function will return a set of result tuples if you do not specify `:order-by`, `:limit` or `:offset`;
  otherwise, it will return a vector of result tuples."
  [db q & args]
  (q* db q (object-array args)))

(defprotocol TransactionFnContext
  (indexing-tx [tx-fn-ctx]))

(defn open-q
  "lazily q[uery] a Crux db.
  query param is a datalog query in map, vector or string form.

  This function returns a Cursor of result tuples - once you've consumed
  as much of the sequence as you need to, you'll need to `.close` the sequence.
  A common way to do this is using `with-open`:

  (with-open [res (crux/open-q db '{:find [...]
                                    :where [...]})]
    (doseq [row (iterator-seq res)]
      ...))

  Once the sequence is closed, attempting to iterate it is undefined."
  ^crux.api.ICursor [db q & args]
  (open-q* db q (object-array args)))

(let [arglists '(^crux.api.ICursor
                 [db eid sort-order]
                 ^crux.api.ICursor
                 [db eid sort-order {:keys [with-docs? with-corrections?]
                                     {start-vt :crux.db/valid-time
                                      start-tt :crux.tx/tx-time
                                      start-tx-id :crux.tx/tx-id} :start
                                     {end-vt :crux.db/valid-time
                                      end-tt :crux.tx/tx-time
                                      end-tx-id :crux.tx/tx-id} :end}])]
  (alter-meta! #'entity-history assoc :arglists arglists)
  (alter-meta! #'open-entity-history assoc :arglists arglists))
