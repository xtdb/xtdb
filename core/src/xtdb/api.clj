(ns xtdb.api
  "Public API of XTDB."
  (:refer-clojure :exclude [sync])
  (:require [clojure.spec.alpha :as s]
            [xtdb.codec :as c]
            [xtdb.system :as sys])
  (:import [xtdb.api IXtdb IXtdbSubmitClient RemoteClientOptions]
           java.lang.AutoCloseable
           java.time.Duration
           [java.util Date Map]
           java.util.function.Supplier))

(def ^:private date? (partial instance? Date))

(s/def ::tx-id nat-int?)
(s/def ::tx-time date?)
(s/def ::tx (s/keys :opt [::tx-id ::tx-time]))

(s/def ::submit-tx-opts (s/keys :opt [::tx-time]))

(s/def :crux.db/id c/valid-id?)
(s/def :xt/id c/valid-id?)
(s/def ::evicted? boolean?)
(s/def :crux.db.fn/args (s/coll-of any? :kind vector?))

(s/def ::fn-db-arg (s/or :sym symbol? :destructuring map?))
(s/def ::fn-arg (s/or :sym symbol? :destructuring (s/or :map map? :vec vector?)))

(s/def ::fn
  (s/cat :fn #{'fn}
         :args (s/and vector? (s/cat :db ::fn-db-arg :args (s/* ::fn-arg)))
         :body (s/* any?)))

(defprotocol PXtdb
  "Provides API access to XTDB."
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
    "Attaches a listener to XTDB's event bus, the supplied callback `f` will be invoked as new events occur, receiving the event as the first argument.

  Specify an event type `:xtdb.api/event-type` and options in the map `event-opts`

  For example, this listener prints indexed transactions:

  (def listener (xtdb.api/listen node {:xtdb.api/event-type :xtdb.api/indexed-tx} prn))

  Use `.close` on the returned object to detach the listener:

  (.close listener)

  ---

  A listener will receive events in the order they were submitted (say by the indexer) each listener receives events asynchronously on a standalone thread. It does not block query, writes, indexing or other listeners.

  If you start/stop many listeners over the lifetime of your program, ensure you .close listeners you no longer need to free those threads.

  See below for detail on supported event types:

  ---

  Event type `:xtdb.api/indexed-tx`

  Occurs when a transaction has been processed by the indexer, and as such its effects will be visible to query if it was committed.

  Example event:

  {:xtdb.api/event-type :xtdb.api/indexed-tx
   :xtdb.api/tx-time #inst \"2022-11-09T10:13:33.028-00:00\",
   :xtdb.api/tx-id 4

   ;; can be false in the case of a rollback or failure, for example, if an ::xt/fn op throws, or a match condition is not met.
   :committed? true

   ;; if :with-tx-ops? is true
   :xtdb.api/tx-ops ([:xtdb.api/put {:name \"Fred\", :xt/id \"foo\"}])}

  This event might be useful if you require reactive or dependent processing of indexed transactions, and do not want to block with `await-tx`.
  Because you receive an event for transactions that did not commit (:committed? false), you could respond to transaction function failures in some way,
  such as specific logging, or raising an alert.

  Options:

  - :with-tx-ops? (default false)
     If true includes the indexed tx ops itself the key :xtdb.api/tx-ops
     For transaction functions and match ops, you will see the expansion (i.e results), not the function call or match.
     Be aware this option may require fetching transactions from storage per event if the transactions are not in cache.")

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

(defprotocol PXtdbSubmitClient
  "Provides API access to XTDB transaction submission."
  (submit-tx-async
    [node tx-ops]
    [node tx-ops opts]
    "Writes transactions to the log for processing tx-ops datalog
  style transactions. Non-blocking.  Returns a deref with map with
  details about the submitted transaction, including tx-time and
  tx-id.

  opts (map):
  - ::tx-time
    overrides tx-time for the transaction.
    mustn't be earlier than any previous tx-time, and mustn't be later than the tx-log's clock.")

  (submit-tx
    [node tx-ops]
    [node tx-ops opts]
    "Writes transactions to the log for processing
  tx-ops datalog style transactions.
  Returns a map with details about the submitted transaction,
  including tx-time and tx-id.

  opts (map):
   - ::tx-time
     overrides tx-time for the transaction.
     mustn't be earlier than any previous tx-time, and mustn't be later than the tx-log's clock.")

  (open-tx-log ^java.io.Closeable [this after-tx-id with-ops?]
    "Reads the transaction log. Optionally includes
  operations, which allow the contents under the ::tx-ops
  key to be piped into (submit-tx tx-ops) of another
  XTDB instance.

  after-tx-id      optional transaction id to start after.
  with-ops?       should the operations with documents be included?

  Returns an iterator of the TxLog"))

(defprotocol PXtdbDatasource
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
    "q[uery] an XTDB db.
  query param is a datalog query in map, vector or string form.
  This function will return a set of result tuples if you do not specify `:order-by`, `:limit` or `:offset`;
  otherwise, it will return a vector of result tuples.")

  (open-q* ^xtdb.api.ICursor [db query args]
    "lazily q[uery] an XTDB db.
  query param is a datalog query in map, vector or string form.

  This function returns a Cursor of result tuples - once you've consumed
  as much of the sequence as you need to, you'll need to `.close` the sequence.
  A common way to do this is using `with-open`:

  (with-open [res (xt/open-q db '{:find [...]
                                  :where [...]})]
    (doseq [row (iterator-seq res)]
      ...))

  Once the sequence is closed, attempting to iterate it is undefined.
  Therefore, be cautious with lazy evaluation.")

  (pull [db query eid]
    "Returns the requested data for the given entity ID, based on the projection spec

  e.g. `(pull db [:film/name :film/year] :spectre)`
    => `{:film/name \"Spectre\", :film/year 2015}`

  See https://xtdb.com/reference/queries.html#pull for details of the spec format.")

  (pull-many [db query eids]
    "Returns the requested data for the given entity IDs, based on the projection spec

  e.g. `(pull-many db [:film/name :film/year] #{:spectre :skyfall})`
    => `[{:film/name \"Spectre\", :film/year 2015}, {:film/name \"Skyfall\", :film/year 2012}]`

  See https://xtdb.com/reference/queries.html#pull for details of the spec format.")

  (entity-history
    [db eid sort-order]
    [db eid sort-order opts]
    "Eagerly retrieves entity history for the given entity.

  Options:
  * `sort-order`: `#{:asc :desc}`
  * `:with-docs?` (boolean, default false): specifies whether to include documents in the entries under the `:xtdb.api/doc` key
  * `:with-corrections?` (boolean, default false): specifies whether to include bitemporal corrections in the sequence, sorted first by valid-time, then tx-id
  * `:start-valid-time` (inclusive, default unbounded)
  * `:start-tx`: (map, all keys optional)
    - `:xtdb.api/tx-time` (Date, inclusive, default unbounded)
    - `:xtdb.api/tx-id` (Long, inclusive, default unbounded)
  * `:end-valid-time` (exclusive, default unbounded)
  * `:end-tx`: (map, all keys optional)
    - `:xtdb.api/tx-time` (Date, exclusive, default unbounded)
    - `:xtdb.api/tx-id` (Long, exclusive, default unbounded)

  No matter what `start-*` and `end-*` parameters you specify, you won't receive results later than the valid-time and tx-id of this DB value.

  Each entry in the result contains the following keys:
  * `:xtdb.api/valid-time`,
  * `:xtdb.api/tx-time`,
  * `:xtdb.api/tx-id`,
  * `:xtdb.api/content-hash`
  * `:xtdb.api/doc` (see `with-docs?`).")

  (open-entity-history
    ^xtdb.api.ICursor [db eid sort-order]
    ^xtdb.api.ICursor [db eid sort-order opts]
    "Lazily retrieves entity history for the given entity.
  Don't forget to close the cursor when you've consumed enough history!
  Consuming after the cursor is closed is undefined (e.g. may cause a JVM
  segfault crash when using RocksDB). Therefore, be cautious with lazy
  evaluation.
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
    "returns the basis of this db snapshot - a map containing `:xtdb.api/valid-time` and `:xtdb.api/tx`")

  (^java.io.Closeable with-tx [db tx-ops]
   "Returns a new db value with the tx-ops speculatively applied.
  The tx-ops will only be visible in the value returned from this function - they're not submitted to the cluster, nor are they visible to any other database value in your application.
  If the transaction doesn't commit (eg because of a failed 'match'), this function returns nil."))

(defn start-node
  "NOTE: requires any dependencies on the classpath that the XTDB modules may need.

  Accepts a map, or a JSON/EDN file or classpath resource.

  See https://xtdb.com/reference/configuration.html for details.

  Returns a node which implements: DBProvider, PXtdb, PXtdbSubmitClient and java.io.Closeable.

  Latter allows the node to be stopped by calling `(.close node)`.

  Throws IndexVersionOutOfSyncException if the index needs rebuilding."
  ^java.io.Closeable [options]
  (let [system (-> (sys/prep-system (into [{:xtdb/node 'xtdb.node/->node
                                            :xtdb/index-store 'xtdb.kv.index-store/->kv-index-store
                                            :xtdb/bus 'xtdb.bus/->bus
                                            :xtdb.bus/bus-stop 'xtdb.bus/->bus-stop
                                            :xtdb/tx-ingester 'xtdb.tx/->tx-ingester
                                            :xtdb/tx-indexer 'xtdb.tx/->tx-indexer
                                            :xtdb/document-store 'xtdb.kv.document-store/->document-store
                                            :xtdb/tx-log 'xtdb.kv.tx-log/->tx-log
                                            :xtdb/query-engine 'xtdb.query/->query-engine
                                            :xtdb/secondary-indices 'xtdb.tx/->secondary-indices}]
                                          (cond-> options (not (vector? options)) vector)))
                   (sys/start-system))]
    (reset! (get-in system [:xtdb/node :!system]) system)
    (-> (:xtdb/node system)
        (assoc :close-fn #(.close ^AutoCloseable system)))))

(defn- ->RemoteClientOptions [{:keys [->jwt-token] :as opts}]
  (RemoteClientOptions. (when ->jwt-token
                          (reify Supplier
                            (get [_] (->jwt-token))))))

(defn new-api-client
  "Creates a new remote API client.

  This implements: DBProvider, PXtdb, PXtdbSubmitClient and java.io.Closeable.

  The remote client requires valid and transaction time to be specified for all calls to `db`.

  NOTE: Requires either clj-http or http-kit on the classpath,
  See https://xtdb.com/reference/http.html for more information.

  url the URL to an XTDB HTTP end-point.
  (OPTIONAL) auth-supplier a supplier function which provides an auth token string for the XTDB HTTP end-point.

  returns a remote API client."
  (^java.io.Closeable [url]
   (:node (IXtdb/newApiClient url)))
  (^java.io.Closeable [url opts]
   (:node (IXtdb/newApiClient url (->RemoteClientOptions opts)))))

(defn new-submit-client
  "Starts a submit client for transacting into XTDB without running a full local node with index.
  Accepts a map, or a JSON/EDN file or classpath resource.
  See https://xtdb.com/reference/configuration.html for details.
  Returns a component that implements java.io.Closeable and PXtdbSubmitClient.
  Latter allows the node to be stopped by calling `(.close node)`."
  ^java.io.Closeable [options]
  (:client (IXtdbSubmitClient/newSubmitClient ^Map options)))

(defn conform-tx-ops [tx-ops]
  (->> tx-ops
       (mapv
        (fn [tx-op]
          (mapv #(if (instance? Map %) (into {} %) %)
                tx-op)))))

(defprotocol DBProvider
  (db
    [node]
    [node valid-time-or-basis]
    ^:deprecated [node valid-time tx-time]
    "Returns a DB snapshot at the given time. The snapshot is not thread-safe.

  db-basis: (optional map, all keys optional)
    - `:xtdb.api/valid-time` (Date):
        If provided, DB won't return any data with a valid-time greater than the given time.
        Defaults to now.
    - `:xtdb.api/tx` (Map):
        If provided, DB will be a snapshot as of the given transaction.
        Defaults to the latest completed transaction.
    - `:xtdb.api/tx-time` (Date):
        Shorthand for `{::tx {::tx-time <>}}`

  Providing both `:xtdb.api/tx` and `:xtdb.api/tx-time` is undefined.
  Arities passing dates directly (`node vt` and `node vt tt`) are deprecated and will be removed in a later release.

  If the node hasn't yet indexed a transaction at or past the given transaction, this throws NodeOutOfSyncException")

  (open-db
    ^java.io.Closeable [node]
    ^java.io.Closeable [node valid-time-or-basis]
    ^java.io.Closeable ^:deprecated [node valid-time tx-time]
    "Opens a DB snapshot at the given time.

  db-basis: (optional map, all keys optional)
    - `:xtdb.api/valid-time` (Date):
        If provided, DB won't return any data with a valid-time greater than the given time.
        Defaults to now.
    - `:xtdb.api/tx` (Map):
        If provided, DB will be a snapshot as of the given transaction.
        Defaults to the latest completed transaction.
    - `:xtdb.api/tx-time` (Date):
        Shorthand for `{::tx {::tx-time <>}}`

  Providing both `:xtdb.api/tx` and `:xtdb.api/tx-time` is undefined.
  Arities passing dates directly (`node vt` and `node vt tt`) are deprecated and will be removed in a later release.

  If the node hasn't yet indexed a transaction at or past the given transaction, this throws NodeOutOfSyncException

  This DB opens up shared resources to make multiple requests faster - it must be `.close`d when you've finished
  using it (for example, in a `with-open` block). Be cautious with lazy evaluation that may attempt to make requests
  after the DB is closed."))

(let [db-args '(^java.io.Closeable [node]
                ^java.io.Closeable [node db-basis]
                ^java.io.Closeable ^:deprecated [node valid-time]
                ^java.io.Closeable ^:deprecated [node valid-time tx-time])]
  (alter-meta! #'db assoc :arglists db-args)
  (alter-meta! #'open-db assoc :arglists db-args))

(defn q
  "q[uery] an XTDB db.
  query param is a datalog query in map, vector or string form.
  This function will return a set of result tuples if you do not specify `:order-by`, `:limit` or `:offset`;
  otherwise, it will return a vector of result tuples."
  [db q & args]
  (q* db q (object-array args)))

(defprotocol TransactionFnContext
  (indexing-tx [tx-fn-ctx]))

(defn open-q
  "lazily q[uery] an XTDB db.
  query param is a datalog query in map, vector or string form.

  This function returns a Cursor of result tuples - once you've consumed
  as much of the sequence as you need to, you'll need to `.close` the sequence.
  A common way to do this is using `with-open`:

  (with-open [res (xt/open-q db '{:find [...]
                                    :where [...]})]
    (doseq [row (iterator-seq res)]
      ...))

  Once the sequence is closed, attempting to consume it is undefined (e.g. may
  cause a JVM segfault crash when using RocksDB). Therefore, be cautious with
  lazy evaluation."
  ^xtdb.api.ICursor [db q & args]
  (open-q* db q (object-array args)))

(let [arglists '(^xtdb.api.ICursor
                 [db eid sort-order]
                 ^xtdb.api.ICursor
                 [db eid sort-order {:keys [with-docs?
                                            with-corrections?
                                            start-valid-time
                                            end-valid-time]
                                     {start-tt ::tx-time
                                      start-tx-id ::tx-id} :start-tx
                                     {end-tt ::tx-time
                                      end-tx-id ::tx-id} :end-tx}])]
  (alter-meta! #'entity-history assoc :arglists arglists)
  (alter-meta! #'open-entity-history assoc :arglists arglists))
