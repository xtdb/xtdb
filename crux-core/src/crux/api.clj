(ns crux.api
  "Public API of Crux."
  (:refer-clojure :exclude [sync])
  (:require [clojure.spec.alpha :as s]
            [crux.codec :as c]
            [clojure.tools.logging :as log]
            [crux.io :as cio])
  (:import [crux.api Crux ICruxAPI ICruxIngestAPI
            ICruxAsyncIngestAPI ICruxDatasource ICursor]
           java.io.Closeable
           java.util.Date
           java.time.Duration))

(s/def :crux.db/id (s/and (complement string?) c/valid-id?))
(s/def :crux.db/evicted? boolean?)
(s/def :crux.db.fn/args (s/coll-of any? :kind vector?))
(s/def :crux.db.fn/body (s/cat :fn #{'fn}
                               :args (s/coll-of symbol? :kind vector? :min-count 1)
                               :body (s/* any?)))
(s/def ::doc (s/and (s/map-of keyword? any?)
                    (s/keys :req [:crux.db/id] :opt [:crux.db.fn/body :crux.db.fn/args])))

(def ^:private date? (partial instance? Date))

(defmulti tx-op first)

(defmethod tx-op :crux.tx/put [_] (s/cat :op #{:crux.tx/put}
                                         :doc ::doc
                                         :start-valid-time (s/? date?)
                                         :end-valid-time (s/? date?)))

(defmethod tx-op :crux.tx/delete [_] (s/cat :op #{:crux.tx/delete}
                                            :id :crux.db/id
                                            :start-valid-time (s/? date?)
                                            :end-valid-time (s/? date?)))

(defmethod tx-op :crux.tx/cas [_] (s/cat :op #{:crux.tx/cas}
                                         :old-doc (s/nilable ::doc)
                                         :new-doc ::doc
                                         :at-valid-time (s/? date?)))

(defmethod tx-op :crux.tx/evict [_] (s/cat :op #{:crux.tx/evict}
                                           :id :crux.db/id))

(s/def ::args-doc (s/and ::doc (s/keys :req [:crux.db.fn/args])))

(defmethod tx-op :crux.tx/fn [_] (s/cat :op #{:crux.tx/fn}
                                        :id :crux.db/id
                                        :args-doc (s/? ::args-doc)))

(s/def ::tx-op (s/multi-spec tx-op first))
(s/def ::tx-ops (s/coll-of ::tx-op :kind vector?))

(defn- conform-tx-ops  [tx-ops]
  (->> tx-ops
       (mapv
        (fn [tx-op]
          (map
           #(if (instance? java.util.Map %) (into {} %) %)
           tx-op)))
       (mapv vec)))

(defprotocol PCruxNode
  "Provides API access to Crux."
  (db
    [node]
    [node ^Date valid-time]
    [node ^Date valid-time ^Date transaction-time]
    "Will return the latest value of the db currently known. Non-blocking.

  When a valid time is specified then returned db value contains only those
  documents whose valid time is before the specified time. Non-blocking.

  When both valid and transaction time are specified returns a db value
  as of the valid time and the latest transaction time indexed at or before
  the specified transaction time. Non-blocking. The index may not yet
  contain a transaction with the specified transaction time.")

  (document [node content-hash]
    "Reads a document from the document store based on its
  content hash.")

  (documents [node content-hashes-set]
    "Reads the set of documents from the document store based on their
  respective content hashes. Returns a map content-hash->document")

  (history [node eid]
    "Returns the transaction history of an entity, in reverse
  chronological order. Includes corrections, but does not include
  the actual documents.")

  (history-range [node eid
                  ^Date valid-time-start
                  ^Date transaction-time-start
                  ^Date valid-time-end
                  ^Date transaction-time-end]
    "Returns the transaction history of an entity, ordered by valid
  time / transaction time in chronological order, earliest
  first. Includes corrections, but does not include the actual
  documents.

  Giving nil as any of the date arguments makes the range open
  ended for that value.")

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
    ^:deprecated [node ^Date transaction-time ^Duration timeout]
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

  (latest-completed-tx [node]
    "Returns the latest transaction to have been indexed by this node.")

  (latest-submitted-tx [node]
    "Returns the latest transaction to have been submitted to this cluster")

  (attribute-stats [node]
    "Returns frequencies map for indexed attributes"))

(defprotocol PCruxIngestClient
  "Provides API access to Crux ingestion."
  (submit-tx [node tx-ops]
    "Writes transactions to the log for processing
  tx-ops datalog style transactions.
  Returns a map with details about the submitted transaction,
  including tx-time and tx-id.")

  ;; returning an iterator

  (open-tx-log ^java.io.Closeable [this after-tx-id with-ops?]
    "Reads the transaction log. Optionally includes
  operations, which allow the contents under the :crux.api/tx-ops
  key to be piped into (submit-tx tx-ops) of another
  Crux instance.

  after-tx-id      optional transaction id to start after.
  with-ops?       should the operations with documents be included?

  Returns an iterator of the TxLog"))

(extend-protocol PCruxNode
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

  (documents [this content-hash-set]
    (.documents this content-hash-set))

  (history [this eid]
    (.history this eid))

  (history-range [this eid valid-time-start transaction-time-start valid-time-end transaction-time-end]
    (.historyRange this eid valid-time-start transaction-time-start valid-time-end transaction-time-end))

  (status [this]
    (.status this))

  (tx-committed? [this submitted-tx]
    (.hasTxCommitted this submitted-tx))

  (sync
    ([this]
     (.sync this nil))
    ([this timeout]
     (.sync this timeout))

    ([this tx-time timeout]
     (defonce warn-on-deprecated-sync
       (log/warn "(sync tx-time <timeout?>) is deprecated, replace with either (await-tx-time tx-time <timeout?>) or, preferably, (await-tx tx <timeout?>)"))
     (.awaitTxTime this tx-time timeout)))

  (await-tx
    ([this submitted-tx]
     (await-tx this submitted-tx nil))
    ([this submitted-tx timeout]
     (.awaitTx this submitted-tx timeout)))

  (await-tx-time
    ([this tx-time]
     (await-tx-time this tx-time nil))
    ([this tx-time timeout]
     (.awaitTxTime this tx-time timeout)))

  (latest-completed-tx [node]
    (.latestCompletedTx node))

  (latest-submitted-tx [node]
    (.latestSubmittedTx node))

  (attribute-stats [this]
    (.attributeStats this)))

(extend-protocol PCruxIngestClient
  ICruxIngestAPI
  (submit-tx [this tx-ops]
    (.submitTx this (conform-tx-ops tx-ops)))

  (open-tx-log ^crux.api.ICursor [this after-tx-id with-ops?]
    (cio/<-cursor (.openTxLog this after-tx-id with-ops?))))

(defprotocol PCruxDatasource
  "Represents the database as of a specific valid and
  transaction time."

  (entity
    [db eid]
    [db snapshot eid]
    "queries a document map for an entity.
  eid is an object which can be coerced into an entity id.
  returns the entity document map.")

  (entity-tx [db eid]
    "returns the transaction details for an entity. Details
  include tx-id and tx-time.
  eid is an object that can be coerced into an entity id.")

  (new-snapshot ^java.io.Closeable [db]
     "Returns a new implementation specific snapshot allowing for lazy query results in a
  try-with-resources block using (q db  snapshot  query)}.
  Can also be used for
  (history-ascending db snapshot  eid) and
  (history-descending db snapshot  eid)
  returns an implementation specific snapshot")

  (q
    [db query]
    [db snapshot query]
    "q[uery] a Crux db.
  query param is a datalog query in map, vector or string form.
  First signature will evaluate eagerly and will return a set or vector
  of result tuples.
  Second signature accepts a db snapshot, see `new-snapshot`.
  Evaluates *lazily* consequently returns lazy sequence of result tuples.")

  (open-q ^java.io.Closeable [db query]
    ;; TODO doc
    )

  (history-ascending
    [db snapshot eid]
    "Retrieves entity history lazily in chronological order
  from and including the valid time of the db while respecting
  transaction time. Includes the documents.")

  (history-descending
    [db snapshot eid]
    "Retrieves entity history lazily in reverse chronological order
  from and including the valid time of the db while respecting
  transaction time. Includes the documents.")

  (valid-time [db]
    "returns the valid time of the db.
  If valid time wasn't specified at the moment of the db value retrieval
  then valid time will be time of the latest transaction.")

  (transaction-time [db]
    "returns the time of the latest transaction applied to this db value.
  If a tx time was specified when db value was acquired then returns
  the specified time."))

(extend-protocol PCruxDatasource
  ICruxDatasource
  (entity
    ([this eid]
     (.entity this eid))
    ([this snapshot eid]
     (.entity this snapshot eid)))

  (entity-tx [this eid]
    (.entityTx this eid))

  (new-snapshot [this]
    (.newSnapshot this))

  (q
    ([this query]
     (.q this query))
    ([this snapshot query]
     (.q this snapshot query)))

  (open-q [this query] (cio/<-cursor (.openQuery this query)))

  (history-ascending [this snapshot eid] (.historyAscending this snapshot eid))
  (history-descending [this snapshot eid] (.historyDescending this snapshot eid))

  (valid-time [this] (.validTime this))
  (transaction-time [this] (.transactionTime this)))

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
  "NOTE: requires any dependendies on the classpath that the Crux modules may need.

  options {:crux.node/topology 'crux.standalone/topology}

  Options are specified as keywords using their long format name, like
  :crux.kafka/bootstrap-servers etc. See the individual modules used in the specified
  topology for option descriptions.

  returns a node which implements ICruxAPI and
  java.io.Closeable. Latter allows the node to be stopped by
  calling `(.close node)`.

  throws IndexVersionOutOfSyncException if the index needs rebuilding.
  throws NonMonotonicTimeException if the clock has moved backwards since
    last run. Only applicable when using the event log."
  ^ICruxAPI [options]
  (Crux/startNode options))

(defn new-api-client
  "Creates a new remote API client ICruxAPI. The remote client
  requires valid and transaction time to be specified for all
  calls to `db`.

  NOTE: requires crux-http-client on the classpath, see
  crux.remote-api-client/*internal-http-request-fn* for more
  information.

  url the URL to a Crux HTTP end-point.

  returns a remote API client."
  ^ICruxAPI [url]
  (Crux/newApiClient url))

(defn new-ingest-client
  "Starts an ingest client for transacting into Kafka without running a
  full local node with index.

  For valid options, see crux.kafka/default-options. Options are
  specified as keywords using their long format name, like
  :crux.kafka/bootstrap-servers etc.

  options
  {:crux.kafka/bootstrap-servers  \"kafka-cluster-kafka-brokers.crux.svc.cluster.local:9092\"
   :crux.kafka/group-id           \"group-id\"
   :crux.kafka/tx-topic           \"crux-transaction-log\"
   :crux.kafka/doc-topic          \"crux-docs\"
   :crux.kafka/create-topics      true
   :crux.kafka/doc-partitions     1
   :crux.kafka/replication-factor 1}

  Returns a crux.api.ICruxIngestAPI component that implements
  java.io.Closeable, which allows the client to be stopped by calling
  close."
  ^ICruxAsyncIngestAPI [options]
  (Crux/newIngestClient options))
