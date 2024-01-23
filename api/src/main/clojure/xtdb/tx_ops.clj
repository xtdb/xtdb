(ns xtdb.tx-ops
  (:require [clojure.spec.alpha :as s]
            [xtdb.error :as err]
            [xtdb.time :as time]
            [xtdb.xtql.edn :as xtql.edn])
  (:import [java.util List]
           (xtdb.api.tx TxOp$AssertExists TxOp$AssertNotExists TxOp$Delete TxOp$Erase TxOp$Insert TxOps TxOp$Update TxOp$XtqlAndArgs)))

(defmulti parse-tx-op
  (fn [tx-op]
    (when-not (vector? tx-op)
      (throw (err/illegal-arg :xtql/malformed-tx-op {:tx-op tx-op})))

    (let [[op] tx-op]
      (when-not (keyword? op)
        (throw (err/illegal-arg :xtql/malformed-tx-op {:tx-op tx-op})))

      op))
  :default ::default)

(defprotocol Unparse
  (unparse-tx-op [this]))

(defmethod parse-tx-op ::default [[op]]
  (throw (err/illegal-arg :xtql/unknown-tx-op {:op op})))

(def ^:private eid? (some-fn uuid? integer? string? keyword?))

(def ^:private table? keyword?)

(defn- expect-table-name ^String [table-name]
  (when-not (table? table-name)
    (throw (err/illegal-arg :xtdb.tx/invalid-table
                            {::err/message "expected table name" :table table-name})))

  (str (symbol table-name)))

(defn- expect-eid [eid]
  (if-not (eid? eid)
    (throw (err/illegal-arg :xtdb.tx/invalid-eid
                            {::err/message "expected xt/id", :xt/id eid}))
    eid))

(defn- expect-doc [doc]
  (when-not (map? doc)
    (throw (err/illegal-arg :xtdb.tx/expected-doc
                            {::err/message "expected doc map", :doc doc})))
  (expect-eid (or (:xt/id doc) (get doc "xt/id")))

  (-> doc
      (update-keys (fn [k]
                     (cond-> k
                       (keyword? k) (-> symbol str))))))

(defn- expect-instant ^java.time.Instant [instant]
  (when-not (s/valid? ::time/datetime-value instant)
    (throw (err/illegal-arg :xtdb/invalid-date-time
                            {::err/message "expected date-time"
                             :timestamp instant})))

  (time/->instant instant))

(defmethod parse-tx-op :sql [[_ sql & arg-rows]]
  (if-not (string? sql)
    (throw (err/illegal-arg :xtdb.tx/expected-sql
                            {::err/message "Expected SQL query",
                             :sql sql}))

    (cond-> (TxOps/sql sql)
      (seq arg-rows) (.argRows ^List (vec arg-rows)))))

(defmethod parse-tx-op :put [[_ table-or-opts & docs]]
  (let [{table :into, :keys [valid-from valid-to]} (cond
                                                     (map? table-or-opts) table-or-opts
                                                     (keyword? table-or-opts) {:into table-or-opts})]
    (cond-> (TxOps/putDocs (expect-table-name table) ^List (mapv expect-doc docs))
      valid-from (.startingFrom (expect-instant valid-from))
      valid-to (.until (expect-instant valid-to)))))

(defn- expect-fn-id [fn-id]
  (if-not (eid? fn-id)
    (throw (err/illegal-arg :xtdb.tx/invalid-fn-id {::err/message "expected fn-id", :fn-id fn-id}))
    fn-id))

(defn- expect-tx-fn [tx-fn]
  (or tx-fn
      (throw (err/illegal-arg :xtdb.tx/invalid-tx-fn {::err/message "expected tx-fn", :tx-fn tx-fn}))))

(defmethod parse-tx-op :put-fn [[_ id-or-opts tx-fn]]
  (let [{:keys [fn-id valid-from valid-to]} (if (map? id-or-opts)
                                              id-or-opts
                                              {:fn-id id-or-opts})]
    (cond-> (TxOps/putFn (expect-fn-id fn-id) (expect-tx-fn tx-fn))
      valid-from (.startingFrom (expect-instant valid-from))
      valid-to (.until (expect-instant valid-to)))))

(defmethod parse-tx-op :insert-into [[_ table query & arg-rows :as this]]
  (when-not (keyword? table)
    (throw (err/illegal-arg :xtql/malformed-table {:table table, :insert this})))

  (cond-> (TxOps/insert (str (symbol table)) (xtql.edn/parse-query query))
    (seq arg-rows) (.argRows ^List arg-rows)))

(defmethod parse-tx-op :update [[_ opts & arg-rows :as this]]
  (when-not (map? opts)
    (throw (err/illegal-arg :xtql/malformed-opts {:opts opts, :update this})))

  (let [{:keys [table for-valid-time bind unify], set-specs :set} opts]

    (when-not (keyword? table)
      (throw (err/illegal-arg :xtql/malformed-table {:table table, :update this})))

    (when-not (map? set-specs)
      (throw (err/illegal-arg :xtql/malformed-set {:set set-specs, :update this})))

    (when-not (or (nil? bind) (vector? bind))
      (throw (err/illegal-arg :xtql/malformed-bind {:bind bind, :update this})))

    (cond-> (TxOps/update (str (symbol table)) (xtql.edn/parse-col-specs set-specs this))
      for-valid-time (.forValidTime (xtql.edn/parse-temporal-filter for-valid-time :for-valid-time this))
      bind (.binding (xtql.edn/parse-out-specs bind this))
      (seq unify) (.unify (mapv xtql.edn/parse-unify-clause unify))
      (seq arg-rows) (.argRows ^List arg-rows))))

(defmethod parse-tx-op :delete [[_ {table :from, :keys [for-valid-time bind unify]} & arg-rows :as this]]
  (when-not (keyword? table)
    (throw (err/illegal-arg :xtql/malformed-table {:from table, :delete this})))

  (cond-> (TxOps/delete (str (symbol table)))
    for-valid-time (.forValidTime (xtql.edn/parse-temporal-filter for-valid-time :for-valid-time this))
    bind (.binding (xtql.edn/parse-out-specs bind this))
    unify (.unify (mapv xtql.edn/parse-unify-clause unify))
    (seq arg-rows) (.argRows ^List arg-rows)))

(defmethod parse-tx-op :delete-docs [[_ table-or-opts & doc-ids]]
  (let [{table :from, :keys [valid-from valid-to]} (cond
                                                     (map? table-or-opts) table-or-opts
                                                     (keyword? table-or-opts) {:from table-or-opts})]
    (cond-> (TxOps/deleteDocs (expect-table-name table) ^List (mapv expect-eid doc-ids))
      valid-from (.startingFrom (expect-instant valid-from))
      valid-to (.until (expect-instant valid-to)))))

(defmethod parse-tx-op :erase [[_ {table :from, :keys [bind unify]} & arg-rows :as this]]
  (when-not (keyword? table)
    (throw (err/illegal-arg :xtql/malformed-table {:table table, :erase this})))

  (cond-> (TxOps/erase (str (symbol table)))
    bind (.binding (xtql.edn/parse-out-specs bind this))
    unify (.unify (mapv xtql.edn/parse-unify-clause unify))
    (seq arg-rows) (.argRows ^List arg-rows)))

(defmethod parse-tx-op :erase-docs [[_ table & doc-ids]]
  (TxOps/eraseDocs (expect-table-name table) ^List (mapv expect-eid doc-ids)))

(defmethod parse-tx-op :assert-exists [[_ query & arg-rows]]
  (cond-> (TxOps/assertExists (xtql.edn/parse-query query))
    (seq arg-rows) (TxOp$XtqlAndArgs. arg-rows)))

(defmethod parse-tx-op :assert-not-exists [[_ query & arg-rows]]
  (cond-> (TxOps/assertNotExists (xtql.edn/parse-query query))
    (seq arg-rows) (TxOp$XtqlAndArgs. arg-rows)))

(defmethod parse-tx-op :call [[_ f & args]]
  (TxOps/call (expect-fn-id f) (or args [])))

(extend-protocol Unparse
  TxOp$Insert
  (unparse-tx-op [query]
    [:insert-into (keyword (.table query)) (xtql.edn/unparse (.query query))])

  TxOp$Update
  (unparse-tx-op [query]
    (let [for-valid-time (some-> (.forValidTime query) xtql.edn/unparse)
          bind (some->> (.bindSpecs query) (mapv xtql.edn/unparse-out-spec))
          unify (some->> (.unifyClauses query) (mapv xtql.edn/unparse))]
      [:update (cond-> {:table (keyword (.table query))
                        :set (into {} (map xtql.edn/unparse-col-spec) (.setSpecs query))}
                 for-valid-time (assoc :for-valid-time for-valid-time)
                 bind (assoc :bind bind)
                 unify (assoc :unify unify))]))

  TxOp$Delete
  (unparse-tx-op [query]
    (let [for-valid-time (some-> (.forValidTime query) xtql.edn/unparse)
          bind (some->> (.bindSpecs query) (mapv xtql.edn/unparse-out-spec))
          unify (some->> (.unifyClauses query) (mapv xtql.edn/unparse))]
      [:delete (cond-> {:from (keyword (.table query))}
                 for-valid-time (assoc :for-valid-time for-valid-time)
                 bind (assoc :bind bind)
                 unify (assoc :unify unify))]))

  TxOp$Erase
  (unparse-tx-op [query]
    (let [bind (some->> (.bindSpecs query) (mapv xtql.edn/unparse-out-spec))
          unify (some->> (.unifyClauses query) (mapv xtql.edn/unparse))]
      [:erase (cond-> {:from (keyword (.table query))}
                bind (assoc :bind bind)
                unify (assoc :unify unify))]))

  TxOp$AssertExists
  (unparse-tx-op [query]
    [:assert-exists (xtql.edn/unparse (.query query))])

  TxOp$AssertNotExists
  (unparse-tx-op [query]
    [:assert-not-exists (xtql.edn/unparse (.query query))])

  TxOp$XtqlAndArgs
  (unparse-tx-op [query+args]
    (into (unparse-tx-op (.op query+args))
          (.argRows query+args))))
