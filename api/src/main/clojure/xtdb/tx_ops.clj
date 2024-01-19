(ns xtdb.tx-ops
  (:require [xtdb.error :as err]
            [xtdb.xtql.edn :as xtql.edn])
  (:import [java.util List]
           (xtdb.api.tx Xtql Xtql$AssertExists Xtql$AssertNotExists Xtql$Delete Xtql$Erase Xtql$Insert Xtql$Update XtqlAndArgs)))

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

(defmethod parse-tx-op :insert-into [[_ table query & arg-rows :as this]]
  (when-not (keyword? table)
    (throw (err/illegal-arg :xtql/malformed-table {:table table, :insert this})))

  (cond-> (Xtql/insert (str (symbol table)) (xtql.edn/parse-query query))
    (seq arg-rows) (.withArgs ^List arg-rows)))

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

    (cond-> (Xtql/update (str (symbol table)) (xtql.edn/parse-col-specs set-specs this))
      for-valid-time (.forValidTime (xtql.edn/parse-temporal-filter for-valid-time :for-valid-time this))
      bind (.binding (xtql.edn/parse-out-specs bind this))
      (seq unify) (.unify (mapv xtql.edn/parse-unify-clause unify))
      (seq arg-rows) (.withArgs ^List arg-rows))))

(defmethod parse-tx-op :delete [[_ {table :from, :keys [for-valid-time bind unify]} & arg-rows :as this]]
  (when-not (keyword? table)
    (throw (err/illegal-arg :xtql/malformed-table {:from table, :delete this})))

  (cond-> (Xtql/delete (str (symbol table)))
    for-valid-time (.forValidTime (xtql.edn/parse-temporal-filter for-valid-time :for-valid-time this))
    bind (.binding (xtql.edn/parse-out-specs bind this))
    unify (.unify (mapv xtql.edn/parse-unify-clause unify))
    (seq arg-rows) (.withArgs ^List arg-rows)))

(defmethod parse-tx-op :erase [[_ {table :from, :keys [bind unify]} & arg-rows :as this]]
  (when-not (keyword? table)
    (throw (err/illegal-arg :xtql/malformed-table {:table table, :erase this})))

  (cond-> (Xtql/erase (str (symbol table)))
    bind (.binding (xtql.edn/parse-out-specs bind this))
    unify (.unify (mapv xtql.edn/parse-unify-clause unify))
    (seq arg-rows) (.withArgs ^List arg-rows)))

(defmethod parse-tx-op :assert-exists [[_ query & arg-rows]]
  (cond-> (Xtql/assertExists (xtql.edn/parse-query query))
    (seq arg-rows) (XtqlAndArgs. arg-rows)))

(defmethod parse-tx-op :assert-not-exists [[_ query & arg-rows]]
  (cond-> (Xtql/assertNotExists (xtql.edn/parse-query query))
    (seq arg-rows) (XtqlAndArgs. arg-rows)))

(extend-protocol Unparse
  Xtql$Insert
  (unparse-tx-op [query]
    [:insert-into (keyword (.table query)) (xtql.edn/unparse (.query query))])

  Xtql$Update
  (unparse-tx-op [query]
    (let [for-valid-time (some-> (.forValidTime query) xtql.edn/unparse)
          bind (some->> (.bindSpecs query) (mapv xtql.edn/unparse-out-spec))
          unify (some->> (.unifyClauses query) (mapv xtql.edn/unparse))]
      [:update (cond-> {:table (keyword (.table query))
                        :set (into {} (map xtql.edn/unparse-col-spec) (.setSpecs query))}
                 for-valid-time (assoc :for-valid-time for-valid-time)
                 bind (assoc :bind bind)
                 unify (assoc :unify unify))]))

  Xtql$Delete
  (unparse-tx-op [query]
    (let [for-valid-time (some-> (.forValidTime query) xtql.edn/unparse)
          bind (some->> (.bindSpecs query) (mapv xtql.edn/unparse-out-spec))
          unify (some->> (.unifyClauses query) (mapv xtql.edn/unparse))]
      [:delete (cond-> {:from (keyword (.table query))}
                 for-valid-time (assoc :for-valid-time for-valid-time)
                 bind (assoc :bind bind)
                 unify (assoc :unify unify))]))

  Xtql$Erase
  (unparse-tx-op [query]
    (let [bind (some->> (.bindSpecs query) (mapv xtql.edn/unparse-out-spec))
          unify (some->> (.unifyClauses query) (mapv xtql.edn/unparse))]
      [:erase (cond-> {:from (keyword (.table query))}
                bind (assoc :bind bind)
                unify (assoc :unify unify))]))

  Xtql$AssertExists
  (unparse-tx-op [query]
    [:assert-exists (xtql.edn/unparse (.query query))])

  Xtql$AssertNotExists
  (unparse-tx-op [query]
    [:assert-not-exists (xtql.edn/unparse (.query query))])

  XtqlAndArgs
  (unparse-tx-op [query+args]
    (into (unparse-tx-op (.op query+args))
          (.args query+args))))