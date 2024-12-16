(ns xtdb.tx-ops
  (:require [clojure.string :as str]
            [xtdb.error :as err]
            [xtdb.time :as time]
            [xtdb.xtql :as xtql])
  (:import [java.io Writer]
           [java.nio ByteBuffer]
           [java.util List]
           (xtdb.api.query Binding)
           (xtdb.api.tx TxOp TxOps)
           xtdb.types.ClojureForm
           xtdb.util.NormalForm))

(defmulti parse-tx-op
  (fn [tx-op]
    (cond
      (string? tx-op) :sql-str

      (not (vector? tx-op))
      (throw (err/illegal-arg :xtql/malformed-tx-op
                              {::err/message "expected SQL string/vector", :tx-op tx-op}))

      :else (let [[op] tx-op]
              (cond
                (string? op) :sql+args

                (not (keyword? op))
                (throw (err/illegal-arg :xtql/malformed-tx-op
                                        {::err/message "expected SQL string/keyword", :tx-op tx-op, :op op}))

                :else op))))

  :default ::default)

(defmethod parse-tx-op ::default [[op]]
  (throw (err/illegal-arg :xtql/unknown-tx-op {:op op})))

(defprotocol Unparse
  (unparse-tx-op [this]))

(defmethod print-dup TxOp [op ^Writer w]
  (.write w (format "#xt/tx-op %s" (pr-str (unparse-tx-op op)))))

(defmethod print-method TxOp [op ^Writer w]
  (print-dup op w))

(doseq [m [print-dup print-method]
        c [java.util.Map clojure.lang.IPersistentCollection clojure.lang.IRecord]]
  (prefer-method m TxOp c))

(defrecord PutDocs [table-name docs valid-from valid-to]
  TxOp
  Unparse
  (unparse-tx-op [_]
    (into [:put-docs (if (or valid-from valid-to)
                       {:into table-name
                        :valid-from valid-from
                        :valid-to valid-to}
                       table-name)]
          docs)))

(defrecord PatchDocs [table-name docs valid-from valid-to]
  TxOp
  Unparse
  (unparse-tx-op [_]
    (into [:patch-docs (if (or valid-from valid-to)
                         {:into table-name
                          :valid-from valid-from
                          :valid-to valid-to}
                         table-name)]
          docs)))

(defrecord DeleteDocs [table-name doc-ids valid-from valid-to]
  TxOp
  Unparse
  (unparse-tx-op [_]
    (into [:delete-docs (if (or valid-from valid-to)
                          {:from table-name
                           :valid-from valid-from
                           :valid-to valid-to}
                          table-name)]
          doc-ids)))

(defrecord EraseDocs [table-name doc-ids]
  TxOp
  Unparse
  (unparse-tx-op [_]
    (into [:erase-docs table-name] doc-ids)))

(defrecord XtqlAndArgs [op arg-rows]
  TxOp
  Unparse
  (unparse-tx-op [_]
    (into (unparse-tx-op op) arg-rows)))

(defrecord Insert [table query]
  TxOp
  Unparse
  (unparse-tx-op [_]
    [:insert-into table (xtql/unparse-query query)]))

(defrecord Update [table for-valid-time bind-specs set-specs unify-clauses]
  TxOp
  Unparse
  (unparse-tx-op [_]
    [:update (cond-> {:table table
                      :set (into {} (map xtql/unparse-col-spec) set-specs)}
               for-valid-time (assoc :for-valid-time (xtql/unparse for-valid-time))
               bind-specs (assoc :bind (mapv xtql/unparse-out-spec bind-specs))
               unify-clauses (assoc :unify (mapv xtql/unparse-unify-clause unify-clauses)))]))

(defrecord Delete [table for-valid-time bind-specs unify-clauses]
  TxOp
  Unparse
  (unparse-tx-op [_]
    [:delete (cond-> {:from table}
               for-valid-time (assoc :for-valid-time (xtql/unparse for-valid-time))
               bind-specs (assoc :bind (mapv xtql/unparse-out-spec bind-specs))
               unify-clauses (assoc :unify (mapv xtql/unparse-unify-clause unify-clauses)))]))

(defrecord Erase [table bind-specs unify-clauses]
  TxOp
  Unparse
  (unparse-tx-op [_]
    [:erase (cond-> {:from table}
              bind-specs (assoc :bind (mapv xtql/unparse-out-spec bind-specs))
              unify-clauses (assoc :unify (mapv xtql/unparse-unify-clause unify-clauses)))]))

(defrecord AssertExists [query]
  TxOp
  Unparse
  (unparse-tx-op [_]
    [:assert-exists (xtql/unparse-query query)]))

(defrecord AssertNotExists [query]
  TxOp
  Unparse
  (unparse-tx-op [_]
    [:assert-not-exists (xtql/unparse-query query)]) )

(defrecord SqlByteArgs [sql ^ByteBuffer arg-bytes]
  TxOp)

(defrecord Call [fn-id args]
  TxOp
  Unparse
  (unparse-tx-op [_]
    (into [:call fn-id] args)))

(defrecord Abort []
  TxOp
  Unparse
  (unparse-tx-op [_] [:abort]))

(def abort (->Abort))

(def ^:private eid? (some-fn uuid? integer? string? keyword?))

(def ^:private table? keyword?)

(defn- expect-table-name [table-name]
  (when-not (table? table-name)
    (throw (err/illegal-arg :xtdb.tx/invalid-table
                            {::err/message "expected table name" :table table-name})))

  (NormalForm/normalTableName table-name))

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

  doc)

(defmethod parse-tx-op :sql [[_ sql & arg-rows]]
  (if-not (string? sql)
    (throw (err/illegal-arg :xtdb.tx/expected-sql
                            {::err/message "Expected SQL query",
                             :sql sql}))

    (cond-> (TxOps/sql sql)
      (seq arg-rows) (.argRows ^List (vec arg-rows)))))

(defmethod parse-tx-op :sql-str [sql]
  (parse-tx-op [:sql sql]))

(defmethod parse-tx-op :sql+args [[sql & args]]
  (parse-tx-op [:sql sql (vec args)]))

(defmethod parse-tx-op :put-docs [[_ table-or-opts & docs]]
  (let [{table :into, :keys [valid-from valid-to]} (cond
                                                     (map? table-or-opts) table-or-opts
                                                     (keyword? table-or-opts) {:into table-or-opts})]
    (->PutDocs (expect-table-name table)
               (mapv expect-doc docs)
               (some-> valid-from time/expect-instant)
               (some-> valid-to time/expect-instant))))

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
    (->PutDocs "xt/tx_fns"
               [{"_id" (expect-fn-id fn-id)
                 "fn" (ClojureForm. (expect-tx-fn tx-fn))}]
               (some-> valid-from time/expect-instant)
               (some-> valid-to time/expect-instant))))

(defmethod parse-tx-op :patch-docs [[_ table-or-opts & docs]]
  (let [{table :into, :keys [valid-from valid-to]} (cond
                                                     (map? table-or-opts) table-or-opts
                                                     (keyword? table-or-opts) {:into table-or-opts})]
    (when (or valid-from valid-to)
      (throw (UnsupportedOperationException. "valid-from and valid-to are not yet supported for patch-docs")))

    (->PatchDocs (expect-table-name table)
                 (mapv expect-doc docs)
                 (some-> valid-from time/expect-instant)
                 (some-> valid-to time/expect-instant))))

(defmethod parse-tx-op :insert-into [[_ table query & arg-rows]]
  (cond-> (->Insert (expect-table-name table)
                    (xtql/parse-query query))
    (seq arg-rows) (->XtqlAndArgs arg-rows)))

(defmethod parse-tx-op :update [[_ opts & arg-rows :as this]]
  (when-not (map? opts)
    (throw (err/illegal-arg :xtql/malformed-opts
                            {::err/message "expected map", :opts opts, :update this})))

  (let [{:keys [table for-valid-time bind unify], set-specs :set} opts]
    (when-not (map? set-specs)
      (throw (err/illegal-arg :xtql/malformed-set
                              {:err/message "expected map", :set set-specs, :update this})))

    (when-not (or (nil? bind) (vector? bind))
      (throw (err/illegal-arg :xtql/malformed-bind
                              {::err/message "expected nil or vector", :bind bind, :update this})))

    (let [set-specs (xtql/parse-col-specs set-specs this)]
      (when-let [forbidden-cols (not-empty (set (for [^Binding binding set-specs
                                                      :let [binding-name (.getBinding binding)]
                                                      :when (str/starts-with? (NormalForm/normalForm binding-name) "_")]
                                                  binding-name)))]
        (throw (err/illegal-arg :xtql/forbidden-set-cols
                                {::err/message "Invalid set columns for update"
                                 :cols forbidden-cols})))

      (cond-> (->Update (expect-table-name table)
                        (some-> for-valid-time (xtql/parse-temporal-filter :for-valid-time this))
                        (some-> bind (xtql/parse-out-specs this))
                        set-specs
                        (some->> unify (mapv xtql/parse-unify-clause)))
        (seq arg-rows) (->XtqlAndArgs arg-rows)))))

(defmethod parse-tx-op :delete [[_ {table :from, :keys [for-valid-time bind unify]} & arg-rows :as this]]
  (cond-> (->Delete (expect-table-name table)
                    (some-> for-valid-time (xtql/parse-temporal-filter :for-valid-time this))
                    (some-> bind (xtql/parse-out-specs this))
                    (some->> unify (mapv xtql/parse-unify-clause)))
    (seq arg-rows) (->XtqlAndArgs arg-rows)))

(defmethod parse-tx-op :delete-docs [[_ table-or-opts & doc-ids]]
  (let [{table :from, :keys [valid-from valid-to]} (cond
                                                     (map? table-or-opts) table-or-opts
                                                     (keyword? table-or-opts) {:from table-or-opts})]
    (->DeleteDocs (expect-table-name table) (mapv expect-eid doc-ids)
                  (some-> valid-from time/expect-instant)
                  (some-> valid-to time/expect-instant))))

(defmethod parse-tx-op :erase [[_ {table :from, :keys [bind unify]} & arg-rows :as this]]
  (cond-> (->Erase (expect-table-name table)
                   (some-> bind (xtql/parse-out-specs this))
                   (some->> unify (mapv xtql/parse-unify-clause)))
    (seq arg-rows) (->XtqlAndArgs arg-rows)))

(defmethod parse-tx-op :erase-docs [[_ table & doc-ids]]
  (->EraseDocs (expect-table-name table) (mapv expect-eid doc-ids)))

(defmethod parse-tx-op :assert-exists [[_ query & arg-rows]]
  (cond-> (->AssertExists (xtql/parse-query query))
    (seq arg-rows) (->XtqlAndArgs arg-rows)))

(defmethod parse-tx-op :assert-not-exists [[_ query & arg-rows]]
  (cond-> (->AssertNotExists (xtql/parse-query query))
    (seq arg-rows) (->XtqlAndArgs arg-rows)))

(defmethod parse-tx-op :call [[_ f & args]]
  (->Call (expect-fn-id f) (or args [])))

(defmethod parse-tx-op :abort [_] abort)

