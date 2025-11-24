(ns xtdb.tx-ops
  (:require [clojure.string :as str]
            [xtdb.error :as err]
            [xtdb.time :as time])
  (:import xtdb.util.NormalForm))

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

(defrecord Sql [sql arg-rows])
(defrecord SqlByteArgs [sql ^bytes arg-bytes]) ; used in FlightSQL, LSP doesn't spot it
(defrecord PutDocs [table-name docs valid-from valid-to])
(defrecord PutRel [table-name ^bytes rel-bytes])
(defrecord PatchDocs [table-name docs valid-from valid-to])
(defrecord DeleteDocs [table-name doc-ids valid-from valid-to])
(defrecord EraseDocs [table-name doc-ids])

(def ^:private eid? (some-fn uuid? integer? string? keyword?))

(def ^:private table? keyword?)

(defn- expect-table-name [table-name]
  (when-not (table? table-name)
    (throw (err/illegal-arg :xtdb.tx/invalid-table
                            {::err/message "expected table name" :table table-name})))

  (NormalForm/normalTableName table-name))

(defn- expect-eid [eid]
  (if-not (eid? eid)
    (throw (err/incorrect :xtdb.tx/invalid-eid "expected :xt/id or \"_id\""))
    eid))

(defn- expect-doc [doc]
  (when-not (map? doc)
    (throw (err/incorrect :xtdb.tx/expected-doc "expected doc map" {:doc doc})))

  (expect-eid (or (:xt/id doc) (get doc "_id")
                  (throw (err/incorrect :missing-id "expected :xt/id or \"_id\" in doc" {:doc doc}))))

  doc)

(defmethod parse-tx-op :sql [[_ sql & arg-rows]]
  (if-not (string? sql)
    (throw (err/illegal-arg :xtdb.tx/expected-sql
                            {::err/message "Expected SQL query",
                             :sql sql}))

    (->Sql sql (when (seq arg-rows) (vec arg-rows)))))

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

(defmethod parse-tx-op :put-fn [_]
  (throw (err/illegal-arg :xtdb/tx-fns-removed
                          {::err/message (str/join ["tx-fns are no longer supported, as of 2.0.0-beta7. "
                                                    "Please use ASSERTs and SQL DML statements instead - "
                                                    "see the release notes for more information."])})))

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

(defmethod parse-tx-op :insert-into [_]
  (throw (err/illegal-arg :xtdb/xtql-dml-removed
                          {::err/message (str/join ["XTQL DML is no longer supported, as of 2.0.0-beta7. "
                                                    "Please use SQL DML statements instead - "
                                                    "see the release notes for more information."])})))

(defmethod parse-tx-op :update [_]
  (throw (err/illegal-arg :xtdb/xtql-dml-removed
                          {::err/message (str/join ["XTQL DML is no longer supported, as of 2.0.0-beta7. "
                                                    "Please use SQL DML statements instead - "
                                                    "see the release notes for more information."])})))

(defmethod parse-tx-op :delete [_]
  (throw (err/illegal-arg :xtdb/xtql-dml-removed
                          {::err/message (str/join ["XTQL DML is no longer supported, as of 2.0.0-beta7. "
                                                    "Please use SQL DML statements instead - "
                                                    "see the release notes for more information."])})))

(defmethod parse-tx-op :delete-docs [[_ table-or-opts & doc-ids]]
  (let [{table :from, :keys [valid-from valid-to]} (cond
                                                     (map? table-or-opts) table-or-opts
                                                     (keyword? table-or-opts) {:from table-or-opts})]
    (->DeleteDocs (expect-table-name table) (mapv expect-eid doc-ids)
                  (some-> valid-from time/expect-instant)
                  (some-> valid-to time/expect-instant))))

(defmethod parse-tx-op :erase [_]
  (throw (err/illegal-arg :xtdb/xtql-dml-removed
                          {::err/message (str/join ["XTQL DML is no longer supported, as of 2.0.0-beta7. "
                                                    "Please use SQL DML statements instead - "
                                                    "see the release notes for more information."])})))

(defmethod parse-tx-op :erase-docs [[_ table & doc-ids]]
  (->EraseDocs (expect-table-name table) (mapv expect-eid doc-ids)))

(defmethod parse-tx-op :assert-exists [_]
  (throw (err/illegal-arg :xtdb/xtql-dml-removed
                          {::err/message (str/join ["XTQL DML is no longer supported, as of 2.0.0-beta7. "
                                                    "Please use SQL DML statements instead - "
                                                    "see the release notes for more information."])})))

(defmethod parse-tx-op :assert-not-exists [_]
  (throw (err/illegal-arg :xtdb/xtql-dml-removed
                          {::err/message (str/join ["XTQL DML is no longer supported, as of 2.0.0-beta7. "
                                                    "Please use SQL DML statements instead - "
                                                    "see the release notes for more information."])})))

(defmethod parse-tx-op :call [_]
  (throw (err/illegal-arg :xtdb/tx-fns-removed
                          {::err/message (str/join ["tx-fns are no longer supported, as of 2.0.0-beta7. "
                                                    "Please use ASSERTs and SQL DML statements instead - "
                                                    "see the release notes for more information."])})))
