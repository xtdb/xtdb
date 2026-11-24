(ns xtdb.sql.parse
  (:require [xtdb.antlr :as antlr]
            [xtdb.error :as err]
            [xtdb.sql :as sql]
            [xtdb.table :as table])
  (:import (org.antlr.v4.runtime.tree ParseTree)
           (xtdb.antlr SqlVisitor)))

(defrecord StatementVisitor []
  SqlVisitor
  (visitQueryExpr [_ stmt]
    [:query {:stmt stmt}])

  (visitShowSnapshotTokenStatement [_ _]
    [:show-snapshot-token])

  (visitShowClockTimeStatement [_ _]
    [:show-clock-time])

  (visitInsertStatement [_ stmt]
    [:insert {:table (table/->ref (sql/identifier-sym (.targetTable stmt))) :stmt stmt}])

  (visitPatchStatement [_ stmt]
    [:patch {:table (table/->ref (sql/identifier-sym (.targetTable stmt))), :stmt stmt}])

  (visitUpdateStatement [_ stmt]
    [:update {:table (table/->ref (sql/identifier-sym (.targetTable stmt))), :stmt stmt}])

  (visitDeleteStatement [_ stmt]
    [:delete {:table (table/->ref (sql/identifier-sym (.targetTable stmt))), :stmt stmt}])

  (visitEraseStatement [_ stmt]
    [:erase {:table (table/->ref (sql/identifier-sym (.targetTable stmt))), :stmt stmt}])

  (visitAssertStatement [_ stmt]
    [:assert {:stmt stmt, :message (some->> (.message stmt) (sql/accept-visitor sql/string-literal-visitor))}])

  (visitShowVariableStatement [_ stmt]
    [:show-variable {:stmt stmt}])

  (visitShowTransactionIsolationLevel [_ _]
    [:show-tx-isolation-level])

  (visitShowTimeZone [_ _]
    [:show-time-zone])

  (visitExecuteStatement [_ stmt]
    [:execute {:stmt stmt}])

  (visitGrantRoleStatement [_ stmt]
    [:grant-role {:role (str (sql/identifier-sym (.roleName stmt))), :user (str (sql/identifier-sym (.userName stmt)))}])

  (visitRevokeRoleStatement [_ stmt]
    [:revoke-role {:role (str (sql/identifier-sym (.roleName stmt))), :user (str (sql/identifier-sym (.userName stmt)))}])

  (visitCreateTableStatement [this stmt]
    (let [elements (mapv #(.accept ^ParseTree % this) (.createTableElement stmt))
          [primary-key & more-keys] (keep :primary-key elements)]
      (when more-keys
        (throw (err/incorrect :xtdb/multiple-primary-keys "A table has at most one PRIMARY KEY"
                              {:primary-keys (keep :primary-key elements)})))

      (when (next primary-key)
        (throw (err/unsupported :xtdb/composite-primary-key "Multi-column primary keys are not yet supported"
                                {:columns primary-key})))

      (when-let [[col] primary-key]
        (when (and (sql/forbidden-update-col? col) (not= "_id" col))
          (throw (err/incorrect :xtdb/invalid-primary-key-column
                                (format "A primary key can't be the system column %s" col)
                                {:column col}))))

      [:create-table {:table (table/->ref (sql/identifier-sym (.targetTable stmt)))
                      :col-names (into [] (keep :col-name) elements)
                      :primary-key primary-key}]))

  (visitTableColumn [_ el]
    (let [col-name (str (sql/identifier-sym (.columnName el)))]
      (cond-> {:col-name col-name}
        (.PRIMARY el) (assoc :primary-key [col-name]))))

  (visitTablePrimaryKey [_ el]
    {:primary-key (mapv (comp str sql/identifier-sym) (.columnName (.columnNameList el)))}))

(defn parse-statement [stmt opts]
  (if (string? stmt)
    (recur (antlr/parse-statement stmt) opts)
    (sql/accept-visitor (map->StatementVisitor opts) stmt)))
