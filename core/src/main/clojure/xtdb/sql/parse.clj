(ns xtdb.sql.parse
  (:require [xtdb.antlr :as antlr]
            [xtdb.sql :as sql]
            [xtdb.table :as table])
  (:import (xtdb.antlr SqlVisitor)))

(defrecord StatementVisitor [default-db]
  SqlVisitor
  (visitQueryExpr [_ stmt]
    [:query {:stmt stmt}])

  (visitShowSnapshotTokenStatement [_ _]
    [:show-snapshot-token])

  (visitShowClockTimeStatement [_ _]
    [:show-clock-time])

  (visitInsertStatement [_ stmt]
    [:insert {:table (table/->ref default-db (sql/identifier-sym (.targetTable stmt))) :stmt stmt}])

  (visitPatchStatement [_ stmt]
    [:patch {:table (table/->ref default-db (sql/identifier-sym (.targetTable stmt))), :stmt stmt}])

  (visitUpdateStatement [_ stmt]
    [:update {:table (table/->ref default-db (sql/identifier-sym (.targetTable stmt))), :stmt stmt}])

  (visitDeleteStatement [_ stmt]
    [:delete {:table (table/->ref default-db (sql/identifier-sym (.targetTable stmt))), :stmt stmt}])

  (visitEraseStatement [_ stmt]
    [:erase {:table (table/->ref default-db (sql/identifier-sym (.targetTable stmt))), :stmt stmt}])

  (visitAssertStatement [_ stmt]
    [:assert {:stmt stmt, :message (some->> (.message stmt) (sql/accept-visitor sql/string-literal-visitor))}])

  (visitShowVariableStatement [_ stmt]
    [:show-variable {:stmt stmt}])

  (visitShowTransactionIsolationLevel [_ _]
    [:show-tx-isolation-level])

  (visitShowTimeZone [_ _]
    [:show-time-zone])

  (visitCreateUserStatement [_ stmt]
    [:create-user {:username (-> (.userName stmt) (.getText))
                   :password (-> (.password stmt)
                                 (.accept sql/string-literal-visitor))}])

  (visitAlterUserStatement [_ stmt]
    [:alter-user {:username (-> (.userName stmt) (.getText))
                  :password (-> (.password stmt)
                                (.accept sql/string-literal-visitor))}])

  (visitExecuteStatement [_ stmt]
    [:execute {:stmt stmt}]))

(defn parse-statement [stmt opts]
  (if (string? stmt)
    (recur (antlr/parse-statement stmt) opts)
    (sql/accept-visitor (map->StatementVisitor opts) stmt)))
