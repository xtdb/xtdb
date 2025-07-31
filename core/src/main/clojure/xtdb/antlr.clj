(ns xtdb.antlr
  (:import (xtdb.query SqlParser)))

(defn parse-expr [sql] (SqlParser/parseExpr sql))

(defn parse-where [sql] (SqlParser/parseWhere sql))

(defn parse-statement ^xtdb.antlr.Sql$DirectlyExecutableStatementContext [sql]
  (SqlParser/parseStatement sql))

(defn parse-multi-statement [sql]
  (SqlParser/parseMultiStatement sql))
