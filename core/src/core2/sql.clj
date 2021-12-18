(ns core2.sql
  (:require [clojure.java.io :as io]
            [clojure.zip :as zip]
            [core2.rewrite :as rew]
            [instaparse.core :as insta]
            [instaparse.failure]))

(set! *unchecked-math* :warn-on-boxed)

;; Instaparse SQL:2011 parser generator from the official grammar:

(def parse-sql2011
  (insta/parser
   (io/resource "core2/sql/SQL2011.ebnf")
   :auto-whitespace (insta/parser "whitespace = #'\\s+' | #'\\s*--[^\r\n]*\\s*' | #'\\s*/[*].*?([*]/\\s*|$)'")
   :string-ci true))

(def parse
  (memoize
   (fn self
     ([s]
      (self s :directly_executable_statement))
     ([s start-rule]
      (vary-meta (parse-sql2011 s :start start-rule) assoc ::sql s)))))

;; Rewrite to annotate variable scopes.

(declare ->root-annotation-ctx)

(defn- swap-select-list-and-table-expression [query-specification]
  (let [[xs ys] (split-at (- (count query-specification) 2) query-specification)]
    (with-meta (vec (concat xs (reverse ys))) (meta query-specification))))

(defn- ->current-env [ctx-stack]
  (some :env ctx-stack))

(defn- ->env [loc]
  (let [[{:keys [tables] :as ctx} :as ctx-stack] (rew/->ctx-stack loc)]
    (merge (->current-env ctx-stack) tables)))

(defn- find-with-id [loc table-or-query-name]
  (first (for [{:keys [with]} (rew/->ctx-stack loc)
               :let [{:keys [id]} (get with table-or-query-name)]
               :when id]
           id)))

(defn- ->line-info-str [loc]
  (let [{:core2.sql/keys [sql]} (zip/root loc)
        {:instaparse.gll/keys [start-index]} (meta (zip/node loc))
        {:keys [line column]} (instaparse.failure/index->line-column start-index sql)]
    (format "at line %d, column %d" line column)))

(defn- next-id [loc]
  (let [loc (vary-meta loc update ::next-id (fnil inc 0))]
    [loc (::next-id (meta loc))]))

(defn- register-table-primary [loc {:keys [table-or-query-name correlation-name] :as old-ctx}]
  (let [[loc id] (next-id loc)
        with-id (find-with-id loc table-or-query-name)
        correlation-name (or correlation-name table-or-query-name)
        table (cond-> {:table-or-query-name table-or-query-name
                       :correlation-name correlation-name
                       :id id}
                with-id (assoc :with-id with-id))]
    (-> (rew/assoc-in-ctx loc [:tables correlation-name] table)
        (zip/edit vary-meta assoc ::table-id id))))

(defn- register-with-list-element [loc {:keys [query-name] :as old-ctx}]
  (let [[loc id] (next-id loc)]
    (-> (rew/assoc-in-ctx loc [:with query-name] {:query-name query-name :id id})
        (zip/edit vary-meta assoc ::with-id id))))

(defn- validate-column-reference [loc _]
  (let [identifiers (rew/text-nodes loc)]
    (case (count identifiers)
      1 (throw (IllegalArgumentException.
                (format "XTDB requires fully-qualified columns: %s %s"
                        (first identifiers) (->line-info-str loc))))
      (let [env (->env loc)
            table-reference (first identifiers)]
        (if-let [{:keys [id] :as table} (get env table-reference)]
          (-> (zip/edit loc vary-meta assoc ::table-id id)
              (rew/conj-ctx :columns {:identifiers identifiers :table-id id}))
          (throw (IllegalArgumentException.
                  (format "Table not in scope: %s %s"
                          table-reference (->line-info-str loc)))))))))

(defn- ->scoped-env []
  (rew/->scoped {:init (fn [loc]
                         (assoc (rew/->ctx loc) :env (->env loc)))}))

(def ^:private root-annotation-rules
  {:table_primary
   (rew/->scoped {:rule-overrides {:table_name (rew/->text :table-or-query-name)
                                   :query_name (rew/->text :table-or-query-name)
                                   :correlation_name (rew/->text :correlation-name)}
                  :exit register-table-primary})

   :column_reference
   (rew/->after validate-column-reference)

   :from_clause
   (rew/->after (fn [loc _]
                  (rew/assoc-ctx loc :env (->env loc))))

   :qualified_join
   (rew/->scoped {:rule-overrides {:join_condition (->scoped-env)}
                  :init (fn [loc]
                          (assoc (rew/->ctx loc) :tables {}))
                  :exit (fn [loc {:keys [tables]}]
                          (rew/into-ctx loc :tables tables))})

   :lateral_derived_table (->scoped-env)

   :with_list_element
   (rew/->scoped {:rule-overrides {:query_name (rew/->text :query-name)}
                  :exit register-with-list-element})

   ;; NOTE: this temporary swap is done so table_expression is walked
   ;; first, ensuring its variables are in scope by the time
   ;; select_list is walked.
   :query_specification
   (rew/->around (fn move-select-list-after-table-expression [loc _]
                   (zip/edit loc swap-select-list-and-table-expression))
                 (fn restore-select-list [loc _]
                   (zip/edit loc swap-select-list-and-table-expression)))

   :query_expression
   (rew/->scoped {:init (fn [_]
                          (->root-annotation-ctx))
                  :exit (fn [loc old-ctx]
                          (zip/edit loc vary-meta assoc ::scope (select-keys old-ctx [:with :tables :columns])))})})

(defn- ->root-annotation-ctx []
  {:tables {} :columns #{} :with {} :rules root-annotation-rules})

(defn annotate-tree [tree]
  (rew/rewrite-tree tree (->root-annotation-ctx)))

(comment
  (time
   (parse-sql2011
    "SELECT * FROM user WHERE user.id = TIME '20:00:00.000' ORDER BY id DESC"
    :start :directly_executable_statement))

  (time
   (parse
    "SELECT * FROM user WHERE user.id = TIME '20:00:00.000' ORDER BY id DESC"))

  (time
   (parse-sql2011
    "TIME '20:00:00.000'"
    :start :literal))

  (time
   (parse-sql2011
    "TIME (3) WITH TIME ZONE"
    :start :data_type))

  (parse-sql2011
   "GRAPH_TABLE ( myGraph,
    MATCH
      (Creator IS Person WHERE Creator.email = :email1)
      -[ IS Created ]->
      (M IS Message)
      <-[ IS Commented ]-
      (Commenter IS Person WHERE Commenter.email = :email2)
    COLUMNS ( M.creationDate, M.content )
    )"
   :start :graph_table)

  (time
   (parse-sql2011
    "INSERT INTO t1(e,c,b,d,a) VALUES(103,102,100,101,104)"
    :start :directly_executable_statement))

  (time
   (parse-sql2011
    "SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 ORDER BY 1"
    :start :directly_executable_statement)))

;; SQL:2011 official grammar:

;; https://jakewheat.github.io/sql-overview/sql-2011-foundation-grammar.html

;; Repository has both grammars for 1992, 1999, 2003, 2006, 2008, 2011
;; and 2016 and draft specifications for 1992, 1999, 2003, 2006 and
;; 2011: https://github.com/JakeWheat/sql-overview

;; See also Date, SQL and Relational Theory, p. 455-458, A Simplified
;; BNF Grammar

;; High level SQL grammar, from
;; https://calcite.apache.org/docs/reference.html

;; SQLite grammar:
;; https://github.com/bkiers/sqlite-parser/blob/master/src/main/antlr4/nl/bigo/sqliteparser/SQLite.g4
;; https://www.sqlite.org/lang_select.html

;; SQL BNF from the spec:
;; https://ronsavage.github.io/SQL/

;; PostgreSQL Antlr4 grammar:
;; https://github.com/tshprecher/antlr_psql/tree/master/antlr4

;; PartiQL: SQL-compatible access to relational, semi-structured, and nested data.
;; https://partiql.org/assets/PartiQL-Specification.pdf

;; SQL-99 Complete, Really
;; https://crate.io/docs/sql-99/en/latest/index.html

;; Why CockroachDB and PostgreSQL Are Compatible
;; https://www.cockroachlabs.com/blog/why-postgres/

;; GQL Parser
;; https://github.com/OlofMorra/GQL-parser

;; Graph database applications with SQL/PGQ
;; https://download.oracle.com/otndocs/products/spatial/pdf/AnD2020/AD_Develop_Graph_Apps_SQL_PGQ.pdf
