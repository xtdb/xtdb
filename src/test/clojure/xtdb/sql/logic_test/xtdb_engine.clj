(ns xtdb.sql.logic-test.xtdb-engine
  (:require [clojure.string :as str]
            [xtdb.antlr :as antlr]
            [xtdb.api :as xt]
            [xtdb.error :as err]
            xtdb.node.impl
            [xtdb.operator.scan :as scan]
            [xtdb.sql.logic-test.runner :as slt]
            [xtdb.sql.plan :as plan]
            [xtdb.util :as util])
  (:import [java.time Instant]
           (org.antlr.v4.runtime ParserRuleContext)
           (xtdb.antlr SqlVisitor SqlVisitor)
           xtdb.api.query.IKeyFn
           xtdb.node.impl.Node))

(defn- create-table [node {:keys [table-name columns]}]
  (assert (nil? (get-in node [:tables table-name])))
  (assoc-in node [:tables table-name] columns))

(defn- create-view [node {:keys [view-name as]}]
  (assert (nil? (get-in node [:views view-name])))
  (assoc-in node [:views view-name] as))

(defn- execute-record [node record]
  (case (:type record)
    :create-table (create-table node record)
    :create-view (create-view node record)))

(defn skip-statement? [^String x]
  (boolean (re-find #"(?is)^\s*CREATE\s+(UNIQUE\s+)?INDEX\s+(\w+)\s+ON\s+(\w+)\s*\((.+)\)\s*$" x)))

(defn- execute-sql-statement [node sql-statement variables opts]
  (xt/execute-tx node
                 [[:sql sql-statement]]
                 (cond-> opts
                   (get variables "CURRENT_TIMESTAMP") (assoc :current-time (Instant/parse (get variables "CURRENT_TIMESTAMP")))))

  node)

(defn- execute-sql-query [{:keys [live-idx] :as node} sql-statement variables {:keys [direct-sql] :as opts}]
  (let [!cache (atom {})
        plan-stmt plan/plan-statement]

    ;; we remove _id from non-direct SLT queries because `SELECT *` doesn't expect it to be there
    (with-redefs [plan/plan-statement (fn self
                                        ([sql] (self sql {}))
                                        ([sql opts]
                                         (let [plan (plan-stmt sql (cond-> opts
                                                                     (not direct-sql) (update :table-info update-vals #(disj % "_id"))))]
                                           (swap! !cache assoc sql plan)
                                           plan)))]
      (let [res (xt/q node sql-statement
                      (-> opts
                          (assoc :key-fn :snake-case-string)
                          (cond-> (get variables "CURRENT_TIMESTAMP") (assoc :current-time (Instant/parse (get variables "CURRENT_TIMESTAMP"))))))

            ;; we grab the projection afterwards so that xt/q has awaited the tx
            ;; TODO hoping that there'll be a better means of getting hold of this soon
            projection (->> (:col-syms (or (get @!cache sql-statement)
                                           (plan/plan-statement sql-statement {:table-info (scan/tables-with-cols live-idx)})))
                            (mapv (comp #(.denormalize ^IKeyFn (identity #xt/key-fn :snake-case-string) %) str)))]
        (vec
         (for [row res]
           (mapv row projection)))))))

(defn parse-create-table [^String x]
  (when-let [[_ table-name columns] (re-find #"(?is)^\s*CREATE\s+TABLE\s+(\w+)\s*\((.+)\)\s*$" x)]
    {:type :create-table
     :table-name (keyword table-name)
     :columns (vec (for [column (str/split columns #",")]
                     (symbol (->> (str/split column #"\s+")
                                  (remove str/blank?)
                                  (first)))))}))

(defn parse-create-view [^String x]
  (when-let [[_ view-name query] (re-find #"(?is)^\s*CREATE\s+VIEW\s+(\w+)\s+AS\s+(.+)\s*$" x)]
    {:type :create-view
     :view-name (keyword view-name)
     :as query}))

(defrecord InsertOpsVisitor [node statement]
  SqlVisitor
  (visitInsertStmt [this ctx] (.accept (.insertStatement ctx) this))

  (visitInsertStatement [this ctx]
    (-> (.insertColumnsAndSource ctx)
        (.accept (assoc this :insert-table (keyword (plan/identifier-sym (.tableName ctx)))))))

  (visitInsertValues [{{:keys [tables]} :node, :keys [insert-table] :as this} ctx]
    (let [this (-> this
                   (assoc :insert-cols (->> (if-let [col-list (.columnNameList ctx)]
                                              (mapv plan/identifier-sym (.columnName col-list))
                                              (get tables insert-table))
                                            (mapv keyword))))]
      [(into [:put-docs insert-table]
             (for [^ParserRuleContext rvc-ctx (-> (.tableValueConstructor ctx)
                                                  (.rowValueList)
                                                  (.rowValueConstructor))]
               (.accept rvc-ctx this)))]))

  (visitSingleExprRowConstructor [{:keys [insert-cols]} ctx]
    (assert (= 1 (count insert-cols)))
    (let [expr-visitor (plan/->ExprPlanVisitor nil nil)]
      (merge {:xt/id (random-uuid)}
             {(first insert-cols) (.accept (.expr ctx) expr-visitor)})))

  (visitMultiExprRowConstructor [{:keys [insert-cols]} ctx]
    (let [expr-visitor (plan/->ExprPlanVisitor nil nil)]
      (merge {:xt/id (random-uuid)}
             (zipmap insert-cols (for [^ParserRuleContext expr (.expr ctx)]
                                   (.accept expr expr-visitor))))))

  (visitInsertFromSubquery [_ _] [[:sql statement]]))

(defrecord SltStmtVisitor [node statement]
  SqlVisitor
  (visitErrorNode [_ ctx]
    (if-let [record (or (parse-create-table statement)
                        (parse-create-view statement))]
      (execute-record node record)
      (throw (err/illegal-arg :xtdb.sql/parse-error
                              {::err/message (str ctx)
                               :statement statement}))))

  (visitInsertStmt [this ctx] (.accept (.insertStatement ctx) this))

  (visitInsertStatement [_ ctx]
    (let [ops (.accept ctx (->InsertOpsVisitor node statement))]
      (xt/execute-tx node ops)
      node)))

(extend-protocol slt/DbEngine
  Node
  (get-engine-name [_] "xtdb")

  (execute-statement [node statement variables]
    (cond
      (skip-statement? statement) node

      (str/starts-with? statement "CREATE TABLE") (execute-record node (parse-create-table statement))
      (str/starts-with? statement "CREATE VIEW") (execute-record node (parse-create-view statement))

      (:direct-sql slt/*opts*) (execute-sql-statement node statement variables (select-keys slt/*opts* [:decorrelate? :direct-sql]))

      :else (-> (antlr/parse-statement statement)
                (.accept (->SltStmtVisitor node statement)))))

  (execute-query [this query variables]
    (execute-sql-query this query variables (select-keys slt/*opts* [:decorrelate? :direct-sql]))))
