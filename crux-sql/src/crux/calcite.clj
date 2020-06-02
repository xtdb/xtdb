(ns crux.calcite
  (:require [cheshire.core :as json]
            [clojure.edn :as edn]
            [clojure.spec.alpha :as s]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [clojure.walk :refer [postwalk]]
            [crux.api :as crux]
            [crux.calcite.types]
            crux.db)
  (:import crux.calcite.CruxTable
           [crux.calcite.types ArbitraryFn SQLCondition SQLPredicate]
           [java.lang.reflect Field Method]
           [java.sql DriverManager Types]
           [java.util List Properties WeakHashMap]
           [org.apache.calcite.adapter.enumerable EnumUtils RexImpTable RexImpTable$NullAs RexToLixTranslator]
           org.apache.calcite.adapter.java.JavaTypeFactory
           org.apache.calcite.avatica.jdbc.JdbcMeta
           [org.apache.calcite.avatica.remote Driver LocalService]
           [org.apache.calcite.avatica.server HttpServer HttpServer$Builder]
           org.apache.calcite.DataContext
           org.apache.calcite.jdbc.JavaTypeFactoryImpl
           org.apache.calcite.linq4j.Enumerable
           [org.apache.calcite.linq4j.function Function0 Function1 Function2]
           [org.apache.calcite.linq4j.tree Expression Expressions MethodCallExpression ParameterExpression]
           org.apache.calcite.rel.RelFieldCollation
           [org.apache.calcite.rel.type RelDataTypeFactory RelDataTypeFactory$Builder]
           [org.apache.calcite.rex RexCall RexDynamicParam RexInputRef RexLiteral RexNode RexVariable]
           org.apache.calcite.runtime.SqlFunctions
           [org.apache.calcite.sql.fun SqlStdOperatorTable SqlTrimFunction$Flag]
           org.apache.calcite.sql.SqlKind
           org.apache.calcite.sql.type.SqlTypeName
           [org.apache.calcite.util BuiltInMethod Pair]
           clojure.lang.Symbol))

(defonce ^WeakHashMap !crux-nodes (WeakHashMap.))

;; Datalog fns:

(defn -divide [x y]
  (let [z (/ x y)]
    (if (ratio? z) (long z) z)))

(defn -mod [x y]
  (int (mod x y)))

(defn -like [s pattern]
  (SqlFunctions/like s pattern))

(defn -lambda [l & args]
  (condp instance? l
    Function1
    (.apply ^Function1 l (first args))
    Function2
    (.apply ^Function2 l (first args) (second args))
    (.apply ^Function0 l)))

;; SQL Operators:

(def ^:private pred-fns
  {SqlKind/EQUALS '=
   SqlKind/NOT_EQUALS 'not=
   SqlKind/GREATER_THAN '>
   SqlKind/GREATER_THAN_OR_EQUAL '>=
   SqlKind/LESS_THAN '<
   SqlKind/LESS_THAN_OR_EQUAL '<=
   SqlKind/LIKE 'crux.calcite/-like
   SqlKind/IS_NULL 'nil?
   SqlKind/IS_NOT_NULL 'boolean})

(def ^:private arithmetic-fns
  {SqlKind/TIMES '*
   SqlKind/PLUS '+
   SqlKind/MINUS '-
   SqlKind/DIVIDE 'crux.calcite/-divide
   SqlKind/MOD 'crux.calcite/-mod})

(declare ->ast)

(def ^:private ^JavaTypeFactory jtf (JavaTypeFactoryImpl.))

(defn- ->lambda-expression [^MethodCallExpression m parameter-expressions operands]
  (let [l (Expressions/lambda m ^Iterable parameter-expressions)]
    (ArbitraryFn. 'crux.calcite/-lambda (cons l operands))))

(defn- ^MethodCallExpression ->method-call-expression [m parameter-expressions]
  (if (instance? Method m)
    (crux.calcite.CruxUtils/callExpression m (into-array Expression parameter-expressions))
    (EnumUtils/call SqlFunctions m parameter-expressions)))

(defn- method->lambda [^RexCall n schema m]
  (let [parameter-expressions (mapv #(Expressions/parameter (.getJavaClass jtf (condp instance? %
                                                                                 RexVariable
                                                                                 (.getType ^RexVariable %)
                                                                                 RexCall
                                                                                 (.getType ^RexCall %)
                                                                                 RexLiteral
                                                                                 (.getType ^RexLiteral %))))
                                    (.getOperands n))
        m (->method-call-expression m parameter-expressions)]
    (->lambda-expression m parameter-expressions (mapv #(->ast % schema) (.getOperands n)))))

(defn linq-lambda [^RexCall n schema]
  (or (when-let [m ({SqlStdOperatorTable/LOWER (.method BuiltInMethod/LOWER)
                     SqlStdOperatorTable/UPPER (.method BuiltInMethod/UPPER)
                     SqlStdOperatorTable/INITCAP (.method BuiltInMethod/INITCAP)
                     SqlStdOperatorTable/CONCAT (.method BuiltInMethod/STRING_CONCAT)
                     SqlStdOperatorTable/CHAR_LENGTH (.method BuiltInMethod/CHAR_LENGTH)
                     SqlStdOperatorTable/TRUNCATE "struncate"
                     SqlStdOperatorTable/LAST_DAY "lastDay"}
                    (.getOperator n))]
        (method->lambda n schema m))
      (when-let [data-context-fn ({SqlStdOperatorTable/CURRENT_DATE (.method BuiltInMethod/CURRENT_DATE)
                                   SqlStdOperatorTable/CURRENT_TIME (.method BuiltInMethod/CURRENT_TIME)
                                   SqlStdOperatorTable/CURRENT_TIMESTAMP (.method BuiltInMethod/CURRENT_TIMESTAMP)}
                                  (.getOperator n))]
        (->lambda-expression (->method-call-expression data-context-fn [DataContext/ROOT]) [] []))
      (condp = (.getOperator n)
        SqlStdOperatorTable/CEIL
        (if (#{"BIGINT" "INTEGER" "SMALLINT" "TINYINT"} (.getName (.getSqlTypeName (.getType n))))
          (->ast (first (.getOperands n)) schema)
          (method->lambda n schema (.getName (.method BuiltInMethod/CEIL))))
        SqlStdOperatorTable/FLOOR
        (if (#{"BIGINT" "INTEGER" "SMALLINT" "TINYINT"} (.getName (.getSqlTypeName (.getType n))))
          (->ast (first (.getOperands n)) schema)
          (method->lambda n schema (.getName (.method BuiltInMethod/FLOOR))))
        SqlStdOperatorTable/SUBSTRING
        (let [exprs [(Expressions/parameter String)
                     (Expressions/constant (int (.getValue2 ^RexLiteral (nth (.getOperands n) 1))))
                     (Expressions/constant (int (.getValue2 ^RexLiteral (nth (.getOperands n) 2))))]
              operands [(first (.getOperands n))]
              m (->method-call-expression (.method BuiltInMethod/SUBSTRING) exprs)]
          (->lambda-expression m
                               (filter #(instance? ParameterExpression %) exprs)
                               (mapv #(->ast % schema) operands)))
        SqlStdOperatorTable/REPLACE
        (let [exprs [(Expressions/parameter String)
                     (Expressions/constant (.getValue2 ^RexLiteral (nth (.getOperands n) 1)))
                     (Expressions/constant (.getValue2 ^RexLiteral (nth (.getOperands n) 2)))]
              operands [(first (.getOperands n))]
              m (->method-call-expression (.method BuiltInMethod/REPLACE) exprs)]
          (->lambda-expression m
                               (filter #(instance? ParameterExpression %) exprs)
                               (mapv #(->ast % schema) operands)))
        SqlStdOperatorTable/TRIM
        (let [f (.getValue ^RexLiteral (first (.getOperands n)))
              left? (or (= SqlTrimFunction$Flag/BOTH f )
                        (= SqlTrimFunction$Flag/LEADING f))
              right? (or (= SqlTrimFunction$Flag/BOTH f )
                         (= SqlTrimFunction$Flag/TRAILING f))
              exprs [(Expressions/constant left?)
                     (Expressions/constant right?)
                     (Expressions/constant (.getValue2 ^RexLiteral (nth (.getOperands n) 1)))
                     (Expressions/parameter String)
                     (Expressions/constant true)]
              operands [(nth (.getOperands n) 2)]
              m (->method-call-expression (.method BuiltInMethod/TRIM) exprs)]
          (->lambda-expression m
                               (filter #(instance? ParameterExpression %) exprs)
                               (mapv #(->ast % schema) operands)))
        (throw (IllegalArgumentException. (str "Can't understand call " n))))))

(defn- ->ast
  "Turn the Calcite RexNode into a data-structure we can parse."
  [^RexNode n schema]
  (or (when-let [op (pred-fns (.getKind n))]
        (SQLPredicate. op (map #(->ast % schema) (.-operands ^RexCall n))))

      (when (and (= SqlKind/OTHER_FUNCTION (.getKind n))
                 (= "KEYWORD" (str (.-op ^RexCall n))))
        (keyword (.getValue2 ^RexLiteral (first (.-operands ^RexCall n)))))

      (when-let [op (get arithmetic-fns (.getKind n))]
        (ArbitraryFn. op (map #(->ast % schema) (.getOperands ^RexCall n))))

      (and (#{SqlKind/AND SqlKind/OR SqlKind/NOT} (.getKind n))
           (SQLCondition. (.getKind n) (mapv #(->ast % schema) (.-operands ^RexCall n))))

      (and (instance? RexCall n) (linq-lambda n schema))

      n))

(defn- ->vars+clauses+exprs
  "Swaps out Calcs/Predicates for symbols and clauses."
  [schema nodes]
  (let [args (atom {})
        literals (atom {})
        clauses (atom [])]
    [(postwalk (fn [x]
                 (condp instance? x

                   Expression
                   (let [s (gensym)]
                     (swap! literals assoc s x)
                     s)

                   ArbitraryFn
                   (let [s (gensym)]
                     (swap! clauses conj [(apply list (.op ^ArbitraryFn x) (.operands ^ArbitraryFn x)) s])
                     s)

                   RexDynamicParam
                   (let [sym (gensym)]
                     (swap! args assoc (.getName ^RexDynamicParam x) sym)
                     sym)

                   RexLiteral
                   (let [s (gensym)]
                     (swap! literals assoc s x)
                     s)

                   RexInputRef
                   (get-in schema [:crux.sql.table/query :find (.getIndex ^RexInputRef x)])

                   x))
               nodes)
     @clauses @literals @args]))

(defn- ground-vars [or-statement]
  (let [vars (distinct (mapcat #(filter symbol? (rest (first %))) or-statement))]
    (vec
     (for [clause or-statement]
       (apply list 'and clause  (map #(vector (list 'identity %)) vars))))))

(defn ->where [x]
  (condp instance? x

    SQLPredicate
    [(apply list (.op ^SQLPredicate x) (map #(if (symbol? %) % (->where %))
                                            (.operands ^SQLPredicate x)))]

    SQLCondition
    (condp = (.c ^SQLCondition x)
      SqlKind/AND
      (mapv ->where (.clauses ^SQLCondition x))
      SqlKind/OR
      (apply list 'or (ground-vars (map ->where (.clauses ^SQLCondition x))))
      SqlKind/NOT
      (apply list 'not (map ->where (.clauses ^SQLCondition x))))

    Symbol
    [(list '= x true)]

    ;; I.e. keywords, uuids
    x))

(defn- vectorize-value [v]
  (if (or (not (vector? v)) (= 1 (count v))) (vector v) v))

(defn enrich-filter [schema ^RexNode filter]
  (log/debug "Enriching with filter" filter)
  (let [[filters clauses literals args] (->vars+clauses+exprs schema (->ast filter schema))]
    (-> schema
        (update-in [:crux.sql.table/query :where] (comp vec concat) (vectorize-value (->where filters)))
        (update-in [:crux.sql.table/query :where] (comp vec concat) clauses)
        ;; Todo consider case of multiple arg maps
        (update-in [:crux.sql.table/query :args] merge args)
        (update :literals merge literals))))

(defn enrich-project [schema projects]
  (log/debug "Enriching project with" projects)
  (let [[projects clauses literals] (->> (for [rex-node (map #(.-left ^Pair %) projects)]
                                           (->ast rex-node schema))
                                         (->vars+clauses+exprs schema))]
    (-> schema
        (assoc-in [:crux.sql.table/query :find] (vec projects))
        (update-in [:crux.sql.table/query :where] (comp vec concat) clauses)
        (update :literals merge literals))))

(defn enrich-join [s1 s2 join-type condition]
  (log/debug "Enriching join with" condition)
  (let [q1 (:crux.sql.table/query s1)
        q2 (:crux.sql.table/query s2)
        s2-lvars (into {} (map #(vector % (gensym %))) (keys (:crux.sql.table/columns s2)))
        q2 (clojure.walk/postwalk (fn [x] (if (symbol? x) (get s2-lvars x x) x)) q2)
        s3 (-> s1
               (assoc :crux.sql.table/query (merge-with (comp vec concat) q1 q2))
               (update-in [:crux.sql.table/query :args] (partial apply merge))
               (update :literals merge (:literals s2)))]
    (enrich-filter s3 condition)))

(defn enrich-sort-by [schema sort-fields]
  (assoc-in schema [:crux.sql.table/query :order-by]
            (mapv (fn [^RelFieldCollation f]
                    [(nth (get-in schema [:crux.sql.table/query :find]) (.getFieldIndex f))
                     (if (= (.-shortString (.getDirection f)) "DESC") :desc :asc)])
                  sort-fields)))

(defn enrich-limit [schema ^RexNode limit]
  (if limit
    (assoc-in schema [:crux.sql.table/query :limit] (RexLiteral/intValue limit))
    schema))

(defn enrich-offset [schema ^RexNode offset]
  (if offset
    (assoc-in schema [:crux.sql.table/query :offset] (RexLiteral/intValue offset))
    schema))

(defn enrich-limit-and-offset [schema ^RexNode limit ^RexNode offset]
  (-> schema (enrich-limit limit) (enrich-offset offset)))

(defn- transform-result [find-vars literals tuple]
  (let [tuple (map (fn [find-v tuple-v]
                     (or (literals find-v)
                         (or (and (float? tuple-v) (double tuple-v))
                             ;; Calcite enumerator wants millis for timestamps:
                             (and (inst? tuple-v) (inst-ms tuple-v))
                             (and (keyword? tuple-v) (str tuple-v))
                             tuple-v)))
                   find-vars tuple)]
    (if (= 1 (count tuple))
      (first tuple)
      (to-array tuple))))

(defn- ->enumerator [node valid-time transaction-time literals q]
  (proxy [org.apache.calcite.linq4j.AbstractEnumerable]
      []
    (enumerator []
      (let [_ (log/debug "Executing query:" q)
            db (crux/open-db node valid-time transaction-time)
            results (atom (crux/q db q))
            current (atom nil)]
        (proxy [org.apache.calcite.linq4j.Enumerator]
            []
          (current []
            (transform-result (:find q) literals @current))
          (moveNext []
            (reset! current (first @results))
            (swap! results next)
            (boolean @current))
          (reset []
            (throw (UnsupportedOperationException.)))
          (close []
            (.close db)))))))

(defn ^Enumerable scan [node ^Pair schema+expressions ^DataContext data-context]
  (try
    (let [
          literals (into {} (map (fn [^Pair p]
                                   [(symbol (.-left p)) (.-right p)])
                                 (.-right schema+expressions)))
          {:keys [crux.sql.table/query]} (edn/read-string (.-left schema+expressions))
          query (update query :args (fn [args] [(merge {:data-context data-context}
                                                       (into {} (map (fn [[k v]]
                                                                       [v (.get data-context k)])
                                                                     args))
                                                       literals)]))]
      (->enumerator node (.get data-context "VALIDTIME") (.get data-context "TRANSACTIONTIME") literals query))
    (catch Throwable e
      (log/error e)
      (throw e))))

(defn- ->literal-expression [^RexLiteral l]
  (RexToLixTranslator/translateLiteral l, (.getType ^RexLiteral l), jtf, RexImpTable$NullAs/NOT_POSSIBLE))

(defn- ^List ->literal-pairs [literals]
  (map (fn [[k l]]
         (Expressions/constant (Pair. (str k) (if (instance? Expression l) l (->literal-expression l)))))
       literals))

(defn ->expr [schema]
  (Expressions/constant (Pair. (Expressions/constant (prn-str (dissoc schema :literals)))
                               (Expressions/newArrayInit Pair (->literal-pairs (:literals schema))))))

(defn clojure-helper-fn [f]
  (fn [& args]
    (try
      (apply f args)
      (catch Throwable t
        (log/error t "Exception occured calling Clojure fn")
        (throw t)))))

(def ^:private mapped-types {:keyword SqlTypeName/OTHER})
(def ^:private supported-types #{:boolean :bigint :double :float :timestamp :varchar :decimal})

;; See: https://docs.oracle.com/javase/8/docs/api/java/sql/JDBCType.html
(def ^:private java-sql-types
  (select-keys (into {} (for [^java.lang.reflect.Field f (.getFields Types)]
                          [(keyword (.toLowerCase (.getName f))) (int ^Integer (.get f nil)) ]))
               supported-types))

(defn java-sql-types->calcite-sql-type [java-sql-type]
  (or (get mapped-types java-sql-type)
      (some-> java-sql-type java-sql-types SqlTypeName/getNameForJdbcType)
      (throw (IllegalArgumentException. (str "Unrecognised java.sql.Types: " java-sql-type)))))

(defn- ^String ->column-name [c]
  (string/replace (string/upper-case (str c)) #"^\?" ""))

(defn row-type [^RelDataTypeFactory type-factory node {:keys [:crux.sql.table/query :crux.sql.table/columns] :as table-schema}]
  (let [field-info  (RelDataTypeFactory$Builder. type-factory)]
    (doseq [c (:find query)]
      (let [col-name (->column-name c)
            col-type ^SqlTypeName (java-sql-types->calcite-sql-type (columns c))]
        (when-not col-type
          (throw (IllegalArgumentException. (str "Unrecognized column: " c))))
        (log/trace "Adding column" col-name col-type)
        (doto field-info
          (.add col-name col-type)
          (.nullable true))))
    (.build field-info)))

(s/def :crux.sql.table/name string?)
(s/def :crux.sql.table/columns (s/map-of symbol? java-sql-types->calcite-sql-type))
(s/def ::table (s/keys :req [:crux.db/id :crux.sql.table/name :crux.sql.table/columns]))

(defn- lookup-schema [node]
  (let [db (crux/db node)]
    (map first (crux/q db '{:find [e]
                            :where [[e :crux.sql.table/name]]
                            :full-results? true}))))

(defn create-schema [parent-schema name operands]
  (let [node (get !crux-nodes (get operands "CRUX_NODE"))
        scan-only? (get operands "SCAN_ONLY")]
    (assert node)
    (proxy [org.apache.calcite.schema.impl.AbstractSchema] []
      (getTableMap []
        (into {}
              (for [table-schema (lookup-schema node)]
                (do (when-not (s/valid? ::table table-schema)
                      (throw (IllegalStateException. (str "Invalid table schema: " (prn-str table-schema)))))
                    [(string/upper-case (:crux.sql.table/name table-schema))
                     (CruxTable. node table-schema scan-only?)])))))))

(def ^:private model
  {:version "1.0",
   :defaultSchema "crux",
   :schemas [{:name "crux",
              :type "custom",
              :factory "crux.calcite.CruxSchemaFactory",
              :functions [{:name "KEYWORD", :className "crux.calcite.KeywordFn"}]}]})

(defn- model-properties [node-uuid scan-only?]
  (doto (Properties.)
    (.put "model" (str "inline:" (-> model
                                     (update-in [:schemas 0 :operand] assoc "CRUX_NODE" node-uuid)
                                     (update-in [:schemas 0 :operand] assoc "SCAN_ONLY" scan-only?)
                                     json/generate-string)))))

(defrecord CalciteAvaticaServer [^HttpServer server node-uuid]
  java.io.Closeable
  (close [this]
    (.remove !crux-nodes node-uuid)
    (.stop server)))

(defonce registration (java.sql.DriverManager/registerDriver (crux.calcite.CruxJdbcDriver.)))

(defn ^java.sql.Connection jdbc-connection
  ([node] (jdbc-connection node false))
  ([node scan-only?]
   (assert node)
   (DriverManager/getConnection "jdbc:crux:" (-> node meta :crux.node/topology ::server :node-uuid (model-properties scan-only?)))))

(defn start-server [{:keys [:crux.node/node]} {:keys [::port ::scan-only?]}]
  (let [node-uuid (str (java.util.UUID/randomUUID))]
    (.put !crux-nodes node-uuid node)
    (let [server (.build (doto (HttpServer$Builder.)
                           (.withHandler (LocalService. (JdbcMeta. "jdbc:crux:" (model-properties node-uuid scan-only?)))
                                         org.apache.calcite.avatica.remote.Driver$Serialization/PROTOBUF)
                           (.withPort port)))]
      (.start server)
      (CalciteAvaticaServer. server node-uuid))))

(def module {::server {:start-fn start-server
                       :args {::port {:doc "JDBC Server Port"
                                      :default 1501
                                      :crux.config/type :crux.config/nat-int}
                              ::scan-only? {:doc "Crux Table Scan Only"
                                            :default false
                                            :crux.config/type :crux.config/boolean}}
                       :deps #{:crux.node/node}}})
