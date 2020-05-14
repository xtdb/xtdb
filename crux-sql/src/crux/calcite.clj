(ns crux.calcite
  (:require [cheshire.core :as json]
            [clojure.edn :as edn]
            [clojure.spec.alpha :as s]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [crux.calcite.types]
            [clojure.walk :refer [postwalk]]
            [crux.api :as crux]
            crux.db)
  (:import crux.calcite.CruxTable
           [crux.calcite.types SQLCondition SQLFunction SQLPredicate WrapLiteral]
           java.lang.reflect.Field
           [java.sql DriverManager Types]
           [java.util Properties WeakHashMap]
           org.apache.calcite.avatica.jdbc.JdbcMeta
           [org.apache.calcite.avatica.remote Driver LocalService]
           [org.apache.calcite.avatica.server HttpServer HttpServer$Builder]
           org.apache.calcite.DataContext
           org.apache.calcite.linq4j.Enumerable
           org.apache.calcite.rel.RelFieldCollation
           [org.apache.calcite.rel.type RelDataTypeFactory RelDataTypeFactory$Builder]
           [org.apache.calcite.rex RexCall RexDynamicParam RexInputRef RexLiteral RexNode]
           org.apache.calcite.runtime.SqlFunctions
           [org.apache.calcite.sql SqlFunction SqlKind SqlOperator]
           org.apache.calcite.sql.fun.SqlStdOperatorTable
           org.apache.calcite.sql.type.SqlTypeName
           [org.apache.calcite.util BuiltInMethod Pair]))

(defonce ^WeakHashMap !crux-nodes (WeakHashMap.))

(defn- vectorize-value [v]
  (if (or (not (vector? v)) (= 1 (count v))) (vector v) v))

(defn -like [s pattern]
  (SqlFunctions/like s pattern))

(defn -substring [^String c ^Integer s ^Integer l]
  (SqlFunctions/substring c s l))

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
   SqlKind/MINUS '-})

(def ^:private calcite-built-in-fns
  (let [calcite-built-in-fns (->> (BuiltInMethod/values)
                                  (map #(vector (.name ^BuiltInMethod %) %))
                                  (into {}))]
    (->> (.getFields SqlStdOperatorTable)
         (filter #(.isAssignableFrom SqlFunction (.getType ^Field %)))
         (keep (fn [^Field f]
                 (when-let [built-in-fn ^BuiltInMethod (calcite-built-in-fns (.getName f))]
                   (let [o ^SqlOperator (.get f (SqlStdOperatorTable/instance))]
                     [(.getName o) built-in-fn]))))
         (into {}))))

(comment
  (sort (keys calcite-built-in-fns)))

(defn -built-in [id data-context & args]
  (let [built-in-fn ^BuiltInMethod (calcite-built-in-fns id)
        args (if (= DataContext (first (.getParameterTypes (.method built-in-fn)))) [data-context] args)]
    (crux.calcite.CruxUtils/invokeStaticMethod (.method built-in-fn) (into-array Object args))))

(defn input-ref->attr [^RexInputRef i schema]
  (get-in schema [:crux.sql.table/query :find (.getIndex i)]))

(defn- ground-vars [or-statement]
  (let [vars (distinct (mapcat #(filter symbol? (rest (first %))) or-statement))]
    (vec
     (for [clause or-statement]
       (apply list 'and clause  (map #(vector (list 'identity %)) vars))))))

(declare ->ast)

(defn- ->operand [o schema]
  (if (instance? RexInputRef o)
    (input-ref->attr o schema)
    (->ast o schema)))

(defn- ->ast [^RexNode n schema]
  (or (when-let [op (pred-fns (.getKind n))]
        (SQLPredicate. op (map #(->operand % schema) (.-operands ^RexCall n))))

      (when (and (= SqlKind/OTHER_FUNCTION (.getKind n))
                 (= "KEYWORD" (str (.-op ^RexCall n))))
        (keyword (.getValue2 ^RexLiteral (first (.-operands ^RexCall n)))))

      (when-let [op (get arithmetic-fns (.getKind n))]
        (SQLFunction. (gensym) op (map #(->operand % schema) (.getOperands ^RexCall n))))

      (and (#{SqlKind/AND SqlKind/OR SqlKind/NOT} (.getKind n))
           (SQLCondition. (.getKind n) (mapv #(->ast % schema) (.-operands ^RexCall n))))

      (if (instance? RexCall n)
        (if-let [f (calcite-built-in-fns (.getName (.op ^RexCall n)))]
          (let [operands (into [(.getName (.op ^RexCall n)) 'data-context]
                               (map #(->operand % schema) (.getOperands ^RexCall n)))]
            (SQLFunction. (gensym) 'crux.calcite/-built-in operands))
          (throw (IllegalArgumentException. (str "Can't understand call " n)))))

      n))

(defn- post-process [schema clauses]
  (let [args (atom {})
        sql-ops (atom [])
        clauses (postwalk (fn [x]
                            (condp instance? x

                              SQLFunction
                              (do
                                (swap! sql-ops conj x)
                                (.sym ^SQLFunction x))

                              RexDynamicParam
                              (let [sym (gensym)]
                                (swap! args assoc (.getName ^RexDynamicParam x) sym)
                                sym)

                              RexLiteral
                              (.getValue2 ^RexLiteral x)

                              ;; If used as an argument, the literal should already have been extracted,
                              ;; so we can assume it's being used in a conditional (OR/AND/NOT)
                              RexInputRef
                              [(list '= (input-ref->attr x schema) true)]

                              SQLPredicate
                              [(apply list (.op ^SQLPredicate x) (.operands ^SQLPredicate x))]

                              SQLCondition
                              (condp = (.c ^SQLCondition x)
                                  SqlKind/AND
                                  (.clauses ^SQLCondition x)
                                  SqlKind/OR
                                  (apply list 'or (ground-vars (.clauses ^SQLCondition x)))
                                  SqlKind/NOT
                                  (apply list 'not (.clauses ^SQLCondition x)))

                              x))
                          clauses)
        calc-clauses (for [^SQLFunction op @sql-ops]
                       [(apply list (.op op) (.operands op)) (.sym op)])]
    {:clauses clauses
     :calc-clauses calc-clauses
     :args @args}))

(defn enrich-filter [schema ^RexNode filter]
  (log/debug "Enriching with filter" filter)
  (let [{:keys [clauses calc-clauses args]} (post-process schema (->ast filter schema))]
    (-> schema
        (update-in [:crux.sql.table/query :where] (comp vec concat) (vectorize-value clauses) calc-clauses)
        ;; Todo consider case of multiple arg maps
        (update-in [:crux.sql.table/query :args] merge args))))

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

(defn -literal [v]
  (WrapLiteral. v))

(defn enrich-project [schema projects]
  (log/debug "Enriching project with" projects)
  (let [{:keys [clauses calc-clauses]}
        (->> (for [rex-node (map #(.-left ^Pair %) projects)]
               (if (instance? RexLiteral rex-node)
                 (SQLFunction. (gensym) 'crux.calcite/-literal [rex-node])
                 (->operand rex-node schema)))
             (post-process schema))]
    (-> schema
        (update-in [:crux.sql.table/query :where] (comp vec concat) calc-clauses)
        (assoc-in [:crux.sql.table/query :find] (vec clauses)))))

(defn enrich-join [s1 s2 join-type condition]
  (log/debug "Enriching join with" condition)
  (let [q1 (:crux.sql.table/query s1)
        q2 (:crux.sql.table/query s2)
        s2-lvars (into {} (map #(vector % (gensym %))) (keys (:crux.sql.table/columns s2)))
        q2 (clojure.walk/postwalk (fn [x] (if (symbol? x) (get s2-lvars x x) x)) q2)
        s3 (-> s1
               (assoc :crux.sql.table/query (merge-with (comp vec concat) q1 q2))
               (update-in [:crux.sql.table/query :args] (partial apply merge)))]
    (enrich-filter s3 condition)))

(defn- transform-result [tuple]
  (let [tuple (map #(if (instance? WrapLiteral %)
                      ;; literal numbers expected to be ints:
                      (if (int? (.l ^WrapLiteral %)) (int (.l ^WrapLiteral %)) (.l ^WrapLiteral %))
                      (or (and (float? %) (double %))
                          ;; Calcite enumerator wants millis for timestamps:
                          (and (inst? %) (inst-ms %))
                          (and (keyword? %) (str %))
                          %))
                   tuple)]
    (if (= 1 (count tuple))
      (first tuple)
      (to-array tuple))))

(defn- perform-query [node valid-time q]
  (let [db (if valid-time (crux/db node valid-time) (crux/db node))]
    (proxy [org.apache.calcite.linq4j.AbstractEnumerable]
        []
        (enumerator []
          (let [snapshot (crux/new-snapshot db)
                results (atom (crux/q db snapshot q))
                current (atom nil)]
            (proxy [org.apache.calcite.linq4j.Enumerator]
                []
              (current []
                (transform-result @current))
              (moveNext []
                (reset! current (first @results))
                (swap! results next)
                (boolean @current))
              (reset []
                (throw (UnsupportedOperationException.)))
              (close []
                (.close snapshot))))))))

(defn ^Enumerable scan [node ^String schema ^DataContext data-context]
  (try
    (let [{:keys [crux.sql.table/query]} (edn/read-string schema)
          query (update query :args (fn [args] [(into {:data-context data-context}
                                                      (map (fn [[k v]]
                                                             [v (.get data-context k)])
                                                           args))]))]
      (log/debug "Executing query:" query)
      (perform-query node (.get data-context "VALIDTIME") query))
    (catch Throwable e
      (log/error e)
      (throw e))))

(defn clojure-helper-fn [f]
  (fn [& args]
    (try
      (apply f args)
      (catch Throwable t
        (log/error t "Exception occured calling Clojure fn")
        (throw t)))))

(def ^:private mapped-types {:keyword SqlTypeName/OTHER})
(def ^:private supported-types #{:boolean :bigint :double :float :timestamp :varchar})

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
