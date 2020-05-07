(ns crux.calcite
  (:require [cheshire.core :as json]
            [clojure.edn :as edn]
            [clojure.spec.alpha :as s]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [clojure.walk :refer [postwalk]]
            [crux.api :as crux]
            crux.calcite.types
            crux.db)
  (:import crux.calcite.CruxTable
           crux.calcite.types.SQLFunction
           crux.calcite.types.CruxKeywordFn
           java.lang.reflect.Field
           [java.sql DriverManager Types]
           [java.util Properties WeakHashMap]
           org.apache.calcite.avatica.jdbc.JdbcMeta
           java.util.List
           [org.apache.calcite.avatica.remote Driver LocalService]
           [org.apache.calcite.avatica.server HttpServer HttpServer$Builder]
           org.apache.calcite.DataContext
           org.apache.calcite.linq4j.Enumerable
           org.apache.calcite.rel.RelFieldCollation
           [org.apache.calcite.rel.type RelDataTypeFactory RelDataTypeFactory$Builder]
           [org.apache.calcite.rex RexCall RexDynamicParam RexInputRef RexLiteral RexNode]
           org.apache.calcite.sql.SqlKind
           org.apache.calcite.sql.type.SqlTypeName
           org.apache.calcite.util.Pair))

(defonce ^WeakHashMap !crux-nodes (WeakHashMap.))

(defn -like [s pattern]
  (org.apache.calcite.runtime.SqlFunctions/like s pattern))

(defn -substring [c s l]
  (org.apache.calcite.runtime.SqlFunctions/substring c s l))

(def ^:private sql-fns
  {"SUBSTRING" 'crux.calcite/-substring})

(def ^:private standard-ops
  {SqlKind/EQUALS '=
   SqlKind/NOT_EQUALS 'not=
   SqlKind/GREATER_THAN '>
   SqlKind/GREATER_THAN_OR_EQUAL '>=
   SqlKind/LESS_THAN '<
   SqlKind/LESS_THAN_OR_EQUAL '<=
   SqlKind/LIKE 'crux.calcite/-like
   SqlKind/IS_NULL 'nil?
   SqlKind/IS_NOT_NULL 'boolean
   SqlKind/TIMES '*
   SqlKind/PLUS '+
   SqlKind/MINUS '-})

(defn- lookup-op [^RexCall c]
  (if (= SqlKind/OTHER_FUNCTION (.getKind c))
    (get sql-fns (.getName (.-op c)))
    (get standard-ops (.getKind c))))

(defprotocol RexNodeToVar
  (->var [this schema]))

(defprotocol RexNodeToClauses
  (->clauses [this schema]))

(extend-protocol RexNodeToVar
  RexInputRef
  (->var [this schema]
    (get-in schema [:crux.sql.table/query :find (.getIndex this)]))

  RexLiteral
  (->var [this schema]
    (.getValue2 this))

  RexDynamicParam
  (->var [this schema]
    this)

  RexCall
  (->var [this schema]
    (if (and (= SqlKind/OTHER_FUNCTION (.getKind this))
             (= "KEYWORD" (str (.-op this))))
      (CruxKeywordFn. this)
      (if-let [op (lookup-op this)]
        (SQLFunction. (gensym) op (map #(->var % schema) (.getOperands this)))
        (throw (IllegalArgumentException. (str "Unsupported fn: " this)))))))

(defn- ground-vars [or-statement]
  (let [vars (distinct (mapcat #(filter symbol? (rest (first %))) or-statement))]
    (vec
     (for [clause or-statement]
       (apply list 'and clause  (map #(vector (list 'identity %)) vars))))))

(extend-protocol RexNodeToClauses
  RexInputRef
  (->clauses [this schema]
    [[(list '= (->var this schema) true)]])

  RexCall
  (->clauses [n schema]
    (if-let [op (lookup-op n)]
      [[(apply list op (map #(->var % schema) (.getOperands n)))]]
      (condp = (.getKind n)
        SqlKind/AND
        (->clauses (.-operands n) schema)
        SqlKind/OR
        [(apply list 'or (ground-vars (->clauses (.-operands n) schema)))]
        SqlKind/NOT
        [(apply list 'not (->clauses (.-operands n) schema))])))

  java.util.List
  (->clauses [this schema]
    (mapcat #(->clauses % schema) this)))

(defn- post-process [schema clauses]
  (let [args (atom {})
        sql-ops (atom [])
        clauses (postwalk (fn [x]
                            (condp instance? x

                              SQLFunction
                              (do
                                (swap! sql-ops conj x)
                                (.sym x))

                              RexDynamicParam
                              (let [sym (gensym)]
                                (swap! args assoc (.getName ^RexDynamicParam x) sym)
                                sym)

                              CruxKeywordFn
                              (keyword (->var (first (.-operands (.r ^CruxKeywordFn x))) schema))

                              x))
                          clauses)
        calc-clauses (for [op ^SQLOperation @sql-ops]
                       [(apply list (.op op) (.operands op)) (.sym op)])]
    {:clauses clauses
     :calc-clauses calc-clauses
     :args @args}))

(defn enrich-filter [schema ^RexNode filter]
  (let [{:keys [clauses calc-clauses args]} (post-process schema (->clauses filter schema))]
    (-> schema
        (update-in [:crux.sql.table/query :where] (comp vec concat) clauses calc-clauses)
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

(defn enrich-project [schema projects]
  (let [{:keys [clauses calc-clauses]} (post-process schema (mapv #(->var (.-left %) schema) projects))]
    (-> schema
        ;; any calcs..
        (update-in [:crux.sql.table/query :where] (comp vec concat) calc-clauses)
        (assoc-in [:crux.sql.table/query :find] clauses))))

(defn enrich-join [s1 s2 join-type condition]
  (let [q1 (:crux.sql.table/query s1)
        q2 (:crux.sql.table/query s2)
        s2-lvars (into {} (map #(vector % (gensym %))) (keys (:crux.sql.table/columns s2)))
        q2 (clojure.walk/postwalk (fn [x] (if (symbol? x) (get s2-lvars x x) x)) q2)
        s3 (assoc s1 :crux.sql.table/query (merge-with (comp vec concat) q1 q2))]
    (update-in s3 [:crux.sql.table/query :where] #(vec (concat % (->clauses condition s3))))))

(defn- transform-result [tuple]
  (let [tuple (map #(or (and (float? %) (double %))
                        ;; Calcite enumerator wants millis for timestamps:
                        (and (inst? %) (inst-ms %))
                        (and (keyword? %) (str %))
                        %) tuple)]
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
          query (update query :args (fn [args] [(into {} (map (fn [[k v]]
                                                                [v (.get data-context k)])
                                                              args))]))]
      (log/debug query)
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
(def ^:private supported-types #{:boolean :bigint :double :float :integer :timestamp :varchar})

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
        (log/debug "Adding column" col-name col-type)
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
  (let [node (get !crux-nodes (get operands "CRUX_NODE"))]
    (assert node)
    (proxy [org.apache.calcite.schema.impl.AbstractSchema] []
      (getTableMap []
        (into {}
              (for [table-schema (lookup-schema node)]
                (do (when-not (s/valid? ::table table-schema)
                      (throw (IllegalStateException. (str "Invalid table schema: " (prn-str table-schema)))))
                    [(string/upper-case (:crux.sql.table/name table-schema))
                     (CruxTable. node table-schema)])))))))

(def ^:private model
  {:version "1.0",
   :defaultSchema "crux",
   :schemas [{:name "crux",
              :type "custom",
              :factory "crux.calcite.CruxSchemaFactory",
              :functions [{:name "KEYWORD", :className "crux.calcite.KeywordFn"}]}]})

(defn- model-properties [node-uuid]
  (doto (Properties.)
    (.put "model" (str "inline:" (-> model
                                     (update-in [:schemas 0 :operand] assoc "CRUX_NODE" node-uuid)
                                     json/generate-string)))))

(defrecord CalciteAvaticaServer [^HttpServer server node-uuid]
  java.io.Closeable
  (close [this]
    (.remove !crux-nodes node-uuid)
    (.stop server)))

(defonce registration (java.sql.DriverManager/registerDriver (crux.calcite.CruxJdbcDriver.)))

(defn ^java.sql.Connection jdbc-connection [node]
  (assert node)
  (DriverManager/getConnection "jdbc:crux:" (-> node meta :crux.node/topology ::server :node-uuid model-properties)))

(defn start-server [{:keys [:crux.node/node]} {:keys [::port]}]
  (let [node-uuid (str (java.util.UUID/randomUUID))]
    (.put !crux-nodes node-uuid node)
    (let [server (.build (doto (HttpServer$Builder.)
                           (.withHandler (LocalService. (JdbcMeta. "jdbc:crux:" (model-properties node-uuid)))
                                         org.apache.calcite.avatica.remote.Driver$Serialization/PROTOBUF)
                           (.withPort port)))]
      (.start server)
      (CalciteAvaticaServer. server node-uuid))))

(def module {::server {:start-fn start-server
                       :args {::port {:doc "JDBC Server Port"
                                      :default 1501
                                      :crux.config/type :crux.config/nat-int}}
                       :deps #{:crux.node/node}}})
