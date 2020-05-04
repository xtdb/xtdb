(ns crux.calcite
  (:require [cheshire.core :as json]
            [clojure.edn :as edn]
            [clojure.spec.alpha :as s]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [clojure.walk :refer [postwalk]]
            [crux.api :as crux]
            crux.db)
  (:import crux.calcite.CruxTable
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
           org.apache.calcite.sql.SqlKind
           org.apache.calcite.sql.type.SqlTypeName
           org.apache.calcite.util.Pair))

(defonce ^WeakHashMap !crux-nodes (WeakHashMap.))

(defprotocol OperandToCruxInput
  (operand->v [this schema]))

(extend-protocol OperandToCruxInput
  RexInputRef
  (operand->v [this schema]
    (get-in schema [:crux.sql.table/query :find (.getIndex this)]))

  RexCall
  (operand->v [this schema]
    (case (str (.-op this))
      "KEYWORD"
      (keyword (operand->v (first (.-operands this)) schema))
      (operand->v (first (.-operands this)) schema)))

  RexLiteral
  (operand->v [this schema]
    (.getValue2 this))

  RexDynamicParam
  (operand->v [this schema]
    this))

(defn- ->operands [schema ^RexCall filter*]
  (->> (.getOperands filter*)
       (map #(operand->v % schema))))

(defn -like [s pattern]
  (org.apache.calcite.runtime.SqlFunctions/like s pattern))

(defn- sym-triple [sym schema]
  (first (filter (fn [clause]
                   (and (s/valid? :crux.query/triple clause)
                        (= sym (last clause))))
                 (get-in schema [:crux.sql.table/query :where]))))

(defn- ->crux-where-clauses
  [schema ^RexNode filter*]
  (condp = (.getKind filter*)
    SqlKind/EQUALS
    (let [[o1 o2] (sort-by symbol? (->operands schema filter*))]
      (if (and (symbol? o1) (not (symbol? o2)))
        (let [[e a v] (sym-triple o1 schema)]
          [e a o2])
        [[(apply list '= (->operands schema filter*))]]))
    SqlKind/NOT_EQUALS
    [[(apply list 'not= (->operands schema filter*))]]
    SqlKind/AND
    (mapcat (partial ->crux-where-clauses schema) (.-operands ^RexCall filter*))
    SqlKind/OR
    [(apply list 'or (mapcat (partial ->crux-where-clauses schema) (.-operands ^RexCall filter*)))]
    SqlKind/INPUT_REF
    [[(list '= (operand->v filter* schema) true)]]
    SqlKind/NOT
    [(apply list 'not (mapcat (partial ->crux-where-clauses schema) (.-operands ^RexCall filter*)))]
    SqlKind/GREATER_THAN
    [[(apply list '> (->operands schema filter*))]]
    SqlKind/GREATER_THAN_OR_EQUAL
    [[(apply list '>= (->operands schema filter*))]]
    SqlKind/LESS_THAN
    [[(apply list '< (->operands schema filter*))]]
    SqlKind/LESS_THAN_OR_EQUAL
    [[(apply list '<= (->operands schema filter*))]]
    SqlKind/LIKE
    [[(apply list 'crux.calcite/-like (->operands schema filter*))]]
    SqlKind/IS_NULL
    [[(list 'nil? (first (->operands schema filter*)))]]
    SqlKind/IS_NOT_NULL
    [[(list 'boolean (first (->operands schema filter*)))]]))

(defn enrich-filter [schema ^RexNode filter]
  (let [args (atom {})
        clauses (postwalk (fn [x] (if (instance? RexDynamicParam x)
                                    (let [sym (gensym)]
                                      (swap! args assoc (.getName ^RexDynamicParam x) sym)
                                      sym)
                                    x))
                          (->crux-where-clauses schema filter))]
    (-> schema
        (update-in [:crux.sql.table/query :where] (comp vec concat) clauses)
        ;; Todo consider case of multiple arg maps
        (update-in [:crux.sql.table/query :args] merge @args))))

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

(defprotocol OperandToFieldIndex
  (operand->field-indexes [this]))

(extend-protocol OperandToFieldIndex
  Pair
  (operand->field-indexes [this]
    (operand->field-indexes (.-left this)))

  RexInputRef
  (operand->field-indexes [this]
    [(.getIndex this)])

  RexLiteral
  (operand->field-indexes [this]
    [])

  RexCall
  (operand->field-indexes [this]
    (mapcat operand->field-indexes (.operands this))))

(defn enrich-project [schema projects]
  (update-in schema [:crux.sql.table/query :find]
             (fn [existing-projects]
               (->> projects
                    (mapcat operand->field-indexes)
                    (mapv (fn [i] (nth existing-projects i)))))))

(defn enrich-join [s1 s2 join-type condition]
  (let [q1 (:crux.sql.table/query s1)
        q2 (:crux.sql.table/query s2)
        s2-lvars (into {} (map #(vector % (gensym %))) (keys (:crux.sql.table/columns s2)))
        q2 (clojure.walk/postwalk (fn [x] (if (symbol? x) (get s2-lvars x x) x)) q2)
        s3 (assoc s1 :crux.sql.table/query (merge-with (comp vec concat) q1 q2))]
    (update-in s3 [:crux.sql.table/query :where] #(vec (concat % (->crux-where-clauses s3 condition))))))

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
