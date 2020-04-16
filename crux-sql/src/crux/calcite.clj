(ns crux.calcite
  (:require [cheshire.core :as json]
            [clojure.spec.alpha :as s]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [crux.api :as crux]
            crux.db)
  (:import java.sql.DriverManager
           [java.util Properties WeakHashMap]
           org.apache.calcite.avatica.jdbc.JdbcMeta
           [org.apache.calcite.avatica.remote Driver LocalService]
           [org.apache.calcite.avatica.server HttpServer HttpServer$Builder]
           [org.apache.calcite.rel.type RelDataTypeFactory RelDataTypeFactory$Builder]
           [org.apache.calcite.rex RexCall RexInputRef RexLiteral RexNode]
           org.apache.calcite.sql.SqlKind
           org.apache.calcite.sql.type.SqlTypeName))

(defonce ^WeakHashMap !crux-nodes (WeakHashMap.))

(defprotocol OperandToCruxInput
  (operand->v [this schema]))

(extend-protocol OperandToCruxInput
  RexInputRef
  (operand->v [this schema]
    (get-in schema [:crux.sql.table/query :find (.getIndex this)]))

  org.apache.calcite.rex.RexCall
  (operand->v [this schema]
    (case (str (.-op this))
      "CRUXID"
      (keyword (operand->v (first (.-operands this)) schema))
      (operand->v (first (.-operands this)) schema)))

  RexLiteral
  (operand->v [this schema]
    (.getValue2 this)))

(defn- ->operands [schema ^RexCall filter*]
  (->> (.getOperands filter*)
       (map #(operand->v % schema))))

(defn- ->crux-where-clauses
  [schema ^RexNode filter*]
  (condp = (.getKind filter*)
    SqlKind/EQUALS
    [[(apply list '= (->operands schema filter*))]]
    SqlKind/NOT_EQUALS
    [(list 'not [(apply list '= (->operands schema filter*))])]
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
    [[(apply list 'like (->operands schema filter*))]]
    SqlKind/IS_NULL
    [[(apply list '= (->operands schema filter*))]]
    SqlKind/IS_NOT_NULL
    [[(list 'not (apply list '= (->operands schema filter*)))]]))

(defn- ->crux-query
  [schema filters projects]
  (try
    (let [{:keys [crux.sql.table/columns crux.sql.table/query]} schema
          {:keys [find where]} query]
      {:find (if (seq projects) (mapv find projects) find)
       :where (vec
               (concat
                (mapcat (partial ->crux-where-clauses schema) filters)
                where))
       :args [{:like #(org.apache.calcite.runtime.SqlFunctions/like %1 %2)}]})
    (catch Throwable e
      (log/error e)
      (throw e))))

(def ^:private column-types->sql-types {:varchar SqlTypeName/VARCHAR
                                        :keyword SqlTypeName/VARCHAR
                                        :integer SqlTypeName/INTEGER
                                        :long SqlTypeName/BIGINT
                                        :boolean SqlTypeName/BOOLEAN
                                        :double SqlTypeName/DOUBLE
                                        :datetime SqlTypeName/DATE})

(defn- perform-query [node q]
  (let [db (crux/db node)
        snapshot (crux/new-snapshot db)
        results (atom (crux/q db snapshot q))]
    (proxy [org.apache.calcite.linq4j.AbstractEnumerable]
        []
        (enumerator []
          (let [current (atom nil)]
            (proxy [org.apache.calcite.linq4j.Enumerator]
                []
              (current []
                (to-array @current))
              (moveNext []
                (reset! current (first @results))
                (swap! results next)
                (boolean @current))
              (reset []
                (throw (UnsupportedOperationException.)))
              (close []
                (.close snapshot))))))))

(defn- ^String ->column-name [c]
  (string/replace (string/upper-case (str c)) #"^\?" ""))

(defn- make-table [node {:keys [:crux.sql.table/query :crux.sql.table/columns] :as table-schema}]
  (proxy
      [org.apache.calcite.schema.impl.AbstractTable
       org.apache.calcite.schema.ProjectableFilterableTable]
      []
      (getRowType [^RelDataTypeFactory type-factory]
        (let [field-info  (RelDataTypeFactory$Builder. type-factory)]
          (doseq [c (:find query)]
            (let  [col-name (->column-name c)
                   col-type ^SqlTypeName (column-types->sql-types (columns c))]
              (when-not col-type
                (throw (IllegalArgumentException. (str "Unrecognized column: " c))))
              (log/debug "Adding column" col-name col-type)
              (doto field-info
                (.add col-name col-type)
                (.nullable true))))
          (.build field-info)))
      (scan [root filters projects]
        (perform-query node (doto (->crux-query table-schema filters projects) log/debug)))))

(s/def :crux.sql.table/name string?)
(s/def :crux.sql.table/columns (s/map-of symbol? column-types->sql-types))
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
                    [(string/upper-case (:crux.sql.table/name table-schema)) (make-table node table-schema)])))))))

(def ^:private model
  {:version "1.0",
   :defaultSchema "crux",
   :schemas [{:name "crux",
              :type "custom",
              :factory "crux.calcite.CruxSchemaFactory",
              :functions [{:name "CRUXID", :className "crux.calcite.CruxIdFn"}]}]})

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

(defn ^java.sql.Connection jdbc-connection [node]
  (assert node)
  (DriverManager/getConnection "jdbc:calcite:" (-> node meta :crux.node/topology ::server :node-uuid model-properties)))

(defn start-server [{:keys [:crux.node/node]} {:keys [::port]}]
  (let [node-uuid (str (java.util.UUID/randomUUID))]
    (.put !crux-nodes node-uuid node)
    (let [server (.build (doto (HttpServer$Builder.)
                           (.withHandler (LocalService. (JdbcMeta. "jdbc:calcite:" (model-properties node-uuid)))
                                         org.apache.calcite.avatica.remote.Driver$Serialization/PROTOBUF)
                           (.withPort port)))]
      (.start server)
      (CalciteAvaticaServer. server node-uuid))))

(def module {::server {:start-fn start-server
                       :args {::port {:doc "JDBC Server Port"
                                      :default 1501
                                      :crux.config/type :crux.config/nat-int}}
                       :deps #{:crux.node/node}}})
