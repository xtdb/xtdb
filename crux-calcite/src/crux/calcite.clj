(ns crux.calcite
  (:require [cheshire.core :as json]
            [clojure.spec.alpha :as s]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [crux.api :as crux]
            crux.db)
  (:import java.sql.DriverManager
           java.util.Properties
           org.apache.calcite.avatica.jdbc.JdbcMeta
           [org.apache.calcite.avatica.remote Driver LocalService]
           [org.apache.calcite.avatica.server HttpServer HttpServer$Builder]
           [org.apache.calcite.rel.type RelDataTypeFactory RelDataTypeFactory$Builder]
           [org.apache.calcite.rex RexCall RexInputRef RexLiteral RexNode]
           org.apache.calcite.sql.SqlKind
           org.apache.calcite.sql.type.SqlTypeName))

(defonce !crux-nodes (atom {}))

(defprotocol OperandToCruxInput
  (operand->v [this schema]))

(extend-protocol OperandToCruxInput
  RexInputRef
  (operand->v [this schema]
    (get-in schema [:crux.sql.table/columns (.getIndex this)]))

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
       (map #(operand->v % schema))
       (map #(if (map? %) (::sym %) %))))

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
    [['?e (:crux.sql.column/attribute (operand->v filter* schema)) true]]
    SqlKind/NOT
    [(apply list 'not (mapcat (partial ->crux-where-clauses schema) (.-operands ^RexCall filter*)))]
    SqlKind/GREATER_THAN
    [[(list '> (->operands schema filter*))]]
    SqlKind/GREATER_THAN_OR_EQUAL
    [[(apply list '>= (->operands schema filter*))]]
    SqlKind/LESS_THAN
    [[(apply list '< (->operands schema filter*))]]
    SqlKind/LESS_THAN_OR_EQUAL
    [[(apply list '<= (->operands schema filter*))]]
    SqlKind/LIKE
    [[(apply list 'like (->operands schema filter*))]]
    SqlKind/IS_NULL
    [[(apply list '= (->operands schema filter*))]]))

(defn- ->crux-query
  [schema filters projects]
  (try
    (let [{:keys [crux.sql.table/columns crux.sql.table/query]} schema]
      {:find (mapv ::sym (if (seq projects) (map columns projects) columns))
       :where (vec
               (concat
                (mapcat (partial ->crux-where-clauses schema) filters)
                (mapv (fn [{:keys [:crux.sql.column/attribute ::sym]}] ['?e attribute sym]) columns)
                query))
       :args [{:like #(org.apache.calcite.runtime.SqlFunctions/like %1 %2)}]})
    (catch Throwable e
      (log/error e)
      (throw e))))

(def ^:private column-types {:varchar SqlTypeName/VARCHAR
                             :keyword SqlTypeName/VARCHAR
                             :integer SqlTypeName/INTEGER
                             :long SqlTypeName/BIGINT
                             :boolean SqlTypeName/BOOLEAN
                             :double SqlTypeName/DOUBLE
                             :datetime SqlTypeName/DATE})

(defn- ^java.util.List perform-query [node q]
  (->> (crux/q (crux/db node) q)
       (mapv to-array)))

(defn- make-table [node {:keys [:crux.sql.table/columns] :as table-schema}]
  (proxy
      [org.apache.calcite.schema.impl.AbstractTable
       org.apache.calcite.schema.ProjectableFilterableTable]
      []
      (getRowType [^RelDataTypeFactory type-factory]
        (let [field-info  (RelDataTypeFactory$Builder. type-factory)]
          (doseq [c columns]
            (doto  field-info
              (.add (string/upper-case (:crux.sql.column/name c)) ^SqlTypeName (column-types (:crux.sql.column/type c)))
              (.nullable true)))
          (.build field-info)))
      (scan [root filters projects]
        (org.apache.calcite.linq4j.Linq4j/asEnumerable
         (perform-query node (doto (->crux-query table-schema filters projects) log/debug))))))

(s/def :crux.sql.column/attribute keyword?)
(s/def :crux.sql.column/name string?)
(s/def :crux.sql.column/type column-types)
(s/def :crux.sql.table/name string?)
(s/def :crux.sql.table/columns (s/coll-of (s/keys :req [:crux.sql.column/attribute
                                                        :crux.sql.column/name
                                                        :crux.sql.column/type])))
(s/def ::table
  (s/keys :req [:crux.db/id :crux.sql.table/name :crux.sql.table/columns]))

(defn- conform-schema [s]
  (s/valid? ::table s)
  (update s :crux.sql.table/columns
          (fn [columns]
            (mapv #(assoc % ::sym (gensym (:crux.sql.column/name %))) columns))))

(defn- lookup-schema [node]
  (let [db (crux/db node)]
    (->> (crux/q db '{:find [e]
                      :where [[e :crux.sql.table/name]]})
         (map (comp conform-schema (partial crux/entity db) first)))))

(defn create-schema [parent-schema name operands]
  (let [node (@!crux-nodes (get operands "CRUX_NODE"))]
    (assert node)
    (proxy [org.apache.calcite.schema.impl.AbstractSchema] []
      (getTableMap []
        (into {}
              (for [table-schema (lookup-schema node)]
                [(string/upper-case (:crux.sql.table/name table-schema)) (make-table node table-schema)]))))))

(def ^:private model {:version "1.0",
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
    (swap! !crux-nodes dissoc node-uuid)
    (.stop server)))

(defn ^java.sql.Connection jdbc-connection [node]
  (assert node)
  (DriverManager/getConnection "jdbc:calcite:" (-> node meta :crux.node/topology ::server :node-uuid model-properties)))

(defn start-server [{:keys [:crux.node/node]} {:keys [::port]}]
  (let [node-uuid (str (java.util.UUID/randomUUID))]
    (swap! !crux-nodes assoc node-uuid node)
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
