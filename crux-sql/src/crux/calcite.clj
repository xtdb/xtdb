(ns crux.calcite
  (:require [cheshire.core :as json]
            [clojure.spec.alpha :as s]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [crux.api :as crux]
            crux.db)
  (:import java.sql.DriverManager
           crux.calcite.CruxTable
           org.apache.calcite.linq4j.Enumerable
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
    (let [[e a v] (sym-triple (first (->operands schema filter*)) schema)]
      [[e a '_]])))

(defn- ->crux-query
  [schema filters projects]
  (try
    (let [{:keys [crux.sql.table/columns crux.sql.table/query]} schema
          {:keys [find where]} query]
      {:find (if (seq projects) (mapv find projects) find)
       :where (vec
               (concat
                (mapcat (partial ->crux-where-clauses schema) filters)
                where))})
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
  (let [db (crux/db node)]
    (proxy [org.apache.calcite.linq4j.AbstractEnumerable]
        []
        (enumerator []
          (let [snapshot (crux/new-snapshot db)
                results (atom (crux/q db snapshot q))
                current (atom nil)]
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

(defn row-type [^RelDataTypeFactory type-factory node {:keys [:crux.sql.table/query :crux.sql.table/columns] :as table-schema}]
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

(defn ^Enumerable scan [node table-schema ;;filters projects
                        ]
  (perform-query node (doto (->crux-query table-schema [] [];; filters projects
                                          ) log/debug)))

(defn ->enumerable []
  (throw (IllegalArgumentException. "fo")))

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
                    [(string/upper-case (:crux.sql.table/name table-schema))
                     (CruxTable. node table-schema)])))))))

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
