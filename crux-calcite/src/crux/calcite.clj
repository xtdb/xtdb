(ns crux.calcite
  (:require [clojure.string :as string]
            [crux.codec :as c]
            [crux.api :as crux])
  (:import org.apache.calcite.avatica.jdbc.JdbcMeta
           [org.apache.calcite.avatica.remote Driver LocalService]
           org.apache.calcite.rel.type.RelDataTypeFactory
           [org.apache.calcite.rex RexCall RexInputRef RexLiteral]
           org.apache.calcite.sql.SqlKind
           org.apache.calcite.sql.type.SqlTypeName))

(defonce !node (atom nil))

(defprotocol OperandToCruxInput
  (operand->v [this schema]))

(extend-protocol OperandToCruxInput
  RexInputRef
  (operand->v [this schema]
    (let [attr (-> (get schema (.getIndex this)) string/lower-case)]
      (if (= attr "id")
        :crux.db/id
        (keyword attr))))

  RexLiteral
  (operand->v [this schema]
    (str (.getValue2 this))))

(defn- ->operands [schema ^RexCall filter*]
  (reverse (sort-by c/valid-id? (map #(operand->v % schema) (.getOperands filter*)))))

(defn- ->crux-where-clauses
  [schema ^RexCall filter*]
  (condp = (.getKind filter*)
    SqlKind/EQUALS
    (let [[left right] (->operands schema filter*)]
      [['?e left right]])
    SqlKind/NOT_EQUALS
    (let [[left right] (->operands schema filter*)]
      [(list 'not ['?e left right])])
    SqlKind/AND
    (mapcat (partial ->crux-where-clauses schema) (.-operands filter*))
    SqlKind/OR
    [(apply list 'or (mapcat (partial ->crux-where-clauses schema) (.-operands filter*)))]))

(defn- ->crux-query
  [schema filters projects]
  (let [projects (or (seq projects) (range (count schema)))
        syms (mapv gensym schema)
        find* (mapv syms projects)]
    {:find find*
     :where (vec
             (concat

              ;; Where Clauses
              (mapcat (partial ->crux-where-clauses schema) filters)

              ;; Column names are there as attibutes
              (doto
                  (mapv
                   (fn [project]
                     ['?e
                      (let [attr (-> (get schema project) string/lower-case)]
                        (if (= attr "id")
                          :crux.db/id
                          (keyword attr)))
                      (get syms project)])
                   projects)
                  prn)))}))

(defn make-table [schema]
  (let [schema (conj schema "ID")]
    (proxy
        [org.apache.calcite.schema.impl.AbstractTable
         org.apache.calcite.schema.ProjectableFilterableTable]
        []
      (getRowType [^RelDataTypeFactory type-factory]
        (.createStructType
         type-factory
         ^java.util.List
         (seq
          (into {}
                (for [field schema]
                  [field (.createSqlType type-factory SqlTypeName/VARCHAR)])))))
      (scan [root filters projects]
        (println root)
        (org.apache.calcite.linq4j.Linq4j/asEnumerable
         ^java.util.List
         (mapv to-array
               (crux/q
                (crux/db @!node)
                (doto (->crux-query schema filters projects) prn))))))))

(defn create-schema [parent-schema name operands]
  (proxy [org.apache.calcite.schema.impl.AbstractSchema] []
    (getTableMap []
      {"PLANET"
       (make-table ["NAME" "CLIMATE" "DIAMETER"])

       "PERSON"
       (make-table ["NAME" "HOMEWORLD"])})))

(defn start-server [{:keys [:crux.node/node]} {:keys [::port]}]
  ;; TODO, find a better approach
  (reset! !node node)
  ;; note, offer port configuration
  ;; Todo won't work from a JAR file:
  ;; Pass through the SchemaFactory
  (let [server (.build (doto (org.apache.calcite.avatica.server.HttpServer$Builder.)
                         (.withHandler (LocalService. (JdbcMeta. "jdbc:calcite:model=crux-calcite/resources/model.json"))
                                       org.apache.calcite.avatica.remote.Driver$Serialization/PROTOBUF)
                         (.withPort port)))]
    (.start server)
    (reify java.io.Closeable
      (close [this]
        (.stop server)))))

(def module {::server {:start-fn start-server
                       :args {::port {:doc "JDBC Server Port"
                                      :default 1501
                                      :crux.config/type :crux.config/nat-int}}
                       :deps #{:crux.node/node}}})

;; https://github.com/apache/calcite-avatica/blob/master/standalone-server/src/main/java/org/apache/calcite/avatica/standalone/StandaloneServer.java
;; https://github.com/apache/calcite-avatica/blob/master/core/src/main/java/org/apache/calcite/avatica/remote/LocalService.java
;; https://github.com/apache/calcite/blob/master/core/src/main/java/org/apache/calcite/DataContext.java
