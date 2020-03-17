(ns crux.calcite
  (:require [clojure.string :as string]
            [crux.api :as crux]
            [crux.fixtures.api :refer [*api*]])
  (:import org.apache.calcite.avatica.jdbc.JdbcMeta
           [org.apache.calcite.avatica.remote Driver LocalService]
           org.apache.calcite.rel.type.RelDataTypeFactory
           org.apache.calcite.rex.RexNode
           org.apache.calcite.sql.type.SqlTypeName
           org.apache.calcite.sql.SqlKind))

(defonce !node (atom nil))

(defn- operand->crux-attr [schema operand]
  (let [attr (-> (get schema (.getIndex operand)) string/lower-case)]
    (if (= attr "id")
      :crux.db/id
      (keyword attr))))

(defn- ->crux-where-clauses
  [schema ^RexNode filter*]
  (condp = (.getKind filter*)
    ;; TODO: Assumes left is a column ref and
    ;; right is a constant, but doesn't enforce
    ;; that.
    SqlKind/EQUALS
    (let [left (.. filter* getOperands (get 0))
          right (.. filter* getOperands (get 1))]
      [['?e (operand->crux-attr schema left) (str (.getValue2 right))]])
    SqlKind/NOT_EQUALS
    (let [left (.. filter* getOperands (get 0))
          right (.. filter* getOperands (get 1))]
      [(list 'not ['?e (operand->crux-attr schema left) (str (.getValue2 right))])])
    SqlKind/AND
    (mapcat (partial ->crux-where-clauses schema) (.-operands filter*))))

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
