(ns crux.calcite.schema-factory
  (:require [crux.api :as crux]
            [crux.fixtures.api :refer [*api*]])
  (:import org.apache.calcite.rel.type.RelDataTypeFactory
           org.apache.calcite.sql.type.SqlTypeName
           org.apache.calcite.rex.RexNode
           org.apache.calcite.DataContext))

  ;; TODO, add this to the ns decl:
  ;; (:gen-class
  ;;  :name juxt.calcite_play.SchemaFactory
  ;;  :implements org.apache.calcite.schema.impl.AbstractSchema))

;; TODO, how to define a schema?
(defn schema [^RelDataTypeFactory type-factory]
  (list {"SKU" (.createSqlType type-factory SqlTypeName/VARCHAR)
         "DESCRIPTION" (.createSqlType type-factory SqlTypeName/VARCHAR)}))

(defn query [^DataContext root filters projects]
  (let [projects (or (seq projects) (range (count schema)))
        syms (mapv gensym schema)
        find* (mapv syms projects)
        ->crux-where-clauses (fn ->crux-where-clauses
                               [^RexNode filter*]
                               (condp = (.getKind filter*)
                                 ;; TODO: Assumes left is a column ref and
                                 ;; right is a constant, but doesn't enforce
                                 ;; that.
                                 org.apache.calcite.sql.SqlKind/EQUALS
                                 (let [left (.. filter* getOperands (get 0))
                                       right (.. filter* getOperands (get 1))]
                                   [['?e
                                     (keyword (get schema (.getIndex left)))
                                     (str (.getValue2 right))]])
                                 org.apache.calcite.sql.SqlKind/AND
                                 (mapcat ->crux-where-clauses (.-operands filter*))))]
    (mapv to-array
          (crux/q
           (crux/db *api*)
           {:find find*
            :where (vec
                    (concat
                     (mapcat ->crux-where-clauses filters)
                     ;; Ensure they have all selected columns as attributes
                     (mapv
                      (fn [project]
                        ['?e
                         (keyword (get schema project))
                         (get syms project)])
                      projects)))}))))

(defn -create [this parent-schema name operands]
  (let [operands (into {} operands)]
    (proxy [org.apache.calcite.schema.impl.AbstractSchema] []
      (getTableMap []
        {"PRODUCT"
         (proxy
             [org.apache.calcite.schema.impl.AbstractTable
              org.apache.calcite.schema.ProjectableFilterableTable]
             []
           (getRowType [^RelDataTypeFactory type-factory]
             (.createStructType type-factory ^java.util.List (schema)))
           (scan [root filters projects]
               (org.apache.calcite.linq4j.Linq4j/asEnumerable
                 ^java.util.List (query root filters projects))))}))))
