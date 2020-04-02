(ns crux.fixtures.tpch
  (:require  [clojure.test :as t])
  (:import [io.airlift.tpch TpchTable TpchColumn TpchColumnTypes TpchColumnType TpchColumnType$Base]))

(def tpch-column-types->crux-calcite-type
  {TpchColumnType$Base/INTEGER :integer
   TpchColumnType$Base/VARCHAR :varchar

   ;; TODO ALL TYPES:
   ;; INTEGER,
   ;; IDENTIFIER,
   ;; DATE,
   ;; DOUBLE,
   ;; VARCHAR
   })

(defn tpch-table->crux-sql-schema [^TpchTable t]
  {:crux.db/id (keyword "crux.sql.schema" (.getTableName t))
   :crux.sql.table/name (.getTableName t)
   :crux.sql.table/columns (for [^TpchColumn c (.getColumns t)]
                             {:crux.db/attribute (keyword (.getColumnName c))
                              :crux.sql.column/name (.getColumnName c)
                              :crux.sql.column/type (tpch-column-types->crux-calcite-type (.getBase (.getType c)))})})

(defn tpch-tables->crux-sql-schemas []
  (map tpch-table->crux-sql-schema (TpchTable/getTables)))

(comment
  (tpch-tables->crux-sql-schemas))
