(ns crux.fixtures.tpch
  (:require [crux.fixtures :as fix :refer [*api*]])
  (:import [io.airlift.tpch TpchColumn TpchColumnType TpchColumnType$Base TpchEntity TpchTable]))

(def tpch-column-types->crux-calcite-type
  {TpchColumnType$Base/INTEGER :bigint
   TpchColumnType$Base/VARCHAR :varchar
   TpchColumnType$Base/IDENTIFIER :bigint
   TpchColumnType$Base/DOUBLE :double
   TpchColumnType$Base/DATE :timestamp})

(defn tpch-table->crux-sql-schema [^TpchTable t]
  {:crux.db/id (keyword "crux.sql.schema" (.getTableName t))
   :crux.sql.table/name (.getTableName t)
   :crux.sql.table/query {:find (vec (for [^TpchColumn c (.getColumns t)]
                                       (symbol (.getColumnName c))))
                          :where (vec (for [^TpchColumn c (.getColumns t)]
                                        ['e (keyword (.getSimplifiedColumnName c)) (symbol (.getColumnName c))]))}
   :crux.sql.table/columns (into {} (for [^TpchColumn c (.getColumns t)]
                                      [(symbol (.getColumnName c)) (tpch-column-types->crux-calcite-type (.getBase (.getType c)))]))})

(defn tpch-tables->crux-sql-schemas []
  (map tpch-table->crux-sql-schema (TpchTable/getTables)))

(defn tpch-entity->doc [^TpchTable t ^TpchEntity b]
  (into {:crux.db/id (keyword (str (.getTableName t) "-" (.getRowNumber b)))}
        (for [^TpchColumn c (.getColumns t)]
          [(keyword (.getSimplifiedColumnName c))
           (condp = (.getBase (.getType c))
             TpchColumnType$Base/IDENTIFIER
             (.getIdentifier c b)
             TpchColumnType$Base/INTEGER
             (.getInteger c b)
             TpchColumnType$Base/VARCHAR
             (.getString c b)
             TpchColumnType$Base/DOUBLE
             (.getDouble c b)
             TpchColumnType$Base/DATE
             (.getDate c b))])))

;; 0.05 = 7500 customers, 75000 orders, 299814 lineitems, 10000 part, 40000 partsupp, 500 supplier, 25 nation, 5 region
(defn tpch-table->docs [^TpchTable t]
  ;; first happens to be customers (;; 150000)
  (map (partial tpch-entity->doc t) (seq (.createGenerator ^TpchTable t 0.05 1 1))))

(defn with-tpch-schema [f]
  (fix/transact! *api* (tpch-tables->crux-sql-schemas))
  (f))

(comment
  (first (tpch-tables->crux-sql-schemas))
  (first (tpch-table->docs (first (TpchTable/getTables)) )))
