(ns jon
  (:require [crux.api :as c]
            [crux.fixtures :as fix :refer [*api*]])
  (:import [io.airlift.tpch TpchColumn TpchColumnType TpchColumnType$Base TpchEntity TpchTable]))

;; add this to your project.clj
;; [io.airlift.tpch/tpch "0.10"]

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
                                        ['e (keyword (.getColumnName c)) (symbol (.getColumnName c))]))}
   :crux.sql.table/columns (into {} (for [^TpchColumn c (.getColumns t)]
                                      [(symbol (.getColumnName c)) (tpch-column-types->crux-calcite-type (.getBase (.getType c)))]))})

(defn tpch-tables->crux-sql-schemas []
  (map tpch-table->crux-sql-schema (TpchTable/getTables)))

(defn tpch-entity->doc [^TpchTable t ^TpchEntity b]
  (into {:crux.db/id (java.util.UUID/randomUUID)}
        (for [^TpchColumn c (.getColumns t)]
          [(keyword (.getColumnName c))
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

(defn- load-docs! [node]
  (doseq [^TpchTable t (TpchTable/getTables)]
    (let [docs (tpch-table->docs t)]
      (println "Transacting" (count docs) (.getTableName t))
      (fix/transact! node (tpch-table->docs t)))))

(defn with-timing* [f]
  (let [start-time-ms (System/currentTimeMillis)
        ret (try
              (f)
              (catch Exception e
                {:error (.getMessage e)}))]
    (merge (when (map? ret) ret)
           {:time-taken-ms (- (System/currentTimeMillis) start-time-ms)})))

(comment
  (load-docs! (user/crux-node))

  (dotimes [_ 5]
    (println (with-timing*
               (fn [] {:count (count (c/q (c/db (user/crux-node)) '{:find [l_orderkey],
                                                                    :where [[e :l_orderkey l_orderkey]
                                                                            [e :l_partkey l_partkey]
                                                                            [e :l_suppkey l_suppkey]
                                                                            [e :l_linenumber l_linenumber]
                                                                            [e :l_quantity l_quantity]
                                                                            ;; [e :l_extendedprice l_extendedprice]
                                                                            ;; [e :l_discount l_discount]
                                                                            ;; [e :l_tax l_tax]
                                                                            ;; [e :l_returnflag l_returnflag]
                                                                            ;; [e :l_linestatus l_linestatus]
                                                                            ;; [e :l_shipdate l_shipdate]
                                                                            ;; [e :l_commitdate l_commitdate]
                                                                            ;; [e :l_receiptdate l_receiptdate]
                                                                            ;; [e :l_shipinstruct l_shipinstruct]
                                                                            ;; [e :l_shipmode l_shipmode]
                                                                            ;; [e :l_comment l_comment]
                                                                            ]
                                                                    :timeout 100000}))}))))



  )
