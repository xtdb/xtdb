(ns crux.fixtures.tpch
  (:require [crux.api :as c]
            [crux.fixtures :as fix :refer [*api*]]
            [clojure.instant :as i]
            [clojure.string :as str])
  (:import [io.airlift.tpch GenerateUtils TpchColumn TpchColumnType TpchColumnType$Base TpchEntity TpchTable]
           java.util.UUID))


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

(def table->pkey
  {"part" [:p_partkey]
   "supplier" [:s_suppkey]
   "partsupp" [:ps_partkey :ps_suppkey]
   "customer" [:c_custkey]
   "lineitem" [:l_orderkey :l_linenumber]
   "orders" [:o_orderkey]
   "nation" [:n_nationkey]
   "region" [:r_regionkey]})

(defn tpch-entity->doc [^TpchTable t ^TpchEntity b]
  (into {:crux.db/id (UUID/randomUUID)}
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

(defn tpch-entity->pkey-doc [^TpchTable t ^TpchEntity b]
  (let [doc (->> (for [^TpchColumn c (.getColumns t)]
                   [(keyword (.getColumnName c))
                    (condp = (.getBase (.getType c))
                      TpchColumnType$Base/IDENTIFIER
                      (str (str/replace (.getColumnName c) #".+_" "") "_" (.getIdentifier c b))
                      TpchColumnType$Base/INTEGER
                      (long (.getInteger c b))
                      TpchColumnType$Base/VARCHAR
                      (.getString c b)
                      TpchColumnType$Base/DOUBLE
                      (.getDouble c b)
                      TpchColumnType$Base/DATE
                      (i/read-instant-date (GenerateUtils/formatDate (.getDate c b))))])
                 (into {}))
        pkey-columns (get table->pkey (.getTableName t))
        pkey (mapv doc pkey-columns)]
    (assoc doc :crux.db/id (str/join "___" pkey))))

(def default-scale-factor 0.05)

;; 0.05 = 7500 customers, 75000 orders, 299814 lineitems, 10000 part, 40000 partsupp, 500 supplier, 25 nation, 5 region
(defn tpch-table->docs
  ([^TpchTable t]
   (tpch-table->docs t default-scale-factor))
  ([^TpchTable t sf]
   (tpch-table->docs t default-scale-factor tpch-entity->doc))
  ([^TpchTable t sf doc-fn]
   ;; first happens to be customers (;; 150000 for sf 0.05)
   (map (partial doc-fn t) (seq (.createGenerator ^TpchTable t sf 1 1)))))

(defn with-tpch-schema [f]
  (fix/transact! *api* (tpch-tables->crux-sql-schemas))
  (f))

(defn load-docs!
  ([node]
   (load-docs! node default-scale-factor))
  ([node sf]
   (load-docs! node sf tpch-entity->doc))
  ([node sf doc-fn]
   (doseq [^TpchTable t (TpchTable/getTables)]
     (let [docs (tpch-table->docs t sf doc-fn)]
       (println "Transacting" (count docs) (.getTableName t))
       (let [last-tx (->> docs
                          (partition-all 1000)
                          (reduce (fn [last-tx chunk]
                                    (c/submit-tx node (vec (for [doc chunk]
                                                             [:crux.tx/put doc]))))
                                  nil))]
         (c/await-tx node last-tx))))))
