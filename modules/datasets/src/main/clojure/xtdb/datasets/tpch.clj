(ns xtdb.datasets.tpch
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [xtdb.datalog :as xt.d]
            [xtdb.sql :as xt.sql])
  (:import clojure.lang.MapEntry
           [io.airlift.tpch TpchColumn TpchColumnType$Base TpchEntity TpchTable]
           [java.time LocalDate]))

;; 0.05 = 7500 customers, 75000 orders, 299814 lineitems, 10000 part, 40000 partsupp, 500 supplier, 25 nation, 5 region

(def ^:private table->pkey
  {"part" [:p_partkey]
   "supplier" [:s_suppkey]
   "partsupp" [:ps_partkey :ps_suppkey]
   "customer" [:c_custkey]
   "lineitem" [:l_orderkey :l_linenumber]
   "orders" [:o_orderkey]
   "nation" [:n_nationkey]
   "region" [:r_regionkey]})

(defn- read-tpch-cell [^TpchColumn c, ^TpchEntity b]
  (condp = (.getBase (.getType c))
    TpchColumnType$Base/IDENTIFIER (str (str/replace (.getColumnName c) #".+_" "") "_" (.getIdentifier c b))
    TpchColumnType$Base/INTEGER (long (.getInteger c b))
    TpchColumnType$Base/VARCHAR (.getString c b)
    TpchColumnType$Base/DOUBLE (.getDouble c b)
    TpchColumnType$Base/DATE (LocalDate/ofEpochDay (.getDate c b))))

(defn- tpch-table->docs [^TpchTable table scale-factor]
  (let [table-name (.getTableName table)]
    (for [entity (.createGenerator table scale-factor 1 1)]
      (let [doc (->> (for [^TpchColumn col (.getColumns table)]
                       [(keyword (.getColumnName col))
                        (read-tpch-cell col entity)])
                     (into {}))]
        (-> (assoc doc
                   :id (->> (mapv doc (get table->pkey table-name))
                            (str/join "___")))
            (with-meta {:table (symbol table-name)}))))))

(defn submit-docs! [tx-producer scale-factor]
  (log/debug "Transacting TPC-H tables...")
  (->> (TpchTable/getTables)
       (reduce (fn [_last-tx ^TpchTable t]
                 (let [[!last-tx doc-count] (->> (tpch-table->docs t scale-factor)
                                                 (partition-all 1000)
                                                 (reduce (fn [[_!last-tx last-doc-count] batch]
                                                           [(xt.d/submit-tx& tx-producer
                                                                             (vec (for [doc batch]
                                                                                    [:put (:table (meta doc)) doc])))
                                                            (+ last-doc-count (count batch))])
                                                         [nil 0]))]
                   (log/debug "Transacted" doc-count (.getTableName t))
                   @!last-tx))
               nil)))

(defn- tpch-table->dml [^TpchTable table]
  (format "INSERT INTO %s (%s) VALUES (%s)"
          (.getTableName table)
          (->> (cons "id" (for [^TpchColumn col (.getColumns table)]
                            (.getColumnName col)))
               (str/join ", "))
          (->> (repeat (inc (count (.getColumns table))) "?")
               (str/join ", "))))

(defn- tpch-table->dml-params [^TpchTable table, scale-factor]
  (let [table-name (.getTableName table)]
    (for [entity (.createGenerator table scale-factor 1 1)]
      (let [doc (for [^TpchColumn col (.getColumns table)]
                  (MapEntry/create (keyword (.getColumnName col))
                                   (read-tpch-cell col entity)))]
        (cons (->> (mapv (into {} doc) (get table->pkey table-name))
                   (str/join "___"))
              (vals doc))))))

(defn submit-dml! [tx-producer scale-factor]
  (log/debug "Transacting TPC-H tables...")
  (->> (TpchTable/getTables)
       (reduce (fn [_last-tx ^TpchTable table]
                 (let [dml (tpch-table->dml table)
                       [!last-tx doc-count] (->> (tpch-table->dml-params table scale-factor)
                                                 (partition-all 1000)
                                                 (reduce (fn [[_!last-tx last-doc-count] param-batch]
                                                           [(xt.sql/submit-tx& tx-producer
                                                                               [[:sql dml param-batch]])
                                                            (+ last-doc-count (count param-batch))])
                                                         [nil 0]))]
                   (log/debug "Transacted" doc-count (.getTableName table))
                   @!last-tx))
               nil)))
