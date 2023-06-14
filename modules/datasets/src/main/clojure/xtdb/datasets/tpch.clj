(ns xtdb.datasets.tpch
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [xtdb.api :as xt])
  (:import clojure.lang.MapEntry
           [io.airlift.tpch TpchColumn TpchColumnType$Base TpchEntity TpchTable]
           [java.time LocalDate]))

;; 0.05 = 7500 customers, 75000 orders, 299814 lineitems, 10000 part, 40000 partsupp, 500 supplier, 25 nation, 5 region

(def ^:private table->pkey
  {:part [:p_partkey]
   :supplier [:s_suppkey]
   :partsupp [:ps_partkey :ps_suppkey]
   :customer [:c_custkey]
   :lineitem [:l_orderkey :l_linenumber]
   :orders [:o_orderkey]
   :nation [:n_nationkey]
   :region [:r_regionkey]})

(defn- ->cell-reader [^TpchColumn col]
  (comp (let [k (keyword (.getColumnName col))]
          (fn ->map-entry [v]
            (MapEntry/create k v)))

        (condp = (.getBase (.getType col))
          TpchColumnType$Base/IDENTIFIER (let [col-part (str (str/replace (.getColumnName col) #".+_" "") "_")]
                                           (fn [^TpchEntity e]
                                             (str col-part (.getIdentifier col e))))
          TpchColumnType$Base/INTEGER (fn [^TpchEntity e]
                                        (long (.getInteger col e)))
          TpchColumnType$Base/VARCHAR (fn [^TpchEntity e]
                                        (.getString col e))
          TpchColumnType$Base/DOUBLE (fn [^TpchEntity e]
                                       (.getDouble col e))
          TpchColumnType$Base/DATE (fn [^TpchEntity e]
                                     (LocalDate/ofEpochDay (.getDate col e))))))

(defn- tpch-table->docs [^TpchTable table scale-factor]
  (let [cell-readers (mapv ->cell-reader (.getColumns table))
        table-name (keyword (.getTableName table))
        pk-cols (get table->pkey table-name)]
    (for [^TpchEntity e (.createGenerator table scale-factor 1 1)]
      (let [doc (into {} (map #(% e)) cell-readers)
            eid (str/join "___" (map doc pk-cols))]
        (-> (assoc doc :xt/id eid)
            (with-meta {:table table-name}))))))

(defn submit-docs! [tx-producer scale-factor]
  (log/debug "Transacting TPC-H tables...")
  (->> (TpchTable/getTables)
       (reduce (fn [_last-tx ^TpchTable t]
                 (let [[!last-tx doc-count] (->> (tpch-table->docs t scale-factor)
                                                 (partition-all 1000)
                                                 (reduce (fn [[_!last-tx last-doc-count] batch]
                                                           [(xt/submit-tx& tx-producer
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
          (->> (cons "xt$id" (for [^TpchColumn col (.getColumns table)]
                               (.getColumnName col)))
               (str/join ", "))
          (->> (repeat (inc (count (.getColumns table))) "?")
               (str/join ", "))))

(defn- tpch-table->dml-params [^TpchTable table, scale-factor]
  (let [table-name (.getTableName table)
        cell-readers (mapv ->cell-reader (.getColumns table))
        pk-cols (get table->pkey (keyword table-name))]
    (for [^TpchEntity e (.createGenerator table scale-factor 1 1)
          :let [doc (map #(% e) cell-readers)]]
      (cons (->> (mapv (into {} doc) pk-cols)
                 (str/join "___"))
            (vals doc)))))

(defn submit-dml! [tx-producer scale-factor]
  (log/debug "Transacting TPC-H tables...")
  (->> (TpchTable/getTables)
       (reduce (fn [_last-tx ^TpchTable table]
                 (let [dml (tpch-table->dml table)
                       [!last-tx doc-count] (->> (tpch-table->dml-params table scale-factor)
                                                 (partition-all 1000)
                                                 (reduce (fn [[_!last-tx last-doc-count] param-batch]
                                                           [(xt/submit-tx& tx-producer
                                                                           [[:sql-batch (into [dml] param-batch)]])
                                                            (+ last-doc-count (count param-batch))])
                                                         [nil 0]))]
                   (log/debug "Transacted" doc-count (.getTableName table))
                   @!last-tx))
               nil)))
