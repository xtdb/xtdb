(ns core2.tpch
  (:require [clojure.instant :as inst]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [core2.core :as c2])
  (:import [io.airlift.tpch GenerateUtils TpchColumn TpchColumnType$Base TpchEntity TpchTable]
           java.util.Date
           [java.time Instant Period]))

(def table->pkey
  {"part" [:p_partkey]
   "supplier" [:s_suppkey]
   "partsupp" [:ps_partkey :ps_suppkey]
   "customer" [:c_custkey]
   "lineitem" [:l_orderkey :l_linenumber]
   "orders" [:o_orderkey]
   "nation" [:n_nationkey]
   "region" [:r_regionkey]})

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
                      (Date/from (.plus Instant/EPOCH (Period/ofDays (.getDate c b)))))])
                 (into {}))
        table-name (.getTableName t)
        pkey-columns (get table->pkey table-name)
        pkey (mapv doc pkey-columns)]
    (assoc doc
           :_id (str/join "___" pkey)
           :_table table-name)))

(def default-scale-factor 0.05)

;; 0.05 = 7500 customers, 75000 orders, 299814 lineitems, 10000 part, 40000 partsupp, 500 supplier, 25 nation, 5 region
(defn tpch-table->docs
  ([^TpchTable table]
   (tpch-table->docs table default-scale-factor))
  ([^TpchTable table scale-factor]
   (for [doc (.createGenerator table scale-factor 1 1)]
     (tpch-entity->pkey-doc table doc))))

(defn submit-docs!
  ([tx-producer]
   (submit-docs! tx-producer default-scale-factor))
  ([tx-producer scale-factor]
   (log/debug "Transacting TPC-H tables...")
   (->> (TpchTable/getTables)
        (reduce (fn [_last-tx ^TpchTable t]
                  (let [[!last-tx doc-count] (->> (tpch-table->docs t scale-factor)
                                                  (partition-all 1000)
                                                  (reduce (fn [[_!last-tx last-doc-count] batch]
                                                            [(c2/submit-tx tx-producer
                                                                           (vec (for [doc batch]
                                                                                  {:op :put, :doc doc})))
                                                             (+ last-doc-count (count batch))])
                                                          [nil 0]))]
                    (log/debug "Transacted" doc-count (.getTableName t))
                    @!last-tx))
                nil))))
