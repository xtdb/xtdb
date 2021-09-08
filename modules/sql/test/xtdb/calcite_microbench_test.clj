(ns xtdb.calcite-microbench-test
  (:require [xtdb.calcite :as cal]
            [xtdb.api :as xt]
            [xtdb.fixtures :as fix]
            [xtdb.fixtures.calcite :as cf]
            [xtdb.fixtures.tpch :as tf])
  (:import io.airlift.tpch.TpchTable))

(defn- load-docs! [node]
  (doseq [^TpchTable t (TpchTable/getTables)]
    (let [docs (tf/tpch-table->docs t)]
      (println "Transacting" (count docs) (.getTableName t))
      (fix/transact! node (tf/tpch-table->docs t)))))

(defn with-timing* [f]
  (let [start-time-ms (System/currentTimeMillis)
        ret (try
              (f)
              (catch Exception e
                {:error (.getMessage e)}))]
    (merge (when (map? ret) ret)
           {:time-taken-ms (- (System/currentTimeMillis) start-time-ms)})))

(defn ^java.sql.PreparedStatement prepared-query [^java.sql.Connection conn q]
  (.prepareStatement conn q))

(defn query [^java.sql.Connection conn q]
  (with-open [stmt (.createStatement conn)
              rs (.executeQuery stmt q)]
    (->> rs resultset-seq (into []))))

(comment
  (require 'user)
  (load-docs! (user/xtdb-node))
  (fix/transact! (user/xtdb-node) (tf/tpch-tables->xtdb-sql-schemas))
  (def db (xt/db (user/xtdb-node)))
  (def conn (cal/jdbc-connection (user/xtdb-node)))
  (def p (prepared-query conn "SELECT c_name FROM CUSTOMER"))

  (println (with-timing*
             (fn [] {:count (count (xt/q db '{:find [e ?n]
                                              :where [[e :l_partkey ?n]]}))})))

  (println (with-timing*
             (fn [] {:count (count (xt/q db '{:find [l_orderkey],
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
                                              :timeout 100000}))})))

  (println (with-timing*
             (fn [] {:count (first (query conn "SELECT l_orderkey FROM LINEITEM"))})))

  (println (with-timing*
             (fn [] {:count (count (cf/exec-prepared-query
                                    (let [s (prepared-query conn "SELECT * FROM LINEITEM LIMIT 10")]
                                      (.setQueryTimeout s 10)
                                      s)))})))

  ;; Testing vars for range searches

  (println (with-timing*
             (fn [] {:count (count (xt/q db '{:find [e]
                                              :where [[e :l_quantity ?qty]
                                                      [(> ?qty qty)]]
                                              :args [{:qty 30.0}]}))})))

  ;; using args:

  (println (with-timing*
             (fn [] {:count (count (xt/q db '{:find [e]
                                              :where [[e :l_quantity qty]]
                                              :args [{:qty 30.0}]}))})))
  ;; Args penalises
  (println (with-timing*
             (fn [] {:count (count (xt/q db '{:find [e]
                                              :where [[e :l_quantity ?qty]
                                                      [(= qty ?qty)]]
                                              :args [{:qty 30.0}]}))})))

  ;; Not using Args (below)

  (println (with-timing*
             (fn [] {:count (count (xt/q db '{:find [e]
                                              :where [[e :l_quantity 30.0]]}))})))

  ;; Fast:
  (println (with-timing*
             (fn [] {:count (count (xt/q db '{:find [e]
                                              :where [[e :l_quantity ?qty]
                                                      [(= ?qty 30)]]}))})))

  ;; Statement - = is optimised for, except when used with args

  ;; SQL (below)

  (println (with-timing*
             (fn [] {:count (count (query conn "SELECT * FROM LINEITEM WHERE L_QUANTITY = 30.0"))})))

  (println (with-timing*
             (fn [] {:count (count (query conn "SELECT * FROM CUSTOMER"))})))

  (println (with-timing*
             (fn [] {:count (count (cf/exec-prepared-query (prepared-query conn "SELECT c_name FROM CUSTOMER")))})))

  (println (with-timing*
             (fn [] {:count (count (cf/exec-prepared-query p))}))))
