(ns crux.calcite-microbench-test
  (:require [clojure.test :as t]
            [crux.calcite :as cal]
            [crux.api :as c]
            [crux.fixtures :as fix :refer [*api*]]
            [crux.fixtures.calcite :as cf]
            [crux.fixtures.tpch :as tf]
            [user :as user])
  (:import io.airlift.tpch.TpchTable
           java.sql.DriverManager
           java.sql.PreparedStatement))

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

(defn exec-prepared-query [^PreparedStatement p & args]
  (doseq [[i v] args]
    (if (string? v)
      (.setString p i v)))
  (with-open [rs (.executeQuery p)]
    (->> rs resultset-seq (into []))))

(defn prepared-query [^java.sql.Connection conn q & args]
  (.prepareStatement conn q))

(defn query [^java.sql.Connection conn q]
  (with-open [stmt (.createStatement conn)
              rs (.executeQuery stmt q)]
    (->> rs resultset-seq (into []))))

(comment
  (load-docs! (user/crux-node))
  (fix/transact! (user/crux-node) (tf/tpch-tables->crux-sql-schemas))
  (def db (c/db (user/crux-node)))
  (def conn (cal/jdbc-connection (user/crux-node)))
  (def p (prepared-query conn "SELECT c_name FROM CUSTOMER"))

  (println (with-timing*
             (fn [] {:count (count (c/q db '{:find [c_custkey c_name c_address c_nationkey c_phone c_acctbal c_mktsegment c_comment],
                                             :where [[e :custkey c_custkey] [e :name c_name] [e :address c_address] [e :nationkey c_nationkey] [e :phone c_phone] [e :acctbal c_acctbal] [e :mktsegment c_mktsegment] [e :comment c_comment]],
                                             :args []}))})))

  (println (with-timing*
             (fn [] {:count (count (query conn "SELECT * FROM CUSTOMER"))})))

  (println (with-timing*
             (fn [] {:count (count (exec-prepared-query (prepared-query conn "SELECT c_name FROM CUSTOMER")))})))

  (println (with-timing*
             (fn [] {:count (count (exec-prepared-query p))}))))
