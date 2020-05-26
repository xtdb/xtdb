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

(defn prepared-query [^java.sql.Connection conn q & args]
  (let
    (doseq [[i v] args]
      (if (string? v)
        (.setString p i v)))
    (with-open [rs (.executeQuery p)]
      (->> rs resultset-seq (into [])))))

(defn query [^java.sql.Connection conn q]
  (with-open [stmt (.createStatement conn)
              rs (.executeQuery stmt q)]
    (->> rs resultset-seq (into []))))

(comment
  (load-docs! (user/crux-node))
  (fix/transact! (user/crux-node) (tf/tpch-tables->crux-sql-schemas))
  (def db (c/db (user/crux-node)))
  (def conn (cal/jdbc-connection (user/crux-node)))

  (println (with-timing*
             (fn [] {:count (count (c/q db '{:find [e] :where [[e :custkey ?custkey] [e :name ?c_name]]}))})))

  (println (with-timing*
             (fn [] {:count (count (query conn "SELECT * FROM CUSTOMER"))})))

  (println (with-timing*
             (fn [] {:count (count (prepared-query conn "SELECT * FROM CUSTOMER"))}))))
