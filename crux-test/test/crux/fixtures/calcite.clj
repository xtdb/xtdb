(ns crux.fixtures.calcite
  (:require [crux.calcite :as cal]
            [crux.fixtures.api :as fapi]
            [crux.node :as n])
  (:import java.sql.DriverManager
           java.sql.PreparedStatement))

(def ^:dynamic ^java.sql.Connection *conn*)

(defn with-calcite-connection [f]
  (with-open [conn (cal/jdbc-connection fapi/*api*)]
    (binding [*conn* conn]
      (f))))

(defn with-avatica-connection [f]
  (with-open [conn (DriverManager/getConnection "jdbc:avatica:remote:url=http://localhost:1503;serialization=protobuf")]
    (binding [*conn* conn]
      (f))))

(defn with-calcite-module [f]
  (fapi/with-opts (-> fapi/*opts*
                      (update ::n/topology conj cal/module)
                      (assoc :crux.calcite/port 1503))
    f))

(defn query [q]
  (with-open [stmt (.createStatement *conn*)
              rs (.executeQuery stmt q)]
    (->> rs resultset-seq (into []))))

(defn prepared-query [q & args]
  (let [p ^PreparedStatement (.prepareStatement *conn* q)]
    (doseq [[i v] args]
      (if (string? v)
        (.setString p i v)))
    (with-open [rs (.executeQuery p)]
      (->> rs resultset-seq (into [])))))

(defn explain [q]
  (:plan (first (query (str "explain plan for " q)))))
