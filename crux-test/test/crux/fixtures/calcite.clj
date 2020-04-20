(ns crux.fixtures.calcite
  (:require [crux.calcite :as cal]
            [crux.fixtures.api :as fapi]
            [crux.node :as n])
  (:import java.sql.DriverManager))

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
