(ns crux.fixtures.calcite
  (:require [crux.calcite :as cal]
            [crux.fixtures.api :as fapi]
            [crux.node :as n])
  (:import java.sql.DriverManager))

(def ^:dynamic ^java.sql.Connection *conn*)

(defn with-calcite-connection [f]
  (with-open [conn (cal/jdbc-connection)]
    (binding [*conn* conn]
      (f))))

(defn with-avatica-connection [f]
  (with-open [conn (DriverManager/getConnection "jdbc:avatica:remote:url=http://localhost:1501;serialization=protobuf")]
    (binding [*conn* conn]
      (f))))

(defn with-calcite-module [f]
  (fapi/with-opts (-> fapi/*opts*
                      (update ::n/topology conj cal/module))
    f))

(defn query [q]
  (let [stmt (.createStatement *conn*)]
    (->> q (.executeQuery stmt) resultset-seq)))
