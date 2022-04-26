(ns core2.sql.jdbc
  (:require [core2.sql.plan :as plan]
            [core2.sql.analyze :as sem]
            [core2.sql :as sql]
            [core2.api :as c2]
            [core2.local-node :as node]
            [core2.rewrite :as r])
  (:import [java.sql Connection Driver DriverManager DriverPropertyInfo
            ResultSet ResultSetMetaData PreparedStatement SQLException SQLFeatureNotSupportedException]))

;; Spike using wrapping a local node in JDBC. Should really work on
;; Arrow and not the already converted maps.

(defn- ->result-set-meta-data [projection]
  (reify ResultSetMetaData
    (getTableName [_ idx]
      "")

    (getColumnLabel [_ idx]
      (name (get projection (dec idx))))

    (getColumnCount [_]
      (count projection))))

(defn- ->result-set [result projection]
  (reify ResultSet
    (getObject [_ ^int idx]
      (try
        (get (:row @result) (get projection (dec idx)))
        (catch Exception e
          (throw (SQLException. e)))))

    (next [_]
      (try
        (if (seq (:result @result))
          (do (swap! result (fn [{:keys [row result]}]
                              {:row (first result)
                               :result (rest result)}))
              true)
          false)
        (catch Exception e
          (throw (SQLException. e)))))

    (getMetaData [_]
      (->result-set-meta-data projection))

    (close [_]
      (reset! result nil))))

(defn- ->prepared-statement [node sql]
  (let [projection (->> (r/$ (r/vector-zip (sql/parse sql)) 1)
                        (sem/projected-columns)
                        (first)
                        (mapv (comp keyword plan/unqualified-projection-symbol)))
        result (atom nil)]
    (reify PreparedStatement
      (execute [_]
        (try
          (reset! result {:row nil :result (c2/sql-query node sql)})
          true
          (catch Exception e
            (throw (SQLException. e)))))

      (getResultSet [_]
        (->result-set result projection))

      (close [_]
        (reset! result nil)))))

(defn- ->xtdb-memory-connection [url info]
  (let [node (node/start-node {})]
    (reify Connection
      (prepareStatement [_ sql]
        (try
          (->prepared-statement node sql)
          (catch Exception e
            (throw (SQLException. e)))))

      (close [_]
        (.close node)))))

(defonce ^:private xtdb-jdbc-driver
  (reify Driver
    (acceptsURL [_ url]
      (= "jdbc:xtdb:mem" url))

    (connect [this url info]
      (when (.acceptsURL this url)
        ((resolve '->xtdb-memory-connection) url info)))

    (getMajorVersion [_] 2)

    (getMinorVersion [_] 0)

    (getPropertyInfo [_ url info]
      (make-array DriverPropertyInfo 0))

    (jdbcCompliant [_] false)

    (getParentLogger [_]
      (throw (SQLFeatureNotSupportedException.)))))

(defonce ^:private register-xtdb-driver
  (DriverManager/registerDriver xtdb-jdbc-driver))

(comment
  (require '[juxt.clojars-mirrors.nextjdbc.v1v2v674.next.jdbc :as jdbc])
  (with-open [conn (jdbc/get-connection "jdbc:xtdb:mem")]
    (jdbc/execute! conn ["SELECT * FROM (VALUES 1, 2) AS x (a), (VALUES 3, 4) AS y (b)"]))
  ;;=>
  [{:a 1, :b 3} {:a 1, :b 4} {:a 2, :b 3} {:a 2, :b 4}])
