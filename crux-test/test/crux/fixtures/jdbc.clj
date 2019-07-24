(ns crux.fixtures.jdbc
  (:require [next.jdbc :as jdbc]
            [crux.fixtures.api :refer [*api*]]
            [crux.io :as cio])
  (:import [crux.api Crux ICruxAPI]))

(defn with-jdbc-node [dbtype f]
  (let [db-dir (str (cio/create-tmpdir "kv-store"))
        jdbc-event-log-dir (str (cio/create-tmpdir "jdbc-event-log-dir"))
        options {:dbtype (name dbtype)
                 :dbname "cruxtest"
                 :db-dir db-dir
                 :kv-backend "crux.kv.memdb.MemKv"
                 :jdbc-event-log-dir jdbc-event-log-dir}
        ds (jdbc/get-datasource options)]
    (case dbtype
      :h2
      (jdbc/execute! ds ["DROP ALL OBJECTS"])
      :sqlite
      (jdbc/execute! ds ["DROP TABLE IF EXISTS tx_events"]))
    (try
      (with-open [standalone-node (Crux/startJDBCNode options)]
        (binding [*api* standalone-node]
          (f)))
      (finally
        (cio/delete-dir db-dir)
        (cio/delete-dir jdbc-event-log-dir)))))
