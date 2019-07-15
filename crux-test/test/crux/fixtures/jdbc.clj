(ns crux.fixtures.jdbc
  (:require [next.jdbc :as jdbc]
            [crux.fixtures.api :refer [*api*]]
            [crux.io :as cio])
  (:import [crux.api Crux ICruxAPI]))

(defn with-jdbc-node [f]
  (let [db-dir (str (cio/create-tmpdir "kv-store"))
        jdbc-event-log-dir (str (cio/create-tmpdir "jdbc-event-log-dir"))
        options {:dbtype "h2"
                 :dbname "cruxtest"
                 :db-dir db-dir
                 :kv-backend "crux.kv.memdb.MemKv"
                 :jdbc-event-log-dir jdbc-event-log-dir}
        ds (jdbc/get-datasource options)]
    (jdbc/execute! ds ["DROP ALL OBJECTS"])
    (try
      (with-open [standalone-node (Crux/startJDBCNode options)]
        (binding [*api* standalone-node]
          (f)))
      (finally
        (cio/delete-dir db-dir)
        (cio/delete-dir jdbc-event-log-dir)))))
