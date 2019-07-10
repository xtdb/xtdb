(ns crux.jdbc.fixtures.jdbc
  (:require [next.jdbc :as jdbc]
            [crux.fixtures.api :refer [*api*]]
            [crux.io :as cio])
  (:import [crux.api Crux ICruxAPI]))

(def db {:dbtype "h2"
         :dbname "test-eventlog"})

(def ^:dynamic *ds*)

(defn with-datasource [f]
  (binding [*ds* (jdbc/get-datasource db)]
    (try
      (f)
      (finally
        (jdbc/execute! *ds* ["DROP ALL OBJECTS"])))))

(defn with-jdbc-system [f]
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
      (with-open [standalone-system (Crux/startJDBCSystem options)]
        (binding [*api* standalone-system]
          (f)))
      (finally
        (cio/delete-dir db-dir)
        (cio/delete-dir jdbc-event-log-dir)))))
