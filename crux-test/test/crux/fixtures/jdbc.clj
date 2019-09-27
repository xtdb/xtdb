(ns crux.fixtures.jdbc
  (:require [next.jdbc :as jdbc]
            [crux.fixtures.api :refer [*api*]]
            [crux.fixtures.kv :refer [*kv-backend*]]
            [crux.io :as cio]
            [crux.jdbc :as j])
  (:import [crux.api Crux ICruxAPI]))

(def ^:dynamic *dbtype* nil)

(defn with-jdbc-node [dbtype f & [opts]]
  (let [dbtype (name dbtype)
        db-dir (str (cio/create-tmpdir "kv-store"))
        options (merge {:crux.bootstrap/node-config :crux.jdbc/node-config
                        :dbtype (name dbtype)
                        :dbname "cruxtest"
                        :db-dir db-dir
                        :kv-backend *kv-backend*}
                       opts)
        ds (jdbc/get-datasource options)]
    (binding [*dbtype* dbtype]
      (j/prep-for-tests! dbtype ds)
      (try
        (with-open [standalone-node (Crux/startNode options)]
          (binding [*api* standalone-node]
            (f)))
        (finally
          (cio/delete-dir db-dir))))))
