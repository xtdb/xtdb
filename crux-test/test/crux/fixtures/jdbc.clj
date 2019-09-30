(ns crux.fixtures.jdbc
  (:require [crux.fixtures.api :as api]
            [crux.fixtures.kv :as kvf :refer [*kv-backend*]]
            [crux.io :as cio]
            [crux.jdbc :as j]
            [next.jdbc :as jdbc]))

(def ^:dynamic *dbtype* nil)

(defn with-jdbc-node [dbtype f & [opts]]
  (let [dbtype (name dbtype)
        options (merge {:crux.bootstrap/node-config :crux.jdbc/node-config
                        :dbtype (name dbtype)
                        :dbname "cruxtest"
                        :kv-backend *kv-backend*}
                       opts)
        ds (jdbc/get-datasource options)]
    (binding [*dbtype* dbtype]
      (j/prep-for-tests! dbtype ds)
      (api/with-opts options f))))
