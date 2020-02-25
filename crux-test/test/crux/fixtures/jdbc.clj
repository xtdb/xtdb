(ns crux.fixtures.jdbc
  (:require [clojure.java.io :as io]
            [crux.fixtures :as f]
            [crux.fixtures.api :as api]
            [crux.jdbc :as j]
            [next.jdbc :as jdbc]))

(def ^:dynamic *dbtype* nil)

(defn with-jdbc-node [dbtype f & [opts]]
  (f/with-tmp-dir "db-dir" [db-dir]
    (let [dbtype (name dbtype)
          options (merge {:crux.node/topology 'crux.jdbc/topology
                          :crux.jdbc/dbtype (name dbtype)
                          :crux.jdbc/dbname (str (io/file db-dir "cruxtest"))}
                         opts)
          ds (jdbc/get-datasource (j/conform-next-jdbc-properties options))]
      (binding [*dbtype* dbtype]
        (j/prep-for-tests! dbtype ds)
        (api/with-opts options f)))))
