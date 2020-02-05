(ns crux.fixtures.jdbc
  (:require [crux.fixtures.api :as api]
            [crux.io :as cio]
            [crux.jdbc :as j]
            [next.jdbc :as jdbc]
            [clojure.test :as t]
            [crux.kafka :as k]
            [crux.fixtures :as f]
            [clojure.java.io :as io]))

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
