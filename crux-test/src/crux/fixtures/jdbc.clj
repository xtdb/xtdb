(ns crux.fixtures.jdbc
  (:require [clojure.java.io :as io]
            [crux.fixtures :as fix]
            [crux.jdbc :as j]
            [next.jdbc :as jdbc]
            [crux.system :as sys])
  (:import com.opentable.db.postgres.embedded.EmbeddedPostgres))

(def ^:dynamic *dbtype* nil)

(defn- with-jdbc-opts [dbtype ds-dep f]
  (binding [*dbtype* dbtype]
    (fix/with-opts {:crux/tx-log {:crux/module `j/->tx-log
                                  :data-source ::data-source
                                  :dbtype dbtype}
                    :crux/document-store {:crux/module `j/->document-store,
                                          :data-source ::data-source
                                          :dbtype dbtype}
                    ::tx-consumer `j/->tx-consumer
                    ::data-source ds-dep}
      f)))

(defn with-h2-opts [f]
  (fix/with-tmp-dir "db-dir" [db-dir]
    (with-jdbc-opts "h2" {:crux/module `crux.jdbc.h2/->data-source,
                          :db-file (io/file db-dir "cruxtest")}
      f)))

(defn with-sqlite-opts [f]
  (fix/with-tmp-dir "db-dir" [db-dir]
    (with-jdbc-opts "sqlite" {:crux/module `crux.jdbc.sqlite/->data-source
                              :db-file (io/file db-dir "cruxtest")}
      f)))

(defn with-mssql-opts [db-spec f]
  (with-jdbc-opts "mssql" {:crux/module `crux.jdbc.mssql/->data-source
                           :db-spec ~db-spec}
    f))

(defn with-mysql-opts [db-spec f]
  (with-jdbc-opts "mysql" {:crux/module `crux.jdbc.mysql/->data-source
                           :db-spec db-spec}
    f))

(defn with-embedded-postgres [f]
  (with-open [pg (.start (EmbeddedPostgres/builder))]
    (with-jdbc-opts "postgresql" {:crux/module `crux.jdbc.psql/->data-source
                                  :db-spec {:port (.getPort pg)
                                            :dbname "postgres"
                                            :user "postgres"}}
      f)))
