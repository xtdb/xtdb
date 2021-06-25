(ns crux.fixtures.jdbc
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [crux.fixtures :as fix]
            [crux.jdbc :as j])
  (:import com.opentable.db.postgres.embedded.EmbeddedPostgres))

(defn- with-jdbc-opts [{:keys [pool-opts dialect db-spec]} f]
  (fix/with-opts {::j/connection-pool {:pool-opts pool-opts, :dialect dialect, :db-spec db-spec}
                  :crux/tx-log {:crux/module `j/->tx-log
                                :connection-pool ::j/connection-pool}
                  :crux/document-store {:crux/module `j/->document-store,
                                        :connection-pool ::j/connection-pool}}
    f))

(defn with-h2-opts [f]
  (fix/with-tmp-dirs #{db-dir}
    (with-jdbc-opts {:dialect 'crux.jdbc.h2/->dialect
                     :db-spec {:dbname (str (io/file db-dir "cruxtest"))}}
      f)))

(defn with-sqlite-opts [f]
  (fix/with-tmp-dirs #{db-dir}
    (with-jdbc-opts {:dialect 'crux.jdbc.sqlite/->dialect
                     :db-spec  {:dbname (str (io/file db-dir "cruxtest"))}}
      f)))

(defn with-mssql-opts [{:keys [db-spec pool-opts]} f]
  (with-jdbc-opts {:dialect 'crux.jdbc.mssql/->dialect
                   :pool-opts pool-opts
                   :db-spec db-spec}
    f))

(defn with-mysql-opts [db-spec f]
  (with-jdbc-opts {:dialect 'crux.jdbc.mysql/->dialect
                   :db-spec db-spec}
    f))

(defn with-embedded-postgres [f]
  (with-open [pg (.start (EmbeddedPostgres/builder))]
    (with-jdbc-opts {:dialect 'crux.jdbc.psql/->dialect
                     :db-spec {:port (.getPort pg)
                               :dbname "postgres"
                               :user "postgres"}}
      f)))

(defn with-postgres-opts [db-spec f]
  (with-jdbc-opts {:dialect 'crux.jdbc.psql/->dialect
                   :db-spec db-spec}
    f))

(defn with-each-jdbc-node [f]
  (t/testing "H2"
    (with-h2-opts f))

  (t/testing "SQLite"
    (with-sqlite-opts f))

  (t/testing "embedded Postgres"
    (with-embedded-postgres f))

  ;; Optional:
  ;; in `crux-jdbc`: `docker-compose up` (`docker-compose up -d` for background)

  #_
  (t/testing "Postgres"
    (with-open [conn (jdbc/get-connection {:dbtype "postgresql", :user "postgres", :password "postgres"})]
      (jdbc/execute! conn ["DROP DATABASE IF EXISTS cruxtest"])
      (jdbc/execute! conn ["CREATE DATABASE cruxtest"]))
    (with-postgres-opts {:dbname "cruxtest", :user "postgres", :password "postgres"}
      f))

  #_
  (t/testing "MySQL Database"
    (with-open [conn (jdbc/get-connection {:dbtype "mysql", :user "root", :password "my-secret-pw"})]
      (jdbc/execute! conn ["DROP DATABASE IF EXISTS cruxtest"])
      (jdbc/execute! conn ["CREATE DATABASE cruxtest"]))
    (with-mysql-opts {:dbname "cruxtest", :user "root", :password "my-secret-pw"}
      f))

  #_
  (t/testing "MSSQL Database"
    (with-open [conn (jdbc/get-connection {:dbtype "mssql", :user "sa", :password "yourStrong(!)Password"})]
      (jdbc/execute! conn ["DROP DATABASE IF EXISTS cruxtest"])
      (jdbc/execute! conn ["CREATE DATABASE cruxtest"]))
    (with-mssql-opts {:db-spec {:dbname "cruxtest"}
                      :pool-opts {:username "sa"
                                  :password "yourStrong(!)Password"}}
      f))

  ;; Oh, Oracle.
  ;; Right. Set up. We create a Docker image for Oracle Express Edition (XE)
  ;; - `git clone https://github.com/oracle/docker-images.git /tmp/oracle-docker`
  ;; - `cd /tmp/oracle-docker/OracleDatabase/SingleInstance/dockerfiles`
  ;; - `./buildContainerImage.sh -x -v 18.4.0`
  ;; Go make a cup of coffee.
  ;; Build completed in 530 seconds. Let's continue.

  ;; - `docker-compose -f docker-compose.oracle.yml up -d`
  ;; Time to go make another cup of coffee.

  ;; The below still fails with an auth error, I can't see why. Sorry :(

  #_
  (t/testing "Oracle Database"
    (with-jdbc-opts {:dialect 'crux.jdbc.oracle/->dialect
                     :db-spec {:jdbcUrl "jdbc:oracle:thin:@127.0.0.1:51521:XE"}
                     :pool-opts {:username "sys as sysdba"
                                 :password "mysecurepassword"}}
      f))
  )
