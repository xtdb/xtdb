(ns xtdb.fixtures.jdbc
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [xtdb.fixtures :as fix]
            [xtdb.jdbc :as j]
            [juxt.clojars-mirrors.nextjdbc.v1v2v674.next.jdbc :as jdbc])
  (:import com.opentable.db.postgres.embedded.EmbeddedPostgres))

(def ^:dynamic *jdbc-opts*)

(defn with-opts [opts f]
  (binding [*jdbc-opts* opts]
    (f)))

(defn with-h2-opts [f]
  (fix/with-tmp-dirs #{db-dir}
    (with-opts {:dialect 'xtdb.jdbc.h2/->dialect
                :db-spec {:dbname (str (io/file db-dir "xtdbtest"))}}
      f)))

(defn with-sqlite-opts [f]
  (fix/with-tmp-dirs #{db-dir}
    (with-opts {:dialect 'xtdb.jdbc.sqlite/->dialect
                :db-spec {:dbname (str (io/file db-dir "xtdbtest"))}}
      f)))

(defn with-mssql-opts [db-spec f]
  (with-opts {:dialect 'xtdb.jdbc.mssql/->dialect
              :db-spec db-spec}
    f))

(defn with-mysql-opts [db-spec f]
  (with-opts {:dialect 'xtdb.jdbc.mysql/->dialect
              :db-spec db-spec}
    f))

(defn with-embedded-postgres [f]
  (with-open [pg (.start (EmbeddedPostgres/builder))]
    (with-opts {:dialect 'xtdb.jdbc.psql/->dialect
                :db-spec {:port (.getPort pg)
                          :dbname "postgres"
                          :user "postgres"}}
      f)))

(defn with-postgres-opts [db-spec f]
  (with-opts {:dialect 'xtdb.jdbc.psql/->dialect
              :db-spec db-spec}
    f))

(def ^:private jdbc-dialects
  #{:h2 :sqlite :embedded-postgres
    #_:postgres #_:mysql #_:mssql})

;; Optional:
;; in `xtdb-jdbc`: `docker-compose up` (`docker-compose up -d` for background)

(defn with-each-jdbc-dialect [f]
  (when (:h2 jdbc-dialects)
    (t/testing "H2"
      (with-h2-opts f)))

  (when (:sqlite jdbc-dialects)
    (t/testing "SQLite"
      (with-sqlite-opts f)))

  (when (:embedded-postgres jdbc-dialects)
    (t/testing "embedded Postgres"
      (with-embedded-postgres f)))

  (when (:postgres jdbc-dialects)
    (t/testing "Postgres"
      (with-open [conn (jdbc/get-connection {:dbtype "postgresql", :user "postgres", :password "postgres"})]
        (jdbc/execute! conn ["DROP DATABASE IF EXISTS xtdbtest"])
        (jdbc/execute! conn ["CREATE DATABASE xtdbtest"]))
      (with-postgres-opts {:dbname "xtdbtest", :user "postgres", :password "postgres"}
        f)))

  (when (:mysql jdbc-dialects)
    (t/testing "MySQL Database"
      (with-open [conn (jdbc/get-connection {:dbtype "mysql", :user "root", :password "my-secret-pw"})]
        (jdbc/execute! conn ["DROP DATABASE IF EXISTS xtdbtest"])
        (jdbc/execute! conn ["CREATE DATABASE xtdbtest"]))
      (with-mysql-opts {:dbname "xtdbtest", :user "root", :password "my-secret-pw"}
        f)))

  (when (:mssql jdbc-dialects)
    (t/testing "MSSQL Database"
      (with-open [conn (jdbc/get-connection {:dbtype "mssql", :user "sa", :password "yourStrong(!)Password"})]
        (jdbc/execute! conn ["DROP DATABASE IF EXISTS xtdbtest"])
        (jdbc/execute! conn ["CREATE DATABASE xtdbtest"]))
      (with-mssql-opts {:dbname "xtdbtest"
                        :user "sa"
                        :password "yourStrong(!)Password"}
        f)))

  (when (:oracle jdbc-dialects)
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

    (t/testing "Oracle Database"
      (with-opts {:dialect 'xtdb.jdbc.oracle/->dialect
                  :db-spec {:jdbcUrl "jdbc:oracle:thin:@127.0.0.1:51521:XE"
                            :username "sys as sysdba"
                            :password "mysecurepassword"}}
        f))))

(defn with-jdbc-node [f]
  (fix/with-opts {::j/connection-pool *jdbc-opts*
                  :xtdb/tx-log {:xtdb/module `j/->tx-log
                              :connection-pool ::j/connection-pool}
                  :xtdb/document-store {:xtdb/module `j/->document-store,
                                      :connection-pool ::j/connection-pool}}
    f))

#_ ; FIXME #1588
(t/deftest test-jdbc-url-string-key-1588
  (when (:postgres jdbc-dialects)
    (let [jdbc-url "jdbc:postgresql://127.0.0.1:5432/postgres?user=postgres&password=postgres"]
      (with-postgres-opts {"jdbcUrl" jdbc-url}
        (fn []
          (with-jdbc-node
            (fn []
              (fix/with-node
                (fn []
                  (t/is true))))))))))
