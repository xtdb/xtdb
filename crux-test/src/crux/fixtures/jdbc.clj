(ns crux.fixtures.jdbc
  (:require [clojure.java.io :as io]
            [crux.fixtures :as fix]
            [crux.jdbc :as j]
            [clojure.test :as t])
  (:import com.opentable.db.postgres.embedded.EmbeddedPostgres))

(defn- with-jdbc-opts [{:keys [pool-opts dialect db-spec]} f]
  (fix/with-opts {::j/connection-pool {:pool-opts pool-opts, :dialect dialect, :db-spec db-spec}
                  :crux/tx-log {:crux/module `j/->tx-log
                                :connection-pool ::j/connection-pool}
                  :crux/document-store {:crux/module `j/->document-store,
                                        :connection-pool ::j/connection-pool}}
    f))

(defn with-h2-opts [f]
  (fix/with-tmp-dir "db-dir" [db-dir]
    (with-jdbc-opts {:dialect 'crux.jdbc.h2/->dialect
                     :db-spec {:dbname (str (io/file db-dir "cruxtest"))}}
      f)))

(defn with-sqlite-opts [f]
  (fix/with-tmp-dir "db-dir" [db-dir]
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

(defn with-each-jdbc-node [f]
  (t/testing "H2 Database"
    (with-h2-opts f))
  (t/testing "SQLite Database"
    (with-sqlite-opts f))
  (t/testing "Postgresql Database"
    (with-embedded-postgres f))

  ;; Optional:
  #_(t/testing "MySQL Database"
      ;; docker run --rm --name crux-mysql -e MYSQL_ROOT_PASSWORD=my-secret-pw -p 3306:3306 -d mysql:8.0.21
      ;; mariadb -h 127.0.0.1 -u root -p
      ;; CREATE DATABASE cruxtest;
      (with-mysql-opts {:dbname "cruxtest", :user "root", :password "my-secret-pw"} f))

  #_(when (.exists (clojure.java.io/file ".testing-oracle.edn"))
      (t/testing "Oracle Database"
        (with-jdbc-node "oracle" f
          (read-string (slurp ".testing-oracle.edn")))))

  #_(t/testing "MSSQL Database"
      ;; docker run --rm --name crux-mssql -e 'ACCEPT_EULA=Y' -e 'SA_PASSWORD=yourStrong(!)Password' -e 'MSSQL_PID=Express' -p 1433:1433 -d mcr.microsoft.com/mssql/server:2017-latest-ubuntu
      ;; mssql-cli
      ;; CREATE DATABASE cruxtest;
      (with-mssql-opts {:db-spec {:dbname "cruxtest"}
                        :pool-opts {:username "sa"
                                    :password "yourStrong(!)Password"}}
        f)))
