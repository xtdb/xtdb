(ns crux.fixtures.jdbc
  (:require [clojure.java.io :as io]
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
