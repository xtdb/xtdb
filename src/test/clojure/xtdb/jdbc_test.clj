(ns xtdb.jdbc-test
  (:require [xtdb.jdbc :as xt-jdbc]
            [xtdb.jdbc.postgresql :as xt-psql]
            [xtdb.object-store-test :as ost]
            [juxt.clojars-mirrors.nextjdbc.v1v2v674.next.jdbc :as jdbc]
            [juxt.clojars-mirrors.integrant.core :as ig]))

(defn- with-obj-store [opts f]
  (let [sys (-> (merge opts {::xt-jdbc/object-store {}})
                ig/prep
                ig/init)]
    (try
      (f (::xt-jdbc/object-store sys))
      (finally
        (ig/halt! sys)))))

(ost/def-obj-store-tests ^:requires-docker ^:jdbc jdbc-postgres [f]
  (let [db-spec {:user "postgres", :password "postgres", :database "xtdbtest"}]
    (with-open [conn (jdbc/get-connection (merge db-spec {:dbtype "postgresql"}))]
      (jdbc/execute! conn ["DROP DATABASE IF EXISTS xtdbtest"])
      (jdbc/execute! conn ["CREATE DATABASE xtdbtest"]))

    (with-obj-store {::xt-psql/dialect {:drop-tables? true}
                     ::xt-jdbc/default-pool {:db-spec db-spec}}
      f)))
