(ns xtdb.jdbc-test
  (:require [clojure.test :as t]
            [xtdb.jdbc :as xt-jdbc]
            [xtdb.jdbc.postgresql :as xt-psql]
            [xtdb.object-store-test :as ost]
            [juxt.clojars-mirrors.nextjdbc.v1v2v674.next.jdbc :as jdbc]
            [juxt.clojars-mirrors.integrant.core :as ig]))

(defn postgres-sys []
  (let [db-spec {:user "postgres", :password "postgres", :database "xtdbtest"}]
    (with-open [conn (jdbc/get-connection (merge db-spec {:dbtype "postgresql"}))]
      (jdbc/execute! conn ["DROP DATABASE IF EXISTS xtdbtest"])
      (jdbc/execute! conn ["CREATE DATABASE xtdbtest"]))
    (-> {::xt-psql/dialect {:drop-tables? true}
         ::xt-jdbc/default-pool {:db-spec db-spec}
         ::xt-jdbc/object-store {}}
        ig/prep
        ig/init)))

(t/deftest ^:requires-docker ^:jdbc put-delete-test
  (let [sys (postgres-sys)]
    (try
      (ost/test-put-delete (::xt-jdbc/object-store sys))
      (finally
        (ig/halt! sys)))))

(t/deftest ^:requires-docker ^:jdbc list-test
  (let [sys (postgres-sys)]
    (try
      (ost/test-list-objects (::xt-jdbc/object-store sys))
      (finally
        (ig/halt! sys)))))

(t/deftest ^:requires-docker ^:jdbc range-test
  (let [sys (postgres-sys)]
    (try
      (ost/test-range (::xt-jdbc/object-store sys))
      (finally
        (ig/halt! sys)))))
