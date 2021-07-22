(ns core2.jdbc-test
  (:require [core2.jdbc :as c2-jdbc]
            [core2.jdbc.postgresql :as c2-psql]
            [core2.object-store-test :as ost]
            [juxt.clojars-mirrors.nextjdbc.v1v2v674.next.jdbc :as jdbc]
            [integrant.core :as ig]))

(defn- with-obj-store [opts f]
  (let [sys (-> (merge opts {::c2-jdbc/object-store {}})
                ig/prep
                ig/init)]
    (try
      (f (::c2-jdbc/object-store sys))
      (finally
        (ig/halt! sys)))))

(ost/def-obj-store-tests jdbc-postgres [f]
  (let [db-spec {:user "postgres", :password "postgres", :database "core2test"}]
    (with-open [conn (jdbc/get-connection (merge db-spec {:dbtype "postgresql"}))]
      (jdbc/execute! conn ["DROP DATABASE IF EXISTS core2test"])
      (jdbc/execute! conn ["CREATE DATABASE core2test"]))

    (with-obj-store {::c2-psql/dialect {:drop-tables? true}
                     ::c2-jdbc/default-pool {:db-spec db-spec}}
      f)))
