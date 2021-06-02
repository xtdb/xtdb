(ns core2.jdbc-test
  (:require [core2.jdbc :as c2-jdbc]
            [core2.jdbc.postgresql :as c2-psql]
            [core2.object-store-test :as ost]
            [core2.system :as sys]
            [next.jdbc :as jdbc]))

(defn- with-obj-store [pool-opts f]
  (with-open [sys (-> (sys/prep-system {::c2-jdbc/object-store
                                        {:connection-pool pool-opts}})
                      (sys/start-system))]
    (f (::c2-jdbc/object-store sys))))

(defn- setup-postgres []
  (let [db-spec {:user "postgres", :password "postgres"}]

    (with-open [conn (jdbc/get-connection {:dbtype "postgresql", :user "postgres", :password "postgres"})]
      (jdbc/execute! conn ["DROP DATABASE IF EXISTS core2test"])
      (jdbc/execute! conn ["CREATE DATABASE core2test"]))

    {:dialect {:core2/module `c2-psql/->dialect
               :drop-tables? true}
     :db-spec (assoc db-spec :database "core2test")}))

(ost/def-obj-store-tests jdbc-postgres [f]
  (with-obj-store (setup-postgres)
    f))
