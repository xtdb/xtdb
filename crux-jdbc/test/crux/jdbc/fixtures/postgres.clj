(ns crux.jdbc.fixtures.postgres
  (:require [crux.io :as cio]
            [next.jdbc :as jdbc]
            [crux.fixtures.api :refer [*api*]])
  (:import [com.opentable.db.postgres.embedded EmbeddedPostgres]
           [crux.api Crux ICruxAPI]))

(defn with-embedded-postgres [f]
  (with-open [pg (.start (EmbeddedPostgres/builder))]
    (let [db-dir (str (cio/create-tmpdir "kv-store"))
          options {:dbtype "postgresql"
                   :dbname "postgres"
                   :user "postgres"
                   :db-dir db-dir
                   :port (.getPort pg)
                   :kv-backend "crux.kv.memdb.MemKv"}
          ds (jdbc/get-datasource options)]
      (try
        (with-open [standalone-system (Crux/startJDBCSystem options)]
          (binding [*api* standalone-system]
            (f)))
        (finally
          (cio/delete-dir db-dir))))))
