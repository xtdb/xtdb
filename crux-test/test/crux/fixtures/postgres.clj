(ns crux.fixtures.postgres
  (:require [crux.fixtures.jdbc :as fj])
  (:import com.opentable.db.postgres.embedded.EmbeddedPostgres))

(defn with-embedded-postgres [f]
  (with-open [pg (.start (EmbeddedPostgres/builder))]
    (fj/with-jdbc-node "postgresql" f {:port (.getPort pg)
                                       :dbname "postgres"
                                       :user "postgres"})))
