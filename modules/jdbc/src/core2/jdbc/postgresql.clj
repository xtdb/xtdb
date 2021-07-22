(ns core2.jdbc.postgresql
  (:require [core2.jdbc :as j]
            [juxt.clojars-mirrors.nextjdbc.v1v2v674.next.jdbc :as jdbc]
            [integrant.core :as ig]))

(derive ::dialect ::j/dialect)

(defmethod ig/init-key ::dialect [_ {:keys [drop-tables?]}]
  (reify j/Dialect
    (db-type [_] :postgresql)

    (setup-object-store-schema! [_ pool]
      (when drop-tables?
        (jdbc/execute! pool ["DROP TABLE IF EXISTS objects"]))

      (doto pool
        (jdbc/execute! ["
CREATE TABLE IF NOT EXISTS objects (
  key TEXT PRIMARY KEY,
  blob BYTEA)"])))

    (upsert-object-sql [_]
      "INSERT INTO objects (key, blob) VALUES (?, ?) ON CONFLICT DO NOTHING")))
