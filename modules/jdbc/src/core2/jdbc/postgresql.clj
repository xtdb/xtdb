(ns core2.jdbc.postgresql
  (:require [core2.jdbc :as j]
            [core2.system :as sys]
            [next.jdbc :as jdbc]))

(defn ->dialect {::sys/args {:drop-tables? {:spec ::sys/boolean, :default false}}}
  [{:keys [drop-tables?]}]
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
