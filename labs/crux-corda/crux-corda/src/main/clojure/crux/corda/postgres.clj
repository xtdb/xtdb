(ns crux.corda.postgres
  (:require [crux.corda :as crux-corda]
            [crux.tx :as tx]
            [juxt.clojars-mirrors.nextjdbc.v1v2v674.next.jdbc :as jdbc])
  (:import (java.util Date)
           (java.sql Timestamp)))

(defn ->dialect [_]
  (reify crux-corda/SQLDialect
    (db-type [_] :postgres)

    (setup-tx-schema! [_ jdbc-session]
      (doseq [q ["
CREATE TABLE IF NOT EXISTS crux_txs (
  crux_tx_id INT GENERATED ALWAYS AS IDENTITY NOT NULL PRIMARY KEY,
  crux_tx_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
  corda_tx_id VARCHAR(64) NOT NULL UNIQUE REFERENCES node_transactions(tx_id)
)"

                 "
DROP TRIGGER IF EXISTS crux_tx_mapping ON node_transactions"

                 "
CREATE OR REPLACE FUNCTION crux_insert_tx() RETURNS TRIGGER AS $$
  BEGIN
    INSERT INTO crux_txs (corda_tx_id) VALUES (NEW.tx_id);
    RETURN NEW;
  END;
$$ LANGUAGE plpgsql"

                 "
CREATE TRIGGER crux_tx_mapping
  AFTER INSERT OR UPDATE
  ON node_transactions
  FOR EACH ROW
  EXECUTE PROCEDURE crux_insert_tx();
"]]
        (jdbc/execute-one! jdbc-session [q])))))

(defmethod crux-corda/tx-row->tx :postgres [tx-row _]
  {::tx/tx-id (:crux_tx_id tx-row)
   ::tx/tx-time (Date. (.getTime ^Timestamp (:crux_tx_time tx-row)))
   :corda-tx-id (:corda_tx_id tx-row)})

(comment
  "
CREATE TABLE node_transactions (
  tx_id VARCHAR(64) NOT NULL PRIMARY KEY,
  transaction_value BYTEA,
  state_machine_run_id VARCHAR(36),
  status VARCHAR(1) NOT NULL DEFAULT 'V',
  timestamp TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"

  (def foo-db-spec
    {:dbtype "postgresql"
     :user "postgres"
     :password "my-secret-pw"
     :dbname "cruxtest"
     :host "localhost"})

  (def foo-dialect (->dialect nil))

  (crux-corda/setup-tx-schema! foo-dialect foo-db-spec)

  (let [dialect foo-dialect]
    (with-open [conn (jdbc/get-connection foo-db-spec)
                stmt (jdbc/prepare conn ["SELECT * FROM crux_txs ORDER BY crux_tx_id"])
                rs (.executeQuery stmt)]
      (->> (resultset-seq rs)
           (mapv #(crux-corda/tx-row->tx % dialect)))))
  )
