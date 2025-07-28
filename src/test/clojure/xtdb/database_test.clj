(ns xtdb.database-test
  (:require [clojure.test :as t]
            [next.jdbc :as jdbc]
            [xtdb.api :as xt]
            [xtdb.error :as err]
            [xtdb.pgwire :as pgw]
            [xtdb.pgwire-test :as pgw-test]
            [xtdb.test-util :as tu]))

(t/use-fixtures :each tu/with-node pgw-test/with-server-and-port)

(t/deftest test-create-database
  (with-open [xt-db-conn (pgw-test/jdbc-conn)]
    (jdbc/execute! xt-db-conn ["INSERT INTO foo RECORDS {_id: 'xtdb'}"])
    (t/is (= {:_id "xtdb"} (jdbc/execute-one! xt-db-conn ["SELECT * FROM foo"])))

    (jdbc/execute! xt-db-conn ["CREATE DATABASE new_db"])

    (with-open [new-db-conn (pgw-test/jdbc-conn {:dbname "new_db"})]
      (jdbc/execute! new-db-conn ["INSERT INTO foo RECORDS {_id: 'new-db'}"])

      (t/is (= {:_id "new-db"} (jdbc/execute-one! new-db-conn ["SELECT * FROM foo"])))
      (t/is (= [{:xt/id "new-db"}] (xt/q new-db-conn ["SELECT * FROM foo"])))

      (jdbc/execute! xt-db-conn ["INSERT INTO foo RECORDS {_id: 'xtdb', version: 2}"])

      (t/is (= {:_id "xtdb", :version 2} (jdbc/execute-one! xt-db-conn ["SELECT * FROM foo"])))

      (t/is (= [{:xt/id "new-db"}] (xt/q new-db-conn ["SELECT * FROM foo"])))

      (t/is (anomalous? [:incorrect ::pgw/invalid-db
                         "CREATE DATABASE is only allowed on the primary database"]
                        (err/wrap-anomaly {}
                          (jdbc/execute-one! new-db-conn ["CREATE DATABASE not_on_xtdb_db"])))))))
