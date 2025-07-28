(ns xtdb.database-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [next.jdbc :as jdbc]
            [xtdb.api :as xt]
            [xtdb.check-pbuf :as cpb]
            [xtdb.error :as err]
            [xtdb.pgwire :as pgw]
            [xtdb.pgwire-test :as pgw-test]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import [java.nio.file Path]
           xtdb.api.storage.Storage))

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

(t/deftest test-block-boundary
  (util/with-tmp-dir* "node"
    (fn [^Path node-dir]
      (util/with-open [node (tu/->local-node {:node-dir node-dir})
                       xt-db-conn (-> (.createConnectionBuilder node) (.build))]
        (jdbc/execute! xt-db-conn ["INSERT INTO foo RECORDS {_id: 'xtdb'}"])
        (jdbc/execute! xt-db-conn ["CREATE DATABASE new_db"])
        (tu/flush-block! node)

        (util/with-open [new-db-conn (-> (.createConnectionBuilder node) (.database "new_db") (.build))]
          (jdbc/execute! new-db-conn ["INSERT INTO foo RECORDS {_id: 'new-db'}"])
          (t/is (= {:_id "new-db"} (jdbc/execute-one! new-db-conn ["SELECT * FROM foo"])))))

      (cpb/check-pbuf (.toPath (io/as-file (io/resource "xtdb/database-test/block-boundary")))
                      (.resolve node-dir (format "objects/%s/blocks" Storage/STORAGE_ROOT)))

      (util/with-open [node2 (tu/->local-node {:node-dir node-dir})
                       xt-db-conn (.build (.createConnectionBuilder node2))]
        (t/is (= {:_id "xtdb"} (jdbc/execute-one! xt-db-conn ["SELECT * FROM foo"])))
        (util/with-open [new-db-conn (-> (.createConnectionBuilder node2)
                                         (.database "new_db")
                                         (.build))]

          ;; atm, this is an in-memory database, so don't expect to see the previous data
          (jdbc/execute! new-db-conn ["INSERT INTO foo RECORDS {_id: 'new-db', version: 2}"])
          (t/is (= {:_id "new-db", :version 2}
                   (jdbc/execute-one! new-db-conn ["SELECT * FROM foo"])))

          ;; `xtdb` db is persisted though.
          (t/is (= {:_id "xtdb"}
                   (jdbc/execute-one! xt-db-conn ["SELECT * FROM foo"]))))))))
