(ns xtdb.sql.multi-db-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [next.jdbc :as jdbc]
            [xtdb.api :as xt]
            [xtdb.check-pbuf :as pbuf]
            [xtdb.db-catalog :as db]
            [xtdb.log :as xt-log]
            [xtdb.node :as xtn]
            [xtdb.pgwire :as pgw]
            [xtdb.pgwire-test :as pgw-test]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import [java.nio.file Path]
           (org.postgresql.util PSQLException)
           xtdb.storage.LocalStorage))

(t/deftest test-resolves-other-db
  (with-open [pg (pgw/open-playground)
              xtdb-conn (.build (.createConnectionBuilder pg))
              new-db-conn (.build (-> (.createConnectionBuilder pg)
                                      (.database "new_db")))]
    (jdbc/execute! xtdb-conn ["INSERT INTO foo RECORDS {_id: 'xtdb'}"])
    (t/is (= {:_id "xtdb"} (jdbc/execute-one! xtdb-conn ["SELECT * FROM foo"])))
    (jdbc/execute! new-db-conn ["INSERT INTO foo RECORDS {_id: 'new-db'}"])

    (t/is (= [{:_id "new-db"} {:_id "xtdb"}]
             (jdbc/execute! new-db-conn ["SELECT _id FROM foo UNION ALL SELECT _id FROM xtdb.foo"])))

    (t/is (= [{:_id "new-db"} {:_id "xtdb"}]
             (jdbc/execute! new-db-conn ["SELECT _id FROM new_db.foo UNION ALL SELECT _id FROM xtdb.foo"])))

    (t/is (= [{:_id "new-db"} {:_id "xtdb"}]
             (jdbc/execute! xtdb-conn ["SELECT _id FROM new_db.foo UNION ALL SELECT _id FROM xtdb.foo"])))

    (t/is (= [{:_id "new-db"} {:_id "xtdb"}]
             (jdbc/execute! xtdb-conn ["SELECT _id FROM new_db.foo UNION ALL SELECT _id FROM foo"])))))

(t/deftest two-databases-with-same-files
  (let [node-dir (util/->path "target/multi-db/overlapping-caches")]
    (util/delete-dir node-dir)
    (with-open [node (xtn/start-node {:log [:local {:path (.resolve node-dir "xtdb/log")}]
                                      :storage [:local {:path (.resolve node-dir "xtdb/objects")}]})
                xtdb-conn (.build (.createConnectionBuilder node))]

      (jdbc/execute! xtdb-conn ["
         ATTACH DATABASE new_db WITH $$
           log: !Local
             path: 'target/multi-db/overlapping-caches/new_db/log'
           storage: !Local
             path: 'target/multi-db/overlapping-caches/new_db/objects'
         $$"])

      (with-open [new-db-conn (.build (-> (.createConnectionBuilder node)
                                          (.database "new_db")))]
        (jdbc/execute! xtdb-conn ["INSERT INTO foo RECORDS {_id: 'xtdb'}"])
        (jdbc/execute! new-db-conn ["INSERT INTO foo RECORDS {_id: 'new-db'}"])
        (tu/flush-block! node)
        (t/is (= [{:xt/id "new-db"}] (xt/q new-db-conn "SELECT * FROM foo")))
        (t/is (= [{:xt/id "xtdb"}] (xt/q xtdb-conn "SELECT * FROM foo")))))))

(t/deftest deals-with-table-ref-ambiguities
  (with-open [pg (pgw/open-playground)
              xtdb-conn (.build (.createConnectionBuilder pg))
              public-conn (.build (-> (.createConnectionBuilder pg)
                                      (.database "public")))]

    (t/testing "public schema vs public database"
      (jdbc/execute! xtdb-conn ["INSERT INTO foo RECORDS {_id: 'xtdb'}"])
      (t/is (= {:_id "xtdb"} (jdbc/execute-one! xtdb-conn ["SELECT * FROM foo"])))
      (jdbc/execute! public-conn ["INSERT INTO foo RECORDS {_id: 'public'}"])

      (t/is (= [{:_id "public"} {:_id "xtdb"}]
               (jdbc/execute! public-conn ["SELECT _id FROM foo UNION ALL SELECT _id FROM xtdb.foo"])))

      (t/is (= [{:_id "public"} {:_id "xtdb"}]
               (jdbc/execute! public-conn ["SELECT _id FROM public.foo UNION ALL SELECT _id FROM xtdb.foo"])))

      (t/is (thrown-with-msg? PSQLException
                              #"Ambiguous table reference: public\.foo"
                              (jdbc/execute! xtdb-conn ["SELECT _id FROM public.foo UNION ALL SELECT _id FROM xtdb.foo"])))

      (t/is (thrown-with-msg? PSQLException
                              #"Ambiguous table reference: public\.foo"
                              (jdbc/execute! xtdb-conn ["SELECT _id FROM public.foo UNION ALL SELECT _id FROM foo"])))

      (t/is (= [{:_id "public"} {:_id "xtdb"}]
               (jdbc/execute! xtdb-conn ["SELECT _id FROM public.public.foo UNION ALL SELECT _id FROM xtdb.public.foo"]))
            "fully qualified"))

    (t/testing "xtdb schema in public database"
      (jdbc/execute! public-conn ["INSERT INTO xtdb.bar RECORDS {_id: 'public-bar'}"])
      (jdbc/execute! public-conn ["INSERT INTO xtdb.baz RECORDS {_id: 'public-baz'}"])
      (jdbc/execute! xtdb-conn ["INSERT INTO bar RECORDS {_id: 'xtdb-bar'}"])

      (t/is (= [{:_id "public-baz"}]
               (jdbc/execute! public-conn ["SELECT _id FROM xtdb.baz"]))
            "there's no baz in the xtdb database")

      (t/is (= [{:_id "xtdb-bar"}]
               (jdbc/execute! xtdb-conn ["SELECT _id FROM xtdb.bar"])))

      (t/is (thrown-with-msg? PSQLException
                              #"Ambiguous table reference: xtdb\.bar"
                              (jdbc/execute! public-conn ["SELECT _id FROM xtdb.bar"])))

      (t/testing "fully qualified"
        (t/is (= [{:_id "xtdb-bar"}]
                 (jdbc/execute! public-conn ["SELECT _id FROM xtdb.public.bar"])))

        (t/is (= [{:_id "public-bar"}]
                 (jdbc/execute! public-conn ["SELECT _id FROM public.xtdb.bar"])))

        (t/is (= [{:_id "xtdb-bar"}]
                 (jdbc/execute! xtdb-conn ["SELECT _id FROM xtdb.public.bar"])))

        (t/is (= [{:_id "public-bar"}]
                 (jdbc/execute! xtdb-conn ["SELECT _id FROM public.xtdb.bar"])))))))

(t/deftest attach-database
  (let [node-dir (util/->path "target/multi-db/attach-db")]
    (util/delete-dir node-dir)
    (util/delete-dir (util/->path "target/multi-db/attach-new-db"))

    (with-open [node (tu/->local-node {:node-dir node-dir, :compactor-threads 0})]
      (jdbc/execute! node ["
ATTACH DATABASE new_db WITH $$
  log: !Local
    path: 'target/multi-db/attach-new-db/log'
  storage: !Local
    path: 'target/multi-db/attach-new-db/storage'
  $$"])

      (t/is (= [{:_id 0, :committed true}]
               (jdbc/execute! node ["SELECT _id, committed FROM xt.txs"])))

      (with-open [conn (-> (.createConnectionBuilder node)
                           (.database "new_db")
                           (.build))]
        (jdbc/execute! conn ["INSERT INTO foo RECORDS {_id: 'new-db'}"])
        (t/is (= {:_id "new-db"} (jdbc/execute-one! conn ["SELECT * FROM foo"])))))

    (t/testing "restart node"
      (let [^Path root-path (util/with-open [node (tu/->local-node {:node-dir node-dir, :compactor-threads 0})]
                              (xt-log/sync-node node)
                              (with-open [conn (-> (.createConnectionBuilder node)
                                                   (.database "new_db")
                                                   (.build))]
                                (t/is (= {:_id "new-db"} (jdbc/execute-one! conn ["SELECT * FROM foo"]))))

                              (tu/flush-block! node)

                              (-> ^LocalStorage (.getBufferPool (db/primary-db node))
                                  (.getRootPath)))]

        (pbuf/check-pbuf (.toPath (io/as-file (io/resource "xtdb/multi-db/attach-db")))
                         (.resolve root-path "blocks")
                         {:update-fn #(dissoc % :latest-completed-tx)})))

    (t/testing "restart node with flushed block"
      (util/with-open [node (tu/->local-node {:node-dir node-dir, :compactor-threads 0})]
        (with-open [conn (-> (.createConnectionBuilder node)
                             (.database "new_db")
                             (.build))]
          (t/is (= {:_id "new-db"} (jdbc/execute-one! conn ["SELECT * FROM foo"]))))))))

(t/deftest disallow-attach-db-in-tx
  (with-open [node (xtn/start-node)
              conn (jdbc/get-connection node)]
    (jdbc/execute! conn ["BEGIN"])

    (try
      (t/is (= {:sql-state "08P01",
                :message "Cannot attach a database in a transaction.",
                :detail #xt/error [:incorrect :xtdb.pgwire/attach-db-in-tx
                                   "Cannot attach a database in a transaction."
                                   {:db-name "new_db"}]}
               (pgw-test/reading-ex
                (jdbc/execute! conn ["ATTACH DATABASE new_db"]))))
      (finally
        (jdbc/execute! conn ["ROLLBACK"])))))

(t/deftest disallow-duplicate-db-names
  (with-open [node (xtn/start-node)]
    (t/is (= {:sql-state "XX000",
              :message "Database already exists",
              :detail #xt/error [:conflict :xtdb/db-exists "Database already exists"
                                 {:db-name "xtdb"}]}
             (pgw-test/reading-ex
              (jdbc/execute! node ["ATTACH DATABASE xtdb"]))))

    (jdbc/execute! node ["ATTACH DATABASE new_db"])

    (t/is (= {:sql-state "XX000",
              :message "Database already exists",
              :detail #xt/error [:conflict :xtdb/db-exists "Database already exists"
                                 {:db-name "new_db"}]}
             (pgw-test/reading-ex
              (jdbc/execute! node ["ATTACH DATABASE new_db"]))))))

(t/deftest dodgy-yaml
  (with-open [node (xtn/start-node)]
    (t/is (= {:sql-state "08P01"
              :message "Invalid database config in `ATTACH DATABASE`: Unknown property 'somethingElse'. Known properties are: log, mode, storage" ,
              :detail #xt/error [:incorrect :xtdb/invalid-database-config
                                 "Invalid database config in `ATTACH DATABASE`: Unknown property 'somethingElse'. Known properties are: log, mode, storage"
                                 {:sql "ATTACH DATABASE new_db WITH $$ somethingElse: $$ "}]},

             (pgw-test/reading-ex
              (jdbc/execute! node ["ATTACH DATABASE new_db WITH $$ somethingElse: $$ "]))))))

(t/deftest connected-to-secondary-db
  (with-open [node (pgw/open-playground)
              conn (-> (.createConnectionBuilder node)
                       (.database "new_db")
                       (.build))]

    (t/is (= {:sql-state "08P01",
              :message "Can only attach databases when connected to the primary 'xtdb' database.",
              :detail #xt/error [:incorrect :xtdb.pgwire/attach-db-on-secondary
                                 "Can only attach databases when connected to the primary 'xtdb' database."
                                 {:db "new_db"}]}
             (pgw-test/reading-ex
              (jdbc/execute! conn ["ATTACH DATABASE nope"]))))))

(t/deftest detach-database
  (with-open [node (xtn/start-node)
              xtdb-conn (.build (.createConnectionBuilder node))]

    (jdbc/execute! xtdb-conn ["ATTACH DATABASE test_db"])

    (with-open [test-conn (.build (-> (.createConnectionBuilder node)
                                      (.database "test_db")))]
      (jdbc/execute! test-conn ["INSERT INTO foo RECORDS {_id: 'test'}"])
      (t/is (= {:_id "test"} (jdbc/execute-one! test-conn ["SELECT * FROM foo"]))))

    (jdbc/execute! xtdb-conn ["DETACH DATABASE test_db"])

    (t/is (= {:sql-state "3D000", :message "database 'test_db' does not exist"}
             (pgw-test/reading-ex
              (with-open [test-conn (.build (-> (.createConnectionBuilder node)
                                                (.database "test_db")))]
                (jdbc/execute! test-conn ["SELECT 1"])))))))

(t/deftest disallow-detach-db-in-tx
  (with-open [node (xtn/start-node)
              conn (jdbc/get-connection node)]
    (jdbc/execute! conn ["ATTACH DATABASE test_db"])
    (jdbc/execute! conn ["BEGIN"])

    (try
      (t/is (= {:sql-state "08P01",
                :message "Cannot detach a database in a transaction.",
                :detail #xt/error [:incorrect :xtdb.pgwire/detach-db-in-tx
                                   "Cannot detach a database in a transaction."
                                   {:db-name "test_db"}]}
               (pgw-test/reading-ex
                (jdbc/execute! conn ["DETACH DATABASE test_db"]))))
      (finally
        (jdbc/execute! conn ["ROLLBACK"])))))

(t/deftest disallow-detach-primary-db
  (with-open [node (xtn/start-node)
              conn (jdbc/get-connection node)]
    (t/is (= {:sql-state "08P01",
              :message "Cannot detach the primary 'xtdb' database",
              :detail #xt/error [:incorrect :xtdb/cannot-detach-primary
                                 "Cannot detach the primary 'xtdb' database"
                                 {:db-name "xtdb"}]}
             (pgw-test/reading-ex
              (jdbc/execute! conn ["DETACH DATABASE xtdb"]))))))




