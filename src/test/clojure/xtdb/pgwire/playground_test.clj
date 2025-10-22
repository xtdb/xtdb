(ns xtdb.pgwire.playground-test
  (:require [clojure.test :as t]
            [next.jdbc :as jdbc]
            [xtdb.api :as xt]
            [xtdb.pgwire-test :as pgw-test]
            [xtdb.util :as util]))

(t/use-fixtures :each pgw-test/with-playground)

(t/deftest test-multi-db
  (letfn [(latest-completed-txs []
            (-> (:latest-completed-txs (xt/status (:node pgw-test/*server*)))
                (update-vals #(mapv :tx-id %))))
          (show-latest-completed-txs [conn]
            (->> (jdbc/execute! conn ["SHOW LATEST_COMPLETED_TXS"])
                 (sort-by (juxt :db_name :part))
                 (mapv #(select-keys % [:db_name :tx_id :part]))))]
    (util/with-open [xt-db-conn (pgw-test/jdbc-conn)]
      (jdbc/execute! xt-db-conn ["INSERT INTO foo RECORDS {_id: 'xtdb'}"])
      (t/is (= {:_id "xtdb"} (jdbc/execute-one! xt-db-conn ["SELECT * FROM foo"])))

      (t/is (= {"xtdb" [0]} (latest-completed-txs)))
      (t/is (= [{:db_name "xtdb", :tx_id 0, :part 0}]
               (show-latest-completed-txs xt-db-conn)))

      (with-open [new-db-conn (pgw-test/jdbc-conn {:dbname "new-db"})]
        (t/is (empty? (jdbc/execute! new-db-conn ["SELECT * FROM foo"])))

        (jdbc/execute! new-db-conn ["INSERT INTO foo RECORDS {_id: 'new-db'}"])

        (t/is (= {:_id "new-db"} (jdbc/execute-one! new-db-conn ["SELECT * FROM foo"])))
        (t/is (= [{:xt/id "new-db"}] (xt/q new-db-conn ["SELECT * FROM foo"])))

        (t/is (= [{:db_name "new-db", :part 0, :tx_id 0}, {:db_name "xtdb", :part 0, :tx_id 0}]
                 (show-latest-completed-txs new-db-conn)))

        (t/is (= [{:db_name "new-db", :part 0, :tx_id 0}, {:db_name "xtdb", :part 0, :tx_id 0}]
                 (show-latest-completed-txs xt-db-conn)))

        (t/is (= {"xtdb" [0], "new-db" [0]}
                 (latest-completed-txs)))

        (t/is (= {:_id "xtdb"} (jdbc/execute-one! xt-db-conn ["SELECT * FROM foo"])))))))

(t/deftest ingestion-stopped-assert-4837
  (util/with-open [conn (pgw-test/jdbc-conn {:dbname "osm"})]
    (jdbc/execute! conn ["BEGIN READ WRITE"])
    (jdbc/execute! conn ["ASSERT EXISTS (SELECT 1 FROM system WHERE _id=?), 'not_found'" "id"])
    (t/is (anomalous? [:conflict :xtdb/assert-failed "not_found"]
                      (throw (:detail (pgw-test/reading-ex
                                        (jdbc/execute! conn ["COMMIT"]))))))))

(t/deftest update-filter-repro-4894
  (with-open [conn (pgw-test/jdbc-conn {:dbname "foo"})]
    (xt/execute-tx conn [["INSERT INTO docs RECORDS ?" {:xt/id "doc-1"}]])
    (t/is (= [{:xt/id "doc-1"}] (xt/q conn ["FROM docs SELECT *"])))

    (t/testing "should update - foo is null"
      (xt/execute-tx conn [["UPDATE docs SET foo = 'bar' WHERE _id = ? AND foo IS NULL" "doc-1"]])
      (t/is (= [{:xt/id "doc-1" :foo "bar"}] (xt/q conn ["FROM docs SELECT *"])))
      (t/is (= [{:doc-count 2}] (xt/q conn ["SELECT COUNT(*) as doc_count FROM docs FOR VALID_TIME ALL WHERE _id = ?" "doc-1"]))))

    (t/testing "should not update -  foo is not null"
      (xt/execute-tx conn [["UPDATE docs SET foo = 'baz' WHERE _id = ? AND foo IS NULL" "doc-1"]])
      (t/is (= [{:xt/id "doc-1" :foo "bar"}] (xt/q conn ["FROM docs SELECT *"])))
      (t/is (= [{:doc-count 2}] (xt/q conn ["SELECT COUNT(*) as doc_count FROM docs FOR VALID_TIME ALL WHERE _id = ?" "doc-1"]))))))
