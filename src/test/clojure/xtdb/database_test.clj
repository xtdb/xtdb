(ns xtdb.database-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [next.jdbc :as jdbc]
            [xtdb.api :as xt]
            [xtdb.check-pbuf :as cpb]
            [xtdb.compactor :as c]
            [xtdb.node :as xtn]
            [xtdb.pgwire-test :as pgw-test]
            [xtdb.test-json :as tj]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import [java.nio.file Path]
           xtdb.api.storage.Storage))

(t/use-fixtures :each tu/with-allocator tu/with-mock-clock)

(t/deftest test-multi-db
  (with-open [node (xtn/start-node)
              xtdb-conn (.build (.createConnectionBuilder node))]
    (jdbc/execute! xtdb-conn ["ATTACH DATABASE new_db"])

    (with-open [new-db-conn (.build (-> (.createConnectionBuilder node)
                                        (.database "new_db")))]
      (jdbc/execute! xtdb-conn ["INSERT INTO foo RECORDS {_id: 'xtdb'}"])
      (t/is (= {:_id "xtdb"} (jdbc/execute-one! xtdb-conn ["SELECT * FROM foo"])))

      (jdbc/execute! new-db-conn ["INSERT INTO foo RECORDS {_id: 'new-db'}"])

      (t/is (= {:_id "new-db"} (jdbc/execute-one! new-db-conn ["SELECT * FROM foo"])))
      (t/is (= [{:xt/id "new-db"}] (xt/q new-db-conn ["SELECT * FROM foo"])))

      (jdbc/execute! xtdb-conn ["INSERT INTO foo RECORDS {_id: 'xtdb', version: 2}"])

      (t/is (= {:_id "xtdb", :version 2} (jdbc/execute-one! xtdb-conn ["SELECT * FROM foo"])))

      (t/is (= [{:xt/id "new-db"}] (xt/q new-db-conn ["SELECT * FROM foo"]))))))

#_
(t/deftest test-block-boundary
  (binding [c/*ignore-signal-block?* true]
    (util/with-tmp-dir* "node"
      (fn [^Path node-dir]
        (let [mock-clock (tu/->mock-clock)
              node-config {:log [:local {:path (.resolve node-dir "xt/log")
                                         :instant-src mock-clock}]
                           :storage [:local {:path (.resolve node-dir "xt/objects")}]}]
          (util/with-open [node (xtn/start-node node-config)
                           xtdb-conn (-> (.createConnectionBuilder node) (.build))]
            (jdbc/execute! xtdb-conn [(format "ATTACH DATABASE new_db WITH $$
              log: !Local
                path: '%s/new_db/log'
              storage: !Local
                path: '%s/new_db/objects'
              $$" node-dir node-dir)])

            (with-open [new-db-conn (.build (-> (.createConnectionBuilder node)
                                                (.database "new_db")))]
              (jdbc/execute! xtdb-conn ["INSERT INTO foo RECORDS {_id: 'xtdb', _valid_from: TIMESTAMP '2020-01-01Z[UTC]'}"])

              (jdbc/execute! new-db-conn ["INSERT INTO foo RECORDS {_id: 'new-db', _valid_from: TIMESTAMP '2020-01-02Z[UTC]'}"])
              (t/is (= {:_id "new-db"} (jdbc/execute-one! new-db-conn ["SELECT * FROM foo"])))

              (tu/flush-block! node)))

          (tj/check-json (.toPath (io/as-file (io/resource "xtdb/database-test/block-boundary")))
                         node-dir)

          (cpb/check-pbuf (.toPath (io/as-file (io/resource "xtdb/database-test/block-boundary")))
                          node-dir)

          (util/with-open [node2 (xtn/start-node node-config)
                           xt-db-conn (.build (.createConnectionBuilder node2))
                           new-db-conn (-> (.createConnectionBuilder node2)
                                           (.database "new_db")
                                           (.build))]
            (t/is (= {:_id "xtdb"} (jdbc/execute-one! xt-db-conn ["SELECT * FROM foo"])))

            (jdbc/execute! new-db-conn ["INSERT INTO foo RECORDS {_id: 'new-db', version: 2, _valid_from: TIMESTAMP '2020-01-03Z[UTC]'}"])
            (t/is (= [{:_id "new-db", :version 2, :_valid_from #xt/zdt "2020-01-03Z[UTC]"}
                      {:_id "new-db", :version nil, :_valid_from #xt/zdt "2020-01-02Z[UTC]"}]
                     (jdbc/execute! new-db-conn ["SELECT *, _valid_from FROM foo FOR ALL VALID_TIME"])))

            (t/is (= {:_id "xtdb"}
                     (jdbc/execute-one! xt-db-conn ["SELECT * FROM foo"])))

            (jdbc/execute! xt-db-conn ["DETACH DATABASE new_db"])
            (tu/flush-block! node2)

            (cpb/check-pbuf (.toPath (io/as-file (io/resource "xtdb/database-test/block-boundary-post-detach")))
                            (.resolve node-dir (format "xt/objects/%s/blocks" Storage/STORAGE_ROOT))
                            {:update-fn #(dissoc % :latest-completed-tx)})))))))

(t/deftest test-multi-db-through-xt-api-4652
  (with-open [node (xtn/start-node)]
    (jdbc/execute! node ["ATTACH DATABASE new_db"])
    (xt/submit-tx node [[:put-docs :foo {:xt/id "xtdb"}]])

    (xt/submit-tx node [[:put-docs :foo {:xt/id "new-db"}]]
                  {:database :new-db})

    (t/is (= [{:xt/id "new-db"}]
             (xt/q node ["SELECT * FROM foo"]
                   {:database :new-db})))

    (t/is (= [{:xt/id "xtdb"}]
             (xt/q node ["SELECT * FROM xtdb.foo"]
                   {:database :new-db})))

    (t/is (= [{:xt/id "new-db"}]
             (xt/q node ["SELECT * FROM new_db.foo"]
                   {:database :xtdb})))

    (t/testing "errors"
      (t/is (= {:sql-state "3D000",
                :message "database 'non_existent_db' does not exist"}
               (pgw-test/reading-ex
                 (xt/submit-tx node [[:put-docs :foo {:xt/id "non-existent"}]]
                               {:database :non-existent-db}))))

      (t/is (= {:sql-state "3D000",
                :message "database 'non_existent_db' does not exist"}
               (pgw-test/reading-ex
                 (xt/q node ["SELECT * FROM foo"]
                       {:database :non-existent-db}))))

      (t/testing "throws if database already selected (via connection)")
      (with-open [xtdb-conn (.build (.createConnectionBuilder node))]
        (t/is (anomalous? [:incorrect :cannot-set-db
                           "Can't set :default-db when connectable is not an XT node"]
                          (xt/submit-tx xtdb-conn [[:put-docs :foo {:xt/id "xtdb"}]]
                                        {:database :new_db})))

        (t/is (anomalous? [:incorrect :cannot-set-db
                           "Can't set :default-db when connectable is not an XT node"]
                          (xt/q xtdb-conn ["SELECT * FROM foo"]
                                {:database :new_db})))))))
