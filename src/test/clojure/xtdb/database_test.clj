(ns xtdb.database-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [next.jdbc :as jdbc]
            [xtdb.api :as xt]
            [xtdb.check-pbuf :as cpb]
            [xtdb.compactor :as c]
            [xtdb.node :as xtn]
            [xtdb.test-json :as tj]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import [java.nio.file Path]))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-multi-db
  (with-open [node (xtn/start-node {:databases {:xtdb {}, :new-db {}}})
              xtdb-conn (.build (.createConnectionBuilder node))
              new-db-conn (.build (-> (.createConnectionBuilder node)
                                      (.database "new_db")))]
    (jdbc/execute! xtdb-conn ["INSERT INTO foo RECORDS {_id: 'xtdb'}"])
    (t/is (= {:_id "xtdb"} (jdbc/execute-one! xtdb-conn ["SELECT * FROM foo"])))

    (jdbc/execute! new-db-conn ["INSERT INTO foo RECORDS {_id: 'new-db'}"])

    (t/is (= {:_id "new-db"} (jdbc/execute-one! new-db-conn ["SELECT * FROM foo"])))
    (t/is (= [{:xt/id "new-db"}] (xt/q new-db-conn ["SELECT * FROM foo"])))

    (jdbc/execute! xtdb-conn ["INSERT INTO foo RECORDS {_id: 'xtdb', version: 2}"])

    (t/is (= {:_id "xtdb", :version 2} (jdbc/execute-one! xtdb-conn ["SELECT * FROM foo"])))

    (t/is (= [{:xt/id "new-db"}] (xt/q new-db-conn ["SELECT * FROM foo"])))))

(t/deftest test-block-boundary
  (binding [c/*ignore-signal-block?* true]
    (util/with-tmp-dir* "node"
      (fn [^Path node-dir]
        (let [mock-clock (tu/->mock-clock)
              node-config {:databases {:xtdb {:log [:local {:path (.resolve node-dir "xt/log")
                                                            :instant-src mock-clock}]
                                              :storage [:local {:path (.resolve node-dir "xt/objects")}]},
                                       :new-db {:log [:local {:path (.resolve node-dir "new_db/log")
                                                              :instant-src mock-clock}]
                                                :storage [:local {:path (.resolve node-dir "new_db/objects")}]}}}]
          (util/with-open [node (xtn/start-node node-config)
                           xtdb-conn (-> (.createConnectionBuilder node) (.build))
                           new-db-conn (.build (-> (.createConnectionBuilder node)
                                                   (.database "new_db")))]
            (jdbc/execute! xtdb-conn ["INSERT INTO foo RECORDS {_id: 'xtdb'}"])

            (jdbc/execute! new-db-conn ["INSERT INTO foo RECORDS {_id: 'new-db'}"])
            (t/is (= {:_id "new-db"} (jdbc/execute-one! new-db-conn ["SELECT * FROM foo"])))

            (tu/flush-block! node))

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

            (jdbc/execute! new-db-conn ["INSERT INTO foo RECORDS {_id: 'new-db', version: 2}"])
            (t/is (= [{:_id "new-db", :version 2, :_valid_from #xt/zdt "2020-01-03Z[UTC]"}
                      {:_id "xtdb", :version nil, :_valid_from #xt/zdt "2020-01-01Z[UTC]"}]
                     (jdbc/execute! new-db-conn ["SELECT *, _valid_from FROM foo FOR ALL VALID_TIME"])))

            (t/is (= {:_id "xtdb"}
                     (jdbc/execute-one! xt-db-conn ["SELECT * FROM foo"])))))))))
