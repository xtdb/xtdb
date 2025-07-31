(ns xtdb.sql.multi-db-test
  (:require [clojure.test :as t]
            [next.jdbc :as jdbc]
            [xtdb.pgwire :as pgw])
  (:import (org.postgresql.util PSQLException)))

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
