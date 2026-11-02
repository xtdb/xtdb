(ns xtdb.sql.correlated-subquery-nulls-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.test-util :as tu]))

(t/use-fixtures :each tu/with-mock-clock tu/with-node)

(t/deftest not-exists-ignores-inner-rows-its-where-filters-out-6313
  (xt/execute-tx tu/*node* ["INSERT INTO x RECORDS {_id: 1, y: 1}, {_id: 2, y: NULL}, {_id: 3, y: 3}"
                            "INSERT INTO z RECORDS {_id: 1, k: 3}, {_id: 2, k: NULL}"])

  (doseq [sql ["SELECT x._id FROM x WHERE NOT EXISTS (SELECT 1 FROM z WHERE z.k = x.y) ORDER BY x._id"
               "SELECT x._id FROM x WHERE NOT EXISTS (SELECT z.k FROM z WHERE z.k = x.y) ORDER BY x._id"
               "SELECT x._id FROM x WHERE NOT EXISTS (SELECT 1 FROM z WHERE z.k = x.y AND z.k IS NOT NULL) ORDER BY x._id"]]
    (t/is (= [{:xt/id 1} {:xt/id 2}] (xt/q tu/*node* sql)) sql)))

(t/deftest exists-in-a-projection-is-false-when-its-where-filters-out-every-row-6313
  (xt/execute-tx tu/*node* ["INSERT INTO x RECORDS {_id: 1, v: 5, q: 1}"
                            "INSERT INTO t RECORDS {_id: 1, z: 5, p: NULL}, {_id: 2, z: 6, p: 2}"])

  (t/is (= [{:e false}]
           (xt/q tu/*node* "SELECT EXISTS (SELECT 1 FROM t WHERE t.p = x.q) AS e FROM x"))))

(t/deftest correlated-in-is-false-when-its-where-filters-out-every-row-6313
  (xt/execute-tx tu/*node* ["INSERT INTO x RECORDS {_id: 1, v: 5, q: 1}"
                            "INSERT INTO t RECORDS {_id: 1, z: NULL}, {_id: 2, z: 6}"])

  (t/is (= [{:m false}]
           (xt/q tu/*node* "SELECT x.v IN (SELECT t.z FROM t WHERE t.z = x.q) AS m FROM x")))

  (t/is (= [{:xt/id 1}]
           (xt/q tu/*node* "SELECT x._id FROM x WHERE x.v NOT IN (SELECT t.z FROM t WHERE t.z = x.q)"))))

(t/deftest not-in-stays-unknown-against-a-null-the-where-keeps
  (xt/execute-tx tu/*node* ["INSERT INTO x RECORDS {_id: 1, v: 5, q: 1}"
                            "INSERT INTO t RECORDS {_id: 1, z: NULL, p: 1}, {_id: 2, z: 6, p: 1}"])

  (t/is (= [] (xt/q tu/*node* "SELECT x._id FROM x WHERE x.v NOT IN (SELECT t.z FROM t WHERE t.p = x.q)")))
  (t/is (= [] (xt/q tu/*node* "SELECT x._id FROM x WHERE x.v NOT IN (SELECT t.z FROM t)")))

  (t/is (= [{}]
           (xt/q tu/*node* "SELECT x.v IN (SELECT t.z FROM t WHERE t.p = x.q) AS m FROM x"))
        "a NULL mark comes back as an absent key"))
