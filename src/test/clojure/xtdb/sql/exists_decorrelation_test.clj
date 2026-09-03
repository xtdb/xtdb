(ns xtdb.sql.exists-decorrelation-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.serde]
            [xtdb.sql :as sql]
            [xtdb.test-util :as tu]
            [xtdb.types]))

(t/use-fixtures :each tu/with-mock-clock tu/with-node)

(defn- operators [plan]
  (->> plan (tree-seq vector? seq) (filter keyword?) set))

(defn- plan-ops
  ([sql] (plan-ops sql {#xt/table x #{"y" "z"} #xt/table y #{"z"}}))
  ([sql table-info] (operators (sql/plan sql {:table-info table-info}))))

(t/deftest correlated-exists-over-a-constant-projection-reaches-semi-join-6012
  (doseq [[sql join-op]
          [["SELECT x.y FROM x WHERE EXISTS (SELECT 1 FROM y WHERE y.z = x.y)" :semi-join]
           ["SELECT x.y FROM x WHERE NOT EXISTS (SELECT 1 FROM y WHERE y.z = x.y)" :anti-join]
           ["SELECT x.y FROM x WHERE EXISTS (SELECT 1 FROM y WHERE y.z = x.y AND y.z > 2)" :semi-join]]]
    (let [ops (plan-ops sql)]
      (t/is (contains? ops join-op) sql)
      (t/is (not (contains? ops :apply)) sql))))

(t/deftest every-reported-exists-shape-decorrelates-6012
  (let [table-info {#xt/table customers #{"_id" "ref"}
                    #xt/table orders #{"_id" "customer_id" "cust_ref" "amount"}}]
    (doseq [[what sql join-op]
            [["equality correlation on the primary key"
              "SELECT count(*) FROM customers c WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c._id AND o.amount > 9500)"
              :semi-join]

             ["equality correlation on a non-key column"
              "SELECT count(*) FROM customers c WHERE EXISTS (SELECT 1 FROM orders o WHERE o.cust_ref = c.ref AND o.amount > 9500)"
              :semi-join]

             ["NOT EXISTS, equality correlation on the primary key"
              "SELECT count(*) FROM customers c WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c._id AND o.amount > 9500)"
              :anti-join]]]
      (let [ops (plan-ops sql table-info)]
        (t/is (contains? ops join-op) what)
        (t/is (not (contains? ops :apply)) what)))))

(t/deftest correlated-exists-over-a-column-projection-still-reaches-semi-join
  (let [ops (plan-ops "SELECT x.y FROM x WHERE EXISTS (SELECT y.z FROM y WHERE y.z = x.y)")]
    (t/is (contains? ops :semi-join))
    (t/is (not (contains? ops :apply)))))

(t/deftest decorrelated-exists-returns-the-rows-the-apply-plan-returned
  (xt/execute-tx tu/*node* ["INSERT INTO x RECORDS {_id: 1, y: 1}, {_id: 2, y: 2}, {_id: 3, y: 9}"
                            "INSERT INTO y RECORDS {_id: 1, z: 1}, {_id: 2, z: 2}, {_id: 3, z: 2}"])

  (t/is (= [{:y 1} {:y 2}]
           (xt/q tu/*node* "SELECT x.y FROM x WHERE EXISTS (SELECT 1 FROM y WHERE y.z = x.y) ORDER BY x.y"))
        "EXISTS matches once per outer row however many inner rows match")

  (t/is (= [{:y 9}]
           (xt/q tu/*node* "SELECT x.y FROM x WHERE NOT EXISTS (SELECT 1 FROM y WHERE y.z = x.y) ORDER BY x.y")))

  (t/is (= (xt/q tu/*node* "SELECT x.y FROM x WHERE x.y IN (SELECT y.z FROM y) ORDER BY x.y")
           (xt/q tu/*node* "SELECT x.y FROM x WHERE EXISTS (SELECT 1 FROM y WHERE y.z = x.y) ORDER BY x.y"))
        "the two spellings now compile to the same operator and must still agree"))

;; EXISTS as a projected value keeps its mark-join: a semi-join can't carry the null the
;; three-valued result needs
(t/deftest exists-in-a-projection-keeps-its-mark-join
  (let [ops (plan-ops "SELECT EXISTS (SELECT 1 FROM y WHERE y.z = x.y) AS e FROM x")]
    (t/is (not (contains? ops :semi-join)))))
