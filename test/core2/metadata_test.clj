(ns core2.metadata-test
  (:require [clojure.test :as t]
            [core2.api :as c2]
            [core2.metadata :as meta]
            [core2.test-util :as tu]
            [core2.datasets.tpch :as tpch]))

(t/use-fixtures :each tu/with-node)

(t/deftest test-row-id->cols
  (-> (tpch/submit-docs! tu/*node* 0.001)
      (tu/then-await-tx tu/*node*))

  (tu/finish-chunk! tu/*node*)

  (let [metadata-mgr (tu/component ::meta/metadata-manager)]
    (letfn [(row-id->col-names [table row-id]
              (let [{:keys [cols]} (meta/row-id->cols metadata-mgr table row-id)]
                {:col-names (into #{} (map (comp keyword :col-name)) cols)
                 :block-idxs (into #{} (map :block-idx) cols)}))]
      (t/is (= {:col-names #{:id
                             :c_acctbal :c_address :c_comment :c_custkey :c_mktsegment
                             :c_name :c_nationkey :c_phone}
                :block-idxs #{0}}
               (row-id->col-names "customer" 0)))

      (t/is (= {:col-names #{:id
                             :o_clerk :o_comment :o_custkey :o_orderdate :o_orderkey
                             :o_orderpriority :o_orderstatus :o_shippriority :o_totalprice}
                :block-idxs #{0}}
               (row-id->col-names "orders" 500)))

      (let [li-cols #{:id
                      :l_comment :l_commitdate :l_discount :l_extendedprice
                      :l_linenumber :l_linestatus :l_orderkey :l_partkey
                      :l_quantity :l_receiptdate :l_returnflag :l_shipdate
                      :l_shipinstruct :l_shipmode :l_suppkey :l_tax}]
        (t/is (= {:col-names li-cols, :block-idxs #{1}}
                 (row-id->col-names "lineitem" 1750)))

        (t/is (= {:col-names li-cols, :block-idxs #{2}}
                 (row-id->col-names "lineitem" 2000)))))))

(t/deftest test-param-metadata-error-310
  (let [!tx1 (c2/submit-tx tu/*node*
                           [[:sql "INSERT INTO users (id, name, application_time_start) VALUES (?, ?, ?)"
                             [["dave", "Dave", #inst "2018"]
                              ["claire", "Claire", #inst "2019"]]]])]

    (t/is (= [{:name "Dave"}]
             (c2/sql-query tu/*node* "SELECT users.name FROM users WHERE users.id = ?"
                           {:basis {:tx !tx1}
                            :? ["dave"]}))
          "#310")))
