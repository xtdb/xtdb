(ns xtdb.metadata-test
  (:require [clojure.test :as t]
            [xtdb.sql :as xt]
            [xtdb.metadata :as meta]
            [xtdb.test-util :as tu]
            [xtdb.datasets.tpch :as tpch]))

(t/use-fixtures :each tu/with-node)

(t/deftest test-row-id->chunk
  (-> (tpch/submit-docs! tu/*node* 0.001)
      (tu/then-await-tx* tu/*node*))

  (tu/finish-chunk! tu/*node*)

  (let [metadata-mgr (tu/component ::meta/metadata-manager)]
    (letfn [(row-id->col-names [table row-id]
              (-> (meta/row-id->chunk metadata-mgr table row-id)
                  (dissoc :chunk-idx)
                  (update :col-names #(into #{} (map keyword) %))))]
      (t/is (= {:col-names #{:xt$id
                             :c_acctbal :c_address :c_comment :c_custkey :c_mktsegment
                             :c_name :c_nationkey :c_phone}
                :block-idx 0}
               (row-id->col-names "customer" 1)))

      (t/is (= {:col-names #{:xt$id
                             :o_clerk :o_comment :o_custkey :o_orderdate :o_orderkey
                             :o_orderpriority :o_orderstatus :o_shippriority :o_totalprice}
                :block-idx 0}
               (row-id->col-names "orders" 500)))

      (let [li-cols #{:xt$id
                      :l_comment :l_commitdate :l_discount :l_extendedprice
                      :l_linenumber :l_linestatus :l_orderkey :l_partkey
                      :l_quantity :l_receiptdate :l_returnflag :l_shipdate
                      :l_shipinstruct :l_shipmode :l_suppkey :l_tax}]
        (t/is (= {:col-names li-cols, :block-idx 1}
                 (row-id->col-names "lineitem" 1750)))

        (t/is (= {:col-names li-cols, :block-idx 2}
                 (row-id->col-names "lineitem" 2750)))))))

(t/deftest test-param-metadata-error-310
  (let [tx1 (xt/submit-tx tu/*node*
                           [[:sql "INSERT INTO users (xt$id, name, xt$valid_from) VALUES (?, ?, ?)"
                             [["dave", "Dave", #inst "2018"]
                              ["claire", "Claire", #inst "2019"]]]])]

    (t/is (= [{:name "Dave"}]
             (xt/q tu/*node* "SELECT users.name FROM users WHERE users.xt$id = ?"
                   {:basis {:tx tx1}
                    :? ["dave"]}))
          "#310")))
