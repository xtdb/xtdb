(ns core2.metadata-test
  (:require [clojure.test :as t]
            [core2.test-util :as tu]
            [core2.tpch :as tpch]
            [core2.metadata :as meta]))

(t/use-fixtures :each tu/with-node)

(t/deftest test-row-id->cols
  (-> (tpch/submit-docs! tu/*node* 0.001)
      (tu/then-await-tx tu/*node*))

  (tu/finish-chunk tu/*node*)

  (let [metadata-mgr (tu/component ::meta/metadata-manager)]
    (letfn [(row-id->col-names [row-id]
              (let [{:keys [cols]} (meta/row-id->cols metadata-mgr row-id)]
                {:col-names (into #{} (map (comp keyword :col-name)) cols)
                 :block-idxs (into #{} (map :block-idx) cols)}))]
      (t/is (= {:col-names #{:_table :id
                             :c_acctbal :c_address :c_comment :c_custkey :c_mktsegment
                             :c_name :c_nationkey :c_phone}
                :block-idxs #{0}}
               (row-id->col-names 0)))

      (t/is (= {:col-names #{:_table :id
                             :o_clerk :o_comment :o_custkey :o_orderdate :o_orderkey
                             :o_orderpriority :o_orderstatus :o_shippriority :o_totalprice}
                :block-idxs #{0}}
               (row-id->col-names 500)))

      (let [li-cols #{:_table :id
                      :l_comment :l_commitdate :l_discount :l_extendedprice
                      :l_linenumber :l_linestatus :l_orderkey :l_partkey
                      :l_quantity :l_receiptdate :l_returnflag :l_shipdate
                      :l_shipinstruct :l_shipmode :l_suppkey :l_tax}]
        (t/is (= {:col-names li-cols, :block-idxs #{1}}
                 (row-id->col-names 1750)))

        (t/is (= {:col-names li-cols, :block-idxs #{2}}
                 (row-id->col-names 2000)))))))
