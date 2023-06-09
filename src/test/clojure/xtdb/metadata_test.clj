(ns xtdb.metadata-test
  (:require [clojure.test :as t :refer [deftest]]
            [xtdb.api :as xt]
            [xtdb.metadata :as meta]
            [xtdb.test-util :as tu]
            [xtdb.datasets.tpch :as tpch])
  (:import (java.time ZoneId)))

(t/use-fixtures :each tu/with-node)
(t/use-fixtures :once tu/with-allocator)

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

      #_#_
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
                          [[:sql-batch ["INSERT INTO users (xt$id, name, xt$valid_from) VALUES (?, ?, ?)"
                                        ["dave", "Dave", #inst "2018"]
                                        ["claire", "Claire", #inst "2019"]]]])]

    (t/is (= [{:name "Dave"}]
             (xt/q tu/*node* ["SELECT users.name FROM users WHERE users.xt$id = ?" "dave"]
                   {:basis {:tx tx1}}))
          "#310")))

(deftest test-bloom-filter-for-num-types-2133
  (let [tx (-> (xt/submit-tx tu/*node* [[:put :xt_docs {:num 0 :xt/id "a"}]
                                        [:put :xt_docs {:num 1 :xt/id "b"}]
                                        [:put :xt_docs {:num 1.0 :xt/id "c"}]
                                        [:put :xt_docs {:num 4 :xt/id "d"}]
                                        [:put :xt_docs {:num (short 3) :xt/id "e"}]
                                        [:put :xt_docs {:num 2.0 :xt/id "f"}]])
               (tu/then-await-tx* tu/*node*))]

    (tu/finish-chunk! tu/*node*)

    (t/is (= [{:num 1} {:num 1.0}]
             (tu/query-ra '[:scan {:table xt_docs}
                            [{num (= num 1)}]]
                          {:node tu/*node* :basis {:tx tx}})))

    (t/is (= [{:num 2.0}]
             (tu/query-ra '[:scan {:table xt_docs}
                            [{num (= num 2)}]]
                          {:node tu/*node* :basis {:tx tx}})))

    (t/is (= [{:num 4}]
             (tu/query-ra '[:scan {:table xt_docs}
                            [{num (= num ?x)}]]
                          {:node tu/*node* :basis {:tx tx} :params {'?x (byte 4)}})))

    (t/is (= [{:num 3}]
             (tu/query-ra '[:scan {:table xt_docs}
                            [{num (= num ?x)}]]
                          {:node tu/*node* :basis {:tx tx} :params {'?x (float 3)}})))))

(deftest test-bloom-filter-for-datetime-types-2133
  (let [tx (-> (xt/submit-tx tu/*node* [[:put :xt_docs {:timestamp #time/date "2010-01-01" :xt/id "a"}]
                                        [:put :xt_docs {:timestamp #time/zoned-date-time "2010-01-01T00:00:00Z" :xt/id "b"}]
                                        [:put :xt_docs {:timestamp #time/date-time "2010-01-01T00:00:00" :xt/id "c"}]
                                        [:put :xt_docs {:timestamp #time/date "2020-01-01" :xt/id "d"}]]
                             {:default-tz #time/zone "Z"})
               (tu/then-await-tx* tu/*node*))]

    (tu/finish-chunk! tu/*node*)

    (t/is (= [{:timestamp #time/date "2010-01-01"}
              {:timestamp #time/zoned-date-time "2010-01-01T00:00Z"}
              {:timestamp #time/date-time "2010-01-01T00:00:00"}]
             (tu/query-ra '[:scan {:table xt_docs}
                            [{timestamp (= timestamp #time/zoned-date-time "2010-01-01T00:00:00Z")}]]
                          {:node tu/*node* :basis {:tx tx} :default-tz #time/zone "Z"})))

    (t/is (= [{:timestamp #time/date "2010-01-01"}
              {:timestamp #time/zoned-date-time "2010-01-01T00:00Z"}
              {:timestamp #time/date-time "2010-01-01T00:00:00"}]
             (tu/query-ra '[:scan {:table xt_docs}
                            [{timestamp (= timestamp ?x)}]]
                          {:node tu/*node* :basis {:tx tx}
                           :default-tz  #time/zone "Z" :params {'?x #time/date "2010-01-01"}})))

    (t/is (= [{:timestamp #time/date "2010-01-01"}
              {:timestamp #time/zoned-date-time "2010-01-01T00:00Z"}
              {:timestamp #time/date-time "2010-01-01T00:00:00"}]
             (tu/query-ra '[:scan {:table xt_docs}
                            [{timestamp (= timestamp #time/date-time "2010-01-01T00:00:00")}]]
                          {:node tu/*node* :basis {:tx tx} :default-tz #time/zone "Z"})))))

(deftest test-bloom-filter-for-time-types
  (let [tx (-> (xt/submit-tx tu/*node* [[:put :xt_docs {:time #time/time "01:02:03" :xt/id "a"}]
                                        [:put :xt_docs {:time #time/time "04:05:06" :xt/id "b"}]]
                             {:default-tz #time/zone "Z"})
               (tu/then-await-tx* tu/*node*))]

    (tu/finish-chunk! tu/*node*)

    (t/is (= [{:time #time/time "04:05:06"}]
             (tu/query-ra '[:scan {:table xt_docs}
                            [{time (= time #time/time "04:05:06")}]]
                          {:node tu/*node* :basis {:tx tx} :default-tz #time/zone "Z"})))))
