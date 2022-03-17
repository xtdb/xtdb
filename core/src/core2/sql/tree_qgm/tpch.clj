(ns core2.sql.tree-qgm.tpch)

(def q1
  {:tree '[:qgm.box/select {:qgm.box.head/columns [l_returnflag l_linestatus
                                                   sum_qty sum_base_price sum_disc_price sum_charge
                                                   avg_qty avg_price avg_disc count_order]
                            :qgm.box.body/columns [[:column q0 l_returnflag]
                                                   [:column q0 l_linestatus]
                                                   [:column q0 sum_qty]
                                                   [:column q0 sum_base_price]
                                                   [:column q0 sum_disc_price]
                                                   [:column q0 sum_charge]
                                                   [:column q0 avg_qty]
                                                   [:column q0 avg_price]
                                                   [:column q0 avg_disc]
                                                   [:column q0 count_order]]
                            :qgm.box.select/ordering [[[:column q0 l_returnflag] :asc]
                                                      [[:column q0 l_linestatus] :asc]]}
           [:qgm.quantifier.type/foreach
            {:qgm.quantifier/id q0
             :qgm.quantifier/columns [l_returnflag l_linestatus
                                      sum_qty sum_base_price sum_disc_price sum_charge
                                      avg_qty avg_price avg_disc count_order]}
            [:qgm.box/grouping {:qgm.box.head/columns [l_returnflag l_linestatus
                                                       sum_qty sum_base_price sum_disc_price sum_charge
                                                       avg_qty avg_price avg_disc count_order]
                                :qgm.box.body/columns [[:column q1 l_returnflag]
                                                       [:column q1 l_linestatus]
                                                       [:agg-call sum [:column q1 l_quantity]]
                                                       [:agg-call sum [:column q1 l_extendedprice]]
                                                       [:agg-call sum [:call *
                                                                       [:column q1 l_extendedprice]
                                                                       [:call -
                                                                        [:literal 1]
                                                                        [:column q1 l_discount]]]]
                                                       [:agg-call sum [:call *
                                                                       [:column q1 l_extendedprice]
                                                                       [:call -
                                                                        [:literal 1]
                                                                        [:column q1 l_discount]]
                                                                       [:call +
                                                                        [:literal 1]
                                                                        [:column q1 l_tax]]]]
                                                       [:agg-call avg [:column q1 l_quantity]]
                                                       [:agg-call avg [:column q1 l_extendedprice]]
                                                       [:agg-call avg [:column q1 l_discount]]
                                                       [:agg-call :count*]]
                                :qgm.box.grouping/grouping-by [[:column q1 l_returnflag]
                                                               [:column q1 l_linestatus]]}
             [:qgm.quantifier/foreach {:qgm.quantifier/id q1
                                       :qgm.quantifier/columns [l_returnflag l_linestatus
                                                                l_quantity l_extendedprice
                                                                l_discount l_tax]}
              [:qgm.box/select {:qgm.box.head/columns [l_returnflag l_linestatus l_shipdate
                                                       l_quantity l_extendedprice
                                                       l_discount l_tax]
                                :qgm.box.body/columns [[:column q2 l_returnflag]
                                                       [:column q2 l_linestatus]
                                                       [:column q2 l_shipdate]
                                                       [:column q2 l_quantity]
                                                       [:column q2 l_extendedprice]
                                                       [:column q2 l_discount]
                                                       [:column q2 l_tax]]}
               [:qgm.quantifier/foreach {:qgm.quantifier/id q2
                                         :qgm.quantifier/columns [l_returnflag l_linestatus l_shipdate
                                                                  l_quantity l_extendedprice
                                                                  l_discount l_tax]}
                [:qgm.box/base-table {:qgm.box.base-table/name lineitem
                                      :qgm.box.head/columns [l_returnflag l_linestatus l_shipdate
                                                             l_quantity l_extendedprice
                                                             l_discount l_tax]}]]]]]]]

   :preds '{p0 [:call <=
                [:column q2 l_shipdate]
                ;; this is a
                [:call -
                 [:literal #inst "1998-12-01"]
                 [:param ?delta]]]}})


(def q2
  '{:tree [:qgm.box/select {:qgm.box.head/columns [s_acctbal s_name
                                                   n_name p_partkey p_mfgr
                                                   s_address s_phone s_comment]
                            :qgm.box.body/columns [[:column q1 s_acctbal]
                                                   [:column q1 s_name]
                                                   [:column q3 n_name]
                                                   [:column q0 p_partkey]
                                                   [:column q0 p_mfgr]
                                                   [:column q1 s_address]
                                                   [:column q1 s_phone]
                                                   [:column q1 s_comment]]
                            :qgm.box.select/ordering [[[:column q1 s_acctbal] :desc]
                                                      [[:column q3 n_name] :asc]
                                                      [[:column q1 s_name] :asc]
                                                      [[:column q0 p_partkey] :asc]]}

           [:qgm.quantifier/foreach {:qgm.quantifier/id q0
                                     :qgm.quantifier/columns [p_partkey p_mfgr p_size p_type]}
            [:qgm.box/base-table {:qgm.box.base-table/name part
                                  :qgm.box.head/columns [p_partkey p_mfgr p_size p_type]}]]
           [:qgm.quantifier/foreach {:qgm.quantifier/id q1
                                     :qgm.quantifier/columns [s_acctbal s_name s_address s_phone s_comment s_suppkey s_nationkey]}
            [:qgm.box/base-table {:qgm.box.base-table/name supplier
                                  :qgm.box.head/columns [s_acctbal s_name s_address s_phone s_comment s_suppkey s_nationkey]}]]
           [:qgm.quantifier/foreach {:qgm.quantifier/id q2
                                     :qgm.quantifier/columns [ps_partkey ps_suppkey ps_supplycost]}
            [:qgm.box/base-table {:qgm.box.base-table/name partsupp
                                  :qgm.box.head/columns [ps_partkey ps_suppkey ps_supplycost]}]]
           [:qgm.quantifier/foreach {:qgm.quantifier/id q3
                                     :qgm.quantifier/columns [n_name n_nationkey n_regionkey]}
            [:qgm.box/base-table {:qgm.box.base-table/name nation
                                  :qgm.box.head/columns [n_name n_nationkey n_regionkey]}]]
           [:qgm.quantifier/foreach {:qgm.quantifier/id q4
                                     :qgm.quantifier/columns [r_regionkey r_name]}
            [:qgm.box/base-table {:qgm.box.base-table/name region
                                  :qgm.box.head/columns [r_regionkey r_name]}]]

           [:qgm.quantifier/scalar {:qgm.quantifier/id q5
                                    :qgm.quantifier/columns [$column_1$]}
            [:qgm.box/select {:qgm.box.head/columns [$column_1$]
                              :qgm.box.body/columns [[:column q6 $column_1$]]}
             [:qgm.quantifier/foreach {:qgm.quantifier/id q6
                                       :qgm.quantifier/columns [$column_1$]}
              [:qgm.box/grouping {:qgm.box.head/columns [$column_1$]
                                  :qgm.box.body/columns [[:agg-call min [:column q7 ps_supplycost]]]
                                  :qgm.box.grouping/grouping-by []}
               [:qgm.box/foreach {:qgm.quantifier/id q7
                                  :qgm.quantifier/columns [ps_supplycost]}
                [:qgm.box/select {:qgm.box.head/columns [ps_supplycost]
                                  :qgm.box.body/columns [[:column q8 ps_supplycost]]}

                 [:qgm.quantifier/foreach {:qgm.quantifier/id q8
                                           :qgm.quantifier/columns [ps_partkey ps_suppkey ps_supplycost]}
                  [:qgm.box/base-table {:qgm.box.base-table/name partsupp
                                        :qgm.box.head/columns [ps_partkey ps_suppkey ps_supplycost]}]]
                 [:qgm.quantifier/foreach {:qgm.quantifier/id q9
                                           :qgm.quantifier/columns [s_suppkey s_nationkey]}
                  [:qgm.box/base-table {:qgm.box.base-table/name supplier
                                        :qgm.box.head/columns [s_suppkey s_nationkey]}]]
                 [:qgm.quantifier/foreach {:qgm.quantifier/id q10
                                           :qgm.quantifier/columns [n_nationkey n_regionkey]}
                  [:qgm.box/base-table {:qgm.box.base-table/name nation
                                        :qgm.box.head/columns [n_nationkey n_regionkey]}]]
                 [:qgm.quantifier/foreach {:qgm.quantifier/id q11
                                           :qgm.quantifier/columns [r_regionkey r_name]}
                  [:qgm.box/base-table {:qgm.box.base-table/name region
                                        :qgm.box.head/columns [r_regionkey r_name]}]]]]]]]]]

    :preds {p0 [:call = [:column q0 p_partkey] [:column q2 ps_partkey]]
            p1 [:call = [:column q1 s_suppkey] [:column q2 ps_suppkey]]
            p2 [:call = [:column q0 p_size] [:param ?size]]
            p3 [:call like [:column q0 p_type] [:call str [:literal "%"] [:param ?type]]]
            p4 [:call = [:column q1 s_nationkey] [:column q3 n_nationkey]]
            p5 [:call = [:column q3 n_regionkey] [:column q4 r_regionkey]]
            p6 [:call = [:column q4 r_name] [:param ?region]]
            p7 [:call = [:column q2 ps_supplycost] [:column q5 $column_1$]]

            p8 [:call = [:column q0 p_partkey] [:column q8 ps_partkey]]
            p9 [:call = [:column q9 s_suppkey] [:column q8 ps_suppkey]]
            p10 [:call = [:column q9 s_nationkey] [:column q10 n_nationkey]]
            p11 [:call = [:column q11 n_regionkey] [:column q11 r_regionkey]]
            p12 [:call = [:column q11 r_name] [:param ?region]]}})

(def q21
  '{:tree [:qgm.box/select {:qgm.box.head/columns [s_name numwait]
                            :qgm.box.body/columns [[:column q0 s_name], [:column q0 numwait]]
                            :qgm.box.select/ordering [[[:column q0 numwait] :desc]
                                                      [[:column q0 s_name] :asc]]}

           [:qgm.quantifier.type/foreach {:qgm.quantifier/id q0
                                          :qgm.quantifier/columns [s_name numwait]}

            [:qgm.box/grouping {:qgm.box.head/columns [s_name numwait]
                                :qgm.box.body/columns [[:column q1 s_name]
                                                       [:agg-call :count*]]
                                :qgm.box.grouping/grouping-by [[:column q1 s_name]]}

             [:qgm.quantifier/foreach {:qgm.quantifier/id q1
                                       :qgm.quantifier/columns [s_name]}

              [:qgm.box/select {:qgm.box.head/columns [s_name]
                                :qgm.box.body/columns [[:column q2 s_name]]}

               [:qgm.quantifier/foreach {:qgm.quantifier/id q2
                                         :qgm.quantifier/columns [s_name s_suppkey s_nationkey s_name]}
                [:qgm.box/base-table {:qgm.box.base-table/name supplier
                                      :qgm.box.head/columns [s_name s_suppkey s_nationkey s_name]}]]

               [:qgm.quantifier/foreach {:qgm.quantifier/id q3
                                         :qgm.quantifier/columns [l_suppkey l_orderkey l_receiptdate l_commitdate]}
                [:qgm.box/base-table {:qgm.box.base-table/name lineitem
                                      :qgm.box.head/columns [l_suppkey l_orderkey l_receiptdate l_commitdate]}]]

               [:qgm.quantifier/foreach {:qgm.quantifier/id q4
                                         :qgm.quantifier/columns [o_orderkey o_orderstatus]}
                [:qgm.box/base-table {:qgm.box.base-table/name orders
                                      :qgm.box.head/columns [o_orderkey o_orderstatus]}]]

               [:qgm.quantifier/foreach {:qgm.quantifier/id q5
                                         :qgm.quantifier/columns [n_name n_nationkey]}
                [:qgm.box/base-table {:qgm.box.base-table/name nation
                                      :qgm.box.head/columns [n_name n_nationkey]}]]

               [:qgm.quantifier/existential {:qgm.quantifier/id q6
                                             :qgm.quantifier/columns [$column_1$]}
                [:qgm.box/select {:qgm.box.head/columns [$column_1$]
                                  :qgm.box.body/columns [[:literal true]]}
                 [:qgm.quantifier/foreach {:qgm.quantifier/id q7
                                           :qgm.quantifier/columns [l_orderkey l_suppkey]}
                  [:qgm.box/base-table {:qgm.box.base-table/name lineitem
                                        :qgm.box.head/columns [l_orderkey l_suppkey]}]]]]

               [:qgm.quantifier/all {:qgm.quantifier/id q8
                                     :qgm.quantifier/columns [$column_1$]}
                [:qgm.box/select {:qgm.box.head/columns [$column_1$]
                                  :qgm.box.body/columns [[:literal true]]}
                 [:qgm.quantifier/foreach {:qgm.quantifier/id q9
                                           :qgm.quantifier/columns [l_orderkey l_suppkey l_receiptdate l_commitdate]}
                  [:qgm.box/base-table {:qgm.box.base-table/name lineitem
                                        :qgm.box.head/columns [l_orderkey l_suppkey l_receiptdate l_commitdate]}]]]]]]]]]

    :preds {p0 [:call = [:column q2 s_suppkey] [:column q3 l_suppkey]]
            p1 [:call = [:column q4 o_orderkey] [:column q3 l_orderkey]]
            p2 [:call = [:column q4 o_orderstatus] [:literal "F"]]
            p3 [:call > [:column q3 l_receiptdate] [:column q3 l_commitdate]]
            p4 [:call = [:column q2 s_nationkey] [:column q5 n_nationkey]]
            p5 [:call = [:column q5 n_name] [:param ?nation]]

            p6 [:call = [:column q7 l_orderkey] [:column q3 l_orderkey]]
            p7 [:call <> [:column q7 l_suppkey] [:column q3 l_suppkey]]
            p8 [:call true? [:column q6 $column_1$]]

            p9 [:call = [:column q9 l_orderkey] [:column q3 l_orderkey]]
            p10 [:call <> [:column q9 l_suppkey] [:column q3 l_suppkey]]
            p11 [:call > [:column q9 l_receiptdate] [:column q9 l_commitdate]]
            p12 [:call false? [:column q8 $column_1$]]}})
