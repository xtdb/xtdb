(ns xtdb.datasets.tpch.datalog)

(defn- with-in-args [q in-args]
  (-> q (vary-meta assoc ::in-args in-args)))

;; TODO reintro nested agg exprs
(def q1
  '{:find [l_returnflag
           l_linestatus
           (sum l_quantity)
           (sum l_extendedprice)
           (sum (* l_extendedprice (- 1 l_discount)))
           (sum (* (* l_extendedprice (- 1 l_discount)) (+ 1 l_tax)))
           (avg l_quantity)
           (avg l_extendedprice)
           (avg l_discount)
           (count l)]
    :keys [l_returnflag l_linestatus sum_qty sum_base_price sum_disc_price
           sum_charge avg_qty avg_price avg_disc count_order]
    :where [(match :lineitem [{:id l} l_shipdate l_quantity
                              l_extendedprice l_discount l_tax
                              l_returnflag l_linestatus])
            [(<= l_shipdate #time/date "1998-09-02")]]
    :order-by [[l_returnflag :asc] [l_linestatus :asc]]})

(def q2
  '{:find [s_acctbal s_name n_name p p_mfgr s_address s_phone s_comment]
    :keys [s_acctbal s_name n_name p_partkey p_mfgr s_address s_phone s_comment]

    :where [(match :part [{:id p} p_mfgr {:p_size 15} p_type])
            [(like p_type "%BRASS")]

            (match :partsupp [{:ps_partkey p, :ps_suppkey s} ps_supplycost])
            (match :supplier [{:id s, :s_nationkey n}
                              s_acctbal s_address s_name s_phone s_comment])
            (match :nation [{:id n, :n_regionkey r} n_name])
            (match :region {:id r, :r_name "EUROPE"})

            (q {:find [(min ps_supplycost)]
                :keys [ps_supplycost]
                :in [p]
                :where [(match :partsupp [{:ps_partkey p, :ps_suppkey s}
                                          ps_supplycost])
                        (match :supplier {:id s, :s_nationkey n})
                        (match :nation {:id n, :n_regionkey r})
                        (match :region {:id r, :r_name "EUROPE"})]})]

    :order-by [[s_acctbal :desc] [n_name :asc] [s_name :asc] [p :asc]]
    :limit 100})

(def q3
  (-> '{:find [o (sum revenue) o_orderdate o_shippriority]
        :keys [l_orderkey revenue o_orderdate o_shippriority]
        :in [?segment]

        :where [(match :customer {:id c, :c_mktsegment ?segment})

                (match :orders [{:id o, :o_custkey c} o_shippriority o_orderdate])
                [(< o_orderdate #time/date "1995-03-15")]

                (match :lineitem [{:l_orderkey o} l_discount l_extendedprice l_shipdate])
                [(> l_shipdate #time/date "1995-03-15")]
                [(* l_extendedprice (- 1 l_discount)) revenue]]

        :order-by [[(sum revenue) :desc] [o_orderdate :asc]]
        :limit 10}
      (with-in-args ["BUILDING"])))

(def q4
  '{:find [o_orderpriority (count o)]
    :keys [o_orderpriority order_count]
    :where [(match :orders [{:id o} o_orderdate o_orderpriority])
            [(>= o_orderdate #time/date "1993-07-01")]
            [(< o_orderdate #time/date "1993-10-01")]

            (exists? {:find [o]
                      :where [(match :lineitem [{:l_orderkey o} l_commitdate l_receiptdate])
                              [(< l_commitdate l_receiptdate)]]})]

    :order-by [[o_orderpriority :asc]]})

(def q5
  (-> '{:find [n_name (sum (* l_extendedprice (- 1 l_discount)))]
        :keys [n_name revenue]
        :in [region]
        :where [(match :orders [{:id o, :o_custkey c} o_orderdate])
                [(>= o_orderdate #time/date "1994-01-01")]
                [(< o_orderdate #time/date "1995-01-01")]

                (match :lineitem [{:l_orderkey o, :l_suppkey s}
                                  l_extendedprice l_discount])

                (match :supplier {:id s, :s_nationkey n})
                (match :customer {:id c, :c_nationkey n})
                (match :nation [{:id n, :n_regionkey r} n_name])
                (match :region {:id r, :r_name region})]

        :order-by [[(sum (* l_extendedprice (- 1 l_discount))) :desc]]}
      (with-in-args ["ASIA"])))

(def q6
  '{:find [(sum (* l_extendedprice l_discount))]
    :keys [revenue]
    :where [(match :lineitem [l_shipdate l_quantity l_extendedprice l_discount])
            [(>= l_shipdate #time/date "1994-01-01")]
            [(< l_shipdate #time/date "1995-01-01")]
            [(>= l_discount 0.05)]
            [(<= l_discount 0.07)]
            [(< l_quantity 24.0)]]})

(def q7
  '{:find [supp_nation cust_nation l_year (sum (* l_extendedprice (- 1 l_discount)))]

    :where [(match :orders {:o_custkey c})
            (match :lineitem [{:l_orderkey o, :l_suppkey s}
                              l_shipdate l_discount l_extendedprice])

            [(>= l_shipdate #time/date "1995-01-01")]
            [(<= l_shipdate #time/date "1996-12-31")]
            [(extract "YEAR" l_shipdate) l_year]

            (match :supplier {:id s, :s_nationkey n1})
            (match :nation {:id n1, :n_name supp_nation})
            (match :customer {:id c, :c_nationkey n2})
            (match :nation {:id n2, :n_name cust_nation})

            [(or (and (= "FRANCE" supp_nation)
                      (= "GERMANY" cust_nation))
                 (and (= "GERMANY" supp_nation)
                      (= "FRANCE" cust_nation)))]]

    :order-by [[supp_nation :asc] [cust_nation :asc] [l_year :asc]]})

(def q8
  '{:find [o_year mkt_share]
    :where [(q {:find [o_year
                       (sum (if (= "BRAZIL" nation) volume 0))
                       (sum volume)]
                :keys [o_year brazil_volume volume]
                :where [(q {:find [o_year (sum (* l_extendedprice (- 1 l_discount))) nation]
                            :keys [o_year volume nation]
                            :where [(match :orders [{:id o, :o_custkey c} o_orderdate])
                                    [(>= o_orderdate #time/date "1995-01-01")]
                                    [(<= o_orderdate #time/date "1996-12-31")]
                                    [(extract "YEAR" o_orderdate) o_year]

                                    (match :lineitem [{:id l, :l_orderkey o, :l_suppkey s, :l_partkey p}
                                                      l_extendedprice l_discount])

                                    (match :customer {:id c, :c_nationkey n1})
                                    (match :nation {:id n1, :n_regionkey r1})
                                    (match :region {:id r1, :r_name "AMERICA"})

                                    (match :supplier {:id s, :s_nationkey n2})
                                    (match :nation {:id n2, :n_name nation})

                                    (match :part {:id p, :p_type "ECONOMY ANODIZED STEEL"})]})]})
            [(/ brazil_volume volume) mkt_share]]
    :order-by [[o_year :asc]]})

(def q9
  '{:find [nation o_year
           (sum (- (* l_extendedprice (- 1 l_discount))
                   (* ps_supplycost l_quantity)))]
    :keys [nation o_year sum_profit]

    :where [(match :part [{:id p} p_name])
            [(like p_name "%green%")]

            (match :orders [{:id o} o_orderdate])
            [(extract "YEAR" o_orderdate) o_year]

            (match :lineitem [{:l_orderkey o, :l_suppkey s, :l_partkey p}
                              l_quantity l_extendedprice l_discount])

            (match :partsupp [{:ps_partkey p, :ps_suppkey s} ps_supplycost])

            (match :supplier {:id s, :s_nationkey n})
            (match :nation {:id n, :n_name nation})]

    :order-by [[nation :asc] [o_year :desc]]})

(def q10
  '{:find [c c_name (sum (* l_extendedprice (- 1 l_discount)))
           c_acctbal c_phone n_name c_address c_comment]
    :keys [c_custkey c_name revenue
           c_acctbal c_phone n_name c_address c_comment]

    :where [(match :orders [{:id o, :o_custkey c}, o_orderdate])
            [(>= o_orderdate #time/date "1993-10-01")]
            [(< o_orderdate #time/date "1994-01-01")]

            (match :customer [{:id c, :c_nationkey n}
                              c_name c_address c_phone
                              c_acctbal c_comment])

            (match :nation {:id n, :n_name n_name})

            (match :lineitem [{:l_orderkey o, :l_returnflag "R"}
                              l_extendedprice l_discount])]

    :order-by [[(sum (* l_extendedprice (- 1 l_discount))) :desc]]
    :limit 20})

(def q11
  '{:find [ps_partkey value]
    :where [(q {:find [(sum (* ps_supplycost ps_availqty))]
                :keys [total-value]
                :where [(match :partsupp [{:ps_suppkey s} ps_availqty ps_supplycost])
                        (match :supplier [{:id s, :s_nationkey n}])
                        (match :nation [{:id n, :n_name "GERMANY"}])]})
            (q {:find [ps_partkey (sum (* ps_supplycost ps_availqty))]
                :keys [ps_partkey value]
                :where [(match :partsupp [{:ps_suppkey s} ps_availqty ps_supplycost ps_partkey])
                        (match :supplier [{:id s, :s_nationkey n}])
                        (match :nation [{:id n, :n_name "GERMANY"}])]})
            [(> value (* 0.0001 total-value))]]
    :order-by [[value :desc]]})

(def q12
  (-> '{:find [l_shipmode
               (sum (case o_orderpriority "1-URGENT" 1, "2-HIGH" 1, 0))
               (sum (case o_orderpriority "1-URGENT" 0, "2-HIGH" 0, 1))]
        :keys [l_shipmode high_line_count low_line_count]
        :in [[l_shipmode ...]]
        :where [(match :lineitem [{:l_orderkey o}
                                  l_receiptdate l_commitdate l_shipdate l_shipmode])
                [(>= l_receiptdate #time/date "1994-01-01")]
                [(< l_receiptdate #time/date "1995-01-01")]
                [(< l_commitdate l_receiptdate)]
                [(< l_shipdate l_commitdate)]

                (match :orders [{:id o} o_orderpriority])]
        :order-by [[l_shipmode :asc]]}

      (with-in-args [#{"MAIL" "SHIP"}])))

;; TODO left-join
(def q13
  '{:find [c_count (count c_count)]
    :where [#_(or [(q {:find [c (count o)]
                       :keys [c c_count]
                       :where [(match orders [{:o_custkey c} o_comment])
                               (not [(re-find #".*special.*requests.*" o_comment)])]}) ]
                  (and [c :c_custkey]
                       (not [_ :o_custkey c])
                       [(identity 0) c_count]))]
    :order-by [[(count c_count) :desc] [c_count :desc]]})

(def q14
  '{:find [(* 100 (/ promo total))]
    :keys [promo_revenue]
    :where [(q {:find [(sum (if (like p_type "PROMO%")
                              (* l_extendedprice (- 1 l_discount))
                              0))
                       (sum (* l_extendedprice (- 1 l_discount)))]
                :keys [promo total]
                :where [(match :lineitem [{:l_partkey p}
                                          l_shipdate l_extendedprice l_discount])
                        [(>= l_shipdate #time/date "1995-09-01")]
                        [(< l_shipdate #time/date "1995-10-01")]

                        (match :part [{:id p} p_type])]})]})

(def q15
  '{:find [s s_name s_address s_phone total_revenue]
    :where [[(q {:find [s (* l_extendedprice (- 1 l_discount))]
                 :where [[l :l_suppkey s]
                         [l :l_shipdate l_shipdate]
                         [l :l_extendedprice l_extendedprice]
                         [l :l_discount l_discount]
                         [(>= l_shipdate #time/date "1996-01-01")]
                         [(< l_shipdate #time/date "1996-04-01")]]})
             revenue]
            [(q {:find [(max total_revenue)]
                 :in [$ [[_ total_revenue]]]} revenue) [[total_revenue]]]
            [(identity revenue) [[s total_revenue]]]
            [s :s_name s_name]
            [s :s_address s_address]
            [s :s_phone s_phone]]})

(def q16
  (-> '{:find [p_brand p_type p_size (count-distinct s)]
        :keys [p_brand p_type p_size supplier_cnt]
        :in [[p_size ...]]
        :where [(match :part [{:id p} p_brand p_type p_size])
                [(<> p_brand "Brand#45")]
                [(not (like p_type "MEDIUM POLISHED%"))]

                (match :partsupp [{:ps_partkey p, :ps_suppkey s}])

                (not-exists? {:find [s]
                              :where [(match :supplier [{:id s} s_comment])
                                      [(like "%Customer%Complaints%" s_comment)]]})]
        :order-by [[(count-distinct s) :desc]
                   [p_brand :asc]
                   [p_type :asc]
                   [p_size :asc]]}

      (with-in-args [#{3 9 14 19 23 36 45 49}])))

(def q17
  '{:find [avg_yearly]
    :where [(q {:find [(sum l_extendedprice)]
                :keys [sum_extendedprice]
                :where [(match :part [{:id p, :p_brand "Brand#23", :p_container "MED BOX"}])

                        (q {:find [(avg l_quantity)]
                            :in [p]
                            :keys [avg_quantity]
                            :where [(match :lineitem [{:l_partkey p} l_quantity])]})

                        (match :lineitem [{:l_partkey p} l_quantity l_extendedprice])

                        [(< l_quantity (* 0.2 avg_quantity))]]})

            [(/ sum_extendedprice 7.0) avg_yearly]]})

(def q18
  '{:find [c_name c o o_orderdate o_totalprice sum_quantity]
    :keys [c_name c_custkey o_orderkey o_orderdate o_totalprice sum_qty]
    :where [(match :customer [{:id c} c_name])
            (match :orders [{:id o, :o_custkey c} o_orderdate o_totalprice])
            (q {:find [o (sum l_quantity)]
                :keys [o sum_quantity]
                :where [(match :lineitem [{:l_orderkey o} l_quantity])]})
            [(> sum_quantity 300.0)]]
    :order-by [[o_totalprice :desc] [o_orderdate :asc]]
    :limit 100})

;; TODO union-join with full query
(def q19
  (-> '{:find [(sum (* l_extendedprice (- 1 l_discount)))]
        :in [[l_shipmode ...]]
        :where [(match :part [{:id p} p_size])
                (match :lineitem [{:l_shipinstruct "DELIVER IN PERSON", :l_partkey p}
                                  l_shipmode l_discount l_extendedprice l_quantity])
                (union-join [p l_quantity p_size]
                            (and [p :p_brand "Brand#12"]
                                 [p :p_container #{"SM CASE" "SM BOX" "SM PACK" "SM PKG"}]
                                 [(>= l_quantity 1.0)]
                                 [(<= l_quantity 11.0)]
                                 [(>= p_size 1)]
                                 [(<= p_size 5)])
                            (and [p :p_brand "Brand#23"]
                                 [p :p_container #{"MED BAG" "MED BOX" "MED PKG" "MED PACK"}]
                                 [(>= l_quantity 10.0)]
                                 [(<= l_quantity 20.0)]
                                 [(>= p_size 1)]
                                 [(<= p_size 10)])
                            (and [p :p_brand "Brand#34"]
                                 [p :p_container #{"LG CASE" "LG BOX" "LG PACK" "LG PKG"}]
                                 [(>= l_quantity 20.0)]
                                 [(<= l_quantity 30.0)]
                                 [(>= p_size 1)]
                                 [(<= p_size 15)]))]}

      (with-in-args [#{"AIR" "AIR REG"}])))

(def q20
  '{:find [s_name s_address]
    :where [(match :part [{:id p} p_name])
            [(like p_name "forest%")]

            (match :partsupp [{:ps_suppkey s, :ps_partkey p}
                              ps_availqty])

            (match :supplier [{:id s, :s_nationkey n}
                              s_name s_address])

            (match :nation {:id n, :n_name "CANADA"})

            (q {:find [(sum l_quantity)]
                :keys [sum_quantity]
                :in [p s]
                :where [(match :lineitem [{:l_partkey p, :l_suppkey s}
                                          l_shipdate l_quantity])
                        [(>= l_shipdate #time/date "1994-01-01")]
                        [(< l_shipdate #time/date "1995-01-01")]]})
            [(> ps_availqty (* sum_quantity 0.5))]]
    :order-by [[s_name :asc]]})

(def q21
  '{:find [s_name (count l1)]
    :where [(match :nation [{:id n, :n_name "SAUDI ARABIA"}])
            (match :supplier [{:id s, :s_nationkey n} s_name])
            (match :orders [{:id o, :o_orderstatus "F"}])

            (match :lineitem [{:id l1, :l_suppkey s, :l_orderkey o}
                              l_receiptdate l_commitdate])
            [(> l_receiptdate l_commitdate)]

            (exists? {:find [o]
                      :in [s]
                      :where [(match :lineitem [{:l_orderkey o, :l_suppkey l2s}])
                              [(<> s l2s)]]})

            (not-exists? {:find [o]
                          :in [s]
                          :where [(match :lineitem [{:l_orderkey o, :l_suppkey l3s}
                                                    l_receiptdate l_commitdate])
                                  [(<> s l3s)]
                                  [(> l_receiptdate l_commitdate)]]})]

    :order-by [[(count l1) :desc] [s_name :asc]]
    :limit 100})

(def q22
  '{:find [cntrycode (count c) (sum c_acctbal)]
    :where [(match :customer [c_phone c_acctbal])
            [(subs c_phone 0 2) cntrycode]
            [(contains? #{"13" "31" "23" "29" "30" "18" "17"} cntrycode)]
            (q {:find [(avg c_acctbal)]
                :keys [avg_acctbal]
                :where [(match :customer [c_acctbal c_phone])
                        [(> c_acctbal 0.0)]
                        [(subs c_phone 0 2) cntrycode]
                        [(contains? #{"13" "31" "23" "29" "30" "18" "17"} cntrycode)]]})
            [(> c_acctbal avg_acctbal)]
            (not-exists? {:find [c]
                          :where [(match :orders {:o_custkey c})]})]
    :order-by [[cntrycode :asc]]})

(def queries
  [#'q1 #'q2 #'q3 #'q4 #'q5 #'q6 #'q7 #'q8 #'q9 #'q10 #'q11
   #'q12 #'q13 #'q14 #'q15 #'q16 #'q17 #'q18 #'q19 #'q20 #'q21 #'q22])
