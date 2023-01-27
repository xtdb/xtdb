(ns core2.datasets.tpch.datalog)

(defn- with-in-args [q in-args]
  (-> q (vary-meta assoc ::in-args in-args)))

;; TODO reintro nested agg exprs
(def q1
  '{:find [l_returnflag
           l_linestatus
           (sum l_quantity)
           (sum l_extendedprice)
           (sum disc_price)
           (sum charge)
           (avg l_quantity)
           (avg l_extendedprice)
           (avg l_discount)
           (count l)]
    :keys [l_returnflag l_linestatus sum_qty sum_base_price sum_disc_price
           sum_charge avg_qty avg_price avg_disc count_order]
    :where [[l :_table :lineitem]
            [l :l_shipdate l_shipdate]
            [l :l_quantity l_quantity]
            [l :l_extendedprice l_extendedprice]
            [l :l_discount l_discount]
            [l :l_tax l_tax]
            [l :l_returnflag l_returnflag]
            [l :l_linestatus l_linestatus]
            [(* l_extendedprice (- 1 l_discount)) disc_price]
            [(* (* l_extendedprice (- 1 l_discount)) (+ 1 l_tax)) charge]
            [(<= l_shipdate #inst "1998-09-02")]]
    :order-by [[l_returnflag :asc] [l_linestatus :asc]]})

(def q2
  '{:find [s_acctbal s_name n_name p p_mfgr s_address s_phone s_comment]
    :where [[p :_table :part]
            [ps :_table :partsupp]
            [s :_table :supplier]
            [n :_table :nation]
            [r :_table :region]

            [p :p_mfgr p_mfgr]
            [p :p_size 15]
            [p :p_type p_type]
            [(like p_type "%BRASS")]

            [ps :ps_partkey p]
            [ps :ps_supplycost ps_supplycost]
            [ps :ps_suppkey s]

            (q {:find [(min ps_supplycost)]
                :keys [ps_supplycost]
                :in [p]
                :where [[ps :_table :partsupp]
                        [s :_table :supplier]
                        [n :_table :nation]
                        [r :_table :region]
                        [ps :ps_partkey p]
                        [ps :ps_supplycost ps_supplycost]
                        [ps :ps_suppkey s]
                        [s :s_nationkey n]
                        [n :n_regionkey r]
                        [r :r_name "EUROPE"]]})

            [s :s_acctbal s_acctbal]
            [s :s_address s_address]
            [s :s_name s_name]
            [s :s_phone s_phone]
            [s :s_comment s_comment]
            [s :s_nationkey n]

            [n :n_name n_name]
            [n :n_regionkey r]

            [r :r_name "EUROPE"]]

    :order-by [[s_acctbal :desc]
               [n_name :asc]
               [s_name :asc]
               [p :asc]]
    :limit 100})

(def q3
  (-> '{:find [o (sum revenue) o_orderdate o_shippriority]
        :keys [l_orderkey revenue o_orderdate o_shippriority]
        :in [?segment]
        :where [[c :_table :customer]
                [o :_table :orders]
                [l :_table :lineitem]

                [c :c_mktsegment ?segment]
                [o :o_custkey c]
                [o :o_shippriority o_shippriority]
                [o :o_orderdate o_orderdate]
                [(< o_orderdate #inst "1995-03-15")]
                [l :l_orderkey o]
                [l :l_discount l_discount]
                [l :l_extendedprice l_extendedprice]
                [l :l_shipdate l_shipdate]
                [(> l_shipdate #inst "1995-03-15")]
                [(* l_extendedprice (- 1 l_discount)) revenue]]
        :order-by [[(sum revenue) :desc] [o_orderdate :asc]]
        :limit 10}
      (with-in-args ["BUILDING"])))

(def q4
  '{:find [o_orderpriority (count o)]
    :keys [o_orderpriority order_count]
    :where [[o :_table :orders]
            [o :o_orderdate o_orderdate]
            [o :o_orderpriority o_orderpriority]
            [(>= o_orderdate #inst "1993-07-01")]
            [(< o_orderdate #inst "1993-10-01")]

            (exists? [o]
                     [l :_table :lineitem]
                     [l :l_orderkey o]
                     [l :l_commitdate l_commitdate]
                     [l :l_receiptdate l_receiptdate]
                     [(< l_commitdate l_receiptdate)])]

    :order-by [[o_orderpriority :asc]]})

(def q5
  (-> '{:find [n_name (sum (* l_extendedprice (- 1 l_discount)))]
        :in [?region]
        :where [[o :_table :orders]
                [l :_table :lineitem]
                [s :_table :supplier]
                [c :_table :customer]
                [n :_table :nation]
                [r :_table :region]

                [o :o_custkey c]
                [o :o_orderdate o_orderdate]
                [(>= o_orderdate #inst "1994-01-01")]
                [(< o_orderdate #inst "1995-01-01")]

                [l :l_orderkey o]
                [l :l_suppkey s]
                [l :l_extendedprice l_extendedprice]
                [l :l_discount l_discount]

                [s :s_nationkey n]
                [c :c_nationkey n]
                [n :n_name n_name]
                [n :n_regionkey r]
                [r :r_name ?region]]
        :order-by [[(sum (* l_extendedprice (- 1 l_discount))) :desc]]}
      (with-in-args ["ASIA"])))

;; TODO reintro nested agg exprs
(def q6
  '{:find [(sum revenue)]
    :keys [revenue]
    :where [[l :_table :lineitem]
            [l :l_shipdate l_shipdate]
            [l :l_quantity l_quantity]
            [l :l_extendedprice l_extendedprice]
            [l :l_discount l_discount]
            [(* l_extendedprice l_discount) revenue]
            [(>= l_shipdate #inst "1994-01-01")]
            [(< l_shipdate #inst "1995-01-01")]
            [(>= l_discount 0.05)]
            [(<= l_discount 0.07)]
            [(< l_quantity 24.0)]]})

(def q7
  '{:find [supp_nation cust_nation l_year (sum (* l_extendedprice (- 1 l_discount)))]
    :where [[o :_table :orders]
            [l :_table :lineitem]
            [s :_table supplier]
            [n1 :_table :nation]
            [c :_table :customer]
            [n2 :_table :nation]

            [o :o_custkey c]
            [l :l_orderkey o]
            [l :l_suppkey s]
            [l :l_shipdate l_shipdate]
            [l :l_discount l_discount]
            [l :l_extendedprice l_extendedprice]
            [(>= l_shipdate #inst "1995-01-01")]
            [(<= l_shipdate #inst "1996-12-31")]
            [(extract "YEAR" l_shipdate) l_year]

            [s :s_nationkey n1]
            [n1 :n_name supp_nation]
            [c :c_nationkey n2]
            [n2 :n_name cust_nation]

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
                            :where [[o :_table :orders]
                                    [c :_table :customer]
                                    [l :_table :lineitem]
                                    [s :_table :supplier]
                                    [n1 :_table :nation]
                                    [r1 :_table :region]
                                    [n2 :_table :nation]
                                    [p :_table :part]

                                    [o :o_custkey c]
                                    [o :o_orderdate o_orderdate]
                                    [(>= o_orderdate #inst "1995-01-01")]
                                    [(<= o_orderdate #inst "1996-12-31")]

                                    [l :l_orderkey o]
                                    [l :l_suppkey s]
                                    [l :l_partkey p]
                                    [l :l_discount l_discount]
                                    [l :l_extendedprice l_extendedprice]

                                    [c :c_nationkey n1]
                                    [n1 :n_regionkey r1]
                                    [r1 :r_name "AMERICA"]
                                    [s :s_nationkey n2]
                                    [n2 :n_name nation]
                                    [p :p_type "ECONOMY ANODIZED STEEL"]
                                    [(extract "YEAR" o_orderdate) o_year]]})]})
            [(/ brazil_volume volume) mkt_share]]
    :order-by [[o_year :asc]]})

(def q9
  '{:find [nation o_year
           (sum (- (* l_extendedprice (- 1 l_discount))
                   (* ps_supplycost l_quantity)))]
    :where [[l :_table :lineitem]
            [ps :_table :partsupp]
            [s :_table :supplier]
            [n :_table :nation]
            [p :_table :part]
            [o :_table :orders]

            [l :l_orderkey o]
            [l :l_suppkey s]
            [l :l_partkey p]
            [l :l_quantity l_quantity]
            [l :l_discount l_discount]
            [l :l_extendedprice l_extendedprice]

            [ps :ps_partkey p]
            [ps :ps_suppkey s]
            [ps :ps_supplycost ps_supplycost]

            [s :s_nationkey n]

            [n :n_name nation]

            [p :p_name p_name]
            [(like p_name "%green%")]

            [o :o_orderdate o_orderdate]
            [(extract "YEAR" o_orderdate) o_year]]

    :order-by [[nation :asc] [o_year :desc]]})

(def q10
  '{:find [c c_name (sum (* l_extendedprice (- 1 l_discount)))
           c_acctbal n_name c_address c_phone c_comment]
    :where [[c :_table :customer]
            [l :_table :lineitem]
            [n :_table :nation]
            [o :_table :orders]

            [c :c_nationkey n]
            [c :c_name c_name]
            [c :c_acctbal c_acctbal]
            [c :c_address c_address]
            [c :c_phone c_phone]
            [c :c_comment c_comment]
            [o :o_custkey c]
            [o :o_orderdate o_orderdate]
            [(>= o_orderdate #inst "1993-10-01")]
            [(< o_orderdate #inst "1994-01-01")]
            [l :l_orderkey o]
            [l :l_extendedprice l_extendedprice]
            [l :l_discount l_discount]
            [l :l_returnflag "R"]
            [n :n_name n_name]]
    :order-by [[(sum (* l_extendedprice (- 1 l_discount))) :desc]]
    :limit 20})

(def q11
  '{:find [ps_partkey value]
    :where [(q {:find [(sum (* ps_supplycost ps_availqty))]
                :keys [total-value]
                :where [[ps :_table partsupp]
                        [s :_table :supplier]
                        [n :_table :nation]

                        [ps :ps_availqty ps_availqty]
                        [ps :ps_supplycost ps_supplycost]
                        [ps :ps_suppkey s]
                        [s :s_nationkey n]
                        [n :n_name "GERMANY"]]})
            (q {:find [ps_partkey (sum (* ps_supplycost ps_availqty))]
                :keys [ps_partkey value]
                :where [[ps :_table partsupp]
                        [s :_table :supplier]
                        [n :_table :nation]

                        [ps :ps_availqty ps_availqty]
                        [ps :ps_supplycost ps_supplycost]
                        [ps :ps_partkey ps_partkey]
                        [ps :ps_suppkey s]
                        [s :s_nationkey n]
                        [n :n_name "GERMANY"]]})
            [(> value (* 0.0001 total-value))]]
    :order-by [[value :desc]]})

;; TODO reintro nested agg exprs
(def q12
  (-> '{:find [l_shipmode
               (sum high-line-count)
               (sum low-line-count)]
        :keys [l_shipmode high_line_count low_line_count]
        :in [[l_shipmode ...]]
        :where [[l :_table :lineitem]
                [o :_table :orders]

                [l :l_orderkey o]
                [l :l_receiptdate l_receiptdate]
                [l :l_commitdate l_commitdate]
                [l :l_shipdate l_shipdate]
                [l :l_shipmode l_shipmode]
                [(>= l_receiptdate #inst "1994-01-01")]
                [(< l_receiptdate #inst "1995-01-01")]
                [(< l_commitdate l_receiptdate)]
                [(< l_shipdate l_commitdate)]

                [o :o_orderpriority o_orderpriority]

                [(case o_orderpriority "1-URGENT" 1, "2-HIGH" 1, 0) high-line-count]
                [(case o_orderpriority "1-URGENT" 0, "2-HIGH" 0, 1) low-line-count]]
        :order-by [[l_shipmode :asc]]}

      (with-in-args [#{"MAIL" "SHIP"}])))

(def q13
  '{:find [c_count (count c_count)]
    :where [(or [(q {:find [c (count o)]
                     :keys [c c_count]
                     :where [[o :_table :orders]
                             [o :o_custkey c]
                             [o :o_comment o_comment]
                             (not [(re-find #".*special.*requests.*" o_comment)])]}) ]
                (and [c :c_custkey]
                     (not [_ :o_custkey c])
                     [(identity 0) c_count]))]
    :order-by [[(count c_count) :desc] [c_count :desc]]})

(def q14
  '{:find [(* 100 (/ promo total))]
    :where [(q {:find [(sum (if (clojure.string/starts-with? p_type "PROMO")
                              (* l_extendedprice (- 1 l_discount))
                              0))
                       (sum (* l_extendedprice (- 1 l_discount)))]
                :keys [promo total]
                :where [[l :_table :lineitem]
                        [p :_table :part]

                        [p :p_type p_type]
                        [l :l_partkey p]
                        [l :l_shipdate l_shipdate]
                        [l :l_extendedprice l_extendedprice]
                        [l :l_discount l_discount]
                        [(>= l_shipdate #inst "1995-09-01")]
                        [(< l_shipdate #inst "1995-10-01")]]})]})

(def q15
  '{:find [s s_name s_address s_phone total_revenue]
    :where [[(q {:find [s (* l_extendedprice (- 1 l_discount))]
                 :where [[l :l_suppkey s]
                         [l :l_shipdate l_shipdate]
                         [l :l_extendedprice l_extendedprice]
                         [l :l_discount l_discount]
                         [(>= l_shipdate #inst "1996-01-01")]
                         [(< l_shipdate #inst "1996-04-01")]]})
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
        :where [[p :_table :part]
                [ps :_table :partsupp]

                [p :p_brand p_brand]
                [(<> p_brand "Brand#45")]
                [p :p_type p_type]
                [(not (like p_type "MEDIUM POLISHED%"))]
                [p :p_size p_size]

                [ps :ps_partkey p]
                [ps :ps_suppkey s]

                (not-exists? [s]
                             [s :_table :supplier]
                             [s :s_comment s_comment]
                             [(like "%Customer%Complaints%" s_comment)])]
        :order-by [[(count-distinct s) :desc]
                   [p_brand :asc]
                   [p_type :asc]
                   [p_size :asc]]}

      ;; TODO set
      (with-in-args [[3 9 14 19 23 36 45 49]])))

(def q17
  '{:find [avg_yearly]
    :where [(q {:find [(sum l_extendedprice)]
                :keys [sum_extendedprice]
                :where [[p :_table :part]
                        [l :_table :lineitem]

                        [p :p_brand "Brand#23"]
                        [p :p_container "MED BOX"]
                        [l :l_partkey p]
                        [l :l_quantity l_quantity]
                        [l :l_extendedprice l_extendedprice]

                        (q {:find [(avg l_quantity)]
                            :in [p]
                            :keys [avg_quantity]
                            :where [[l :_table :lineitem]
                                    [l :l_partkey p]
                                    [l :l_quantity l_quantity]]})

                        [(< l_quantity (* 0.2 avg_quantity))]]})

            [(/ sum_extendedprice 7.0) avg_yearly]]})

(def q18
  '{:find [c_name c o o_orderdate o_totalprice sum_quantity]
    :keys [c_name c_custkey o_orderkey o_orderdate o_totalprice sum_qty]
    :where [[o :_table :orders]
            [c :_table :customer]
            (q {:find [o (sum l_quantity)]
                :keys [o sum_quantity]
                :where [[l :_table :lineitem]
                        [l :l_orderkey o]
                        [l :l_quantity l_quantity]]})
            [(> sum_quantity 300.0)]
            [o :o_custkey c]
            [c :c_name c_name]
            [o :o_orderdate o_orderdate]
            [o :o_totalprice o_totalprice]]
    :order-by [[o_totalprice :desc] [o_orderdate :asc]]
    :limit 100})

(def q19
  (-> '{:find [(sum (* l_extendedprice (- 1 l_discount)))]
        :in [[l_shipmode ...]]
        :where [[l :_table :lineitem]
                [p :_table :part]

                [l :l_shipmode l_shipmode]
                [l :l_shipinstruct "DELIVER IN PERSON"]
                [l :l_discount l_discount]
                [l :l_extendedprice l_extendedprice]
                [l :l_partkey p]
                [l :l_quantity l_quantity]
                [p :p_size p_size]

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
    :where [[ps :_table :partsupp]
            [p :_table :part]
            [s :_table :supplier]
            [n :_table :nation]

            [p :p_name p_name]
            [(like p_name "forest%")]

            [s :s_name s_name]
            [s :s_address s_address]
            [s :s_nationkey n]
            [n :n_name "CANADA"]

            [ps :ps_suppkey s]
            [ps :ps_partkey p]
            [ps :ps_availqty ps_availqty]
            (q {:find [(sum l_quantity)]
                :keys [sum_quantity]
                :in [p s]
                :where [[l :_table :lineitem]
                        [l :l_partkey p]
                        [l :l_suppkey s]
                        [l :l_shipdate l_shipdate]
                        [(>= l_shipdate #inst "1994-01-01")]
                        [(< l_shipdate #inst "1995-01-01")]
                        [l :l_quantity l_quantity]]})
            [(> ps_availqty (* sum_quantity 0.5))]]
    :order-by [[s_name :asc]]})

(def q21
  '{:find [s_name (count l1)]
    :where [[o :_table :orders]
            [s :_table :supplier]
            [l1 :_table :lineitem]
            [n :_table :nation]

            [o :o_orderstatus "F"]

            [s :s_name s_name]
            [s :s_nationkey n]
            [n :n_name "SAUDI ARABIA"]

            [l1 :l_suppkey s]
            [l1 :l_orderkey o]
            [l1 :l_receiptdate l_receiptdate]
            [l1 :l_commitdate l_commitdate]

            [(> l_receiptdate l_commitdate)]

            (exists? [o s]
                     [l2 :_table :lineitem]
                     [l2 :l_orderkey o]
                     [l2 :l_suppkey l2s]
                     [(<> s l2s)])

            (not-exists? [o s]
                         [l3 :_table :lineitem]
                         [l3 :l_orderkey o]
                         [l3 :l_suppkey l3s]
                         [(<> s l3s)]
                         [l3 :l_receiptdate l_receiptdate]
                         [l3 :l_commitdate l_commitdate]
                         [(> l_receiptdate l_commitdate)])]

    :order-by [[(count l1) :desc] [s_name :asc]]
    :limit 100})

(def q22
  '{:find [cntrycode (count c) (sum c_acctbal)]
    :where [[c :_table :customer]
            [c :c_phone c_phone]
            [(subs c_phone 0 2) cntrycode]
            [(contains? #{"13" "31" "23" "29" "30" "18" "17"} cntrycode)]
            (q {:find [(avg c_acctbal)]
                :keys [avg_acctbal]
                :where [[c :_table :customer]
                        [c :c_acctbal c_acctbal]
                        [(> c_acctbal 0.0)]
                        [c :c_phone c_phone]
                        [(subs c_phone 0 2) cntrycode]
                        [(contains? #{"13" "31" "23" "29" "30" "18" "17"} cntrycode)]]})
            [c :c_acctbal c_acctbal]
            [(> c_acctbal avg_acctbal)]
            (not-exists? [c]
                         [o :_table :orders]
                         [o :o_custkey c])]
    :order-by [[cntrycode :asc]]})

(def queries
  [#'q1 #'q2 #'q3 #'q4 #'q5 #'q6 #'q7 #'q8 #'q9 #'q10 #'q11
   #'q12 #'q13 #'q14 #'q15 #'q16 #'q17 #'q18 #'q19 #'q20 #'q21 #'q22])
