(ns crux.bench.tpch-test
  (:require [crux.bench :as bench]
            [crux.api :as crux]
            [clojure.tools.logging :as log]
            [crux.fixtures.tpch :as tpch]
            [clojure.edn :as edn]
            [clojure.instant :as i]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :as t]))

(declare tpch-queries parse-tpch-result validate-tpch-query)

(defn run-tpch-query [node n]
  (crux/q (crux/db node) (assoc (get tpch-queries (dec n)) :timeout 120000)))

(defn run-tpch-queries [node {:keys [scale-factor] :as opts}]
  (every? true? (for [n (range 1 23)]
                  (let [actual (run-tpch-query node n)]
                    (if (= 0.01 scale-factor)
                      (every? true? (validate-tpch-query actual (parse-tpch-result n)))
                      (boolean actual))))))

(defn run-tpch-test [node {:keys [scale-factor] :as opts}]
  (let [scale-factor (or scale-factor 0.01)]
    (bench/with-bench-ns :tpch-test
      (bench/with-crux-dimensions
        (bench/run-bench :ingest
                         (bench/with-additional-index-metrics node
                           (tpch/load-docs! node scale-factor tpch/tpch-entity->pkey-doc)))

        ;; TODO we may want to split this up, à la WatDiv, so that we can see if
        ;; specific queries are slower than our comparison databases
        (bench/run-bench :queries
                         {:success? (run-tpch-queries node opts)})

        (bench/run-bench :queries-warm
                         {:success? (run-tpch-queries node opts)})))))

;; "Elapsed time: 21994.835831 msecs"
(def q1 '{:find [l_returnflag
                 l_linestatus
                 (sum l_quantity)
                 (sum l_extendedprice)
                 (sum ret_2)
                 (sum ret_4)
                 (avg l_quantity)
                 (avg l_extendedprice)
                 (avg l_discount)
                 (count l)]
          :where [[l :l_shipdate l_shipdate]
                  [l :l_quantity l_quantity]
                  [l :l_extendedprice l_extendedprice]
                  [l :l_discount l_discount]
                  [l :l_tax l_tax]
                  [l :l_returnflag l_returnflag]
                  [l :l_linestatus l_linestatus]
                  [(<= l_shipdate #inst "1998-09-02")]
                  [(- 1 l_discount) ret_1]
                  [(* l_extendedprice ret_1) ret_2]
                  [(+ 1 l_tax) ret_3]
                  [(* ret_2 ret_3) ret_4]]
          :order-by [[l_returnflag :asc]
                     [l_linestatus :asc]]})

;; "Elapsed time: 1304.42464 msecs"
(def q2 '{:find [s_acctbal
                 s_name
                 n_name
                 p
                 p_mfgr
                 s_address
                 s_phone
                 s_comment]
          :where [[p :p_mfgr p_mfgr]
                  [p :p_size 15]
                  [p :p_type p_type]
                  [(re-find #"^.*BRASS$" p_type)]
                  [ps :ps_partkey p]
                  [ps :ps_supplycost ps_supplycost]
                  [(q {:find [(min ps_supplycost)]
                       :in [$ p]
                       :where [[ps :ps_partkey p]
                               [ps :ps_supplycost ps_supplycost]
                               [ps :ps_suppkey s]
                               [s :s_nationkey n]
                               [n :n_regionkey r]
                               [r :r_name "EUROPE"]]} p) [[ps_supplycost]]]
                  [ps :ps_suppkey s]
                  [s :s_acctbal s_acctbal]
                  [s :s_address s_address]
                  [s :s_name s_name]
                  [s :s_phone s_phone]
                  [s :s_comment s_comment]
                  [n :n_name n_name]
                  [s :s_nationkey n]
                  [n :n_regionkey r]
                  [r :r_name "EUROPE"]]
          :order-by [[s_acctbal :desc]
                     [n_name :asc]
                     [s_name :asc]
                     [p :asc]]
          :limit 100})

;; "Elapsed time: 5786.047011 msecs"
(def q3 '{:find [o
                 (sum ret_2)
                 o_orderdate
                 o_shippriority]
          :where [[c :c_mktsegment "BUILDING"]
                  [o :o_custkey c]
                  [o :o_shippriority o_shippriority]
                  [o :o_orderdate o_orderdate]
                  [(< o_orderdate #inst "1995-03-15")]
                  [l :l_orderkey o]
                  [l :l_discount l_discount]
                  [l :l_extendedprice l_extendedprice]
                  [l :l_shipdate l_shipdate]
                  [(> l_shipdate #inst "1995-03-15")]
                  [(- 1 l_discount) ret_1]
                  [(* l_extendedprice ret_1) ret_2]]
          :order-by [[(sum ret_2) :desc]
                     [o_orderdate :asc]]
          :limit 10})

;; "Elapsed time: 1292.874285 msecs"
(def q4 '{:find [o_orderpriority
                 (count o)]
          :where [[o :o_orderdate o_orderdate]
                  [o :o_orderpriority o_orderpriority]
                  [(>= o_orderdate #inst "1993-07-01")]
                  [(< o_orderdate #inst "1993-10-01")]
                  (or-join [o]
                           (and [l :l_orderkey o]
                                [l :l_commitdate l_commitdate]
                                [l :l_receiptdate l_receiptdate]
                                [(< l_commitdate l_receiptdate)]))]
          :order-by [[o_orderpriority :asc]]})

;; "Elapsed time: 5319.179341 msecs"
(def q5 '{:find [n_name
                 (sum ret_2)]
          :where [[o :o_custkey c]
                  [l :l_orderkey o]
                  [l :l_suppkey s]
                  [s :s_nationkey n]
                  [c :c_nationkey n]
                  [n :n_name n_name]
                  [n :n_regionkey r]
                  [r :r_name "ASIA"]
                  [l :l_extendedprice l_extendedprice]
                  [l :l_discount l_discount]
                  [o :o_orderdate o_orderdate]
                  [(>= o_orderdate #inst "1994-01-01")]
                  [(< o_orderdate #inst "1995-01-01")]
                  [(- 1 l_discount) ret_1]
                  [(* l_extendedprice ret_1) ret_2]]
          :order-by [[(sum ret_2) :desc]]})

;; "Elapsed time: 1773.63273 msecs"
(def q6 '{:find [(sum ret_1)]
          :where [[l :l_shipdate l_shipdate]
                  [l :l_quantity l_quantity]
                  [l :l_extendedprice l_extendedprice]
                  [l :l_discount l_discount]
                  [(>= l_shipdate #inst "1994-01-01")]
                  [(< l_shipdate #inst "1995-01-01")]
                  [(>= l_discount 0.05)]
                  [(<= l_discount 0.07)]
                  [(< l_quantity 24.0)]
                  [(* l_extendedprice l_discount) ret_1]]})

;; "Elapsed time: 58567.852989 msecs"
(def q7 '{:find [supp_nation
                 cust_nation
                 l_year
                 (sum ret_2)]
          :where [[o :o_custkey c]
                  [l :l_orderkey o]
                  [l :l_suppkey s]
                  [s :s_nationkey n1]
                  [n1 :n_name supp_nation]
                  [c :c_nationkey n2]
                  [n2 :n_name cust_nation]
                  (or (and [(= "FRANCE" supp_nation)]
                           [(= "GERMANY" cust_nation)])
                      (and [(= "GERMANY" supp_nation)]
                           [(= "FRANCE" cust_nation)]))
                  [l :l_shipdate l_shipdate]
                  [l :l_discount l_discount]
                  [l :l_extendedprice l_extendedprice]
                  [(>= l_shipdate #inst "1995-01-01")]
                  [(<= l_shipdate #inst "1996-12-31")]
                  [(- 1 l_discount) ret_1]
                  [(* l_extendedprice ret_1) ret_2]
                  [(pr-str l_shipdate) ret_3]
                  [(subs ret_3 7 11) l_year]]
          :order-by [[supp_nation :asc] [cust_nation :asc] [l_year :asc]]})

;; "Elapsed time: 22993.637183 msecs"
(def q8 '{:find [o_year
                 mkt_share]
          :where [[(q {:find [o_year
                              (sum brazil_volume)
                              (sum volume)]
                       :where [[(q {:find [o_year
                                           (sum ret_2)
                                           nation]
                                    :where [[o :o_custkey c]
                                            [l :l_orderkey o]
                                            [l :l_suppkey s]
                                            [l :l_partkey p]
                                            [c :c_nationkey n1]
                                            [n1 :n_regionkey r1]
                                            [r1 :r_name "AMERICA"]
                                            [s :s_nationkey n2]
                                            [n2 :n_name nation]
                                            [l :l_discount l_discount]
                                            [l :l_extendedprice l_extendedprice]
                                            [o :o_orderdate o_orderdate]
                                            [(>= o_orderdate #inst "1995-01-01")]
                                            [(<= o_orderdate #inst "1996-12-31")]
                                            [p :p_type "ECONOMY ANODIZED STEEL"]
                                            [(- 1 l_discount) ret_1]
                                            [(* l_extendedprice ret_1) ret_2]
                                            [(pr-str o_orderdate) ret_3]
                                            [(subs ret_3 7 11) o_year]]})
                                [[o_year volume nation]]]
                               [(hash-map "BRAZIL" volume) brazil_lookup]
                               [(get brazil_lookup nation 0) brazil_volume]]}) [[o_year brazil_volume volume]]]
                  [(/ brazil_volume volume) mkt_share]]
          :order-by [[o_year :asc]]})

;; "Elapsed time: 10066.352268 msecs"
(def q9 '{:find [nation
                 o_year
                 (sum ret_4)]
          :where [[l :l_orderkey o]
                  [l :l_suppkey s]
                  [l :l_partkey p]
                  [ps :ps_partkey p]
                  [ps :ps_suppkey s]
                  [ps :ps_supplycost ps_supplycost]
                  [s :s_nationkey n]
                  [n :n_name nation]
                  [p :p_name p_name]
                  [(re-find #".*green.*" p_name)]
                  [l :l_quantity l_quantity]
                  [l :l_discount l_discount]
                  [l :l_extendedprice l_extendedprice]
                  [o :o_orderdate o_orderdate]
                  [(- 1 l_discount) ret_1]
                  [(* l_extendedprice ret_1) ret_2]
                  [(* ps_supplycost l_quantity) ret_3]
                  [(- ret_2 ret_3) ret_4]
                  [(pr-str o_orderdate) ret_5]
                  [(subs ret_5 7 11) o_year]]
          :order-by [[nation :asc] [o_year :desc]]})

;; "Elapsed time: 8893.984112 msecs"
(def q10 '{:find [c
                  c_name
                  (sum ret_2)
                  c_acctbal
                  n_name
                  c_address
                  c_phone
                  c_comment]
           :where [[o :o_custkey c]
                   [l :l_orderkey o]
                   [c :c_nationkey n]
                   [n :n_name n_name]
                   [c :c_name c_name]
                   [c :c_acctbal c_acctbal]
                   [c :c_address c_address]
                   [c :c_phone c_phone]
                   [c :c_comment c_comment]
                   [l :l_extendedprice l_extendedprice]
                   [l :l_discount l_discount]
                   [o :o_orderdate o_orderdate]
                   [(>= o_orderdate #inst "1993-10-01")]
                   [(< o_orderdate #inst "1994-01-01")]
                   [l :l_returnflag "R"]
                   [(- 1 l_discount) ret_1]
                   [(* l_extendedprice ret_1) ret_2]]
           :order-by [[(sum ret_2) :desc]]
           :limit 20})

;; "Elapsed time: 190.177707 msecs"
(def q11 '{:find [ps_partkey
                  value]
           :where [[(q {:find [(sum ret_1)]
                        :where [[ps :ps_availqty ps_availqty]
                                [ps :ps_supplycost ps_supplycost]
                                [ps :ps_suppkey s]
                                [s :s_nationkey n]
                                [n :n_name "GERMANY"]
                                [(* ps_supplycost ps_availqty) ret_1]]}) [[ret_1]]]
                   [(q {:find [ps_partkey
                               (sum ret_1)]
                        :where [[ps :ps_availqty ps_availqty]
                                [ps :ps_supplycost ps_supplycost]
                                [ps :ps_partkey ps_partkey]
                                [ps :ps_suppkey s]
                                [s :s_nationkey n]
                                [n :n_name "GERMANY"]
                                [(* ps_supplycost ps_availqty) ret_1]]}) [[ps_partkey value]]]
                   [(* 0.0001 ret_1) ret_2]
                   [(> value ret_2)]]
           :order-by [[value :desc]]})

;; "Elapsed time: 4115.160435 msecs"
(def q12 '{:find [l_shipmode
                  (sum high_line_count)
                  (sum low_line_count)]
           :where [[l :l_orderkey o]
                   [l :l_receiptdate l_receiptdate]
                   [l :l_commitdate l_commitdate]
                   [l :l_shipdate l_shipdate]
                   [(>= l_receiptdate #inst "1994-01-01")]
                   [(< l_receiptdate #inst "1995-01-01")]
                   [(< l_commitdate l_receiptdate)]
                   [(< l_shipdate l_commitdate)]
                   [l :l_shipmode l_shipmode]
                   [l :l_shipmode #{"MAIL" "SHIP"}]
                   [o :o_orderpriority o_orderpriority]
                   [(get {"1-URGENT" [1 0]
                          "2-HIGH" [1 0]} o_orderpriority [0 1])
                    [high_line_count low_line_count]]]
           :order-by [[l_shipmode :asc]]})

;; "Elapsed time: 2187.862433 msecs"
(def q13 '{:find [c_count
                  (count c_count)]
           :where [(or [(q {:find [c
                                   (count o)]
                            :where [[o :o_custkey c]
                                    [o :o_comment o_comment]
                                    (not [(re-find #".*special.*requests.*" o_comment)])]}) [[c c_count]]]
                       (and [c :c_custkey]
                            (not [_ :o_custkey c])
                            [(identity 0) c_count]))]
           :order-by [[(count c_count) :desc] [c_count :desc]]})

;; "Elapsed time: 12734.7558 msecs"
(def q14 '{:find [promo_revenue]
           :where [[(q {:find [(sum ret_3)
                               (sum ret_2)]
                        :where [[l :l_partkey p]
                                [p :p_type p_type]
                                [l :l_shipdate l_shipdate]
                                [l :l_extendedprice l_extendedprice]
                                [l :l_discount l_discount]
                                [(>= l_shipdate #inst "1995-09-01")]
                                [(< l_shipdate #inst "1995-10-01")]
                                [(- 1 l_discount) ret_1]
                                [(* l_extendedprice ret_1) ret_2]
                                [(clojure.string/starts-with? p_type "PROMO") promo?]
                                [(hash-map true ret_2) promo_lookup]
                                [(get promo_lookup promo? 0) ret_3]]}) [[promo not_promo]]]
                   [(/ promo not_promo) ret_1]
                   [(* 100 ret_1) promo_revenue]]})

;; "Elapsed time: 5651.664312 msecs"
(def q15 '{:find [s
                  s_name
                  s_address
                  s_phone
                  total_revenue]
           :where [[(q {:find [s
                               (sum ret_2)]
                        :where [[l :l_suppkey s]
                                [l :l_shipdate l_shipdate]
                                [l :l_extendedprice l_extendedprice]
                                [l :l_discount l_discount]
                                [(>= l_shipdate #inst "1996-01-01")]
                                [(< l_shipdate #inst "1996-04-01")]
                                [(- 1 l_discount) ret_1]
                                [(* l_extendedprice ret_1) ret_2]]}) revenue]
                   [(q {:find [(max total_revenue)]
                        :in [$ [[_ total_revenue]]]} revenue) [[total_revenue]]]
                   [(identity revenue) [[s total_revenue]]]
                   [s :s_name s_name]
                   [s :s_address s_address]
                   [s :s_phone s_phone]]})

;; "Elapsed time: 357.971209 msecs"
(def q16 '{:find [p_brand
                  p_type
                  p_size
                  (count-distinct s)]
           :where [[p :p_brand p_brand]
                   [(not= p_brand "Brand#45")]
                   [p :p_type p_type]
                   (not [(re-find #"^MEDIUM POLISHED.*" p_type)])
                   [p :p_size p_size]
                   [p :p_size #{49 14 23 45 19 3 36 9}]
                   [ps :ps_partkey p]
                   [ps :ps_suppkey s]
                   (not-join [s]
                             [s :s_comment s_comment]
                             [(re-find #".*Customer.*Complaints.*" s_comment)])]
           :order-by [[(count-distinct s) :desc]
                      [p_brand :asc]
                      [p_type :asc]
                      [p_size :asc]]})

;; "Elapsed time: 7.41559 msecs" -- unclear if this works, as it returns nothing.
(def q17 '{:find [avg_yearly]
           :where [[(q {:find [(sum l_extendedprice)]
                        :where [[p :p_brand "Brand#23"]
                                [p :p_container "MED BOX"]
                                [l :l_partkey p]
                                [(q {:find [(avg l_quantity)]
                                     :in [$ p]
                                     :where [[l :l_partkey p]
                                             [l :l_quantity l_quantity]]} p) [[avg_quantity]]]
                                [(* 0.2 avg_quantity) ret_1]
                                [l :l_quantity l_quantity]
                                [(< l_quantity ret_1)]
                                [l :l_extendedprice l_extendedprice]]}) [[sum_extendedprice]]]
                   [(/ sum_extendedprice 7.0) avg_yearly]]})

;; "Elapsed time: 13222.491411 msecs"
(def q18 '{:find [c_name
                  c
                  o
                  o_orderdate
                  o_totalprice
                  (sum l_quantity)]
           :where [[(q {:find [o (sum l_quantity)]
                        :where [[l :l_orderkey o]
                                [l :l_quantity l_quantity]]}) [[o sum_quantity]]]
                   [(> sum_quantity 300.0)]
                   [o :o_custkey c]
                   [c :c_name c_name]
                   [o :o_orderdate o_orderdate]
                   [o :o_totalprice o_totalprice]
                   [l :l_orderkey o]
                   [l :l_quantity l_quantity]]
           :order-by [[o_totalprice :desc] [o_orderdate :asc]]
           :limit 100})

;; "Elapsed time: 4693.23977 msecs"
(def q19 '{:find [(sum ret_2)]
           :where [[l :l_shipmode #{"AIR" "AIR REG"}]
                   [l :l_shipinstruct "DELIVER IN PERSON"]
                   [l :l_discount l_discount]
                   [l :l_extendedprice l_extendedprice]
                   [l :l_partkey p]
                   [l :l_quantity l_quantity]
                   [p :p_size p_size]
                   (or (and [p :p_brand "Brand#12"]
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
                            [(<= p_size 15)]))
                   [(- 1 l_discount) ret_1]
                   [(* l_extendedprice ret_1) ret_2]]})

;; "Elapsed time: 1607.843909 msecs"
(def q20 '{:find [s_name
                  s_address]
           :where [[ps :ps_suppkey s]
                   [ps :ps_partkey p]
                   [p :p_name p_name]
                   [(re-find #"^forest.*" p_name)]
                   [(q {:find [(sum l_quantity)]
                        :in [$ p s]
                        :where [[l :l_partkey p]
                                [l :l_suppkey s]
                                [l :l_shipdate l_shipdate]
                                [(>= l_shipdate #inst "1994-01-01")]
                                [(< l_shipdate #inst "1995-01-01")]
                                [l :l_quantity l_quantity]]} p s) [[sum_quantity]]]
                   [(* sum_quantity 0.5) ret_1]
                   [(long ret_1) ret_2]
                   [ps :ps_availqty ps_availqty]
                   [(> ps_availqty ret_2)]
                   [s :s_name s_name]
                   [s :s_address s_address]
                   [s :s_nationkey n]
                   [n :n_name "CANADA"]]
           :order-by [[s_name :asc]]})

;; "Elapsed time: 6805.455401 msecs"
(def q21 '{:find [s_name
                  (count l1)]
           :where [[l1 :l_suppkey s]
                   [s :s_name s_name]
                   [l1 :l_orderkey o]
                   [o :o_orderstatus "F"]
                   [l1 :l_receiptdate l_receiptdate]
                   [l1 :l_commitdate l_commitdate]
                   [(> l_receiptdate l_commitdate)]
                   (or-join [o s]
                            (and [l2 :l_orderkey o]
                                 (not [l2 :l_suppkey s])))
                   (not-join [o s]
                             [l3 :l_orderkey o]
                             (not [l3 :l_suppkey s])
                             [l3 :l_receiptdate l_receiptdate]
                             [l3 :l_commitdate l_commitdate]
                             [(> l_receiptdate l_commitdate)])
                   [s :s_nationkey n]
                   [n :n_name "SAUDI ARABIA"]]
           :order-by  [[(count l1) :desc] [s_name :asc]]
           :limit 100})

;; "Elapsed time: 270.855812 msecs"
(def q22 '{:find [cntrycode
                  (count c)
                  (sum c_acctbal)]
           :where [[c :c_phone c_phone]
                   [(subs c_phone 0 2) cntrycode]
                   [(contains? #{"13" "31" "23" "29" "30" "18" "17"} cntrycode)]
                   [(q {:find [(avg c_acctbal)]
                        :where [[c :c_acctbal c_acctbal]
                                [(> c_acctbal 0.0)]
                                [c :c_phone c_phone]
                                [(subs c_phone 0 2) cntrycode]
                                [(contains? #{"13" "31" "23" "29" "30" "18" "17"} cntrycode)]]}) [[avg_acctbal]]]
                   [c :c_acctbal c_acctbal]
                   [(> c_acctbal avg_acctbal)]
                   (not-join [c]
                             [o :o_custkey c])]
           :order-by [[cntrycode :asc]]})

(def tpch-queries [q1 q2 q3 q4 q5 q6 q7 q8 q9 q10 q11 q12 q13 q14 q15 q16 q17 q18 q19 q20 q21 q22])

(defn parse-tpch-result [n]
  (let [result-csv (slurp (io/resource (format "io/airlift/tpch/queries/q%d.result" n)))
        [head & lines] (str/split-lines result-csv)]
    (vec (for [l lines]
           (vec (for [x (str/split l #"\|")
                      :let [x (try
                                (cond
                                  (re-find #"[ ,]" x)
                                  x
                                  (re-find #"\d\d\d\d-\d\d-\d\d" x)
                                  (i/read-instant-date x)
                                  :else
                                  (edn/read-string x))
                                (catch Exception e
                                  x))]]
                  (cond-> x
                    (symbol? x) (str))))))))

(defn validate-tpch-query [actual expected]
  (vec (for [[expected-row actual-row] (map vector expected (if (empty? actual)
                                                              [["null"]]
                                                              actual))
             :let [msg (pr-str [expected-row actual-row])]]
         (and (t/is (= (count expected-row)
                       (count actual-row)))
              (every? true?
                      (mapv
                       (fn [x y]
                         (cond
                           (and (number? x) (string? y))
                           (t/is (str/ends-with? y (str x)) msg)

                           (and (number? x) (number? y))
                           (let [epsilon 0.01
                                 diff (Math/abs (- (double x) (double y)))]
                             (t/is (<= diff epsilon)))
                           :else
                           (t/is (= x y) msg)))
                       expected-row
                       actual-row))))))

(comment
  ;; SF 0.01
  (crux.fixtures.tpch/load-docs! (user/crux-node) 0.01 crux.fixtures.tpch/tpch-entity->pkey-doc)

  ;; SQL:
  (slurp (clojure.java.io/resource "io/airlift/tpch/queries/q1.sql"))
  ;; Results:
  (slurp (clojure.java.io/resource "io/airlift/tpch/queries/q1.result"))

  (let [node (user/crux-node)]
    (time
     (doseq [n (range 1 23)]
       (time
        (let [actual (crux.bench.tpch-test/run-tpch-query node n)]
          (prn n (every? true? (crux.bench.tpch-test/validate-tpch-query actual (crux.bench.tpch-test/parse-tpch-result n))))))))))
