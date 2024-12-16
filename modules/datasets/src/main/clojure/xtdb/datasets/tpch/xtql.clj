(ns xtdb.datasets.tpch.xtql)

(defn- with-args [q args]
  (vary-meta q assoc ::args (vec args)))

(def q1
  '(-> (from :lineitem [l-shipdate l-quantity
                        l-extendedprice l-discount l-tax
                        l-returnflag l-linestatus])

       (where (<= l-shipdate #xt/date "1998-09-02"))

       (aggregate l-returnflag
                  l-linestatus
                  {:sum-qty (sum l-quantity)
                   :sum-base-price (sum l-extendedprice)
                   :sum-disc-price (sum (* l-extendedprice (- 1 l-discount)))
                   :sum-charge (sum (* (* l-extendedprice (- 1 l-discount)) (+ 1 l-tax)))
                   :avg-qty (avg l-quantity)
                   :avg-price (avg l-extendedprice)
                   :avg-disc (avg l-discount)
                   :count-order (row-count)})

       (order-by l-returnflag l-linestatus)))

(def q2
  '(-> (unify (from :part [{:xt/id p} p-mfgr {:p-size 15} p-type])
              (where (like p-type "%BRASS"))

              (from :partsupp [{:ps-partkey p, :ps-suppkey s} ps-supplycost])
              (from :supplier [{:xt/id s, :s-nationkey n}
                               s-acctbal s-address s-name s-phone s-comment])
              (from :nation [{:xt/id n, :n-regionkey r} n-name])
              (from :region [{:xt/id r, :r-name "EUROPE"}])

              (where (= ps-supplycost
                        (q (-> (unify (from :partsupp [{:ps-partkey $p, :ps-suppkey s} ps-supplycost])
                                      (from :supplier [{:xt/id s, :s-nationkey n}])
                                      (from :nation [{:xt/id n, :n-regionkey r}])
                                      (from :region [{:xt/id r, :r-name "EUROPE"}]))
                               (aggregate {:min-supplycost (min ps-supplycost)}))
                           {:args [p]}))))

       (order-by {:val s-acctbal, :dir :desc} n-name s-name p)
       (limit 100)))

(def q3
  (-> '(-> (unify (from :customer [{:xt/id c, :c-mktsegment segment}])

                  (from :orders [{:xt/id o, :o-custkey c} o-shippriority o-orderdate])
                  (where (< o-orderdate #xt/date "1995-03-15"))

                  (from :lineitem [{:l-orderkey o} l-discount l-extendedprice l-shipdate])
                  (where (> l-shipdate #xt/date "1995-03-15")))

           (aggregate {:l-orderkey o}
                      {:revenue (sum (* l-extendedprice (- 1 l-discount)))}
                      o-orderdate o-shippriority)
           (order-by {:val revenue, :dir :desc} o-orderdate)
           (limit 10))

      (with-args [{:segment "BUILDING"}])))

(def q4
  '(-> (from :orders [{:xt/id o} o-orderdate o-orderpriority])
       (where (>= o-orderdate #xt/date "1993-07-01")
              (< o-orderdate #xt/date "1993-10-01")

              (exists? (-> (from :lineitem [{:l-orderkey $o} l-commitdate l-receiptdate])
                           (where (< l-commitdate l-receiptdate)))
                       {:args [o]}))

       (aggregate o-orderpriority {:order-count (count o)})
       (order-by o-orderpriority)))

(def q5
  (-> '(-> (unify (from :orders [{:xt/id o, :o-custkey c} o-orderdate])
                  (where (>= o-orderdate #xt/date "1994-01-01")
                         (< o-orderdate #xt/date "1995-01-01"))

                  (from :lineitem [{:l-orderkey o, :l-suppkey s}
                                   l-extendedprice l-discount])

                  (from :supplier [{:xt/id s, :s-nationkey n}])
                  (from :customer [{:xt/id c, :c-nationkey n}])
                  (from :nation [{:xt/id n, :n-regionkey r} n-name])
                  (from :region [{:xt/id r, :r-name region}]))
           (aggregate n-name {:revenue (sum (* l-extendedprice (- 1 l-discount)))})
           (order-by {:val revenue, :dir :desc}))

      (with-args [{:region "ASIA"}])))

(def q6
  '(-> (unify (from :lineitem [l-shipdate l-quantity l-extendedprice l-discount])
              (where (>= l-shipdate #xt/date "1994-01-01")
                     (< l-shipdate #xt/date "1995-01-01")
                     (>= l-discount 0.05)
                     (<= l-discount 0.07)
                     (< l-quantity 24.0)))

       (aggregate {:revenue (sum (* l-extendedprice l-discount))})))

(def q7
  '(-> (unify (from :orders [{:o-custkey c}])
              (from :lineitem [{:l-orderkey o, :l-suppkey s}
                               l-shipdate l-discount l-extendedprice])

              (where (>= l-shipdate #xt/date "1995-01-01")
                     (<= l-shipdate #xt/date "1996-12-31"))

              (from :supplier [{:xt/id s, :s-nationkey n1}])
              (from :nation [{:xt/id n1, :n-name supp-nation}])
              (from :customer [{:xt/id c, :c-nationkey n2}])
              (from :nation [{:xt/id n2, :n-name cust-nation}])

              (where (or (and (= "FRANCE" supp-nation)
                              (= "GERMANY" cust-nation))
                         (and (= "GERMANY" supp-nation)
                              (= "FRANCE" cust-nation)))))

       (with {:l-year (extract "YEAR" l-shipdate)})

       (aggregate supp-nation cust-nation l-year {:revenue (sum (* l-extendedprice (- 1 l-discount)))})

       (order-by supp-nation cust-nation l-year)))

(def q8
  '(-> (unify (from :orders [{:xt/id o, :o-custkey c} o-orderdate])
              (where (>= o-orderdate #xt/date "1995-01-01")
                     (<= o-orderdate #xt/date "1996-12-31"))

              (from :lineitem [{:xt/id l, :l-orderkey o, :l-suppkey s, :l-partkey p}
                               l-extendedprice l-discount])

              (from :customer [{:xt/id c, :c-nationkey n1}])
              (from :nation [{:xt/id n1, :n-regionkey r1}])
              (from :region [{:xt/id r1, :r-name "AMERICA"}])

              (from :supplier [{:xt/id s, :s-nationkey n2}])
              (from :nation [{:xt/id n2, :n-name nation}])

              (from :part [{:xt/id p, :p-type "ECONOMY ANODIZED STEEL"}]))

       (return {:o-year (extract "YEAR" o-orderdate)}
               {:volume (* l-extendedprice (- 1 l-discount))}
               nation)

       (aggregate o-year
                  {:mkt-share (/ (sum (if (= "BRAZIL" nation) volume 0))
                                 (sum volume))})

       (order-by o-year)))

(def q9
  '(-> (unify (from :part [{:xt/id p} p-name])
              (where (like p-name "%green%"))

              (from :orders [{:xt/id o} o-orderdate])

              (from :lineitem [{:l-orderkey o, :l-suppkey s, :l-partkey p}
                               l-quantity l-extendedprice l-discount])

              (from :partsupp [{:ps-partkey p, :ps-suppkey s} ps-supplycost])

              (from :supplier [{:xt/id s, :s-nationkey n}])
              (from :nation [{:xt/id n, :n-name nation}]))

       (with {:o-year (extract "YEAR" o-orderdate)})

       (aggregate nation o-year
                  {:sum-profit (sum (- (* l-extendedprice (- 1 l-discount))
                                       (* ps-supplycost l-quantity)))})))

(def q10
  '(-> (unify (from :orders [{:xt/id o, :o-custkey c} o-orderdate])
              (where (>= o-orderdate #xt/date "1993-10-01")
                     (< o-orderdate #xt/date "1994-01-01"))

              (from :customer [{:xt/id c, :c-nationkey n}
                               c-name c-address c-phone
                               c-acctbal c-comment])

              (from :nation [{:xt/id n, :n-name n-name}])

              (from :lineitem [{:l-orderkey o, :l-returnflag "R"}
                               l-extendedprice l-discount]))

       (aggregate c c-name
                  {:revenue (sum (* l-extendedprice (- 1 l-discount)))}
                  c-acctbal c-phone n-name c-address c-comment)

       (order-by {:val revenue, :dir :desc})
       (limit 20)))

(def q11
  '(-> (unify (join (-> (unify (from :partsupp [{:ps-suppkey s} ps-availqty ps-supplycost])
                               (from :supplier [{:xt/id s, :s-nationkey n}])
                               (from :nation [{:xt/id n, :n-name "GERMANY"}]))
                        (aggregate {:total-value (sum (* ps-supplycost ps-availqty))}))

                    [total-value])

              (join (-> (unify (from :partsupp [{:ps-suppkey s} ps-availqty ps-supplycost ps-partkey])
                               (from :supplier [{:xt/id s, :s-nationkey n}])
                               (from :nation [{:xt/id n, :n-name "GERMANY"}]))
                        (aggregate ps-partkey {:value (sum (* ps-supplycost ps-availqty))}))

                    [ps-partkey value]))

       (where (> value (* 0.0001 total-value)))
       (return ps-partkey value)
       (order-by {:val value, :dir :desc})))

(def q12
  (-> '(-> (unify (from :lineitem [{:l-orderkey o} l-receiptdate l-commitdate l-shipdate l-shipmode])
                  ;; TODO `in?`
                  (where (in? l-shipmode $ship-modes)

                         (>= l-receiptdate #xt/date "1994-01-01")
                         (< l-receiptdate #xt/date "1995-01-01")
                         (< l-commitdate l-receiptdate)
                         (< l-shipdate l-commitdate))

                  (from :orders [{:xt/id o} o-orderpriority]))

           (aggregate l-shipmode
                      {:high-line-count (sum (case o-orderpriority "1-URGENT" 1, "2-HIGH" 1, 0))}
                      {:low-line-count (sum (case o-orderpriority "1-URGENT" 0, "2-HIGH" 0, 1))})

           (order-by l-shipmode))

      (with-args [{:ship-modes #{"MAIL" "SHIP"}}])))

(def q13
  '(-> (unify (from :customer [{:xt/id c}])
              (left-join (unify (from :orders [{:xt/id o, :o-custkey c} o-comment])
                                (where (not (like o-comment "%special%requests%"))))
                         [c o]))
       (aggregate c {:c-count (count o)})
       (aggregate c-count {:custdist (row-count)})
       (order-by {:val custdist, :dir :desc} {:val c-count, :dir :desc})))

(def q14
  '(-> (unify (from :lineitem [{:l-partkey p} l-shipdate l-extendedprice l-discount])
              (where (>= l-shipdate #xt/date "1995-09-01")
                     (< l-shipdate #xt/date "1995-10-01"))

              (from :part [{:xt/id p} p-type]))

       (aggregate {:promo (sum (if (like p-type "PROMO%")
                                (* l-extendedprice (- 1 l-discount))
                                0))}
                  {:total (sum (* l-extendedprice (- 1 l-discount)))})

       (return {:promo-revenue (* 100 (/ promo total))})))

(def q15
  '(letfn [(revenue []
             (q (-> (from :lineitem [{:l-suppkey s} l-shipdate l-extendedprice l-discount])
                    (where (>= l-shipdate #xt/date "1996-01-01")
                           (< l-shipdate #xt/date "1996-04-01"))
                    (aggregate s {:total-revenue (sum (* l-extendedprice (- 1 l-discount)))}))))]

     (-> (unify (call (revenue) [s total-revenue])

                (where (= total-revenue
                          (q (-> (call (revenue) [total-revenue])
                                 (aggregate {:max-revenue (max total-revenue)})))))

                (from :supplier [{:xt/id s} s-name s-address s-phone]))

         (return s s-name s-address s-phone total-revenue))))

(def q16
  (-> '(-> (unify (from :part [{:xt/id p} p-brand p-type p-size])
                  ;; TODO `in?`
                  (where (in? p-size sizes)
                         (<> p-brand "Brand#45")
                         (not (like p-type "MEDIUM POLISHED%")))

                  (from :partsupp [{:ps-partkey p, :ps-suppkey s}])

                  (where (not (exists? (-> (from :supplier [{:xt/id $s} s-comment])
                                           (where (like "%Customer%Complaints%" s-comment)))
                                       {:args [s]}))))
           (aggregate p-brand p-type p-size {:supplier-cnt (count-distinct s)})
           (order-by {:val supplier-cnt, :dir :desc} p-brand p-type p-size))

      (with-args [{:sizes #{3 9 14 19 23 36 45 49}}])))

(def q17
  '(-> (unify (from :part [{:xt/id p, :p-brand "Brand#23", :p-container "MED BOX"}])
              (from :lineitem [{:l-partkey p} l-quantity l-extendedprice])

              (join (-> (from :lineitem [{:l-partkey $p} l-quantity])
                        (aggregate {:avg-quantity (avg l-quantity)}))
                    {:args [p]
                     :bind [avg-quantity]})

              (where (< l-quantity (* 0.2 avg-quantity))))

       (aggregate {:sum-extendedprice (sum l-extendedprice)})

       (return {:avg-yearly (/ sum-extendedprice 7.0)})))

(def q18
  '(-> (unify (from :customer [{:xt/id c} c-name])
              (from :orders [{:xt/id o, :o-custkey c} o-orderdate o-totalprice]))

       ;; TODO `in?`
       (where (in? o
                   (q (-> (from :lineitem [{:l-orderkey o} l-quantity])
                          (aggregate o {:sum-quantity (sum l-quantity)})
                          (where (> sum-quantity 300.0))
                          (return o)))))

       (return c-name {:c-custkey c} {:o-orderkey o} o-orderdate o-totalprice sum-qty)

       (order-by {:val o-totalprice, :dir :desc} o-orderdate)
       (limit 100)))

(def q19
  (-> '(-> (unify (from :part [{:xt/id p} p-size p-brand p-container])

                  (from :lineitem
                        [{:l-shipinstruct "DELIVER IN PERSON", :l-partkey p}
                         l-shipmode l-discount l-extendedprice l-quantity])

                  ;; TODO `in?`
                  (where (in? l-shipmode ship-modes)

                         (or (and (= p-brand "Brand#12")
                                  ;; TODO `in?`
                                  (in? p-container #{"SM CASE" "SM BOX" "SM PACK" "SM PKG"})
                                  (>= l-quantity 1.0)
                                  (<= l-quantity 11.0)
                                  (>= p-size 1)
                                  (<= p-size 5))

                             (and (= p-brand "Brand#23")
                                  ;; TODO `in?`
                                  (in? p-container #{"MED BAG" "MED BOX" "MED PKG" "MED PACK"})
                                  (>= l-quantity 10.0)
                                  (<= l-quantity 20.0)
                                  (>= p-size 1)
                                  (<= p-size 10))

                             (and (= p-brand "Brand#34")
                                  ;; TODO `in?`
                                  (in? p-container #{"LG CASE" "LG BOX" "LG PACK" "LG PKG"})
                                  (>= l-quantity 20.0)
                                  (<= l-quantity 30.0)
                                  (>= p-size 1)
                                  (<= p-size 15)))))

           (aggregate {:revenue (sum (* l-extendedprice (- 1 l-discount)))}))

      (with-args [{:ship-modes #{"AIR" "AIR REG"}}])))

(def q20
  '(-> (unify (from :part [{:xt/id p} p-name])
              (where (like p-name "forest%"))

              (from :partsupp [{:ps-suppkey s, :ps-partkey p} ps-availqty])
              (from :supplier [{:xt/id s, :s-nationkey n} s-name s-address])
              (from :nation [{:xt/id n, :n-name "CANADA"}])

              (where (> ps-availqty
                        (* 0.5
                           (q (-> (unify (from :lineitem [{:l-partkey $p, :l-suppkey $s} l-shipdate l-quantity])
                                         (where (>= l-shipdate #xt/date "1994-01-01")
                                                (< l-shipdate #xt/date "1995-01-01")))
                                  (aggregate {:sum-quantity (sum l-quantity)}))
                              {:args [s p]})))))

       (return s-name s-address)
       (order-by s-name)))

(def q21
  '(-> (unify (from :nation [{:xt/id n, :n-name "SAUDI ARABIA"}])
              (from :supplier [{:xt/id s, :s-nationkey n} s-name])
              (from :orders [{:xt/id o, :o-orderstatus "F"}])

              (from :lineitem [{:xt/id l1, :l-suppkey s, :l-orderkey o}
                               l-receiptdate l-commitdate]))
       (where (> l-receiptdate l-commitdate)

              (exists? (-> (from :lineitem [{:l-orderkey o, :l-suppkey l2s}])
                           (where (<> $s l2s)))
                       {:args [o s]})

              (not (exists? (-> (from :lineitem [{:l-orderkey o, :l-suppkey l3s}
                                                 l-receiptdate l-commitdate])
                                (where (<> $s l3s)
                                       (> l-receiptdate l-commitdate)))
                            {:args [o s]})))

       (aggregate s-name {:numwait (count l1)})
       (order-by {:val numwait, :dir :desc} s-name)
       (limit 100)))

(def q22
  '(-> (from :customer [c-phone c-acctbal])
       ;; TODO `in?`
       (where (in? (subs c-phone 0 2) #{"13" "31" "23" "29" "30" "18" "17"})

              (> c-acctbal
                 (q (-> (from :customer [c-acctbal c-phone])
                        (where (> c-acctbal 0.0)
                               ;; TODO `in?`
                               (in? (subs c-phone 0 2) #{"13" "31" "23" "29" "30" "18" "17"}))
                        (aggregate {:avg-acctbal (avg c-acctbal)}))))

              ;; TODO `in?`
              (not (in? c (q (from :orders [{:o-custkey c}])))))

       (aggregate cntrycode
                  {:numcust (count c)}
                  {:totacctbal (sum c-acctbal)})

       (order-by cntrycode)))

(def queries
  [#'q1 #'q2 #'q3 #'q4 #'q5 #'q6 #'q7 #'q8 #'q9 #'q10 #'q11
   #'q12 #'q13 #'q14 #'q15 #'q16 #'q17 #'q18 #'q19 #'q20 #'q21 #'q22])
