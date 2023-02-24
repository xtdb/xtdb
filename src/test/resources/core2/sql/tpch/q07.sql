SELECT
  shipping.supp_nation,
  shipping.cust_nation,
  shipping.l_year,
  SUM(shipping.volume) AS revenue
FROM (
       SELECT
         n1.n_name                              AS supp_nation,
         n2.n_name                              AS cust_nation,
         EXTRACT(YEAR FROM l.l_shipdate)        AS l_year,
         l.l_extendedprice * (1 - l.l_discount) AS volume
       FROM
         supplier AS s,
         lineitem AS l,
         orders AS o,
         customer AS c,
         nation AS n1,
         nation AS n2
       WHERE
         s.s_suppkey = l.l_suppkey
         AND o.o_orderkey = l.l_orderkey
         AND c.c_custkey = o.o_custkey
         AND s.s_nationkey = n1.n_nationkey
         AND c.c_nationkey = n2.n_nationkey
         AND (
           (n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
           OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')
         )
         AND l.l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
     ) AS shipping
GROUP BY
  shipping.supp_nation,
  shipping.cust_nation,
  shipping.l_year
ORDER BY
  shipping.supp_nation,
  shipping.cust_nation,
  shipping.l_year
