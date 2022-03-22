SELECT
  profit.nation,
  profit.o_year,
  SUM(profit.amount) AS sum_profit
FROM (
       SELECT
         n.n_name                                                                 AS nation,
         EXTRACT(YEAR FROM o.o_orderdate)                                         AS o_year,
         l.l_extendedprice * (1 - l.l_discount) - ps.ps_supplycost * l.l_quantity AS amount
       FROM
         part AS p,
         supplier AS s,
         lineitem AS l,
         partsupp AS ps,
         orders AS o,
         nation AS n
       WHERE
         s.s_suppkey = l.l_suppkey
         AND ps.ps_suppkey = l.l_suppkey
         AND ps.ps_partkey = l.l_partkey
         AND p.p_partkey = l.l_partkey
         AND o.o_orderkey = l.l_orderkey
         AND s.s_nationkey = n.n_nationkey
         AND p.p_name LIKE '%green%'
     ) AS profit
GROUP BY
  profit.nation,
  profit.o_year
ORDER BY
  profit.nation,
  profit.o_year DESC
