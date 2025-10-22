SELECT
  all_nations.o_year,
  SUM(CASE
      WHEN all_nations.nation = 'BRAZIL'
        THEN all_nations.volume
      ELSE 0
      END) / SUM(all_nations.volume) AS mkt_share
FROM (
       SELECT
         EXTRACT(YEAR FROM o.o_orderdate)       AS o_year,
         l.l_extendedprice * (1 - l.l_discount) AS volume,
         n2.n_name                              AS nation
       FROM
         part AS p,
         supplier AS s,
         lineitem AS l,
         orders AS o,
         customer AS c,
         nation AS n1,
         nation AS n2,
         region AS r
       WHERE
         p._id = l.l_partkey
         AND s._id = l.l_suppkey
         AND l.l_orderkey = o._id
         AND o.o_custkey = c._id
         AND c.c_nationkey = n1._id
         AND n1.n_regionkey = r._id
         AND r.r_name = 'AMERICA'
         AND s.s_nationkey = n2._id
         AND o.o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
         AND p.p_type = 'ECONOMY ANODIZED STEEL'
     ) AS all_nations
GROUP BY
  all_nations.o_year
ORDER BY
  all_nations.o_year
