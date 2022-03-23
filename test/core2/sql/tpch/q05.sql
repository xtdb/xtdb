SELECT
  n.n_name,
  SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
FROM
  customer AS c,
  orders AS o,
  lineitem AS l,
  supplier AS s,
  nation AS n,
  region AS r
WHERE
  c.c_custkey = o.o_custkey
  AND l.l_orderkey = o.o_orderkey
  AND l.l_suppkey = s.s_suppkey
  AND c.c_nationkey = s.s_nationkey
  AND s.s_nationkey = n.n_nationkey
  AND n.n_regionkey = r.r_regionkey
  AND r.r_name = 'ASIA'
  AND o.o_orderdate >= DATE '1994-01-01'
  AND o.o_orderdate < DATE '1994-01-01' + INTERVAL '1' YEAR
GROUP BY
n.n_name
ORDER BY
revenue DESC
