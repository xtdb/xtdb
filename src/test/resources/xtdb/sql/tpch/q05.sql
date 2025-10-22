FROM
  customer AS c,
  orders AS o,
  lineitem AS l,
  supplier AS s,
  nation AS n,
  region AS r
WHERE
  c._id = o.o_custkey
  AND l.l_orderkey = o._id
  AND l.l_suppkey = s._id
  AND c.c_nationkey = s.s_nationkey
  AND s.s_nationkey = n._id
  AND n.n_regionkey = r._id
  AND r.r_name = 'ASIA'
  AND o.o_orderdate >= DATE '1994-01-01'
  AND o.o_orderdate < DATE '1994-01-01' + INTERVAL '1' YEAR
SELECT
  n.n_name,
  SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
ORDER BY revenue DESC
