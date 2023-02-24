SELECT
  l.l_orderkey,
  SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue,
  o.o_orderdate,
  o.o_shippriority
FROM
  customer AS c,
  orders AS o,
  lineitem AS l
WHERE
  c.c_mktsegment = 'BUILDING'
  AND c.c_custkey = o.o_custkey
  AND l.l_orderkey = o.o_orderkey
  AND o.o_orderdate < DATE '1995-03-15'
  AND l.l_shipdate > DATE '1995-03-15'
GROUP BY
  l.l_orderkey,
  o.o_orderdate,
  o.o_shippriority
ORDER BY
  revenue DESC,
  o.o_orderdate
FETCH FIRST 10 ROWS ONLY
