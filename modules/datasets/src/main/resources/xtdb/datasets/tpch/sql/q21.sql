FROM
  supplier AS  s,
  lineitem AS l1,
  orders AS o,
  nation AS  n
WHERE
  s._id = l1.l_suppkey
  AND o._id = l1.l_orderkey
  AND o.o_orderstatus = 'F'
  AND l1.l_receiptdate > l1.l_commitdate
  AND EXISTS(
    FROM lineitem l2
    WHERE
      l2.l_orderkey = l1.l_orderkey
      AND l2.l_suppkey <> l1.l_suppkey
  )
  AND NOT EXISTS(
    FROM lineitem l3
    WHERE
      l3.l_orderkey = l1.l_orderkey
      AND l3.l_suppkey <> l1.l_suppkey
      AND l3.l_receiptdate > l3.l_commitdate
  )
  AND s.s_nationkey = n._id
  AND n.n_name = 'SAUDI ARABIA'
SELECT s.s_name, COUNT(*) AS numwait
ORDER BY numwait DESC, s.s_name
FETCH FIRST 100 ROWS ONLY
