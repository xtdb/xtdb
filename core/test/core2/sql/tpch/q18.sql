SELECT
  c_name,
  c_custkey,
  o_orderkey,
  o_orderdate,
  o_totalprice,
  SUM(l_quantity)
FROM
  customer,
  orders,
  lineitem
WHERE
  o_orderkey IN (
    SELECT l_orderkey
    FROM
      lineitem
    GROUP BY
      l_orderkey
    HAVING
      SUM(l_quantity) > 300
  )
  AND c_custkey = o_custkey
  AND o_orderkey = l_orderkey
GROUP BY
  c_name,
  c_custkey,
  o_orderkey,
  o_orderdate,
  o_totalprice
ORDER BY
  o_totalprice DESC,
  o_orderdate
FETCH FIRST 100 ROWS ONLY
