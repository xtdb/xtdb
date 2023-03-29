WITH revenue AS
  (SELECT
    l.l_suppkey AS supplier_no,
    SUM(l.l_extendedprice * (1 - l.l_discount)) AS total_revenue
  FROM
    lineitem AS l
  WHERE
    l.l_shipdate >= DATE '1996-01-01'
    AND l.l_shipdate < DATE '1996-01-01' + INTERVAL '3' MONTH
GROUP BY
  l.l_suppkey)

SELECT
  s.s_suppkey,
  s.s_name,
  s.s_address,
  s.s_phone,
  revenue.total_revenue
FROM
  supplier AS s,
  revenue AS revenue
WHERE
  s.s_suppkey = revenue.supplier_no
  AND revenue.total_revenue = (
    SELECT max(revenue.total_revenue)
    FROM
      revenue AS revenue
  )
ORDER BY
  s.s_suppkey
