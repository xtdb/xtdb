SELECT
  l.l_returnflag,
  l.l_linestatus,
  SUM(l.l_quantity)                                           AS sum_qty,
  SUM(l.l_extendedprice)                                      AS sum_base_price,
  SUM(l.l_extendedprice * (1 - l.l_discount))                 AS sum_disc_price,
  SUM(l.l_extendedprice * (1 - l.l_discount) * (1 + l.l_tax)) AS sum_charge,
  AVG(l.l_quantity)                                           AS avg_qty,
  AVG(l.l_extendedprice)                                      AS avg_price,
  AVG(l.l_discount)                                           AS avg_disc,
  COUNT(*)                                                    AS count_order
FROM
  lineitem AS l
WHERE
  l.l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
GROUP BY
l.l_returnflag,
l.l_linestatus
ORDER BY
l.l_returnflag,
l.l_linestatus
