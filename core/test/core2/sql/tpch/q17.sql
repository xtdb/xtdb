SELECT SUM(l.l_extendedprice) / 7.0 AS avg_yearly
FROM
  lineitem AS l,
  part AS p
WHERE
  p.p_partkey = l.l_partkey
  AND p.p_brand = 'Brand#23'
  AND p.p_container = 'MED BOX'
  AND l.l_quantity < (
    SELECT 0.2 * AVG(l.l_quantity)
    FROM
      lineitem AS l
    WHERE
      l.l_partkey = p.p_partkey
  )
