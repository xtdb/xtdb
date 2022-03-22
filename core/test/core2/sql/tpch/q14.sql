SELECT 100.00 * SUM(CASE
                    WHEN p.p_type LIKE 'PROMO%'
                      THEN l.l_extendedprice * (1 - l.l_discount)
                    ELSE 0
                    END) / SUM(l.l_extendedprice * (1 - l.l_discount)) AS promo_revenue
FROM
  lineitem AS l,
  part AS p
WHERE
  l.l_partkey = p.p_partkey
  AND l.l_shipdate >= DATE '1995-09-01'
  AND l.l_shipdate < DATE '1995-09-01' + INTERVAL '1' MONTH
