SELECT
  c_orders.c_count,
  COUNT(*) AS custdist
FROM (
       SELECT
         c.c_custkey,
         COUNT(o.o_orderkey)
       FROM
         customer AS c
         LEFT OUTER JOIN orders AS o ON
                                  c.c_custkey = o.o_custkey
                                  AND o.o_comment NOT LIKE '%special%requests%'
       GROUP BY
         c.c_custkey
     ) AS c_orders (c_custkey, c_count)
GROUP BY
  c_orders.c_count
ORDER BY
  custdist DESC,
  c_orders.c_count DESC
