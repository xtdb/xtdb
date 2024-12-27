FROM customer AS c
  LEFT JOIN orders AS o
    ON c.c_custkey = o.o_custkey AND o.o_comment NOT LIKE '%special%requests%'
SELECT c.c_custkey, COUNT(o.o_orderkey) AS c_count
SELECT c_count, COUNT(*) AS custdist
ORDER BY custdist DESC, c_count DESC
