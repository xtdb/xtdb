FROM (
       FROM
         customer AS c
       WHERE
         SUBSTRING(c.c_phone FROM 1 FOR 2) IN
         ('13', '31', '23', '29', '30', '18', '17')
         AND c.c_acctbal > (
           FROM customer AS c
           WHERE
             c.c_acctbal > 0.00
             AND SUBSTRING(c.c_phone FROM 1 FOR 2) IN
                 ('13', '31', '23', '29', '30', '18', '17')
           SELECT AVG(c.c_acctbal)
         )
         AND NOT EXISTS(
           FROM orders AS o
           WHERE o.o_custkey = c.c_custkey
         )
       SELECT
         SUBSTRING(c.c_phone FROM 1 FOR 2) AS cntrycode,
         c.c_acctbal
     ) AS custsale
SELECT
  custsale.cntrycode,
  COUNT(*)                AS numcust,
  SUM(custsale.c_acctbal) AS totacctbal
ORDER BY custsale.cntrycode
