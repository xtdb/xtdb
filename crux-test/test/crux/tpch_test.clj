(ns crux.tpch-test
  (:require [clojure.test :as t]
            [crux.fixtures :as f]
            [crux.fixtures.api :as fapi :refer [*api*]]
            [crux.fixtures.calcite :as cf]
            [crux.fixtures.kv :as kvf]
            [crux.fixtures.standalone :as fs]
            [crux.fixtures.tpch :as tf])
  (:import io.airlift.tpch.TpchTable))

;; Transaction Processing Performance Council
;; http://www.tpc.org/

(defn- with-tpch-schema [f]
  (f/transact! *api* (tf/tpch-tables->crux-sql-schemas))
  (f))

(defn- with-tpch-dataset [f]
  (doseq [^TpchTable t (TpchTable/getTables)]
    ;; Using a scale-factor of .005 and no take, takes ~3 mins combined for all tests
    (f/transact! *api* (take 10 (tf/tpch-table->docs t))))
  (f))

(t/use-fixtures :each fs/with-standalone-node cf/with-calcite-module kvf/with-kv-dir fapi/with-node cf/with-calcite-connection with-tpch-schema with-tpch-dataset)

(defn query [^String s]
  (cf/query (.replace s "tpch." "")))

(t/deftest test-tpch-schema
  (t/is (= 10 (count (query "SELECT * FROM tpch.customer"))))
  (t/is (= 10 (count (query "SELECT * FROM tpch.orders"))))
  (t/is (= 10 (count (query "SELECT * FROM tpch.part"))))
  (t/is (= 3 (count (query "SELECT * FROM tpch.partsupp"))))
  (t/is (= 10 (count (query "SELECT * FROM tpch.supplier"))))
  (t/is (= 10 (count (query "SELECT * FROM tpch.nation"))))
  (t/is (= 15 (count (query "SELECT * FROM tpch.region")))))

(t/deftest test-001
  (t/is (query (str "select\n"
                    "  l_returnflag,\n"
                    "  l_linestatus,\n"
                    "  sum(l_quantity) as sum_qty,\n"
                    "  sum(l_extendedprice) as sum_base_price,\n"
                    "  sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,\n"
                    "  sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,\n"
                    "  avg(l_quantity) as avg_qty,\n"
                    "  avg(l_extendedprice) as avg_price,\n"
                    "  avg(l_discount) as avg_disc,\n"
                    "  count(*) as count_order\n"
                    "from\n"
                    "  tpch.lineitem\n"
                    "-- where\n"
                    "--  l_shipdate <= date '1998-12-01' - interval '120' day (3)\n"
                    "group by\n"
                    "  l_returnflag,\n"
                    "  l_linestatus\n"
                    "\n"
                    "order by\n"
                    "  l_returnflag,\n"
                    "  l_linestatus"))))

;; Skipping 2: Calcite: @Disabled("Infinite planning")

(t/deftest test-003
  (query (str
          "select\n"
          "  l.l_orderkey,\n"
          "  sum(l.l_extendedprice * (1 - l.l_discount)) as revenue,\n"
          "  o.o_orderdate,\n"
          "  o.o_shippriority\n"
          "\n"
          "from\n"
          "  tpch.customer c,\n"
          "  tpch.orders o,\n"
          "  tpch.lineitem l\n"
          "\n"
          "where\n"
          "  c.c_mktsegment = 'HOUSEHOLD'\n"
          "  and c.c_custkey = o.o_custkey\n"
          "  and l.l_orderkey = o.o_orderkey\n"
          "--  and o.o_orderdate < date '1995-03-25'\n"
          "--  and l.l_shipdate > date '1995-03-25'\n"
          "\n"
          "group by\n"
          "  l.l_orderkey,\n"
          "  o.o_orderdate,\n"
          "  o.o_shippriority\n"
          "order by\n"
          "  revenue desc,\n"
          "  o.o_orderdate\n"
          "limit 10"))
  (t/is true))

;; Skipping 4: Calcite: @Disabled("NoSuchMethodException: SqlFunctions.lt(Date, Date)")
;; Skipping 5: Calcite: @Disabled("OutOfMemoryError")

(t/deftest test-006
  (t/is (query (str "select\n"
                    "  sum(l_extendedprice * l_discount) as revenue\n"
                    "from\n"
                    "  tpch.lineitem\n"
                    "where\n"
                    "--  l_shipdate >= date '1997-01-01'\n"
                    "--  and l_shipdate < date '1997-01-01' + interval '1' year\n"
                    "--  and\n"
                    "  l_discount between 0.03 - 0.01 and 0.03 + 0.01\n"
                    "  and l_quantity < 24"))))

;; Runs but slow
#_(t/deftest test-007
  (t/is (query (str "select\n"
                    "  supp_nation,\n"
                    "  cust_nation,\n"
                    "  l_year,\n"
                    "  sum(volume) as revenue\n"
                    "from\n"
                    "  (\n"
                    "    select\n"
                    "      n1.n_name as supp_nation,\n"
                    "      n2.n_name as cust_nation,\n"
                    "      extract(year from l.l_shipdate) as l_year,\n"
                    "      l.l_extendedprice * (1 - l.l_discount) as volume\n"
                    "    from\n"
                    "      tpch.supplier s,\n"
                    "      tpch.lineitem l,\n"
                    "      tpch.orders o,\n"
                    "      tpch.customer c,\n"
                    "      tpch.nation n1,\n"
                    "      tpch.nation n2\n"
                    "    where\n"
                    "      s.s_suppkey = l.l_suppkey\n"
                    "      and o.o_orderkey = l.l_orderkey\n"
                    "      and c.c_custkey = o.o_custkey\n"
                    "      and s.s_nationkey = n1.n_nationkey\n"
                    "      and c.c_nationkey = n2.n_nationkey\n"
                    "      and (\n"
                    "        (n1.n_name = 'EGYPT' and n2.n_name = 'UNITED STATES')\n"
                    "        or (n1.n_name = 'UNITED STATES' and n2.n_name = 'EGYPT')\n"
                    "      )\n"
                    "--      and l.l_shipdate between date '1995-01-01' and date '1996-12-31'\n"
                    "  ) as shipping\n"
                    "group by\n"
                    "  supp_nation,\n"
                    "  cust_nation,\n"
                    "  l_year\n"
                    "order by\n"
                    "  supp_nation,\n"
                    "  cust_nation,\n"
                    "  l_year"))))

;; Runs but slow
#_(t/deftest test-008
  (query (str "select\n"
              "  o_year,\n"
              "  sum(case\n"
              "    when nation = 'EGYPT' then volume\n"
              "    else 0\n"
              "  end) / sum(volume) as mkt_share\n"
              "from\n"
              "  (\n"
              "    select\n"
              "      extract(year from o.o_orderdate) as o_year,\n"
              "      l.l_extendedprice * (1 - l.l_discount) as volume,\n"
              "      n2.n_name as nation\n"
              "    from\n"
              "      tpch.part p,\n"
              "      tpch.supplier s,\n"
              "      tpch.lineitem l,\n"
              "      tpch.orders o,\n"
              "      tpch.customer c,\n"
              "      tpch.nation n1,\n"
              "      tpch.nation n2,\n"
              "      tpch.region r\n"
              "    where\n"
              "      p.p_partkey = l.l_partkey\n"
              "      and s.s_suppkey = l.l_suppkey\n"
              "      and l.l_orderkey = o.o_orderkey\n"
              "      and o.o_custkey = c.c_custkey\n"
              "      and c.c_nationkey = n1.n_nationkey\n"
              "      and n1.n_regionkey = r.r_regionkey\n"
              "      and r.r_name = 'MIDDLE EAST'\n"
              "      and s.s_nationkey = n2.n_nationkey\n"
              "      and o.o_orderdate between date '1995-01-01' and date '1996-12-31'\n"
              "      and p.p_type = 'PROMO BRUSHED COPPER'\n"
              "  ) as all_nations\n"
              "group by\n"
              "  o_year\n"
              "order by\n"
              "  o_year"))
    (t/is true))

;; Skipping 9: Calcite: @Disabled("no method found")

(t/deftest test-010
  (query (str "select\n"
              "  c.c_custkey,\n"
              "  c.c_name,\n"
              "  sum(l.l_extendedprice * (1 - l.l_discount)) as revenue,\n"
              "  c.c_acctbal,\n"
              "  n.n_name,\n"
              "  c.c_address,\n"
              "  c.c_phone,\n"
              "  c.c_comment\n"
              "from\n"
              "  tpch.customer c,\n"
              "  tpch.orders o,\n"
              "  tpch.lineitem l,\n"
              "  tpch.nation n\n"
              "where\n"
              "  c.c_custkey = o.o_custkey\n"
              "  and l.l_orderkey = o.o_orderkey\n"
              "  and o.o_orderdate >= date '1994-03-01'\n"
              "  and o.o_orderdate < date '1994-03-01' + interval '3' month\n"
              "  and l.l_returnflag = 'R'\n"
              "  and c.c_nationkey = n.n_nationkey\n"
              "group by\n"
              "  c.c_custkey,\n"
              "  c.c_name,\n"
              "  c.c_acctbal,\n"
              "  c.c_phone,\n"
              "  n.n_name,\n"
              "  c.c_address,\n"
              "  c.c_comment\n"
              "order by\n"
              "  revenue desc\n"
              "limit 20"))
  (t/is true))

;; Skipping 11: Calcite: @Disabled("CannotPlanException")
;; Skipping 12: Calcite: @Disabled("NoSuchMethodException: SqlFunctions.lt(Date, Date)")
;; Skipping 13: Calcite: @Disabled("CannotPlanException")

(t/deftest test-014
  (t/is (= [{:promo_revenue nil}]
           (query (str "select\n"
                       "  100.00 * sum(case\n"
                       "    when p.p_type like 'PROMO%'\n"
                       "      then l.l_extendedprice * (1 - l.l_discount)\n"
                       "    else 0\n"
                       "  end) / sum(l.l_extendedprice * (1 - l.l_discount)) as promo_revenue\n"
                       "from\n"
                       "  tpch.lineitem l,\n"
                       "  tpch.part p\n"
                       "where\n"
                       "  l.l_partkey = p.p_partkey\n"
                       "  and l.l_shipdate >= date '1994-08-01'\n"
                       "  and l.l_shipdate < date '1994-08-01' + interval '1' month")))))

;; Skipping 15: Calcite: @Disabled("AssertionError")

(t/deftest test-016
  (query (str "select\n"
              "  p.p_brand,\n"
              "  p.p_type,\n"
              "  p.p_size,\n"
              "  count(distinct ps.ps_suppkey) as supplier_cnt\n"
              "from\n"
              "  tpch.partsupp ps,\n"
              "  tpch.part p\n"
              "where\n"
              "  p.p_partkey = ps.ps_partkey\n"
              "  and p.p_brand <> 'Brand#21'\n"
              "  and p.p_type not like 'MEDIUM PLATED%'\n"
              "  and p.p_size in (38, 2, 8, 31, 44, 5, 14, 24)\n"
              "  and ps.ps_suppkey not in (\n"
              "    select\n"
              "      s_suppkey\n"
              "    from\n"
              "      tpch.supplier\n"
              "    where\n"
              "      s_comment like '%Customer%Complaints%'\n"
              "  )\n"
              "group by\n"
              "  p.p_brand,\n"
              "  p.p_type,\n"
              "  p.p_size\n"
              "order by\n"
              "  supplier_cnt desc,\n"
              "  p.p_brand,\n"
              "  p.p_type"))
  (t/is true))

(t/deftest test-017
  (query (str "select\n"
              "sum(l.l_extendedprice) / 7.0 as avg_yearly\n"
              "from\n"
              "tpch.lineitem l,\n"
              "  tpch.part p\n"
              "where\n"
              "  p.p_partkey = l.l_partkey\n"
              "  and p.p_brand = 'Brand#13'\n"
              "  and p.p_container = 'JUMBO CAN'\n"
              "  and l.l_quantity < (\n"
              "    select\n"
              "      0.2 * avg(l2.l_quantity)\n"
              "    from\n"
              "      tpch.lineitem l2\n"
              "    where\n"
              "      l2.l_partkey = p.p_partkey\n"
              ")"))
  (t/is true))

(t/deftest test-018
  (query (str "select\n"
              "  c.c_name,\n"
              "  c.c_custkey,\n"
              "  o.o_orderkey,\n"
              "  o.o_orderdate,\n"
              "  o.o_totalprice,\n"
              "  sum(l.l_quantity)\n"
              "from\n"
              "  tpch.customer c,\n"
              "  tpch.orders o,\n"
              "  tpch.lineitem l\n"
              "where\n"
              "  o.o_orderkey in (\n"
              "    select\n"
              "      l_orderkey\n"
              "    from\n"
              "      tpch.lineitem\n"
              "    group by\n"
              "      l_orderkey having\n"
              "        sum(l_quantity) > 313\n"
              "  )\n"
              "  and c.c_custkey = o.o_custkey\n"
              "  and o.o_orderkey = l.l_orderkey\n"
              "group by\n"
              "  c.c_name,\n"
              "  c.c_custkey,\n"
              "  o.o_orderkey,\n"
              "  o.o_orderdate,\n"
              "  o.o_totalprice\n"
              "order by\n"
              "  o.o_totalprice desc,\n"
              "  o.o_orderdate\n"
              "limit 100"))
  (t/is true))

;; Skipping 19: Calcite: @Timeout(value = 10, unit = TimeUnit.MINUTES)

(t/deftest test-020
  (query (str "select\n"
              "  s.s_name,\n"
              "  s.s_address\n"
              "from\n"
              "  tpch.supplier s,\n"
              "  tpch.nation n\n"
              "where\n"
              "  s.s_suppkey in (\n"
              "    select\n"
              "      ps.ps_suppkey\n"
              "    from\n"
              "      tpch.partsupp ps\n"
              "    where\n"
              "      ps. ps_partkey in (\n"
              "        select\n"
              "          p.p_partkey\n"
              "        from\n"
              "          tpch.part p\n"
              "        where\n"
              "          p.p_name like 'antique%'\n"
              "      )\n"
              "      and ps.ps_availqty > (\n"
              "        select\n"
              "          0.5 * sum(l.l_quantity)\n"
              "        from\n"
              "          tpch.lineitem l\n"
              "        where\n"
              "          l.l_partkey = ps.ps_partkey\n"
              "          and l.l_suppkey = ps.ps_suppkey\n"
              "          and l.l_shipdate >= date '1993-01-01'\n"
              "          and l.l_shipdate < date '1993-01-01' + interval '1' year\n"
              "      )\n"
              "  )\n"
              "  and s.s_nationkey = n.n_nationkey\n"
              "  and n.n_name = 'KENYA'\n"
              "order by\n"
              "  s.s_name"))
  (t/is true))

(t/deftest test-021
  (query (str "select\n"
              "  s.s_name,\n"
              "  count(*) as numwait\n"
              "from\n"
              "  tpch.supplier s,\n"
              "  tpch.lineitem l1,\n"
              "  tpch.orders o,\n"
              "  tpch.nation n\n"
              "where\n"
              "  s.s_suppkey = l1.l_suppkey\n"
              "  and o.o_orderkey = l1.l_orderkey\n"
              "  and o.o_orderstatus = 'F'\n"
              "  and l1.l_receiptdate > l1.l_commitdate\n"
              "  and exists (\n"
              "    select\n"
              "      *\n"
              "    from\n"
              "      tpch.lineitem l2\n"
              "    where\n"
              "      l2.l_orderkey = l1.l_orderkey\n"
              "      and l2.l_suppkey <> l1.l_suppkey\n"
              "  )\n"
              "  and not exists (\n"
              "    select\n"
              "      *\n"
              "    from\n"
              "      tpch.lineitem l3\n"
              "    where\n"
              "      l3.l_orderkey = l1.l_orderkey\n"
              "      and l3.l_suppkey <> l1.l_suppkey\n"
              "      and l3.l_receiptdate > l3.l_commitdate\n"
              "  )\n"
              "  and s.s_nationkey = n.n_nationkey\n"
              "  and n.n_name = 'BRAZIL'\n"
              "group by\n"
              "  s.s_name\n"
              "order by\n"
              "  numwait desc,\n"
              "  s.s_name\n"
              "limit 100"))
  (t/is true))

(t/deftest test-022
  (query (str "select\n"
              "  cntrycode,\n"
              "  count(*) as numcust,\n"
              "  sum(c_acctbal) as totacctbal\n"
              "from\n"
              "  (\n"
              "    select\n"
              "      substring(c_phone from 1 for 2) as cntrycode,\n"
              "      c_acctbal\n"
              "    from\n"
              "      tpch.customer c\n"
              "    where\n"
              "      substring(c_phone from 1 for 2) in\n"
              "        ('24', '31', '11', '16', '21', '20', '34')\n"
              "      and c_acctbal > (\n"
              "        select\n"
              "          avg(c_acctbal)\n"
              "        from\n"
              "          tpch.customer\n"
              "        where\n"
              "          c_acctbal > 0.00\n"
              "          and substring(c_phone from 1 for 2) in\n"
              "            ('24', '31', '11', '16', '21', '20', '34')\n"
              "      )\n"
              "      and not exists (\n"
              "        select\n"
              "          *\n"
              "        from\n"
              "          tpch.orders o\n"
              "        where\n"
              "          o.o_custkey = c.c_custkey\n"
              "      )\n"
              "  ) as custsale\n"
              "group by\n"
              "  cntrycode\n"
              "order by\n"
              "  cntrycode"))
  (t/is true))
