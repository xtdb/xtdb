(ns crux.tpch-test
  (:require [clojure.test :as t]
            [crux.fixtures :as fix :refer [*api*]]
            [crux.fixtures.calcite :as cf]
            [crux.fixtures.tpch :as tf])
  (:import io.airlift.tpch.TpchTable))

;; Transaction Processing Performance Council
;; http://www.tpc.org/

(defn- with-tpch-dataset [f]
  (doseq [^TpchTable t (TpchTable/getTables)]
    ;; Using a scale-factor of .005 and no take, takes ~3 mins combined for all tests
    (fix/transact! *api* (take 10 (tf/tpch-table->docs t))))
  (f))

(t/use-fixtures :each cf/with-calcite-module fix/with-node cf/with-calcite-connection tf/with-tpch-schema with-tpch-dataset)

(defn query [^String s]
  (cf/query (.replace s "tpch." "")))

(defn explain [^String s]
  (cf/explain (.replace s "tpch." "")))

(t/deftest test-tpch-schema
  (t/is (= 10 (count (query "SELECT * FROM tpch.customer"))))
  (t/is (= 10 (count (query "SELECT * FROM tpch.orders"))))
  (t/is (= 10 (count (query "SELECT * FROM tpch.part"))))
  (t/is (= 10 (count (query "SELECT * FROM tpch.partsupp"))))
  (t/is (= 10 (count (query "SELECT * FROM tpch.supplier"))))
  (t/is (= 10 (count (query "SELECT * FROM tpch.nation"))))
  (t/is (= 5 (count (query "SELECT * FROM tpch.region")))))

(t/deftest test-001
  (let [q (str "select\n"
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
               "  l_linestatus")]
    (t/is (query q))
    (t/is (= (str "EnumerableCalc(expr#0..12=[{inputs}], expr#13=[0], expr#14=[=($t3, $t13)], expr#15=[null:DOUBLE], expr#16=[CASE($t14, $t15, $t2)], expr#17=[=($t5, $t13)], expr#18=[CASE($t17, $t15, $t4)], expr#19=[=($t7, $t13)], expr#20=[CASE($t19, $t15, $t6)], expr#21=[=($t9, $t13)], expr#22=[CASE($t21, $t15, $t8)], expr#23=[/($t16, $t3)], expr#24=[/($t18, $t5)], expr#25=[=($t11, $t13)], expr#26=[CASE($t25, $t15, $t10)], expr#27=[/($t26, $t11)], proj#0..1=[{exprs}], SUM_QTY=[$t16], SUM_BASE_PRICE=[$t18], SUM_DISC_PRICE=[$t20], SUM_CHARGE=[$t22], AVG_QTY=[$t23], AVG_PRICE=[$t24], AVG_DISC=[$t27], COUNT_ORDER=[$t12])\n"
                  "  EnumerableSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[ASC])\n"
                  "    EnumerableAggregate(group=[{0, 1}], SUM_QTY=[$SUM0($2)], agg#1=[COUNT($2)], SUM_BASE_PRICE=[$SUM0($3)], agg#3=[COUNT($3)], SUM_DISC_PRICE=[$SUM0($4)], agg#5=[COUNT($4)], SUM_CHARGE=[$SUM0($5)], agg#7=[COUNT($5)], agg#8=[$SUM0($6)], agg#9=[COUNT($6)], COUNT_ORDER=[COUNT()])\n"
                  "      CruxToEnumerableConverter\n"
                  "        CruxProject(L_RETURNFLAG=[$8], L_LINESTATUS=[$9], L_QUANTITY=[$4], L_EXTENDEDPRICE=[$5], $f4=[*($5, -(1, $6))], $f5=[*(*($5, -(1, $6)), +(1, $7))], L_DISCOUNT=[$6])\n"
                  "          CruxTableScan(table=[[crux, LINEITEM]])\n")
             (explain q)))))

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
  (let [q (str "select\n"
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
               "  p.p_type")]
    (query q)
    (t/is (= (str "EnumerableSort(sort0=[$3], sort1=[$0], sort2=[$1], dir0=[DESC], dir1=[ASC], dir2=[ASC])\n"
                  "  EnumerableAggregate(group=[{1, 2, 3}], SUPPLIER_CNT=[COUNT($0)])\n    EnumerableAggregate(group=[{1, 3, 4, 5}])\n"
                  "      EnumerableCalc(expr#0..9=[{inputs}], expr#10=[0], expr#11=[=($t6, $t10)], expr#12=[IS NULL($t9)], expr#13=[>=($t7, $t6)], expr#14=[IS NOT NULL($t1)], expr#15=[AND($t12, $t13, $t14)], expr#16=[OR($t11, $t15)], proj#0..9=[{exprs}], $condition=[$t16])\n"
                  "        EnumerableHashJoin(condition=[=($1, $8)], joinType=[left])\n"
                  "          EnumerableNestedLoopJoin(condition=[true], joinType=[inner])\n"
                  "            CruxToEnumerableConverter\n"
                  "              CruxJoin(condition=[=($2, $0)], joinType=[inner])\n"
                  "                CruxProject(PS_PARTKEY=[$0], PS_SUPPKEY=[$1])\n"
                  "                  CruxTableScan(table=[[crux, PARTSUPP]])\n"
                  "                CruxProject(P_PARTKEY=[$0], P_BRAND=[$3], P_TYPE=[$4], P_SIZE=[$5])\n"
                  "                  CruxFilter(condition=[AND(<>($3, 'Brand#21'), OR(=($5, 38), =($5, 2), =($5, 8), =($5, 31), =($5, 44), =($5, 5), =($5, 14), =($5, 24)), NOT(LIKE($4, 'MEDIUM PLATED%')))])\n"
                  "                    CruxTableScan(table=[[crux, PART]])\n"
                  "            EnumerableAggregate(group=[{}], c=[COUNT()], ck=[COUNT($0)])\n"
                  "              CruxToEnumerableConverter\n"
                  "                CruxFilter(condition=[LIKE($6, '%Customer%Complaints%')])\n"
                  "                  CruxTableScan(table=[[crux, SUPPLIER]])\n"
                  "          EnumerableAggregate(group=[{0, 1}])\n"
                  "            CruxToEnumerableConverter\n"
                  "              CruxProject(S_SUPPKEY=[$0], i=[true])\n"
                  "                CruxFilter(condition=[LIKE($6, '%Customer%Complaints%')])\n"
                  "                  CruxTableScan(table=[[crux, SUPPLIER]])\n")
             (explain q))))
  (t/is true))

(t/deftest test-017
  (let [q (str "select\n"
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
              ")")]
    (query q)
    (t/is (= (str "EnumerableCalc(expr#0=[{inputs}], expr#1=[7.0:DECIMAL(2, 1)], expr#2=[/($t0, $t1)], AVG_YEARLY=[$t2])\n"
                  "  EnumerableAggregate(group=[{}], agg#0=[SUM($7)])\n"
                  "    EnumerableHashJoin(condition=[AND(=($0, $2), <($6, *(0.2:DECIMAL(2, 1), $1)))], joinType=[inner])\n"
                  "      EnumerableCalc(expr#0..2=[{inputs}], expr#3=[0], expr#4=[=($t2, $t3)], expr#5=[null:DOUBLE], expr#6=[CASE($t4, $t5, $t1)], expr#7=[/($t6, $t2)], L_PARTKEY=[$t0], $f1=[$t7])\n"
                  "        EnumerableAggregate(group=[{1}], agg#0=[$SUM0($4)], agg#1=[COUNT($4)])\n"
                  "          CruxToEnumerableConverter\n"
                  "            CruxFilter(condition=[IS NOT NULL($1)])\n"
                  "              CruxTableScan(table=[[crux, LINEITEM]])\n"
                  "      CruxToEnumerableConverter\n"
                  "        CruxJoin(condition=[=($0, $3)], joinType=[inner])\n"
                  "          CruxProject(P_PARTKEY=[$0], P_BRAND=[$3], P_CONTAINER=[$6])\n"
                  "            CruxFilter(condition=[AND(=($3, 'Brand#13'), =($6, 'JUMBO CAN'))])\n"
                  "              CruxTableScan(table=[[crux, PART]])\n"
                  "          CruxProject(L_PARTKEY=[$1], L_QUANTITY=[$4], L_EXTENDEDPRICE=[$5])\n"
                  "            CruxTableScan(table=[[crux, LINEITEM]])\n")
             (explain q))))
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
