(ns crux.tpch-test
  (:require [clojure.test :as t]
            [crux.fixtures :as f]
            [crux.fixtures.api :as fapi :refer [*api*]]
            [crux.fixtures.calcite :as cf]
            [crux.fixtures.kv :as kvf]
            [crux.fixtures.standalone :as fs]
            [crux.fixtures.tpch :as tf])
  (:import [io.airlift.tpch TpchTable Customer Order]))

(defn- with-tpch-schema [f]
  (f/transact! *api* (tf/tpch-tables->crux-sql-schemas))
  (f))

(t/use-fixtures :each fs/with-standalone-node cf/with-calcite-module kvf/with-kv-dir fapi/with-node cf/with-calcite-connection with-tpch-schema)

(defn query [^String s]
  (cf/query (.replace s "tpch." "")))

(t/deftest test-tpch-schema
  (doseq [^TpchTable t (TpchTable/getTables)]
    (f/transact! *api* (tf/tpch-table->docs t)))

  (t/is (= 50 (count (query "SELECT * FROM tpch.customer"))))
  (t/is (= 1 (count (query "SELECT count(*) FROM tpch.lineitem"))))
  (t/is (= 5 (count (query "SELECT * FROM tpch.orders"))))
  (t/is (= 5 (count (query "SELECT * FROM tpch.part"))))
  (t/is (= 5 (count (query "SELECT * FROM tpch.partsupp"))))
  (t/is (= 3 (count (query "SELECT * FROM tpch.supplier"))))
  (t/is (= 25 (count (query "SELECT * FROM tpch.nation"))))
  (t/is (= 30 (count (query "SELECT * FROM tpch.region")))))

#_(t/deftest test-001-query
  (t/is (= 5 (query "select\n"
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
                    "  l_linestatus"
                    ))))

#_(t/deftest test-002-query
  (t/is (= 5 (query (str "select\n"
                         "  s.s_acctbal,\n"
                         "  s.s_name,\n"
                         "  n.n_name,\n"
                         "  p.p_partkey,\n"
                         "  p.p_mfgr,\n"
                         "  s.s_address,\n"
                         "  s.s_phone,\n"
                         "  s.s_comment\n"
                         "from\n"
                         "  tpch.part p,\n"
                         "  tpch.supplier s,\n"
                         "  tpch.partsupp ps,\n"
                         "  tpch.nation n,\n"
                         "  tpch.region r\n"
                         "where\n"
                         "  p.p_partkey = ps.ps_partkey\n"
                         "  and s.s_suppkey = ps.ps_suppkey\n"
                         "  and p.p_size = 41\n"
                         "  and p.p_type like '%NICKEL'\n"
                         "  and s.s_nationkey = n.n_nationkey\n"
                         "  and n.n_regionkey = r.r_regionkey\n"
                         "  and r.r_name = 'EUROPE'\n"
                         "  and ps.ps_supplycost = (\n"
                         "\n"
                         "    select\n"
                         "      min(ps.ps_supplycost)\n"
                         "\n"
                         "    from\n"
                         "      tpch.partsupp ps,\n"
                         "      tpch.supplier s,\n"
                         "      tpch.nation n,\n"
                         "      tpch.region r\n"
                         "    where\n"
                         "      p.p_partkey = ps.ps_partkey\n"
                         "      and s.s_suppkey = ps.ps_suppkey\n"
                         "      and s.s_nationkey = n.n_nationkey\n"
                         "      and n.n_regionkey = r.r_regionkey\n"
                         "      and r.r_name = 'EUROPE'\n"
                         "  )\n"
                         "\n"
                         "order by\n"
                         "  s.s_acctbal desc,\n"
                         "  n.n_name,\n"
                         "  s.s_name,\n"
                         "  p.p_partkey\n"
                         "limit 100")))))

(t/deftest test-003-query
  (doseq [^TpchTable t (TpchTable/getTables)]
    (println "Ingesting" (.getTableName t))
    (f/transact! *api* (tf/tpch-table->docs t)))

  (t/is (= 5 (query (str
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
                     "limit 10")))))
