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

(t/use-fixtures :each fs/with-standalone-node cf/with-calcite-module kvf/with-kv-dir fapi/with-node cf/with-calcite-connection with-tpch-schema)

(defn query [^String s]
  (cf/query (.replace s "tpch." "")))

(t/deftest test-tpch-schema
  (doseq [^TpchTable t (TpchTable/getTables)]
    (f/transact! *api* (take 5 (tf/tpch-table->docs t))))

  (t/is (= 5 (count (query "SELECT * FROM tpch.customer"))))
  (t/is (= 5 (count (query "SELECT * FROM tpch.orders"))))
  (t/is (= 5 (count (query "SELECT * FROM tpch.part"))))
  (t/is (= 2 (count (query "SELECT * FROM tpch.partsupp"))))
  (t/is (= 5 (count (query "SELECT * FROM tpch.supplier"))))
  (t/is (= 5 (count (query "SELECT * FROM tpch.nation"))))
  (t/is (= 10 (count (query "SELECT * FROM tpch.region")))))

(t/deftest test-001-query
  (doseq [^TpchTable t (TpchTable/getTables)]
    (println "Ingesting" (.getTableName t))
    (f/transact! *api* (tf/tpch-table->docs t)))

  (t/is (= 4 (count (query (str "select\n"
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
                                "  l_linestatus"))))))

(t/deftest test-003-query
  (doseq [^TpchTable t (TpchTable/getTables)]
    (println "Ingesting" (.getTableName t))
    (f/transact! *api* (tf/tpch-table->docs t)))

  (t/is (= 10 (count (query (str
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
                             "limit 10"))))))

;; Hangs:
#_(t/deftest test-007-query
  (doseq [^TpchTable t  (TpchTable/getTables)]
    (println "Ingesting" (.getTableName t))
    (f/transact! *api* (take 10 (tf/tpch-table->docs t))))

  (t/is (= :a (query (str "select\n"
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
                          "  l_year")))))
