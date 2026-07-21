(ns xtdb.pgwire.pgjdbc-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.test-util :as tu]))

(t/use-fixtures :each tu/with-node)

(t/deftest get-columns-test
  ;; pgjdbc's DatabaseMetaData.getColumns - what a driver runs to sync a table's columns.
  ;; The LEFT JOIN onto pg_attrdef (adrelid/adnum) fetches column defaults; we model no
  ;; defaults, so pg_attrdef is empty, but the probe must plan for schema sync to work.
  (xt/execute-tx tu/*node* [[:sql "INSERT INTO foo (_id, name) VALUES (1, 'test')"]])
  (t/is (= #{"_id" "_system_from" "_system_to" "_valid_from" "_valid_to" "name"}
           (into #{} (map :attname)
                 (xt/q tu/*node*
                       "SELECT * FROM (
                          SELECT n.nspname, c.relname, a.attname, a.atttypid,
                                 a.attnotnull OR (t.typtype = 'd' AND t.typnotnull) AS attnotnull,
                                 a.atttypmod, a.attlen, t.typtypmod,
                                 row_number() OVER (PARTITION BY a.attrelid ORDER BY a.attnum) AS attnum,
                                 nullif(a.attidentity, '') AS attidentity,
                                 nullif(a.attgenerated, '') AS attgenerated,
                                 pg_catalog.pg_get_expr(def.adbin, def.adrelid) AS adsrc,
                                 dsc.description, t.typbasetype, t.typtype
                          FROM pg_catalog.pg_namespace n
                          JOIN pg_catalog.pg_class c ON (c.relnamespace = n.oid)
                          JOIN pg_catalog.pg_attribute a ON (a.attrelid = c.oid)
                          JOIN pg_catalog.pg_type t ON (a.atttypid = t.oid)
                          LEFT JOIN pg_catalog.pg_attrdef def ON (a.attrelid = def.adrelid AND a.attnum = def.adnum)
                          LEFT JOIN pg_catalog.pg_description dsc ON (c.oid = dsc.objoid AND a.attnum = dsc.objsubid)
                          LEFT JOIN pg_catalog.pg_class dc ON (dc.oid = dsc.classoid AND dc.relname = 'pg_class')
                          LEFT JOIN pg_catalog.pg_namespace dn ON (dc.relnamespace = dn.oid AND dn.nspname = 'pg_catalog')
                          WHERE c.relkind IN ('r','p','v','f','m') AND a.attnum > 0 AND NOT a.attisdropped
                            AND n.nspname LIKE 'public' AND c.relname LIKE '%') c
                        WHERE true
                        ORDER BY nspname, c.relname, attnum")))
        "pgjdbc getColumns probe plans, including the LEFT JOIN onto pg_attrdef"))
