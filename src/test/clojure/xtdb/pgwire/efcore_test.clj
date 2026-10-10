(ns xtdb.pgwire.efcore-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.test-util :as tu]))

(t/use-fixtures :each tu/with-node)

(t/deftest scaffolder-database-collation-probe-test
  (t/is (= []
           (xt/q tu/*node* "SELECT datcollate
FROM pg_database
WHERE datname=current_database() AND datcollate <> (SELECT datcollate FROM pg_database WHERE datname='template1')"))
        "no template1 to differ from, so the database has no collation of its own")

  (t/is (= [{:datcollate "C"}]
           (xt/q tu/*node* "SELECT datcollate FROM pg_database WHERE datname = current_database()"))))

(t/deftest scaffolder-opclass-probe-test
  (t/is (= [] (xt/q tu/*node* "SELECT oid, opcname, opcdefault FROM pg_opclass"))))

(t/deftest scaffolder-collation-probes-test
  (t/is (= [] (xt/q tu/*node* "SELECT oid, collname FROM pg_collation")))

  (t/is (= []
           (xt/q tu/*node* "SELECT
    nspname, collname, collprovider, collcollate, collctype,
    colliculocale AS colllocale,
    collisdeterministic
FROM pg_collation coll
    JOIN pg_namespace ns ON ns.oid=coll.collnamespace
WHERE
    nspname NOT IN ('pg_catalog', 'information_schema')"))))

(t/deftest pg-sequence-is-empty-test
  (t/is (= [] (xt/q tu/*node* "SELECT seqrelid, seqtypid, seqstart, seqincrement, seqmax, seqmin, seqcache, seqcycle FROM pg_sequence"))))

(t/deftest reloptions-and-attcompression-test
  (xt/execute-tx tu/*node* [[:put-docs :foo {:xt/id 1}]])

  (t/is (= [{:relname "foo"}]
           (xt/q tu/*node* "SELECT relname, reloptions FROM pg_class WHERE relname = 'foo'")))

  (t/is (= [{:attname "_id", :attcompression ""}]
           (xt/q tu/*node* "SELECT attname, attcompression FROM pg_attribute a JOIN pg_class c ON a.attrelid = c.oid WHERE c.relname = 'foo' AND attname = '_id'"))))

(t/deftest pg-indexam-has-property-test
  (t/is (= [{:amname "btree"} {:amname "hash"} {:amname "heap"}]
           (xt/q tu/*node* "SELECT amname, pg_indexam_has_property(oid, 'can_order') AS amcanorder FROM pg_am ORDER BY amname"))
        "XTDB reports no indexes, so no access method claims a property"))
