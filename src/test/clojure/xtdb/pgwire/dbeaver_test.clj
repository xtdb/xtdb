(ns xtdb.pgwire.dbeaver-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.test-util :as tu]))

(t/use-fixtures :each tu/with-node)

(t/deftest dbeaver-query-4528-test
  (t/is (= [{:oid 1980112537, :nspname "information_schema", :nspowner 1376455703}
            {:oid 2125819141, :nspname "pg_catalog", :nspowner 1376455703}
            {:oid 1106696632, :nspname "public", :nspowner 1376455703}]
           (xt/q tu/*node* "SELECT n.oid,n.*,d.description FROM pg_catalog.pg_namespace n LEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid=n.oid AND d.objsubid=0 AND d.classoid='pg_namespace'::regclass ORDER BY nspname"))))
