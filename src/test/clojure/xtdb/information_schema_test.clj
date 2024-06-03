(ns xtdb.information-schema-test
  (:require [clojure.test :as t :refer [deftest]]
            [xtdb.api :as xt]
            [xtdb.test-util :as tu]))

(t/use-fixtures :each tu/with-allocator tu/with-node)

(def test-data [[:put-docs :beanie {:xt/id :foo, :col1 "foo1"}]
                [:put-docs :beanie {:xt/id :bar, :col1 123}]
                [:put-docs :baseball {:xt/id :baz, :col1 123 :col2 456}]])

(deftest test-info-schema-columns
  (xt/submit-tx tu/*node* test-data)

  (t/is (= #{{:table-catalog "xtdb",
              :table-schema "public",
              :table-name "beanie",
              :column-name "col1",
              :data-type "[:union #{:utf8 :i64}]"}
             {:table-catalog "xtdb",
              :table-schema "public",
              :table-name "xt$txs",
              :column-name "xt$tx_time",
              :data-type "[:timestamp-tz :micro \"UTC\"]"}
             {:table-catalog "xtdb",
              :table-schema "public",
              :table-name "xt$txs",
              :column-name "xt$id",
              :data-type ":i64"}
             {:table-catalog "xtdb",
              :table-schema "public",
              :table-name "beanie",
              :column-name "xt$id",
              :data-type ":keyword"}
             {:table-catalog "xtdb",
              :table-schema "public",
              :table-name "baseball",
              :column-name "col2",
              :data-type ":i64"}
             {:table-catalog "xtdb",
              :table-schema "public",
              :table-name "xt$txs",
              :column-name "xt$error",
              :data-type "[:union #{:null :transit}]"}
             {:table-catalog "xtdb",
              :table-schema "public",
              :table-name "baseball",
              :column-name "col1",
              :data-type ":i64"}
             {:table-catalog "xtdb",
              :table-schema "public",
              :table-name "baseball",
              :column-name "xt$id",
              :data-type ":keyword"}
             {:table-catalog "xtdb",
              :table-schema "public",
              :table-name "xt$txs",
              :column-name "xt$committed?",
              :data-type ":bool"}}
           (set (tu/query-ra '[:scan
                               {:table information_schema/columns}
                               [table_catalog table_schema table_name column_name data_type]]
                             {:node tu/*node*})))))

(deftest test-info-schema-tables
  (xt/submit-tx tu/*node* test-data)

  (t/is (= #{{:table-catalog "xtdb",
              :table-schema "public",
              :table-name "beanie",
              :table-type "BASE TABLE"}
             {:table-catalog "xtdb",
              :table-schema "public",
              :table-name "xt$txs",
              :table-type "BASE TABLE"}
             {:table-catalog "xtdb",
              :table-schema "public",
              :table-name "baseball",
              :table-type "BASE TABLE"}}
           (set (tu/query-ra '[:scan {:table information_schema/tables} [table_catalog table_schema table_name table_type]]
                             {:node tu/*node*})))))

(deftest test-pg-attribute
  (xt/submit-tx tu/*node* test-data)

  (t/is (=
         #{{:attrelid 159967295, :atttypid 114, :attname "col2", :attlen -1}
           {:attrelid 93927094, :atttypid 114, :attname "xt$id", :attlen -1}
           {:attrelid 499408916, :atttypid 114, :attname "xt$id", :attlen -1}
           {:attrelid 159967295, :atttypid 114, :attname "xt$id", :attlen -1}
           {:attrelid 93927094, :atttypid 114, :attname "xt$tx_time", :attlen -1}
           {:attrelid 93927094, :atttypid 114, :attname "xt$committed?", :attlen -1}
           {:attrelid 159967295, :atttypid 114, :attname "col1", :attlen -1}
           {:attrelid 93927094, :atttypid 114, :attname "xt$error", :attlen -1}
           {:attrelid 499408916, :atttypid 114, :attname "col1", :attlen -1}}
           (set (tu/query-ra '[:scan
                               {:table pg_catalog/pg_attribute}
                               [attrelid attname atttypid attlen]]
                             {:node tu/*node*})))))

(deftest test-pg-tables
  (xt/submit-tx tu/*node* test-data)

  (t/is (= #{{:schemaname "public",
              :tableowner "xtdb",
              :tablename "xt$txs"}
             {:schemaname "public",
              :tableowner "xtdb",
              :tablename "baseball"}
             {:schemaname "public",
              :tableowner "xtdb",
              :tablename "beanie"}}
           (set (tu/query-ra '[:scan
                               {:table pg_catalog/pg_tables}
                               [schemaname tablename tableowner tablespace]]
                             {:node tu/*node*})))))

(deftest test-pg-views
  (t/is (= []
           (tu/query-ra '[:scan
                          {:table pg_catalog/pg_views}
                          [schemaname viewname viewowner]]
                        {:node tu/*node*}))))

(deftest test-mat-views
  (t/is (= []
           (tu/query-ra '[:scan
                          {:table pg_catalog/pg_matviews}
                          [schemaname matviewname matviewowner]]
                        {:node tu/*node*}))))

(deftest test-sql-query
  (xt/submit-tx tu/*node* [[:put-docs :beanie {:xt/id :foo, :col1 "foo1"}]
                           [:put-docs :beanie {:xt/id :bar, :col1 123}]
                           [:put-docs :baseball {:xt/id :baz, :col1 123 :col2 456}]])

  (t/is (= [{:column-name "xt$id"}]
           (xt/q tu/*node*
                 "SELECT information_schema.columns.column_name FROM information_schema.columns LIMIT 1")))

  (t/is (= #{{:attrelid 159967295, :attname "col2"}
             {:attrelid 159967295, :attname "xt$id"}}
           (set
            (xt/q tu/*node*
                  "SELECT pg_attribute.attname, pg_attribute.attrelid FROM pg_attribute LIMIT 2"))))

  (t/is (= [{:table-name "baseball",
             :data-type ":keyword",
             :column-name "xt$id",
             :table-catalog "xtdb",
             :table-schema "public"}]
           (xt/q tu/*node*
                 "FROM information_schema.columns LIMIT 1"))))

(deftest test-selection-and-projection
  (xt/submit-tx tu/*node* [[:put-docs :beanie {:xt/id :foo, :col1 "foo1"}]
                           [:put-docs :beanie {:xt/id :bar, :col1 123}]
                           [:put-docs :baseball {:xt/id :baz, :col1 123 :col2 456}]])

  (t/is (=
         #{{:table-name "xt$txs", :table-schema "public"}
           {:table-name "beanie", :table-schema "public"}
           {:table-name "baseball", :table-schema "public"}}
         (set (tu/query-ra '[:scan {:table information_schema/tables} [table_name table_schema]]
                           {:node tu/*node*})))
        "Only requested cols are projected")

  (t/is (=
         #{{:table-name "baseball", :table-schema "public"}}
         (set (tu/query-ra '[:scan {:table information_schema/tables} [table_schema {table_name (= table_name "baseball")}]]
                           {:node tu/*node*})))
        "col-preds work")

  (t/is (=
         #{{:table-name "beanie", :table-schema "public"}}
         (set (tu/query-ra '[:scan {:table information_schema/tables} [table_schema {table_name (= table_name ?tn)}]]
                           {:node tu/*node* :params {'?tn "beanie"}})))
        "col-preds with params work")


  ;;TODO although this doesn't error, not sure this exctly works, adding a unknown col = null didn't work
  ;;I think another complication could be that these tables don't necesasrily have a primary/unique key due to
  ;;lack of xtid
  (t/is
   (=
    #{{:table-name "baseball"}
      {:table-name "beanie"}
      {:table-name "xt$txs"}}
    (set (tu/query-ra '[:scan {:table information_schema/tables} [table_name unknown_col]]
                      {:node tu/*node*})))
   "cols that don't exist don't error/projected as nulls/absents"))

(deftest test-pg-namespace
  (t/is (=
         #{{:nspowner 1376455703,
            :oid -1980112537,
            :nspname "information_schema"}
           {:nspowner 1376455703,
            :oid -2125819141,
            :nspname "pg_catalog"}
           {:nspowner 1376455703,
            :oid 1106696632,
            :nspname "public"}}
         (set (tu/query-ra '[:scan {:table pg_catalog/pg_namespace} [oid nspname nspowner nspacl]]
                           {:node tu/*node*})))))
(deftest test-schemata
  (t/is (= #{{:catalog-name "xtdb",
              :schema-name "information_schema",
              :schema-owner "xtdb"}
             {:catalog-name "xtdb",
              :schema-name "pg_catalog",
              :schema-owner "xtdb"}
             {:catalog-name "xtdb",
              :schema-name "public",
              :schema-owner "xtdb"}}
           (set (tu/query-ra '[:scan {:table information_schema/schemata} [catalog_name schema_name schema_owner]]
                             {:node tu/*node*})))))

(deftest test-composite-columns
  (xt/submit-tx tu/*node* [[:put-docs :composite-docs {:xt/id 1 :set-column #{"hello world"}}]])

  (t/is (= [{:data-type "[:set :utf8]"}]
           (xt/q tu/*node*
                 "SELECT columns.data_type FROM information_schema.columns AS columns
                  WHERE columns.column_name = 'set_column'"))))

