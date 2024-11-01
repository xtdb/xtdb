(ns xtdb.information-schema-test
  (:require [clojure.test :as t :refer [deftest]]
            [xtdb.api :as xt]
            [xtdb.information-schema :as i-s]
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
              :table-schema "xt",
              :table-name "txs",
              :column-name "system_time",
              :data-type "[:timestamp-tz :micro \"UTC\"]"}
             {:table-catalog "xtdb",
              :table-schema "xt",
              :table-name "txs",
              :column-name "_id",
              :data-type ":i64"}
             {:table-catalog "xtdb",
              :table-schema "public",
              :table-name "beanie",
              :column-name "_id",
              :data-type ":keyword"}
             {:table-catalog "xtdb",
              :table-schema "public",
              :table-name "baseball",
              :column-name "col2",
              :data-type ":i64"}
             {:table-catalog "xtdb",
              :table-schema "xt",
              :table-name "txs",
              :column-name "error",
              :data-type "[:union #{:null :transit}]"}
             {:table-catalog "xtdb",
              :table-schema "public",
              :table-name "baseball",
              :column-name "col1",
              :data-type ":i64"}
             {:table-catalog "xtdb",
              :table-schema "public",
              :table-name "baseball",
              :column-name "_id",
              :data-type ":keyword"}
             {:table-catalog "xtdb",
              :table-schema "xt",
              :table-name "txs",
              :column-name "committed",
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
              :table-schema "xt",
              :table-name "txs",
              :table-type "BASE TABLE"}
             {:table-catalog "xtdb",
              :table-schema "public",
              :table-name "baseball",
              :table-type "BASE TABLE"}}
           (set (tu/query-ra '[:scan {:table information_schema/tables} [table_catalog table_schema table_name table_type]]
                             {:node tu/*node*})))))

(deftest test-pg-attribute
  (xt/submit-tx tu/*node* test-data)

  (t/is (= #{{:atttypmod -1,
              :attrelid 127091884,
              :attidentity "",
              :attgenerated "",
              :attnotnull false,
              :attlen -1,
              :atttypid 114,
              :attnum 1,
              :attname "_id",
              :attisdropped false}
             {:atttypmod -1,
              :attrelid 732573471,
              :attidentity "",
              :attgenerated "",
              :attnotnull false,
              :attlen -1,
              :atttypid 114,
              :attnum 1,
              :attname "_id",
              :attisdropped false}
             {:atttypmod -1,
              :attrelid 127091884,
              :attidentity "",
              :attgenerated "",
              :attnotnull false,
              :attlen -1,
              :atttypid 114,
              :attnum 2,
              :attname "col1",
              :attisdropped false}
             {:atttypmod -1,
              :attrelid 732573471,
              :attidentity "",
              :attgenerated "",
              :attnotnull false,
              :attlen 8,
              :atttypid 20,
              :attnum 2,
              :attname "col2",
              :attisdropped false}
             {:atttypmod -1,
              :attrelid 732573471,
              :attidentity "",
              :attgenerated "",
              :attnotnull false,
              :attlen 8,
              :atttypid 20,
              :attnum 3,
              :attname "col1",
              :attisdropped false}
             {:atttypmod -1,
              :attrelid 598393539,
              :attidentity "",
              :attgenerated "",
              :attnotnull false,
              :attlen -1,
              :atttypid 114,
              :attnum 4,
              :attname "error",
              :attisdropped false}
             {:atttypmod -1,
              :attrelid 598393539,
              :attidentity "",
              :attgenerated "",
              :attnotnull false,
              :attlen 8,
              :atttypid 20,
              :attnum 3,
              :attname "_id",
              :attisdropped false}
             {:atttypmod -1,
              :attrelid 598393539,
              :attidentity "",
              :attgenerated "",
              :attnotnull false,
              :attlen 8,
              :atttypid 1184,
              :attnum 1,
              :attname "system_time",
              :attisdropped false}
             {:atttypmod -1,
              :attrelid 598393539,
              :attidentity "",
              :attgenerated "",
              :attnotnull false,
              :attlen 1,
              :atttypid 16,
              :attnum 2,
              :attname "committed",
              :attisdropped false}}
           (set (tu/query-ra '[:scan
                               {:table pg_catalog/pg_attribute}
                               [attrelid attname atttypid attlen attnum attisdropped
                                attnotnull atttypmod attidentity attgenerated]]
                             {:node tu/*node*})))))

(deftest test-pg-tables
  (xt/submit-tx tu/*node* test-data)

  (t/is (= #{{:schemaname "xt",
              :tableowner "xtdb",
              :tablename "txs"}
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

(deftest test-pg-class
  (xt/submit-tx tu/*node* test-data)

  (t/is (= #{{:relkind "r",
              :relnamespace 1106696632,
              :oid 732573471,
              :relname "baseball"}
             {:relkind "r",
              :relnamespace 1106696632,
              :oid 598393539,
              :relname "txs"}
             {:relkind "r",
              :relnamespace 1106696632,
              :oid 127091884,
              :relname "beanie"}}
           (set (tu/query-ra '[:scan
                               {:table pg_catalog/pg_class}
                               [oid relname relnamespace relkind]]
                             {:node tu/*node*})))))

(deftest test-pg-type
  (xt/submit-tx tu/*node* test-data)
  (t/is (= #{{:typtypmod -1,
              :oid 700,
              :typtype "b",
              :typowner 1376455703,
              :typnotnull false,
              :typname "float4",
              :typnamespace 2125819141,
              :typbasetype 0}
             {:typtypmod -1,
              :oid 114,
              :typtype "b",
              :typowner 1376455703,
              :typnotnull false,
              :typname "json",
              :typnamespace 2125819141,
              :typbasetype 0}
             {:typtypmod -1,
              :oid 3802,
              :typtype "b",
              :typowner 1376455703,
              :typnotnull false,
              :typname "jsonb",
              :typnamespace 2125819141,
              :typbasetype 0}
             {:typtypmod -1,
              :oid 16384,
              :typtype "b",
              :typowner 1376455703,
              :typnotnull false,
              :typname "transit",
              :typnamespace 2125819141,
              :typbasetype 0}
             {:typtypmod -1,
              :oid 701,
              :typtype "b",
              :typowner 1376455703,
              :typnotnull false,
              :typname "float8",
              :typnamespace 2125819141,
              :typbasetype 0}
             {:typtypmod -1,
              :oid 21,
              :typtype "b",
              :typowner 1376455703,
              :typnotnull false,
              :typname "int2",
              :typnamespace 2125819141,
              :typbasetype 0}
             {:typtypmod -1,
              :oid 2950,
              :typtype "b",
              :typowner 1376455703,
              :typnotnull false,
              :typname "uuid",
              :typnamespace 2125819141,
              :typbasetype 0}
             {:typtypmod -1,
              :oid 1043,
              :typtype "b",
              :typowner 1376455703,
              :typnotnull false,
              :typname "varchar",
              :typnamespace 2125819141,
              :typbasetype 0}
             {:typtypmod -1,
              :oid 25,
              :typtype "b",
              :typowner 1376455703,
              :typnotnull false,
              :typname "text",
              :typnamespace 2125819141,
              :typbasetype 0}
             {:typtypmod -1,
              :oid 20,
              :typtype "b",
              :typowner 1376455703,
              :typnotnull false,
              :typname "int8",
              :typnamespace 2125819141,
              :typbasetype 0}
             {:typtypmod -1,
              :oid 23,
              :typtype "b",
              :typowner 1376455703,
              :typnotnull false,
              :typname "int4",
              :typnamespace 2125819141,
              :typbasetype 0}
             {:typtypmod -1,
              :oid 16,
              :typtype "b",
              :typowner 1376455703,
              :typnotnull false,
              :typname "boolean",
              :typnamespace 2125819141,
              :typbasetype 0}
             {:typtypmod -1,
              :oid 1082,
              :typtype "b",
              :typowner 1376455703,
              :typnotnull false,
              :typname "date",
              :typnamespace 2125819141,
              :typbasetype 0}
             {:typtypmod -1,
              :oid 1114,
              :typtype "b",
              :typowner 1376455703,
              :typnotnull false,
              :typname "timestamp",
              :typnamespace 2125819141,
              :typbasetype 0}
             {:typtypmod -1,
              :oid 1184,
              :typtype "b",
              :typowner 1376455703,
              :typnotnull false,
              :typname "timestamptz",
              :typnamespace 2125819141,
              :typbasetype 0}
             {:typtypmod -1,
              :oid 3910,
              :typtype "b",
              :typowner 1376455703,
              :typnotnull false,
              :typname "tstz-range",
              :typnamespace 2125819141,
              :typbasetype 0}
             {:typtypmod -1,
              :oid 2205,
              :typtype "b",
              :typowner 1376455703,
              :typnotnull false,
              :typname "regclass",
              :typnamespace 2125819141,
              :typbasetype 0}
             {:typtypmod -1,
              :oid 1186,
              :typtype "b",
              :typowner 1376455703,
              :typnotnull false,
              :typname "interval",
              :typnamespace 2125819141,
              :typbasetype 0}
             {:typtypmod -1,
              :oid 1007,
              :typtype "b",
              :typowner 1376455703,
              :typnotnull false,
              :typname "_int4",
              :typnamespace 2125819141,
              :typbasetype 0}
             {:typtypmod -1,
              :oid 1016,
              :typtype "b",
              :typowner 1376455703,
              :typnotnull false,
              :typname "_int8",
              :typnamespace 2125819141,
              :typbasetype 0}}
           (set (tu/query-ra '[:scan {:table pg_catalog/pg_type}
                               [oid typname typnamespace typowner typtype typbasetype typnotnull typtypmod]]
                             {:node tu/*node*})))))
(deftest test-pg-description
  (t/is (= []
           (tu/query-ra '[:scan {:table pg_catalog/pg_desciption}
                          [objoid classoid objsubid description]]
                        {:node tu/*node*}))))

(deftest test-pg-views
  (t/is (= []
           (tu/query-ra '[:scan {:table pg_catalog/pg_views}
                          [schemaname viewname viewowner]]
                        {:node tu/*node*}))))

(deftest test-mat-views
  (t/is (= []
           (tu/query-ra '[:scan {:table pg_catalog/pg_matviews}
                          [schemaname matviewname matviewowner]]
                        {:node tu/*node*}))))

(deftest test-sql-query
  (xt/submit-tx tu/*node* [[:put-docs :beanie {:xt/id :foo, :col1 "foo1"}]
                           [:put-docs :beanie {:xt/id :bar, :col1 123}]
                           [:put-docs :baseball {:xt/id :baz, :col1 123 :col2 456}]])

  (t/is (= [{:column-name "_id"}]
           (xt/q tu/*node* "SELECT column_name FROM information_schema.columns LIMIT 1")))

  (t/is (= #{{:attrelid 732573471, :attname "col2"}
             {:attrelid 732573471, :attname "_id"}}
           (set (xt/q tu/*node* "SELECT attname, attrelid FROM pg_attribute LIMIT 2"))))

  (t/is (= [{:table-name "baseball",
             :data-type ":keyword",
             :column-name "_id",
             :table-catalog "xtdb",
             :table-schema "public"}]
           (xt/q tu/*node* "FROM information_schema.columns LIMIT 1"))))

(deftest test-selection-and-projection
  (xt/submit-tx tu/*node* [[:put-docs :beanie {:xt/id :foo, :col1 "foo1"}]
                           [:put-docs :beanie {:xt/id :bar, :col1 123}]
                           [:put-docs :baseball {:xt/id :baz, :col1 123 :col2 456}]])

  (t/is (= #{{:table-name "txs", :table-schema "xt"}
             {:table-name "beanie", :table-schema "public"}
             {:table-name "baseball", :table-schema "public"}}
           (set (tu/query-ra '[:scan {:table information_schema/tables} [table_name table_schema]]
                             {:node tu/*node*})))
        "Only requested cols are projected")

  (t/is (= #{{:table-name "baseball", :table-schema "public"}}
           (set (tu/query-ra '[:scan {:table information_schema/tables} [table_schema {table_name (= table_name "baseball")}]]
                             {:node tu/*node*})))
        "col-preds work")

  (t/is (= #{{:table-name "beanie", :table-schema "public"}}
           (set (tu/query-ra '[:scan {:table information_schema/tables} [table_schema {table_name (= table_name ?tn)}]]
                             {:node tu/*node* :params {'?tn "beanie"}})))
        "col-preds with params work")


  ;;TODO although this doesn't error, not sure this exctly works, adding a unknown col = null didn't work
  ;;I think another complication could be that these tables don't necesasrily have a primary/unique key due to
  ;;lack of xtid
  (t/is (= #{{:table-name "baseball"}
             {:table-name "beanie"}
             {:table-name "txs"}}
           (set (tu/query-ra '[:scan {:table information_schema/tables} [table_name unknown_col]]
                             {:node tu/*node*})))
        "cols that don't exist don't error/projected as nulls/absents"))

(deftest test-pg-namespace
  (t/is (= #{{:nspowner 1376455703,
              :oid 1980112537,
              :nspname "information_schema"}
             {:nspowner 1376455703,
              :oid 2125819141,
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
                 "SELECT data_type FROM information_schema.columns
                  WHERE column_name = 'set_column'"))))

(deftest test-pg-user
  (t/is (= [{:passwd "xtdb", :username "xtdb", :usesuper true}
            {:username "anonymous", :usesuper false}]
           (xt/q tu/*node* "SELECT * FROM pg_user"))))

; required for Postgrex
(deftest test-pg-range-3737
  (let [types (set
               (map
                (fn [typ]
                  (-> typ
                      (select-keys [:oid :typname :typoutput :typreceive :typsend :typinput])
                      (assoc :xt/column-9 []
                             :xt/column-7 (:typelem typ)
                             ;; nasty hack to get this exception to work
                             :xt/column-8 (if (= "tstz-range" (:typname typ)) 1184 0))))
                (i-s/pg-type)))
        results (set
                 (xt/q tu/*node* "
   SELECT t.oid, t.typname, t.typsend, t.typreceive, t.typoutput, t.typinput,
          COALESCE(d.typelem, t.typelem), COALESCE(r.rngsubtype, 0),
          ARRAY (
            SELECT a.atttypid
            FROM pg_attribute AS a
            WHERE a.attrelid = t.typrelid AND a.attnum > 0 AND NOT a.attisdropped
            ORDER BY a.attnum
          )

   FROM pg_type AS t
   LEFT JOIN pg_type AS d ON t.typbasetype = d.oid
   LEFT JOIN pg_range AS r ON r.rngtypid = t.oid OR r.rngmultitypid = t.oid OR (t.typbasetype <> 0 AND r.rngtypid = t.typbasetype)
   WHERE (t.typrelid = 0)
     AND (t.typelem = 0 OR NOT EXISTS (SELECT 1 FROM pg_catalog.pg_type s WHERE s.typrelid != 0 AND s.oid = t.typelem))"))]
    (t/is (= (count types)
             (count results)))
    (t/is (= types
             results))))
