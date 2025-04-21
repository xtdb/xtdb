(ns xtdb.information-schema-test
  (:require [clojure.test :as t :refer [deftest]]
            [xtdb.api :as xt]
            [xtdb.authn :as authn]
            [xtdb.information-schema :as i-s]
            [xtdb.test-util :as tu]
            [xtdb.time :as time]
            [xtdb.compactor :as c]))

(t/use-fixtures :each tu/with-mock-clock tu/with-allocator tu/with-node)

(def test-data [[:put-docs :beanie {:xt/id :foo, :col1 "foo1"}]
                [:put-docs :beanie {:xt/id :bar, :col1 123}]
                [:put-docs :baseball {:xt/id :baz, :col1 123 :col2 456}]])

(deftest test-info-schema-columns
  (xt/submit-tx tu/*node* test-data)

  (t/is (= (into '#{[xtdb public/baseball _id :keyword]
                    [xtdb public/baseball col1 :i64]
                    [xtdb public/baseball col2 :i64]
                    [xtdb public/beanie _id :keyword]
                    [xtdb public/beanie col1 [:union #{:utf8 :i64}]]
                    [xtdb xt/txs _id :i64]
                    [xtdb xt/txs committed :bool]
                    [xtdb xt/txs error [:union #{:null :transit}]]
                    [xtdb xt/txs system_time [:timestamp-tz :micro "UTC"]]}
                 (for [table '[public/baseball public/beanie xt/txs]
                       [col data-type] '[[_system_from [:timestamp-tz :micro "UTC"]]
                                         [_system_to [:union #{[:timestamp-tz :micro "UTC"] :null}]]
                                         [_valid_from [:timestamp-tz :micro "UTC"]]
                                         [_valid_to [:union #{[:timestamp-tz :micro "UTC"] :null}]]]]
                   ['xtdb table col data-type]))
           (into #{} (map (juxt (comp symbol :table-catalog)
                                (fn [{:keys [table-schema table-name]}]
                                  (symbol table-schema table-name))
                                (comp symbol :column-name)
                                (comp read-string :data-type)))
                 (tu/query-ra
                  '[:select (or (= table_schema "public") (= table_schema "xt"))
                    [:scan
                     {:table information_schema/columns}
                     [table_catalog table_schema table_name column_name data_type]]]
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
           (set (tu/query-ra '[:select (or (= table_schema "public") (= table_schema "xt"))
                               [:scan {:table information_schema/tables} [table_catalog table_schema table_name table_type]]]
                             {:node tu/*node*})))))

(deftest test-pg-attribute
  (xt/submit-tx tu/*node* test-data)

  (t/is (= (set (for [[attrelid attname attnum atttypeid attlen] [[127091884 "_id" 1 11111 -1]
                                                                  [127091884 "_system_from" 2 1184 8]
                                                                  [127091884 "_system_to" 3 1184 8]
                                                                  [127091884 "_valid_from" 4 1184 8]
                                                                  [127091884 "_valid_to" 5 1184 8]
                                                                  [127091884 "col1" 6 114 -1]

                                                                  [732573471 "_id" 1 11111 -1]
                                                                  [732573471 "_system_from" 2 1184 8]
                                                                  [732573471 "_system_to" 3 1184 8]
                                                                  [732573471 "_valid_from" 4 1184 8]
                                                                  [732573471 "_valid_to" 5 1184 8]
                                                                  [732573471 "col1" 6 20 8]
                                                                  [732573471 "col2" 7 20 8]

                                                                  [598393539 "_id" 1 20 8]
                                                                  [598393539 "_system_from" 2 1184 8]
                                                                  [598393539 "_system_to" 3 1184 8]
                                                                  [598393539 "_valid_from" 4 1184 8]
                                                                  [598393539 "_valid_to" 5 1184 8]
                                                                  [598393539 "committed" 6 16 1]
                                                                  [598393539 "error" 7 114 -1]
                                                                  [598393539 "system_time" 8 1184 8]]]
                  {:atttypmod -1,
                   :attrelid attrelid,
                   :attidentity "",
                   :attgenerated "",
                   :attnotnull false,
                   :attlen attlen,
                   :atttypid atttypeid,
                   :attnum attnum,
                   :attname attname,
                   :attisdropped false}))

           (set (tu/query-ra '[:select (or (= attname "_id") (= attname "_valid_from")(= attname "_valid_to") (= attname "_system_from") (= attname "_system_to")
                                           (= attname "col1") (= attname "col2")
                                           (= attname "error") (= attname "system_time") (= attname "committed"))
                               [:scan
                                {:table pg_catalog/pg_attribute}
                                [attrelid attname atttypid attlen attnum attisdropped
                                 attnotnull atttypmod attidentity attgenerated]]]
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
           (set (tu/query-ra '[:select (or (= schemaname "public") (= schemaname "xt"))
                               [:scan
                                {:table pg_catalog/pg_tables}
                                [schemaname tablename tableowner tablespace]]]
                             {:node tu/*node*})))))

(deftest test-pg-class
  (xt/submit-tx tu/*node* test-data)

  (t/is (= #{{:relkind "r",
              :relnamespace 1106696632,
              :oid 732573471,
              :relname "baseball",
              :relam 2,
              :relchecks 0,
              :relhasindex true,
              :relhasrules false,
              :relhastriggers false,
              :relrowsecurity false,
              :relforcerowsecurity false,
              :relispartition false,
              :reltablespace 0,
              :reloftype 0,
              :relpersistence "p",
              :relreplident "d",
              :reltoastrelid 0}
             {:relkind "r",
              :relnamespace 683075021,
              :oid 598393539,
              :relname "txs",
              :relam 2,
              :relchecks 0,
              :relhasindex true,
              :relhasrules false,
              :relhastriggers false,
              :relrowsecurity false,
              :relforcerowsecurity false,
              :relispartition false,
              :reltablespace 0,
              :reloftype 0,
              :relpersistence "p",
              :relreplident "d",
              :reltoastrelid 0}
             {:relkind "r",
              :relnamespace 1106696632,
              :oid 127091884,
              :relname "beanie",
              :relam 2,
              :relchecks 0,
              :relhasindex true,
              :relhasrules false,
              :relhastriggers false,
              :relrowsecurity false,
              :relforcerowsecurity false,
              :relispartition false,
              :reltablespace 0,
              :reloftype 0,
              :relpersistence "p",
              :relreplident "d",
              :reltoastrelid 0}}
           (set (tu/query-ra '[:select (or (= relname "beanie") (= relname "baseball") (= relname "txs"))
                               [:scan
                                {:table pg_catalog/pg_class}
                                [reltablespace reloftype relhastriggers relchecks relpersistence relhasrules relispartition relname relforcerowsecurity oid relnamespace relreplident relhasindex reltoastrelid relkind relam relrowsecurity]]]
                             {:node tu/*node*})))))

(deftest test-pg-type
  (xt/submit-tx tu/*node* test-data)
  (t/is (= (set (for [[oid typname] [
                                     [114 "json"] [3802 "jsonb"] [16384 "transit"]
                                     [2950 "uuid"]
                                     [1043 "varchar"] [25 "text"]
                                     [16 "boolean"] [21 "int2"] [23 "int4"] [20 "int8"]
                                     [700 "float4"] [701 "float8"] [1700 "numeric"]
                                     [1082 "date"] [1114 "timestamp"] [1184 "timestamptz"] [3910 "tstz-range"] [1186 "interval"]
                                     [2205 "regclass"]
                                     [1007 "_int4"] [1016 "_int8"]
                                     [17 "bytea"]
                                     [1009 "_text"]
                                     [11111 "keyword"]]]
                  {:typtypmod -1,
                   :oid oid,
                   :typtype "b",
                   :typowner 1376455703,
                   :typnotnull false,
                   :typname typname,
                   :typnamespace 2125819141,
                   :typbasetype 0}))
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
           (xt/q tu/*node* "SELECT column_name FROM information_schema.columns ORDER BY column_name LIMIT 1")))

  (t/is (= [{:attname "_id", :attrelid 127091884}]
           (xt/q tu/*node* "SELECT attname, attrelid FROM pg_attribute ORDER BY attname LIMIT 1")))

  (t/is (= [{:table-name "baseball",
             :data-type ":keyword",
             :column-name "_id",
             :table-catalog "xtdb",
             :table-schema "public"}]
           (xt/q tu/*node* "FROM information_schema.columns ORDER BY table_name, column_name LIMIT 1"))))

(deftest test-selection-and-projection
  (xt/submit-tx tu/*node* [[:put-docs :beanie {:xt/id :foo, :col1 "foo1"}]
                           [:put-docs :beanie {:xt/id :bar, :col1 123}]
                           [:put-docs :baseball {:xt/id :baz, :col1 123 :col2 456}]])

  (t/is (= #{{:table-name "txs", :table-schema "xt"}
             {:table-name "beanie", :table-schema "public"}
             {:table-name "baseball", :table-schema "public"}}
           (set (tu/query-ra ' [:select (or (= table_schema "public") (= table_schema "xt"))
                                [:scan {:table information_schema/tables} [table_name table_schema]]]
                               {:node tu/*node*})))
        "Only requested cols are projected")

  (t/is (= #{{:table-name "baseball", :table-schema "public"}}
           (set (tu/query-ra '[:scan {:table information_schema/tables} [table_schema {table_name (= table_name "baseball")}]]
                             {:node tu/*node*})))
        "col-preds work")

  (t/is (= #{{:table-name "beanie", :table-schema "public"}}
           (set (tu/query-ra '[:scan {:table information_schema/tables} [table_schema {table_name (= table_name ?tn)}]]
                             {:node tu/*node*, :args {:tn "beanie"}})))
        "col-preds with params work")


  ;;TODO although this doesn't error, not sure this exctly works, adding a unknown col = null didn't work
  ;;I think another complication could be that these tables don't necesasrily have a primary/unique key due to
  ;;lack of xtid
  (t/is (= #{{:table-name "baseball" :table-schema "public"}
             {:table-name "beanie" :table-schema "public"}
             {:table-name "txs" :table-schema "xt"}}
           (set (tu/query-ra '[:select (or (= table_schema "public") (= table_schema "xt"))
                               [:scan {:table information_schema/tables} [table_name unknown_col table_schema]]]
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
  (t/is (= [{:username "xtdb", :usesuper true, :xt/valid-from (time/->zdt #inst "1970")}]
           (xt/q tu/*node* "SELECT username, usesuper, _valid_from, _valid_to FROM pg_user")))

  (t/is (= "xtdb" (authn/verify-pw tu/*node* "xtdb" "xtdb")))

  (xt/execute-tx tu/*node* ["CREATE USER ada WITH PASSWORD 'lovelace'"])

  (t/is (= [{:username "ada", :usesuper false}]
           (xt/q tu/*node* "SELECT username, usesuper FROM pg_user WHERE username = 'ada'")))
  (t/is (= "ada" (authn/verify-pw tu/*node* "ada" "lovelace")))

  (xt/execute-tx tu/*node* ["ALTER USER anonymous WITH PASSWORD 'anonymous'"])

  (t/is (= [{:username "anonymous", :usesuper false}] (xt/q tu/*node* "SELECT username, usesuper FROM pg_user WHERE username = 'anonymous'")))
  (t/is (= "anonymous" (authn/verify-pw tu/*node* "anonymous" "anonymous"))))

;; required for Postgrex
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

(deftest schema-for-meta-tables-3550
  (t/is (= [{:table-catalog "xtdb",
             :table-schema "information_schema",
             :table-name "tables",
             :table-type "VIEW"}
            {:table-catalog "xtdb",
             :table-schema "pg_catalog",
             :table-name "pg_user",
             :table-type "BASE TABLE"}]
           (tu/query-ra '[:select (or (and (= table_schema "information_schema") (= table_name "tables"))
                                      (and (= table_schema "pg_catalog") (= table_name "pg_user")))
                          [:scan {:table information_schema/tables} [table_catalog table_schema table_name table_type]]]
                        {:node tu/*node*}))))

(t/deftest trie-stats
  (xt/execute-tx tu/*node* [[:put-docs :foo
                             {:xt/id 1, :xt/valid-from #inst "2020-01-01", :xt/valid-to #inst "2020-01-02"}
                             {:xt/id 1, :xt/valid-from #inst "2020-01-04", :xt/valid-to #inst "2020-01-05"}]])

  (tu/finish-block! tu/*node*)
  (c/compact-all! tu/*node* #xt/duration "PT1S")

  (t/is (= [{:schema-name "public", :table-name "foo", :trie-key "l00-rc-b00",
             :level 0, :trie-state "garbage", :data-file-size 2014}

            {:schema-name "public", :table-name "foo", :trie-key "l01-r20200106-b00",
             :level 1, :trie-state "live", :row-count 2, :data-file-size 2014, :recency #xt/date "2020-01-06",
             :temporal-metadata {:min-valid-from (time/->zdt #inst "2020-01-01")
                                 :max-valid-from (time/->zdt #inst "2020-01-04")
                                 :min-valid-to (time/->zdt #inst "2020-01-02")
                                 :max-valid-to (time/->zdt #inst "2020-01-05")
                                 :min-system-from (time/->zdt #inst "2020-01-01")
                                 :max-system-from (time/->zdt #inst "2020-01-01")}}

            {:schema-name "public", :table-name "foo", :trie-key "l01-rc-b00",
             :level 1, :trie-state "live", :data-file-size 1382}

            {:schema-name "xt", :table-name "txs", :trie-key "l00-rc-b00",
             :level 0, :trie-state "garbage", :data-file-size 2806}

            {:schema-name "xt", :table-name "txs", :trie-key "l01-rc-b00",
             :level 1, :trie-state "live", :row-count 1, :data-file-size 2806,
             :temporal-metadata {:min-valid-from (time/->zdt #inst "2020-01-01")
                                 :max-valid-from (time/->zdt #inst "2020-01-01")
                                 :min-valid-to (time/->zdt time/end-of-time)
                                 :max-valid-to (time/->zdt time/end-of-time)
                                 :min-system-from (time/->zdt #inst "2020-01-01")
                                 :max-system-from (time/->zdt #inst "2020-01-01")}}]

           (xt/q tu/*node* "SELECT * FROM xt.trie_stats ORDER BY table_name, trie_key"))))

(t/deftest test-live-tables
  (xt/execute-tx tu/*node* [[:put-docs :foo {:xt/id 1, :a 1} {:xt/id 2, :b 2}]
                            [:put-docs :bar {:xt/id 1, :a 1} {:xt/id 2, :b 2}]])

  (tu/finish-block! tu/*node*)

  (xt/execute-tx tu/*node* [[:put-docs :foo {:xt/id 3, :a "hello"} {:xt/id 4, :a "world"}]])

  (t/is (= [["public" "foo" 2] ["xt" "txs" 1]]
           (->> (xt/q tu/*node* "SELECT * FROM xt.live_tables ORDER BY schema_name, table_name")
                (mapv (juxt :schema-name :table-name :row-count)))))

  (t/is (= [["public" "foo" "_id" :i64]
            ["public" "foo" "a" :utf8]
            ["xt" "txs" "_id" :i64]
            ["xt" "txs" "committed" :bool]
            ["xt" "txs" "error" [:union #{:null :transit}]]
            ["xt" "txs" "system_time" [:timestamp-tz :micro "UTC"]]]
           (->> (xt/q tu/*node* "SELECT * FROM xt.live_columns ORDER BY schema_name, table_name, col_name")
                (mapv (juxt :schema-name :table-name :col-name (comp read-string :col-type)))))))
