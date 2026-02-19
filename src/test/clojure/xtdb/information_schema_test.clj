(ns xtdb.information-schema-test
  (:require [clojure.set :as set]
            [clojure.test :as t :refer [deftest]]
            [next.jdbc :as jdbc]
            [xtdb.api :as xt]
            [xtdb.authn :as authn]
            [xtdb.compactor :as c]
            [xtdb.information-schema :as i-s]
            [xtdb.pgwire-test :as pgw-test]
            [xtdb.test-util :as tu]
            [xtdb.time :as time])
  (:import [xtdb.api SimpleResult]))

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
                    [xtdb public/beanie col1 #{:utf8 :i64}]
                    [xtdb xt/txs _id :i64]
                    [xtdb xt/txs committed :bool]
                    [xtdb xt/txs error [:? :transit]]
                    [xtdb xt/txs system_time :instant]
                    [xtdb xt/txs user_metadata [:? :struct {}]]}
                 (for [table '[public/baseball public/beanie xt/txs]
                       [col data-type] '[[_system_from :instant]
                                         [_system_to [:? :instant]]
                                         [_valid_from :instant]
                                         [_valid_to [:? :instant]]]]
                   ['xtdb table col data-type]))
           (into #{} (map (juxt (comp symbol :table-catalog)
                                (fn [{:keys [table-schema table-name]}]
                                  (symbol table-schema table-name))
                                (comp symbol :column-name)
                                (comp read-string :data-type)))
                 (xt/q tu/*node*
                       "SELECT table_catalog, table_schema, table_name, column_name, data_type
                        FROM information_schema.columns
                        WHERE table_schema = 'public' OR table_schema = 'xt'")))))

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
           (set (xt/q tu/*node*
                      "SELECT table_catalog, table_schema, table_name, table_type
                       FROM information_schema.tables
                       WHERE table_schema = 'public' OR table_schema = 'xt'")))))

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
                                                                  [598393539 "error" 7 16384 -1]
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

           (set (xt/q tu/*node*
                      "SELECT attrelid, attname, atttypid, attlen, attnum, attisdropped,
                              attnotnull, atttypmod, attidentity, attgenerated
                       FROM pg_catalog.pg_attribute
                       WHERE attname IN ('_id', '_valid_from', '_valid_to', '_system_from', '_system_to',
                                         'col1', 'col2', 'error', 'system_time', 'committed')")))))

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
           (set (xt/q tu/*node*
                      "SELECT schemaname, tablename, tableowner, tablespace
                       FROM pg_catalog.pg_tables
                       WHERE schemaname = 'public' OR schemaname = 'xt'")))))

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
           (set (xt/q tu/*node*
                      "SELECT reltablespace, reloftype, relhastriggers, relchecks, relpersistence, relhasrules, relispartition, relname, relforcerowsecurity, oid, relnamespace, relreplident, relhasindex, reltoastrelid, relkind, relam, relrowsecurity
                       FROM pg_catalog.pg_class
                       WHERE relname IN ('beanie', 'baseball', 'txs')")))))

(deftest test-pg-type
  (xt/submit-tx tu/*node* test-data)

  (let [results (xt/q tu/*node* "SELECT * FROM pg_catalog.pg_type")]
    (t/is (= (set (for [[oid typname] [[114 "json"] [3802 "jsonb"] [16384 "transit"]
                                       [2950 "uuid"]
                                       [1043 "varchar"] [25 "text"]
                                       [16 "boolean"] [21 "int2"] [23 "int4"] [20 "int8"]
                                       [700 "float4"] [701 "float8"] [1700 "numeric"]
                                       [1082 "date"] [1083 "time"] [1114 "timestamp"] [1184 "timestamptz"] [3910 "tstz-range"] [1186 "interval"]
                                       [26 "oid"] [2205 "regclass"] [24 "regproc"]
                                       [1007 "_int4"] [1016 "_int8"]
                                       [17 "bytea"]
                                       [1009 "_text"]
                                       [11111 "keyword"]
                                       [11112 "duration"]]]
                    {:typtypmod -1,
                     :oid oid,
                     :typtype "b",
                     :typowner 1376455703,
                     :typnotnull false,
                     :typname typname,
                     :typnamespace 2125819141,
                     :typbasetype 0}))
             (set (->> results
                    (map #(select-keys % [:oid :typname :typnamespace :typowner :typtype :typbasetype :typnotnull :typtypmod]))))))

    (t/is (= #{"A" "B" "D" "N" "R" "S" "T" "U"}
             (->> results (map :typcategory) set)))

    (t/testing "typarray roundtrips through typelem"
      (let [array-elem-types (filter (comp not zero? :typarray) results)
            types-by-oid (-> (group-by :oid results)
                           (update-vals first))]
        (t/is (seq array-elem-types))
        (t/is (= (map :oid array-elem-types)
                 (map #(-> % :typarray types-by-oid :typelem) array-elem-types)))))))

(deftest test-pg-description
  (t/is (= []
           (xt/q tu/*node*
                 "SELECT objoid, classoid, objsubid, description
                  FROM pg_catalog.pg_description"))))

(deftest test-pg-views
  (t/is (= []
           (xt/q tu/*node*
                 "SELECT schemaname, viewname, viewowner
                  FROM pg_catalog.pg_views"))))

(deftest test-mat-views
  (t/is (= []
           (xt/q tu/*node*
                 "SELECT schemaname, matviewname, matviewowner
                  FROM pg_catalog.pg_matviews"))))

(deftest test-sql-query
  (xt/submit-tx tu/*node* [[:put-docs :beanie {:xt/id :foo, :col1 "foo1"}]
                           [:put-docs :beanie {:xt/id :bar, :col1 123}]
                           [:put-docs :baseball {:xt/id :baz, :col1 123 :col2 456}]])

  (t/is (= [{:column-name "_id"}]
           (xt/q tu/*node* "SELECT column_name FROM information_schema.columns ORDER BY column_name LIMIT 1")))

  (t/is (= [{:attname "_id", :attrelid 127091884}]
           (xt/q tu/*node* "SELECT attname, attrelid FROM pg_attribute ORDER BY attname, attrelid LIMIT 1")))

  (t/is (= [{:table-name "baseball",
             :data-type ":keyword",
             :column-name "_id",
             :table-catalog "xtdb",
             :table-schema "public"}]
           (xt/q tu/*node* "SELECT table_catalog, table_schema, table_name, column_name, data_type FROM information_schema.columns ORDER BY table_name, column_name LIMIT 1"))))

(deftest test-selection-and-projection
  (xt/submit-tx tu/*node* [[:put-docs :beanie {:xt/id :foo, :col1 "foo1"}]
                           [:put-docs :beanie {:xt/id :bar, :col1 123}]
                           [:put-docs :baseball {:xt/id :baz, :col1 123 :col2 456}]])

  (t/is (= #{{:table-name "txs", :table-schema "xt"}
             {:table-name "beanie", :table-schema "public"}
             {:table-name "baseball", :table-schema "public"}}
           (set (xt/q tu/*node*
                      "SELECT table_name, table_schema
                       FROM information_schema.tables
                       WHERE table_schema = 'public' OR table_schema = 'xt'")))
        "Only requested cols are projected")

  (t/is (= #{{:table-name "baseball", :table-schema "public"}}
           (set (xt/q tu/*node*
                      "SELECT table_schema, table_name
                       FROM information_schema.tables
                       WHERE table_name = 'baseball'")))
        "col-preds work")

  (t/is (= #{{:table-name "beanie", :table-schema "public"}}
           (set (xt/q tu/*node*
                      "SELECT table_schema, table_name
                       FROM information_schema.tables
                       WHERE table_name = 'beanie'")))
        "col-preds with params work")


  ;;TODO although this doesn't error, not sure this exctly works, adding a unknown col = null didn't work
  ;;I think another complication could be that these tables don't necesasrily have a primary/unique key due to
  ;;lack of xtid
  (t/is (= #{{:table-name "baseball" :table-schema "public"}
             {:table-name "beanie" :table-schema "public"}
             {:table-name "txs" :table-schema "xt"}}
           (set (xt/q tu/*node*
                      "SELECT table_name, null AS unknown_col, table_schema
                       FROM information_schema.tables
                       WHERE table_schema = 'public' OR table_schema = 'xt'")))
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
              :nspname "public"}
             {:oid 683075021,
              :nspname "xt",
              :nspowner 1376455703}}
           (set (xt/q tu/*node*
                      "SELECT oid, nspname, nspowner, nspacl
                       FROM pg_catalog.pg_namespace")))))
(deftest test-schemata
  (t/is (= #{{:catalog-name "xtdb", :schema-name "information_schema", :schema-owner "xtdb"}
             {:catalog-name "xtdb", :schema-name "pg_catalog", :schema-owner "xtdb"}
             {:catalog-name "xtdb", :schema-name "public", :schema-owner "xtdb"}
             {:catalog-name "xtdb", :schema-name "xt", :schema-owner "xtdb"}}
           (set (xt/q tu/*node*
                      "SELECT catalog_name, schema_name, schema_owner
                       FROM information_schema.schemata")))))

(deftest test-composite-columns
  (xt/submit-tx tu/*node* [[:put-docs :composite-docs {:xt/id 1 :set-column #{"hello world"}}]])

  (t/is (= [{:data-type "[:set :utf8]"}]
           (xt/q tu/*node*
                 "SELECT data_type FROM information_schema.columns
                  WHERE column_name = 'set_column'"))))

(deftest test-pg-user
  (t/is (= [{:username "xtdb", :usesuper true, :xt/valid-from (time/->zdt #inst "1970")}]
           (xt/q tu/*node* "SELECT username, usesuper, _valid_from, _valid_to FROM pg_user")))

  (t/is (= (SimpleResult. "xtdb") (.verifyPassword (authn/<-node tu/*node*) "xtdb" "xtdb")))

  (xt/execute-tx tu/*node* ["CREATE USER ada WITH PASSWORD 'lovelace'"])

  (t/is (= [{:username "ada", :usesuper false}]
           (xt/q tu/*node* "SELECT username, usesuper FROM pg_user WHERE username = 'ada'")))
  (t/is (= (SimpleResult. "ada") (.verifyPassword (authn/<-node tu/*node*) "ada" "lovelace")))

  (xt/execute-tx tu/*node* ["ALTER USER anonymous WITH PASSWORD 'anonymous'"])

  (t/is (= [{:username "anonymous", :usesuper false}] (xt/q tu/*node* "SELECT username, usesuper FROM pg_user WHERE username = 'anonymous'")))
  (t/is (= (SimpleResult. "anonymous") (.verifyPassword (authn/<-node tu/*node*) "anonymous" "anonymous"))))

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
           (xt/q tu/*node*
                 "SELECT table_catalog, table_schema, table_name, table_type
                  FROM information_schema.tables
                  WHERE (table_schema = 'information_schema' AND table_name = 'tables')
                     OR (table_schema = 'pg_catalog' AND table_name = 'pg_user')
                  ORDER BY table_schema, table_name"))))

(t/deftest trie-stats
  (xt/execute-tx tu/*node* [[:put-docs :foo
                             {:xt/id 1, :xt/valid-from #inst "2020-01-01", :xt/valid-to #inst "2020-01-02"}
                             {:xt/id 1, :xt/valid-from #inst "2020-01-04", :xt/valid-to #inst "2020-01-05"}]])

  (tu/flush-block! tu/*node*)
  (c/compact-all! tu/*node* #xt/duration "PT1S")

  (t/is (= [{:schema-name "public", :table-name "foo", :trie-key "l00-rc-b00",
             :level 0, :trie-state "garbage", :data-file-size 1966}

            {:schema-name "public", :table-name "foo", :trie-key "l01-r20200106-b00",
             :level 1, :trie-state "live", :row-count 2, :data-file-size 1966, :recency #xt/date "2020-01-06",
             :temporal-metadata {:min-valid-from (time/->zdt #inst "2020-01-01")
                                 :max-valid-from (time/->zdt #inst "2020-01-04")
                                 :min-valid-to (time/->zdt #inst "2020-01-02")
                                 :max-valid-to (time/->zdt #inst "2020-01-05")
                                 :min-system-from (time/->zdt #inst "2020-01-01")
                                 :max-system-from (time/->zdt #inst "2020-01-01")}}

            {:schema-name "public", :table-name "foo", :trie-key "l01-rc-b00",
             :level 1, :trie-state "live", :data-file-size 1382}

            {:schema-name "xt", :table-name "txs", :trie-key "l00-rc-b00",
             :level 0, :trie-state "garbage", :data-file-size 2894}

            {:schema-name "xt", :table-name "txs", :trie-key "l01-rc-b00",
             :level 1, :trie-state "live", :row-count 1, :data-file-size 2894,
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

  (tu/flush-block! tu/*node*)

  (xt/execute-tx tu/*node* [[:put-docs :foo {:xt/id 3, :a "hello"} {:xt/id 4, :a "world"}]])

  (t/is (= [["public" "foo" 2] ["xt" "txs" 1]]
           (->> (xt/q tu/*node* "SELECT * FROM xt.live_tables ORDER BY schema_name, table_name")
                (mapv (juxt :schema-name :table-name :row-count)))))

  (t/is (= [["public" "foo" "_id" :i64]
            ["public" "foo" "a" :utf8]
            ["xt" "txs" "_id" :i64]
            ["xt" "txs" "committed" :bool]
            ["xt" "txs" "error" [:? :transit]]
            ["xt" "txs" "system_time" :instant]
            ["xt" "txs" "user_metadata" [:? :struct {}]]]
           (->> (xt/q tu/*node* "SELECT * FROM xt.live_columns ORDER BY schema_name, table_name, col_name")
                (mapv (juxt :schema-name :table-name :col-name (comp read-string :col-type)))))))

(t/deftest test-metrics
  (let [res (xt/q tu/*node* "SELECT * FROM xt.metrics_timers")]
    (t/is (= #{:name :tags :count
               :mean-time :p75-time :p95-time :p99-time :p999-time :max-time}
             (set (keys (first res)))))

    (t/is (empty? (set/difference #{"tx.op.timer", "query.timer", "compactor.job.timer"}
                                  (into #{} (map :name) res)))
          "at least has these ones, maybe others"))

  (let [res (xt/q tu/*node* "SELECT * FROM xt.metrics_gauges")]
    (t/is (= #{:tags :name :value}
             (set (keys (first res)))))
    (t/is (empty? (set/difference #{"system.cpu.count"
                                    "xtdb.allocator.memory.allocated"
                                    "jvm.memory.used"
                                    "node.tx.latestSubmittedMsgId"}
                                  (into #{} (map :name) res)))
          "at least has these ones, maybe others"))

  (let [res (xt/q tu/*node* "SELECT * FROM xt.metrics_counters")]
    (t/is (= #{:tags :name :count}
             (set (keys (first res)))))
    (t/is (empty? (set/difference #{"tx.error"
                                    "query.error" "query.warning"
                                    "pgwire.total_connections"}
                                  (into #{} (map :name) res)))
          "at least has these ones, maybe others")))

(t/deftest test-multi-db
  (pgw-test/with-playground
    (fn []
      (with-open [xt-conn (pgw-test/jdbc-conn {:dbname "xtdb"})]
        (xt/submit-tx xt-conn [[:put-docs :foo {:xt/id "xtdb"}]])

        (with-open [new-db-conn (pgw-test/jdbc-conn {:dbname "new-db"})]
          (xt/execute-tx new-db-conn [[:put-docs :foo {:xt/id :new-db}]])
          (t/is (= [{:xt/id "xtdb"}] (xt/q xt-conn ["SELECT * FROM foo"])))
          (t/is (= [{:xt/id :new-db}] (xt/q new-db-conn ["SELECT * FROM foo"])))

          ;; TODO make all databases visible in pg_database, not just the current one
          ;;
          (t/testing "adds database to pg_database"
            (t/is (= [#_{:datname "new-db"} {:datname "xtdb"}]
                     (jdbc/execute! xt-conn ["SELECT datname FROM pg_catalog.pg_database ORDER BY datname"])))

            (t/is (= [{:datname "new-db"} #_{:datname "xtdb"}]
                     (jdbc/execute! new-db-conn ["SELECT datname FROM pg_catalog.pg_database ORDER BY datname"]))))

          (t/testing "information_schema queries"
            (t/is (= #{{:table-catalog "new-db", :table-schema "information_schema"}
                       {:table-catalog "new-db", :table-schema "pg_catalog"}
                       {:table-catalog "new-db", :table-schema "public"}
                       {:table-catalog "new-db", :table-schema "xt"}}
                     (set (xt/q new-db-conn "FROM information_schema.columns SELECT DISTINCT table_catalog, table_schema")))
                  "query information_schema from new-db")

            (t/is (= [{:table-catalog "new-db", :table-schema "public", :table-name "foo", :column-name "_id", :data-type ":keyword"}]
                     (xt/q new-db-conn "SELECT table_catalog, table_schema, table_name, column_name, data_type FROM information_schema.columns WHERE table_schema = 'public' AND column_name = '_id'"))
                  "query information_schema from new-db")

            (t/is (= [{:table-catalog "xtdb", :table-schema "public", :table-name "foo", :column-name "_id", :data-type ":utf8"}]
                     (xt/q xt-conn "SELECT table_catalog, table_schema, table_name, column_name, data_type FROM information_schema.columns WHERE table_schema = 'public' AND column_name = '_id'"))
                  "query information_schema from xtdb")))))))
