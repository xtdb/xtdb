(ns xtdb.expression.pg-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.error]
            [xtdb.test-util :as tu])
  (:import (xtdb.types Oid RegClass RegProc)))

(t/use-fixtures :each tu/with-mock-clock tu/with-node)

(defn- ->table-oid
  "The oid pg_class reports for a table. Resolved rather than hardcoded: it is allocated when the
   catalog first sees the table, so a literal would pin the order the tests happened to create them in."
  [relname]
  (-> (xt/q tu/*node* (format "SELECT oid FROM pg_catalog.pg_class WHERE relname = '%s'" relname))
      first :oid))

(t/deftest test-oid
  (xt/submit-tx tu/*node* [[:sql "INSERT INTO foo (_id) VALUES (1)"]])

  (t/is (= [{:v (Oid. 12345)}]
           (xt/q tu/*node* "SELECT 12345::oid v"))
        "int -> oid")

  (t/is (= [{:v 12345}]
           (xt/q tu/*node* "SELECT 12345::oid::int v"))
        "oid -> int")

  (t/is (= [{:v (Oid. (->table-oid "foo"))}]
           (xt/q tu/*node* "SELECT 'foo'::regclass::oid v"))
        "regclass -> oid")

  (t/is (= [{:v (Oid. 1989914641)}]
           (xt/q tu/*node* "SELECT 'array_in'::regproc::oid v"))
        "regproc -> oid")

  (t/is (= [{:v true}]
           (xt/q tu/*node* "SELECT 12345::oid = 12345::oid v"))
        "oid equality"))

(t/deftest test-col-description
  (t/testing "col_description returns NULL (stub - XTDB doesn't support comments yet)"
    (t/is (= [{}]
             (xt/q tu/*node* "SELECT COL_DESCRIPTION(12345, 1) AS descr")))
    (t/is (= [{}]
             (xt/q tu/*node* "SELECT COL_DESCRIPTION(12345::oid, 1) AS descr"))
          "with explicit cast to oid")))

(t/deftest test-regclass
  (xt/submit-tx tu/*node* [[:sql "INSERT INTO foo (_id) VALUES (1)"]])

  (t/testing "regclass uses search path to resolve unqualified names"
    (t/is (= [{:v (RegClass. (->table-oid "foo"))}]
             (xt/q tu/*node* "SELECT 'public.foo'::regclass v")
             (xt/q tu/*node* "SELECT 'foo'::regclass v"))
          "text -> regclass"))

  (t/testing "regclass name is returned qualified only if the schema does not appear in search path"
    (t/is (= [{:v "foo"}]
             (xt/q tu/*node* "SELECT 'public.foo'::regclass::varchar v"))
          "text -> regclass -> text")

    (t/is (= [{:v "information_schema.columns"}]
             (xt/q tu/*node* "SELECT 'information_schema.columns'::regclass::varchar v"))
          "text -> regclass -> text"))

  (t/is (= [{:v (->table-oid "foo")}]
           (xt/q tu/*node* "SELECT 'public.foo'::regclass::int v"))
        "text -> regclass -> int")

  (t/is (= [{:v (RegClass. 1151209360)}]
           (xt/q tu/*node* "SELECT 1151209360::regclass v"))
        "int -> regclass")

  (t/is (anomalous? [:incorrect nil
                     #"Relation public.baz does not exist"]
                    (xt/q tu/*node* "SELECT 'public.baz'::regclass")))

  (t/testing "regclass that doesn't match a known table"
    (t/is (= [{:v "999999"}]
             (xt/q tu/*node* "SELECT 999999::regclass::varchar v"))
          "returns a stringified oid/int"))

  (t/is (= '[_id _system_from _system_to _valid_from _valid_to]
           (->> (xt/q tu/*node* "SELECT attname FROM pg_attribute WHERE attrelid = 'public.foo'::regclass ORDER BY attname")
                (mapv (comp symbol :attname)))))

  (t/is (= [{:v true}]
           (xt/q tu/*node* (format "SELECT %s::regclass = 'foo'::regclass v" (->table-oid "foo"))))
        "regclass with identical oid are equal")

  (t/testing "testing non-literal cast"
    (xt/submit-tx tu/*node* [[:sql "INSERT INTO bar RECORDS {_id: 1, tn: 'foo'}"]])

    (t/is (= [{:v (RegClass. (->table-oid "foo"))}]
             (xt/q tu/*node* "SELECT tn::regclass v FROM bar"))
          "text -> regclass")))

(t/deftest test-regclass-where-expr
  (xt/submit-tx tu/*node* [[:sql "INSERT INTO bar RECORDS {_id: 1}"]
                           [:sql "INSERT INTO bar RECORDS {_id: 2}"]])

  (t/is (= [{:xt/id 2} {:xt/id 1}]
           (xt/q tu/*node* (format "SELECT _id FROM bar WHERE 'bar'::regclass = %s" (->table-oid "bar"))))))

(t/deftest test-regproc
  (t/is (= [{:v (RegProc. 1989914641)}]
           (xt/q tu/*node* "SELECT 'pg_catalog.array_in'::regproc v")
           (xt/q tu/*node* "SELECT 'array_in'::regproc v"))
        "text -> regproc")

  (t/is (= [{:v (RegProc. 1989914641)}]
           (xt/q tu/*node* "SELECT 1989914641::regproc v"))
        "int -> regproc")

  (t/is (anomalous? [:incorrect nil
                     #"Procedure public.baz does not exist"]
                    (xt/q tu/*node* "SELECT 'public.baz'::regproc")))

  (t/is (= [{:v "999999"}]
           (xt/q tu/*node* "SELECT 999999::regproc::varchar v"))
        "returns a stringified oid/int")

  (t/is (= [{:v true}]
           (xt/q tu/*node* "SELECT 1989914641::regproc = 'array_in'::regproc v"))
        "regproc with identical oid are equal"))

(t/deftest test-regclass-search-path-precedence
  (xt/submit-tx tu/*node* [[:sql "INSERT INTO pg_class RECORDS {_id: 1}"]])

  (t/is (= [{:v (RegClass. 529124840)}]
           (xt/q tu/*node* "SELECT 'pg_class'::regclass v")
           (xt/q tu/*node* "SELECT 'pg_catalog.pg_class'::regclass v"))
        "matches pg_catalog.pg_class over public.pg_class as the former comes first in the search path"))

(t/deftest test-json-arrow-operator
  (xt/submit-tx tu/*node* [[:put-docs :json_data {:xt/id 1 :data {:age 25 :name "Alice" :nested {:inner 42}}}]])

  (t/is (= [{:v 25}]
           (xt/q tu/*node* "SELECT data->'age' AS v FROM json_data"))
        "-> with string literal field access")

  (t/is (= [{:v 42}]
           (xt/q tu/*node* "SELECT data->'nested'->'inner' AS v FROM json_data"))
        "nested field access with chained ->")

  (t/is (= [{}]
           (xt/q tu/*node* "SELECT data->'nonexistent' AS v FROM json_data"))
        "non-existent field returns NULL")

  (t/is (= Long (type (:v (first (xt/q tu/*node* "SELECT data->'age' AS v FROM json_data")))))
        "preserves type (returns integer, not string)"))

(t/deftest test-json-arrow-text-operator
  (xt/submit-tx tu/*node* [[:put-docs :json_data {:xt/id 1 :data {:age 25 :name "Alice" :nested {:inner 42}}}]])

  (t/is (= [{:v "25"}]
           (xt/q tu/*node* "SELECT data->>'age' AS v FROM json_data"))
        "->> with string literal field access returns text")

  (t/is (= [{:v "42"}]
           (xt/q tu/*node* "SELECT data->'nested'->>'inner' AS v FROM json_data"))
        "nested field access with chained ->> returns text")

  (t/is (= [{:v "Alice"}]
           (xt/q tu/*node* "SELECT data->>'name' AS v FROM json_data"))
        "string field returns string value")

  (t/is (= [{}]
           (xt/q tu/*node* "SELECT data->>'nonexistent' AS v FROM json_data"))
        "non-existent field returns NULL"))

(t/deftest test-json-path-operator
  (xt/submit-tx tu/*node* [[:put-docs :json_data {:xt/id 1 :data {:age 25 :name "Alice" :nested {:inner 42}}}]])

  (t/is (= [{:v 42}]
           (xt/q tu/*node* "SELECT data #> ARRAY['nested', 'inner'] AS v FROM json_data"))
        "#> with path array accesses nested fields")

  (t/is (= [{:v 25}]
           (xt/q tu/*node* "SELECT data #> ARRAY['age'] AS v FROM json_data"))
        "#> with single element path")

  (t/is (= [{}]
           (xt/q tu/*node* "SELECT data #> ARRAY['nonexistent'] AS v FROM json_data"))
        "non-existent path returns NULL")

  (t/is (= Long (type (:v (first (xt/q tu/*node* "SELECT data #> ARRAY['nested', 'inner'] AS v FROM json_data")))))
        "preserves type (returns integer, not string)"))

(t/deftest test-json-path-text-operator
  (xt/submit-tx tu/*node* [[:put-docs :json_data {:xt/id 1 :data {:age 25 :name "Alice" :nested {:inner 42}}}]])

  (t/is (= [{:v "42"}]
           (xt/q tu/*node* "SELECT data #>> ARRAY['nested', 'inner'] AS v FROM json_data"))
        "#>> with path array accesses nested fields as text")

  (t/is (= [{:v "25"}]
           (xt/q tu/*node* "SELECT data #>> ARRAY['age'] AS v FROM json_data"))
        "#>> with single element path returns text")

  (t/is (= [{:v "Alice"}]
           (xt/q tu/*node* "SELECT data #>> ARRAY['name'] AS v FROM json_data"))
        "string field returns string value")

  (t/is (= [{}]
           (xt/q tu/*node* "SELECT data #>> ARRAY['nonexistent'] AS v FROM json_data"))
        "non-existent path returns NULL"))

(t/deftest test-transit-field-access
  (xt/submit-tx tu/*node* [[:sql "INSERT INTO foo (_id) VALUES (1)"]])
  (try
    (xt/execute-tx tu/*node* [[:sql "INSERT INTO bar SELECT * FROM nonexistent"]])
    (catch Exception _))

  (t/testing "-> operator returns transit"
    (t/is (seq (xt/q tu/*node*
                     "SELECT error->'sql' AS v FROM xt.txs WHERE error IS NOT NULL"))))

  (t/testing "->> operator returns text"
    (t/is (= [{:v "INSERT INTO bar SELECT * FROM nonexistent"}]
             (xt/q tu/*node*
                   "SELECT error->>'sql' AS v FROM xt.txs WHERE error IS NOT NULL"))))

  (t/testing "#> path access"
    (t/is (seq (xt/q tu/*node*
                     "SELECT error #> ARRAY['tx-key'] AS v FROM xt.txs WHERE error IS NOT NULL"))))

  (t/testing "#>> path access returns text"
    (let [result (xt/q tu/*node*
                       "SELECT error #>> ARRAY['tx-key', 'tx-id'] AS v FROM xt.txs WHERE error IS NOT NULL")]
      (t/is (string? (:v (first result))))))

  (t/testing "chained access"
    (t/is (seq (xt/q tu/*node*
                     "SELECT (error->'tx-key')->>'tx-id' AS v FROM xt.txs WHERE error IS NOT NULL"))))

  (t/testing "cast to decimal"
    (let [result (xt/q tu/*node*
                       "SELECT (error->>'tx-op-idx')::decimal AS v FROM xt.txs WHERE error IS NOT NULL")]
      (t/is (decimal? (:v (first result)))))))

(t/deftest test-format-type
  (t/is (= [{:int8 "bigint", :int4 "integer", :int2 "smallint", :float4 "real", :float8 "double precision"
             :bool "boolean", :text "text", :varchar "character varying", :numeric "numeric"
             :ts "timestamp without time zone", :tstz "timestamp with time zone", :date "date"
             :time "time without time zone", :interval "interval", :bytea "bytea", :int4s "integer[]"
             :int8s "bigint[]", :texts "text[]", :uuid "uuid", :oid "oid", :json "json", :jsonb "jsonb"}]
           (xt/q tu/*node* "SELECT format_type(20, NULL) int8, format_type(23, NULL) int4, format_type(21, NULL) int2,
                                   format_type(700, NULL) float4, format_type(701, NULL) float8, format_type(16, NULL) bool,
                                   format_type(25, NULL) text, format_type(1043, NULL) varchar, format_type(1700, NULL) numeric,
                                   format_type(1114, NULL) ts, format_type(1184, NULL) tstz, format_type(1082, NULL) date,
                                   format_type(1083, NULL) time, format_type(1186, NULL) interval, format_type(17, NULL) bytea,
                                   format_type(1007, NULL) int4s, format_type(1016, NULL) int8s, format_type(1009, NULL) texts,
                                   format_type(2950, NULL) uuid, format_type(26, NULL) oid, format_type(114, NULL) json,
                                   format_type(3802, NULL) jsonb"))
        "format_type spells each oid as Postgres does")

  (t/is (= [{:a "character varying(10)", :b "numeric(10,2)", :c "timestamp(3) without time zone"
             :d "timestamp(6) with time zone", :e "text", :f "bigint"}]
           (xt/q tu/*node* "SELECT format_type(1043, 14) a, format_type(1700, ((10 * 65536) + 2) + 4) b, format_type(1114, 3) c,
                                   format_type(1184, 6) d, format_type(25, 14) e, format_type(20, -1) f"))
        "typmod is applied where the type takes one")

  (t/is (= [{:a "-", :b "???"}]
           (xt/q tu/*node* "SELECT format_type(0, NULL) a, format_type(999999, NULL) b")))

  (t/is (= [{}] (xt/q tu/*node* "SELECT format_type(NULL, NULL) a"))
        "a NULL oid gives NULL")

  (t/is (= [{:v "timestamp with time zone"}]
           (xt/q tu/*node* "SELECT pg_catalog.format_type(atttypid, atttypmod) v FROM pg_attribute WHERE attname = '_system_from' LIMIT 1"))
        "composes with pg_attribute, as psql's describe-result does"))
