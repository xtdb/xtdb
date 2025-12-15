(ns xtdb.pgwire.metabase-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.test-util :as tu]))

(t/use-fixtures :each tu/with-node)

(t/deftest information-schema-columns-test
  (t/testing "information_schema.columns returns udt_schema, udt_name, ordinal_position, is_nullable, is_identity"
    (xt/execute-tx tu/*node* [[:sql "INSERT INTO foo (_id, name, age) VALUES (1, 'test', 42)"]])

    (t/is (= [{:column-name "_id", :udt-schema "pg_catalog", :udt-name "int8",
               :ordinal-position 1, :is-nullable "NO", :is-identity "NO"}
              {:column-name "_system_from", :udt-schema "pg_catalog", :udt-name "timestamptz",
               :ordinal-position 2, :is-nullable "NO", :is-identity "NO"}
              {:column-name "_system_to", :udt-schema "pg_catalog", :udt-name "timestamptz",
               :ordinal-position 3, :is-nullable "YES", :is-identity "NO"}
              {:column-name "_valid_from", :udt-schema "pg_catalog", :udt-name "timestamptz",
               :ordinal-position 4, :is-nullable "NO", :is-identity "NO"}
              {:column-name "_valid_to", :udt-schema "pg_catalog", :udt-name "timestamptz",
               :ordinal-position 5, :is-nullable "YES", :is-identity "NO"}
              {:column-name "age", :udt-schema "pg_catalog", :udt-name "int8",
               :ordinal-position 6, :is-nullable "NO", :is-identity "NO"}
              {:column-name "name", :udt-schema "pg_catalog", :udt-name "text",
               :ordinal-position 7, :is-nullable "NO", :is-identity "NO"}]
             (xt/q tu/*node*
                   "SELECT column_name, udt_schema, udt_name, ordinal_position, is_nullable, is_identity
                    FROM information_schema.columns
                    WHERE table_name = 'foo'
                    ORDER BY ordinal_position")))))

(t/deftest metabase-describe-fields-test
  (t/testing "Metabase query to describe table fields with column metadata"
    (xt/execute-tx tu/*node* [[:sql "INSERT INTO foo (_id, name) VALUES (1, 'test')"]])

    (let [results (xt/q tu/*node*
                     "SELECT \"c\".\"column_name\" AS \"name\",
                             CASE WHEN \"c\".\"udt_schema\" IN ('public', 'pg_catalog')
                                  THEN FORMAT('%s', \"c\".\"udt_name\")
                                  ELSE FORMAT('\"%s\".\"%s\"', \"c\".\"udt_schema\", \"c\".\"udt_name\")
                             END AS \"database-type\",
                             \"c\".\"ordinal_position\" - 1 AS \"database-position\",
                             \"c\".\"table_schema\" AS \"table-schema\",
                             \"c\".\"table_name\" AS \"table-name\",
                             \"pk\".\"column_name\" IS NOT NULL AS \"pk?\",
                             COL_DESCRIPTION(CAST(CAST(FORMAT('%I.%I', CAST(\"c\".\"table_schema\" AS TEXT), CAST(\"c\".\"table_name\" AS TEXT)) AS REGCLASS) AS OID), \"c\".\"ordinal_position\") AS \"field-comment\",
                             ((\"column_default\" IS NULL) OR (LOWER(\"column_default\") = 'null')) AND (\"is_nullable\" = 'NO') AND NOT (((\"column_default\" IS NOT NULL) AND (\"column_default\" LIKE '%nextval(%')) OR (\"is_identity\" <> 'NO')) AS \"database-required\",
                             ((\"column_default\" IS NOT NULL) AND (\"column_default\" LIKE '%nextval(%')) OR (\"is_identity\" <> 'NO') AS \"database-is-auto-increment\"
                      FROM \"information_schema\".\"columns\" AS \"c\"
                      LEFT JOIN (SELECT \"tc\".\"table_schema\", \"tc\".\"table_name\", \"kc\".\"column_name\"
                                 FROM \"information_schema\".\"table_constraints\" AS \"tc\"
                                 INNER JOIN \"information_schema\".\"key_column_usage\" AS \"kc\"
                                   ON (\"tc\".\"constraint_name\" = \"kc\".\"constraint_name\")
                                   AND (\"tc\".\"table_schema\" = \"kc\".\"table_schema\")
                                   AND (\"tc\".\"table_name\" = \"kc\".\"table_name\")
                                 WHERE \"tc\".\"constraint_type\" = 'PRIMARY KEY') AS \"pk\"
                        ON (\"c\".\"table_schema\" = \"pk\".\"table_schema\")
                        AND (\"c\".\"table_name\" = \"pk\".\"table_name\")
                        AND (\"c\".\"column_name\" = \"pk\".\"column_name\")
                      WHERE c.table_schema !~ '^information_schema|catalog_history|pg_'
                        AND (\"c\".\"table_schema\" = 'public')
                      ORDER BY \"table-schema\" ASC, \"table-name\" ASC, \"database-position\" ASC")]

      (t/testing "returns results for foo table"
        (t/is (seq results)))

      (t/testing "database-type contains PostgreSQL type names"
        (t/is (= #{"int8" "timestamptz" "text"}
                 (set (map :database-type results)))))

      (t/testing "database-position is 0-indexed"
        (t/is (= [0 1 2 3 4 5] (map :database-position results))))

      (t/testing "database-required and database-is-auto-increment are booleans"
        (t/is (every? boolean? (map :database-required results)))
        (t/is (every? boolean? (map :database-is-auto-increment results)))))))

(t/deftest metabase-describe-fields-with-pg-catalog-test
  (t/testing "Metabase query for materialized views returns empty (no materialized views in XTDB)"
    (xt/execute-tx tu/*node* [[:sql "INSERT INTO foo (_id, name) VALUES (1, 'test')"]])

    (t/is (= []
             (xt/q tu/*node*
                   "SELECT \"pa\".\"attname\" AS \"name\",
                           CASE WHEN \"ptn\".\"nspname\" IN ('public', 'pg_catalog')
                                THEN FORMAT('%s', \"pt\".\"typname\")
                                ELSE FORMAT('\"%s\".\"%s\"', \"ptn\".\"nspname\", \"pt\".\"typname\")
                           END AS \"database-type\",
                           \"pa\".\"attnum\" - 1 AS \"database-position\",
                           \"pn\".\"nspname\" AS \"table-schema\",
                           \"pc\".\"relname\" AS \"table-name\",
                           FALSE AS \"pk?\",
                           NULL AS \"field-comment\",
                           FALSE AS \"database-required\",
                           FALSE AS \"database-is-auto-increment\"
                    FROM \"pg_catalog\".\"pg_class\" AS \"pc\"
                    INNER JOIN \"pg_catalog\".\"pg_namespace\" AS \"pn\" ON \"pn\".\"oid\" = \"pc\".\"relnamespace\"
                    INNER JOIN \"pg_catalog\".\"pg_attribute\" AS \"pa\" ON \"pa\".\"attrelid\" = \"pc\".\"oid\"
                    INNER JOIN \"pg_catalog\".\"pg_type\" AS \"pt\" ON \"pt\".\"oid\" = \"pa\".\"atttypid\"
                    INNER JOIN \"pg_catalog\".\"pg_namespace\" AS \"ptn\" ON \"ptn\".\"oid\" = \"pt\".\"typnamespace\"
                    WHERE (\"pc\".\"relkind\" = 'm') AND (\"pa\".\"attnum\" >= 1) AND (\"pn\".\"nspname\" = 'public')
                    ORDER BY \"table-schema\" ASC, \"table-name\" ASC, \"database-position\" ASC")))))

(t/deftest metabase-describe-fks-test
  (t/testing "Metabase query to describe foreign key relationships (pg_constraint not yet supported)"
    (t/is (= []
             (xt/q tu/*node*
                   "SELECT \"fk_ns\".\"nspname\" AS \"fk-table-schema\",
                           \"fk_table\".\"relname\" AS \"fk-table-name\",
                           \"fk_column\".\"attname\" AS \"fk-column-name\",
                           \"pk_ns\".\"nspname\" AS \"pk-table-schema\",
                           \"pk_table\".\"relname\" AS \"pk-table-name\",
                           \"pk_column\".\"attname\" AS \"pk-column-name\"
                    FROM \"pg_constraint\" AS \"c\"
                    INNER JOIN \"pg_class\" AS \"fk_table\" ON \"c\".\"conrelid\" = \"fk_table\".\"oid\"
                    INNER JOIN \"pg_namespace\" AS \"fk_ns\" ON \"c\".\"connamespace\" = \"fk_ns\".\"oid\"
                    INNER JOIN \"pg_attribute\" AS \"fk_column\" ON \"c\".\"conrelid\" = \"fk_column\".\"attrelid\"
                    INNER JOIN \"pg_class\" AS \"pk_table\" ON \"c\".\"confrelid\" = \"pk_table\".\"oid\"
                    INNER JOIN \"pg_namespace\" AS \"pk_ns\" ON \"pk_table\".\"relnamespace\" = \"pk_ns\".\"oid\"
                    INNER JOIN \"pg_attribute\" AS \"pk_column\" ON \"c\".\"confrelid\" = \"pk_column\".\"attrelid\"
                    WHERE fk_ns.nspname !~ '^information_schema|catalog_history|pg_'
                      AND (\"c\".\"contype\" = 'f'::char)
                      AND (\"fk_ns\".\"nspname\" = 'public')
                    ORDER BY \"fk-table-schema\" ASC, \"fk-table-name\" ASC")))))

(t/deftest transit-error-column-case-expression-test
  (t/testing "CASE expression on transit error column doesn't throw InvalidWriteObjectException"
    (xt/execute-tx tu/*node* [[:sql "INSERT INTO foo (_id) VALUES (1)"]])
    (try
      (xt/execute-tx tu/*node* [[:sql "INSERT INTO bar SELECT * FROM nonexistent"]])
      (catch Exception _))

    (t/testing "simple query on error column works"
      (t/is (seq (xt/q tu/*node* "SELECT error FROM xt.txs"))))

    (t/testing "CASE expression returning error column directly (Metabase pattern)"
      (t/is (seq (xt/q tu/*node*
                       "SELECT CASE WHEN false
                                    THEN NULL
                                    ELSE error
                               END AS error
                        FROM xt.txs"))))

    (t/testing "CASE expression with LENGTH check on error column"
      (t/is (seq (xt/q tu/*node*
                       "SELECT CASE WHEN 50000 < LENGTH(CAST(error AS TEXT))
                                    THEN NULL
                                    ELSE error
                               END AS error
                        FROM xt.txs"))))))
