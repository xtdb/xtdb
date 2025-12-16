(ns xtdb.pgwire.metabase-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.test-util :as tu]))

(t/use-fixtures :each tu/with-node)

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
