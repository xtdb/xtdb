(ns xtdb.create-table-test
  (:require [clojure.test :as t]
            [next.jdbc :as jdbc]
            [xtdb.api :as xt]
            [xtdb.log :as xt-log]
            [xtdb.node :as xtn]
            [xtdb.test-util :as tu]
            [xtdb.util :as util]))

(defn- public-tables [node]
  (set (xt/q node "SELECT table_schema, table_name, table_type
                   FROM information_schema.tables
                   WHERE table_schema = 'public'")))

(defn- public-columns [node]
  (set (xt/q node "SELECT table_name, column_name, data_type, is_nullable
                   FROM information_schema.columns
                   WHERE table_schema = 'public'")))

(t/deftest declares-bare-columns
  (with-open [node (xtn/start-node)]
    (xt/execute-tx node [[:sql "CREATE TABLE foo (a, b)"]])

    (let [col-names (->> (public-columns node) (map :column-name) set)]
      (t/is (contains? col-names "a") "declared column a surfaces in information_schema.columns")
      (t/is (contains? col-names "b") "declared column b surfaces in information_schema.columns"))

    (t/is (= [] (xt/q node "SELECT a, b FROM foo"))
          "querying declared-but-empty columns returns no rows, no error")))

(defn- col-type [node table col]
  (xt/q node ["SELECT data_type, is_nullable FROM information_schema.columns
               WHERE table_name = ? AND column_name = ?" table col]))

(t/deftest declared-column-widens-cleanly
  (with-open [node (xtn/start-node)]
    (xt/execute-tx node [[:sql "CREATE TABLE foo (a)"]])

    (t/is (= [{:data-type ":nothing", :is-nullable "NO"}] (col-type node "foo" "a"))
          "a declared-but-empty column is the lattice bottom: nothing type, not nullable")

    (xt/execute-tx node [[:sql "INSERT INTO foo (_id, a) VALUES (1, 42)"]])

    (t/is (= [{:data-type ":i64", :is-nullable "NO"}] (col-type node "foo" "a"))
          "first insert widens it cleanly to a non-nullable i64 - NOT Maybe(i64)")))

(t/deftest declared-columns-survive-block-flush
  (with-open [node (xtn/start-node)]
    (xt/execute-tx node [[:sql "CREATE TABLE foo (a, b)"]])
    (tu/flush-block! node)

    (t/is (= [{:data-type ":nothing", :is-nullable "NO"}] (col-type node "foo" "a"))
          "a declared Nothing column round-trips through a block flush as Nothing, not Null")))

(t/deftest declared-column-pgwire-type
  (with-open [node (xtn/start-node)]
    (xt/execute-tx node [[:sql "CREATE TABLE foo (a)"]])

    (with-open [conn (jdbc/get-connection node)
                stmt (.prepareStatement conn "SELECT a FROM foo")
                rs (.executeQuery stmt)]
      ;; Nothing -> PgType.Null, which shares text's OID 25 on the wire ("delegates to Text for
      ;; OID/serialization, but distinct for comparison"), so a JDBC client reports it as text.
      (t/is (= "text" (.getColumnTypeName (.getMetaData rs) 1))
            "a Nothing-typed declared column reports the pgwire null type (wire-compatible with text)"))))

(t/deftest creates-an-empty-table
  (with-open [node (xtn/start-node)]
    (xt/execute-tx node [[:sql "CREATE TABLE foo"]])

    (t/is (= #{{:table-schema "public", :table-name "foo", :table-type "BASE TABLE"}}
             (public-tables node))
          "an empty table is registered and visible in information_schema.tables")

    (t/is (= [] (xt/q node "SELECT * FROM foo"))
          "querying the empty table returns no rows, no warning")))

(t/deftest create-table-is-idempotent
  (with-open [node (xtn/start-node)]
    (xt/execute-tx node [[:sql "CREATE TABLE foo"]])
    (xt/execute-tx node [[:sql "CREATE TABLE foo"]])
    (xt/execute-tx node [[:sql "CREATE OR ALTER TABLE foo"]])

    (t/is (= #{{:table-schema "public", :table-name "foo", :table-type "BASE TABLE"}}
             (public-tables node))
          "re-creating an existing table is a no-op, not an error - both spellings accepted")))

(t/deftest insert-after-create
  (with-open [node (xtn/start-node)]
    (xt/execute-tx node [[:sql "CREATE TABLE foo"]])
    (xt/execute-tx node [[:sql "INSERT INTO foo (_id, name) VALUES (1, 'bar')"]])

    (t/is (= [{:name "bar"}] (xt/q node "SELECT name FROM foo"))
          "a created table accepts inserts and widens its columns from the data")))

(t/deftest no-match-dml-does-not-create-a-table
  (with-open [node (xtn/start-node)]
    (xt/execute-tx node [[:sql "INSERT INTO existing (_id) VALUES (1)"]])
    (xt/execute-tx node [[:sql "UPDATE existing SET x = 1 WHERE _id = 999"]])

    (t/is (= #{{:table-schema "public", :table-name "existing", :table-type "BASE TABLE"}}
             (public-tables node))
          "a no-match UPDATE neither errors nor materialises a phantom table")))

(t/deftest empty-table-survives-block-flush
  (with-open [node (xtn/start-node)]
    (xt/execute-tx node [[:sql "CREATE TABLE foo"]])
    (tu/flush-block! node)

    (t/is (= [{:table-name "public/foo", :row-count 0}]
             (xt/q node ["SELECT table_name, row_count FROM xt.table_block_files
                          WHERE table_name = ? AND block_idx = ?" "public/foo" "00"]))
          "the empty table persists as a 0-row table-block written to storage")

    (t/is (= #{{:table-schema "public", :table-name "foo", :table-type "BASE TABLE"}}
             (public-tables node))
          "still visible in information_schema once the flush has cleared the live index")

    (t/is (= [] (xt/q node "SELECT * FROM foo"))
          "still queryable, empty, read back from the block catalog")))

(t/deftest empty-table-propagates-to-a-follower
  (let [node-dir (util/->path "target/create-table-test/follower")]
    (util/delete-dir node-dir)
    (with-open [primary (xtn/start-node {:log [:local {:path (.resolve node-dir "log")}]
                                         :storage [:local {:path (.resolve node-dir "objects")}]})]
      (xt/execute-tx primary [[:sql "CREATE TABLE foo"]])

      (with-open [secondary (xtn/start-node)]
        (jdbc/execute! secondary ["
ATTACH DATABASE shared_db WITH $$
  log: !Local
    path: 'target/create-table-test/follower/log'
  storage: !Local
    path: 'target/create-table-test/follower/objects'
  mode: read-only
$$"])
        (xt-log/sync-node secondary #xt/duration "PT5S")

        (t/is (= [{:table-name "foo"}]
                 (xt/q secondary "SELECT table_name FROM information_schema.tables WHERE table_name = 'foo'"
                       {:database :shared_db}))
              "an empty table created on the primary propagates to the read-only follower")))))
