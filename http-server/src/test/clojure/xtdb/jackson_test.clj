(ns xtdb.jackson-test
  (:require [clojure.test :as t :refer [deftest]]
            [jsonista.core :as json]
            [xtdb.error :as err]
            [xtdb.jackson :as jackson]
            xtdb.serde)
  (:import (java.time Instant)
           (java.util List)
           (xtdb.api TransactionKey TxOptions)
           (xtdb.jackson XtdbMapper)
           (xtdb.query Basis Binding Expr Expr Query Query$OrderDirection Query$OrderNulls Query$QueryTail Query$Unify QueryOpts QueryRequest TemporalFilter)
           (xtdb.tx TxOp Tx Sql)))

(defn- roundtrip-json-ld [v]
  (-> (json/write-value-as-string v jackson/json-ld-mapper)
      (json/read-value jackson/json-ld-mapper)))

(deftest test-json-ld-roundtripping
  (let [v {:keyword :foo/bar
           :set-key #{:foo :baz}
           :instant #time/instant "2023-12-06T09:31:27.570827956Z"
           :date #time/date "2020-01-01"
           :date-time #time/date-time "2020-01-01T12:34:56.789"
           :zoned-date-time #time/zoned-date-time "2020-01-01T12:34:56.789Z"
           :time-zone #time/zone "America/Los_Angeles"
           :duration #time/duration "PT3H1M35.23S"}]
    (t/is (= v
             (roundtrip-json-ld v))
          "testing json ld values"))

  (let [ex (err/illegal-arg :divison-by-zero {:foo "bar"})
        roundtripped-ex (roundtrip-json-ld ex)]

    (t/testing "testing exception encoding/decoding"
      (t/is (= (ex-message ex)
               (ex-message roundtripped-ex)))

      (t/is (= (ex-data ex)
               (ex-data roundtripped-ex))))))

(defn roundtrip-tx-op [v]
  (.readValue XtdbMapper/TX_OP_MAPPER (json/write-value-as-string v jackson/json-ld-mapper) TxOp))

(deftest deserialize-tx-op-test
  (t/testing "put"
    (t/is (= #xt.tx/put {:table-name :docs,
                         :doc {"foo" :bar, "xt/id" "my-id"},
                         :valid-from nil,
                         :valid-to nil}
             (roundtrip-tx-op {"put" "docs"
                               "doc" {"xt/id" "my-id" "foo" :bar}})))

    (t/is (= #xt.tx/put {:table-name :docs,
                         :doc {"foo" :bar, "xt/id" "my-id"},
                         :valid-from #time/instant "2020-01-01T00:00:00Z",
                         :valid-to #time/instant "2021-01-01T00:00:00Z"}
             (roundtrip-tx-op {"put" "docs"
                               "doc" {"xt/id" "my-id" "foo" :bar}
                               "valid_from" #inst "2020"
                               "valid_to" #inst "2021"})))

    (t/is (thrown-with-msg? IllegalArgumentException #"Illegal argument: ':xtdb/malformed-put'"
                            (roundtrip-tx-op
                             {"put" "docs"
                              "doc" "blob"
                              "valid_from" #inst "2020"
                              "valid_to" #inst "2021"}))))

  (t/testing "delete"
    (t/is (= #xt.tx/delete {:table-name :docs,
                            :xt/id "my-id",
                            :valid-from nil,
                            :valid-to nil}
             (roundtrip-tx-op {"delete" "docs"
                               "id" "my-id"})))

    (t/is (= #xt.tx/delete {:table-name :docs,
                            :xt/id :keyword-id,
                            :valid-from nil,
                            :valid-to nil}
             (roundtrip-tx-op {"delete" "docs"
                               "id" :keyword-id})))

    (t/is (= #xt.tx/delete {:table-name :docs,
                            :xt/id "my-id",
                            :valid-from #time/instant "2020-01-01T00:00:00Z",
                            :valid-to #time/instant "2021-01-01T00:00:00Z"}
             (roundtrip-tx-op {"delete" "docs"
                               "id" "my-id"
                               "valid_from" #inst "2020"
                               "valid_to" #inst "2021"})))

    (t/is (thrown-with-msg? IllegalArgumentException #"Illegal argument: ':xtdb/malformed-delete'"
                            (roundtrip-tx-op
                             {"delete" "docs"
                              "id" "my-id"
                              "valid_from" ["not-a-date"]}))))
  (t/testing "erase"
    (t/is (= #xt.tx/erase {:table-name :docs,
                           :xt/id "my-id"}
             (roundtrip-tx-op {"erase" "docs"
                               "id" "my-id"})))

    (t/is (= #xt.tx/erase {:table-name :docs,
                           :xt/id :keyword-id}
             (roundtrip-tx-op {"erase" "docs"
                               "id" :keyword-id})))

    ;; TODO: Add some unsupported type in here
    ;; (t/is (thrown-with-msg? IllegalArgumentException #"Illegal argument: ':xtdb/malformed-erase"
    ;;                         (roundtrip-tx-op
    ;;                          {"erase" "docs"
    ;;                           "id" "my-id"})))
    )

  (t/testing "call"
    (t/is (= #xt.tx/call {:fn-id :my-fn
                          :args ["args"]}
             (roundtrip-tx-op {"call" :my-fn
                               "args" ["args"]})))

    (t/is (= #xt.tx/call {:fn-id "my-fn"
                          :args ["args"]}
             (roundtrip-tx-op {"call" "my-fn"
                               "args" ["args"]})))

    (t/is (thrown-with-msg? IllegalArgumentException #"Illegal argument: ':xtdb/malformed-call"
                            (roundtrip-tx-op
                             {"call" "my-fn"
                              "args" {"not" "a-list"}}))))

  (t/testing "sql"
    (t/is (= #xt.tx/sql {:sql "INSERT INTO docs (xt$id, foo) VALUES (1, \"bar\")"}
             (roundtrip-tx-op {"sql" "INSERT INTO docs (xt$id, foo) VALUES (1, \"bar\")" })))

    (t/is (= #xt.tx/sql {:sql "INSERT INTO docs (xt$id, foo) VALUES (?, ?)", :arg-rows [[1 "bar"] [2 "toto"]]}
             (roundtrip-tx-op {"sql" "INSERT INTO docs (xt$id, foo) VALUES (?, ?)"
                               "arg_rows" [[1 "bar"] [2 "toto"]]})))

    (t/is (thrown-with-msg? IllegalArgumentException #"Illegal argument: ':xtdb/malformed-sql-op"
                            (roundtrip-tx-op
                             {"sql" "INSERT INTO docs (xt$id, foo) VALUES (?, ?)"
                              "arg_rows" [1 "bar"]})))))

(defn roundtrip-tx [v]
  (.readValue XtdbMapper/TX_OP_MAPPER (json/write-value-as-string v jackson/json-ld-mapper) Tx))

(deftest deserialize-tx-test
  (t/is (= (Tx. [#xt.tx/put {:table-name :docs,
                             :doc {"xt/id" "my-id"},
                             :valid-from nil,
                             :valid-to nil}],
                (TxOptions.))
           (roundtrip-tx {"tx_ops" [{"put" "docs"
                                     "doc" {"xt/id" "my-id"}}]})))

  (t/is (= (Tx. [#xt.tx/put {:table-name :docs,
                             :doc {"xt/id" "my-id"},
                             :valid-from nil,
                             :valid-to nil}],
                (TxOptions. #time/instant "2020-01-01T12:34:56.789Z"
                            #time/zone "America/Los_Angeles"
                            false))
           (roundtrip-tx {"tx_ops" [{"put" "docs"
                                     "doc" {"xt/id" "my-id"}}]
                          "tx_options" {"system_time" #time/instant "2020-01-01T12:34:56.789Z"
                                        "default_tz" #time/zone "America/Los_Angeles"}}))
        "transaction options")

  (t/is (thrown-with-msg? IllegalArgumentException #"Illegal argument: ':xtdb/malformed-tx-ops'"
                          (roundtrip-tx {"tx_ops" {"put" "docs"
                                                   "doc" {"xt/id" "my-id"}}}))
        "put not wrapped throws"))

(defn- roundtrip-expr [e]
  (.readValue XtdbMapper/QUERY_MAPPER (json/write-value-as-string e jackson/json-ld-mapper) Expr))

(deftest deserialize-expr-test
  (t/is (= Expr/NULL
           (roundtrip-expr nil))
        "null")
  (t/is (= (Expr/val "foo")
           (roundtrip-expr "foo"))
        "string")
  (t/is (= (Expr/lVar "foo")
           (roundtrip-expr {"xt:lvar" "foo"}))
        "logic-var")
  (t/is (= (Expr/param "foo")
           (roundtrip-expr {"xt:param" "foo"}))
        "param")
  (t/is (= Expr/FALSE
           (roundtrip-expr false)))
  (t/is (= (Expr/val (long 1))
           (roundtrip-expr (long 1))))
  (t/is (= (Expr/val (double 1.2))
           (roundtrip-expr 1.2)))

  (t/is (= (Expr/exists (-> (Query/from "docs")
                            (.binding [(Binding. "xt/id" (Expr/lVar "xt/id"))]))
                        [])
           (roundtrip-expr {"xt:exists" {"query" {"from" "docs"
                                                  "bind" ["xt/id"]}
                                         "bind" []}}))
        "exists")

  (t/is (= (Expr/q (-> (Query/from "docs")
                       (.binding [(Binding. "xt/id" (Expr/lVar "xt/id"))]))
                   [])
           (roundtrip-expr {"xt:q" {"query" {"from" "docs"
                                             "bind" ["xt/id"]}
                                    "bind" []}}))
        "subquery")

  (t/is (= (Expr/pull (-> (Query/from "docs")
                          (.binding [(Binding. "xt/id" (Expr/lVar "xt/id"))]))
                      [])
           (roundtrip-expr {"xt:pull" {"query" {"from" "docs"
                                                "bind" ["xt/id"]}
                                       "bind" []}}))
        "pull")

  (t/is (= (Expr/pullMany (-> (Query/from "docs")
                              (.binding [(Binding. "xt/id" (Expr/lVar "xt/id"))]))
                          [])
           (roundtrip-expr {"xt:pull_many" {"query" {"from" "docs"
                                                     "bind" ["xt/id"]}
                                            "bind" []}}))
        "pull")

  (t/is (= (Expr/call "+" [(Expr/val "foo") (Expr/val "bar")])
           (roundtrip-expr {"xt:call" {"f" "+"
                                       "args" ["foo" "bar"]}}))
        "call")

  (t/is (= (Expr/call "+" [(Expr/val "foo") (Expr/val "bar")])
           (roundtrip-expr {"xt:call" {"f" "+"
                                       "args" ["foo" "bar"]}}))
        "call")

  (t/is (= (Expr/list (list (Expr/val 1) (Expr/val 2)))
           (roundtrip-expr [1 2]))
        "list")

  (t/is (= (Expr/map {"foo" (Expr/val 1)})
           (roundtrip-expr {"foo" 1}))
        "maps")

  (t/is (= (Expr/set #{(Expr/val 1) (Expr/val :foo)})
           (roundtrip-expr #{1 :foo}))
        "sets"))

(defn- roundtrip-temporal-filter [v]
  (.readValue XtdbMapper/QUERY_MAPPER (json/write-value-as-string v jackson/json-ld-mapper) TemporalFilter))

(deftest deserialize-temporal-filter-test
  (t/is (= TemporalFilter/ALL_TIME (roundtrip-temporal-filter "all_time"))
        "all-time")
  (t/is (thrown-with-msg? IllegalArgumentException #"Illegal argument: ':xtql/malformed-temporal-filter'"
                          (roundtrip-temporal-filter "all_times"))
        "all-time (wrong format)")

  (t/is (= (TemporalFilter/at (Expr/val #time/instant "2020-01-01T00:00:00Z"))
           (roundtrip-temporal-filter {"at" #inst "2020"}))
        "at")
  (t/is (= (TemporalFilter/from (Expr/val #time/instant "2020-01-01T00:00:00Z"))
           (roundtrip-temporal-filter {"from" #inst "2020"}))
        "from")
  (t/is (= (TemporalFilter/to (Expr/val #time/instant "2020-01-01T00:00:00Z"))
           (roundtrip-temporal-filter {"to" #inst "2020"}))
        "to")
  (t/is (= (TemporalFilter/in (Expr/val #time/instant "2020-01-01T00:00:00Z") (Expr/val #time/instant "2021-01-01T00:00:00Z"))
           (roundtrip-temporal-filter {"in" [#inst "2020" #inst "2021"]}))
        "in")
  (t/is (thrown-with-msg? IllegalArgumentException #"In TemporalFilter expects array of 2 timestamps"
                          (roundtrip-temporal-filter {"in" [#inst "2020"]}))
        "in with wrong arguments"))

(defn- roundtrip-query [v]
  (.readValue XtdbMapper/QUERY_MAPPER (json/write-value-as-string v jackson/json-ld-mapper) Query))

(deftest deserialize-query-test
  (t/is (= (-> (Query/from "docs")
               (.binding [(Binding. "xt/id" (Expr/lVar "xt/id"))
                          (Binding. "a" (Expr/lVar "b"))]))
           (roundtrip-query {"from" "docs"
                             "bind" ["xt/id" {"a" {"xt:lvar" "b"}}]})))

  (t/is (= (-> (Query/from "docs")
               (.binding [(Binding. "xt/id" (Expr/lVar "xt/id"))])
               (.forValidTime (TemporalFilter/at (Expr/val #time/instant "2020-01-01T00:00:00Z")))
               (.forSystemTime TemporalFilter/ALL_TIME))
           (roundtrip-query {"from" "docs"
                             "bind" ["xt/id"]
                             "for_valid_time" {"at" #inst "2020"}
                             "for_system_time" "all_time"}))
        "from with temporal bounds")

  (t/is (thrown-with-msg? IllegalArgumentException #"Illegal argument: ':xtdb/malformed-spec'"
                          (roundtrip-query {"from" "docs"
                                            "bind" "xt/id"}))
        "bind not an array")

  (t/is (= (Query/pipeline (-> (Query/from "docs")
                               (.binding [(Binding. "xt/id" (Expr/lVar "xt/id"))]))
                           [(Query/limit 10)])
           (roundtrip-query [{"from" "docs"
                              "bind" ["xt/id"]}
                             {"limit" 10}]))
        "pipeline")

  (t/is (= (Query/unify [(-> (Query/from "docs") (.binding [(Binding. "xt/id" (Expr/lVar "xt/id"))]))
                         (-> (Query/from "docs") (.binding [(Binding. "xt/id" (Expr/lVar "xt/id"))]))])
           (roundtrip-query {"unify" [{"from" "docs"
                                       "bind" ["xt/id"]}
                                      {"from" "docs"
                                       "bind" ["xt/id"]}]}))
        "unify")

  (t/testing "rel"
    (t/is (= (Query/relation ^List (list {"foo" (Expr/val :bar)}) ^List (list (Binding. "foo" (Expr/lVar "foo"))))
             (roundtrip-query {"rel" [{"foo" :bar}]
                               "bind" ["foo"]})))
    (t/is (= (Query/relation (Expr/param "bar") ^List (list (Binding. "foo" (Expr/lVar "foo"))))
             (roundtrip-query {"rel" {"xt:param" "bar"}
                               "bind" ["foo"]})))))

(defn- roundtrip-query-tail [v]
  (.readValue XtdbMapper/QUERY_MAPPER (json/write-value-as-string v jackson/json-ld-mapper) Query$QueryTail))

(deftest deserialize-query-tail-test
  (t/testing "where"
    (t/is (= (Query/where [(Expr/call ">=" [(Expr/val 1) (Expr/val 2)])
                           (Expr/call "<" [(Expr/val 1) (Expr/val 2)])])
             (roundtrip-query-tail {"where" [{"xt:call" {"f" ">="
                                                         "args" [1 2]}}
                                             {"xt:call" {"f" "<"
                                                         "args" [1 2]}}]})))

    (t/is (thrown-with-msg? IllegalArgumentException #"Where should be a list of expressions"
                            (roundtrip-query-tail {"where" "not-a-list"}))
          "should fail when not a list"))

  (t/testing "limit"
    (t/is (= (Query/limit 100)
             (roundtrip-query-tail {"limit" 100})))

    (t/is (thrown-with-msg? IllegalArgumentException #"Limit should be a valid number"
                            (roundtrip-query-tail {"limit" "not-a-limit"}))))

  (t/testing "offset"
    (t/is (= (Query/offset 100)
             (roundtrip-query-tail {"offset" 100})))

    (t/is (thrown-with-msg? IllegalArgumentException #"Offset should be a valid number"
                            (roundtrip-query-tail {"offset" "not-an-offset"}))))

  (t/testing "orderBy"
    (t/is (= (Query/orderBy [(Query/orderSpec (Expr/lVar "someField") nil nil)])
             (roundtrip-query-tail {"orderBy" ["someField"]})))

    (t/is (= (Query/orderBy [(Query/orderSpec (Expr/lVar "someField") Query$OrderDirection/ASC Query$OrderNulls/FIRST)])
             (roundtrip-query-tail {"orderBy" [{"val" {"xt:lvar" "someField"}, "dir" "asc", "nulls" "first"}]})))

    (t/is (thrown-with-msg? IllegalArgumentException #"Invalid orderBy direction"
                            (roundtrip-query-tail {"orderBy" [{"val" {"lvar" "someField"}, "dir" "invalid-direction"}]})))

    (t/is (thrown-with-msg? IllegalArgumentException #"Invalid orderBy nulls"
                            (roundtrip-query-tail {"orderBy" [{"val" {"lvar" "someField"}, "nulls" "invalid-nulls"}]}))))

  (t/testing "return"
    (t/is (= (Query/returning [(Binding. "a" (Expr/lVar "a"))
                               (Binding. "b" (Expr/lVar "b"))])
             (roundtrip-query-tail {"return" ["a" "b"]})))

    (t/is (= (Query/returning [(Binding. "a" (Expr/lVar "a"))
                               (Binding. "b" (Expr/lVar "c"))])
             (roundtrip-query-tail {"return" [{"a" {"xt:lvar" "a"} "b" {"xt:lvar" "c"}}]})))

    (t/is (thrown-with-msg? IllegalArgumentException #"Return should be a list of values"
                            (roundtrip-query-tail {"return" "a"}))))

  (t/testing "unnest"
    (t/is (= (Query/unnestCol (Binding. "a" (Expr/lVar "b")))
             (roundtrip-query-tail {"unnest" {"a" {"xt:lvar" "b"}}})))

    (t/is (thrown-with-msg? IllegalArgumentException #"Unnest should be an object with only a single binding"
                            (roundtrip-query-tail {"unnest" {"a" {"xt:lvar" "b"} "c" {"xt:lvar" "d"}}}))
          "should fail with >1 binding"))

  (t/testing "with"
    (t/is (= (Query/withCols [(Binding. "a" (Expr/lVar "a"))
                              (Binding. "b" (Expr/lVar "b"))])
             (roundtrip-query-tail {"with" ["a" "b"]})))

    (t/is (= (Query/withCols [(Binding. "a" (Expr/lVar "b"))
                              (Binding. "c" (Expr/lVar "d"))])
             (roundtrip-query-tail {"with" [{"a" {"xt:lvar" "b"} "c" {"xt:lvar" "d"}}]})))

    (t/is (thrown-with-msg? IllegalArgumentException #"With should be a list of bindings"
                            (roundtrip-query-tail {"with" "a"}))
          "should fail when not a list"))

  (t/testing "without"
    (t/is (= (Query/without ["a" "b"])
             (roundtrip-query-tail {"without" ["a" "b"]})))

    (t/is (thrown-with-msg? IllegalArgumentException #"Without should be a list of strings"
                            (roundtrip-query-tail {"without" "a"}))
          "should fail when not a list"))

  (t/testing "aggregate"
    (t/is (= (Query/aggregate [(Binding. "bar" (Expr/lVar "bar"))
                               (Binding. "baz" (Expr/call "sum" [(Expr/val 1)]))])
             (roundtrip-query-tail {"aggregate" ["bar" {"baz" {"xt:call" {"f" "sum"
                                                                          "args" [1]}}}]})))

    (t/is (thrown-with-msg? IllegalArgumentException #"Illegal argument: ':xtdb/malformed-spec'"
                            (roundtrip-query-tail {"aggregate" "a"}))
          "should fail when not a list")))

(defn roundtrip-unify [v]
  (.readValue XtdbMapper/QUERY_MAPPER (json/write-value-as-string v jackson/json-ld-mapper) Query$Unify))


(deftest deserialize-unify-test
  (let [parsed-q (-> (Query/from "docs")
                     (.binding [(Binding. "xt/id" (Expr/lVar "xt/id"))]))]
    (t/is (= (Query/unify [parsed-q])
             (roundtrip-unify {"unify" [{"from" "docs"
                                         "bind" ["xt/id"]}]})))

    (t/is (= (Query/unify [parsed-q
                           (Query/where [(Expr/call ">=" [(Expr/val 1) (Expr/val 2)])])
                           (Query/unnestVar (Binding. "a" (Expr/lVar "b")))
                           (Query/with [(Binding. "a" (Expr/lVar "a"))
                                        (Binding. "b" (Expr/lVar "b"))])
                           (-> (Query/join parsed-q [(Binding. "id" (Expr/lVar "id"))])
                               (.binding ^List (list (Binding. "id" (Expr/lVar "id")))))
                           (-> (Query/leftJoin parsed-q [(Binding. "id" (Expr/lVar "id"))])
                               (.binding ^List (list (Binding. "id" (Expr/lVar "id")))))
                           (Query/relation (Expr/param "bar") ^List (list (Binding. "foo" (Expr/lVar "foo"))))])
             (roundtrip-unify {"unify" [{"from" "docs"
                                         "bind" ["xt/id"]}
                                        {"where" [{"xt:call" {"f" ">="
                                                              "args" [1 2]}}]}
                                        {"unnest" {"a" {"xt:lvar" "b"}}}
                                        {"with" ["a" "b"]}
                                        {"join" {"from" "docs"
                                                 "bind" ["xt/id"]}
                                         "args" ["id"]
                                         "bind" ["id"]}
                                        {"left_join" {"from" "docs"
                                                      "bind" ["xt/id"]}
                                         "args" ["id"]
                                         "bind" ["id"]}
                                        {"rel" {"xt:param" "bar"}
                                         "bind" ["foo"]}]}))))

  (t/is (thrown-with-msg? IllegalArgumentException #"Illegal argument: ':xtql/malformed-unify"
                          (roundtrip-unify {"unify" "foo"}))
        "unify value not an array"))

(defn roundtrip-query-map [v]
  (.readValue XtdbMapper/QUERY_MAPPER (json/write-value-as-string v jackson/json-ld-mapper) QueryRequest))

(deftest deserialize-query-map-test
  (let [tx-key (TransactionKey. 1 #time/instant "2023-12-06T09:31:27.570827956Z")]
    (t/is (= (QueryRequest. (-> (Query/from "docs")
                                (.binding [(Binding. "xt/id" (Expr/lVar "xt/id"))]))
                            (QueryOpts.

                             {"id" :foo}
                             (Basis. tx-key Instant/EPOCH)
                             tx-key
                             #time/duration "PT3H"
                             #time/zone "America/Los_Angeles"
                             true
                             "clojure"))
             (roundtrip-query-map {"query" {"from" "docs"
                                            "bind" ["xt/id"]}
                                   "query_opts" {
                                                 "args" {"id" :foo}
                                                 "basis" {"at_tx" {"tx_id" 1
                                                                   "system_time" #time/instant "2023-12-06T09:31:27.570827956Z"}
                                                          "current_time" Instant/EPOCH}
                                                 "after_tx" {"tx_id" 1
                                                             "system_time" #time/instant "2023-12-06T09:31:27.570827956Z"}
                                                 "tx_timeout" #time/duration "PT3H"
                                                 "default_tz" #time/zone "America/Los_Angeles"
                                                 "explain" true
                                                 "key_fn" "clojure"}}))))

  (t/is (thrown-with-msg? IllegalArgumentException #"Illegal argument: ':xtql/missing-query"
                          (roundtrip-query-map {"explain" true}))
        "query map without query"))
