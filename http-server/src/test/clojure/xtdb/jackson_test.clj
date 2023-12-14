(ns xtdb.jackson-test
  (:require [clojure.test :as t :refer [deftest]]
            [jsonista.core :as json]
            [xtdb.error :as err]
            [xtdb.jackson :as jackson])
  (:import (java.util List)
           (java.time Instant)
           (xtdb.tx Ops Tx)
           (xtdb.query Query Query$OrderDirection Query$OrderNulls Query$QueryTail
                       ColSpec OutSpec VarSpec Expr Query$Unify QueryMap Basis TransactionKey Expr
                       ArgSpec)))

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
  (.readValue jackson/tx-op-mapper (json/write-value-as-string v jackson/json-ld-mapper) Ops))

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
                              "args" {"not" "a-list"}})))))

(defn roundtrip-tx [v]
  (.readValue jackson/tx-op-mapper (json/write-value-as-string v jackson/json-ld-mapper) Tx))

(deftest deserialize-tx-test
  (t/is (= (Tx. [#xt.tx/put {:table-name :docs,
                             :doc {"xt/id" "my-id"},
                             :valid-from nil,
                             :valid-to nil}], nil, nil)
           (roundtrip-tx {"tx_ops" [{"put" "docs"
                                     "doc" {"xt/id" "my-id"}}]})))

  (t/is (= (Tx. [#xt.tx/put {:table-name :docs,
                             :doc {"xt/id" "my-id"},
                             :valid-from nil,
                             :valid-to nil}],
                #time/date-time "2020-01-01T12:34:56.789"
                #time/zone "America/Los_Angeles")
           (roundtrip-tx {"tx_ops" [{"put" "docs"
                                     "doc" {"xt/id" "my-id"}}]
                          "system_time" #time/date-time "2020-01-01T12:34:56.789"
                          "default_tz" #time/zone "America/Los_Angeles"}))
        "transaction options")

  (t/is (thrown-with-msg? IllegalArgumentException #"Illegal argument: ':xtdb/malformed-tx'"
                          (roundtrip-tx {"tx_ops" {"put" "docs"
                                                   "doc" {"xt/id" "my-id"}}}))
        "put not wrapped throws"))

(defn- roundtrip-expr [e]
  (.readValue jackson/query-mapper (json/write-value-as-string e jackson/json-ld-mapper) Expr))

(type (.readValue jackson/json-ld-mapper (json/write-value-as-string 1 jackson/json-ld-mapper) Object))


(deftest deserialize-expr-test
  (t/is (= (Expr/lVar "foo")
           (roundtrip-expr "foo"))
        "logic-var")
  (t/is (= (Expr/param "$foo")
           (roundtrip-expr "$foo"))
        "param")
  (t/is (= Expr/FALSE
           (roundtrip-expr false)))
  (t/is (= (Expr/val (long 1))
           (roundtrip-expr (long 1))))
  (t/is (= (Expr/val (double 1.2))
           (roundtrip-expr 1.2)))

  (t/is (= (Expr/exists (-> (Query/from "docs")
                            (.binding [(OutSpec/of "xt/id" (Expr/lVar "xt/id"))]))
                        [])
           (roundtrip-expr {"exists" {"from" "docs"
                                      "bind" ["xt/id"]}
                            "bind" []}))
        "exists")

  (t/is (= (Expr/q (-> (Query/from "docs")
                       (.binding [(OutSpec/of "xt/id" (Expr/lVar "xt/id"))]))
                   [])
           (roundtrip-expr {"q" {"from" "docs"
                                 "bind" ["xt/id"]}
                            "bind" []}))
        "subquery")

  (t/is (= (Expr/pull (-> (Query/from "docs")
                          (.binding [(OutSpec/of "xt/id" (Expr/lVar "xt/id"))]))
                      [])
           (roundtrip-expr {"pull" {"from" "docs"
                                    "bind" ["xt/id"]}
                            "bind" []}))
        "pull")

  (t/is (= (Expr/pullMany (-> (Query/from "docs")
                              (.binding [(OutSpec/of "xt/id" (Expr/lVar "xt/id"))]))
                          [])
           (roundtrip-expr {"pull_many" {"from" "docs"
                                         "bind" ["xt/id"]}
                            "bind" []}))
        "pull")

  (t/is (= (Expr/call "+" [(Expr/lVar "foo") (Expr/lVar "bar")])
           (roundtrip-expr {"call" "+"
                            "args" ["foo" "bar"]}))
        "call"))

(defn roundtrip-query [v]
  (.readValue jackson/query-mapper (json/write-value-as-string v jackson/json-ld-mapper) Query))

(deftest deserialize-query-test
  (t/is (= (-> (Query/from "docs")
               (.binding [(OutSpec/of "xt/id" (Expr/lVar "xt/id"))
                          (OutSpec/of "a" (Expr/lVar "b"))]))
           (roundtrip-query {"from" "docs"
                             "bind" ["xt/id" {"a" "b"}]})))

  (t/is (thrown-with-msg? IllegalArgumentException #"Illegal argument: ':xtdb/malformed-spec'"
                          (roundtrip-query {"from" "docs"
                                            "bind" "xt/id"}))
        "bind not an array")

  (t/is (= (Query/pipeline (-> (Query/from "docs")
                               (.binding [(OutSpec/of "xt/id" (Expr/lVar "xt/id"))]))
                           [(Query/limit 10)])
           (roundtrip-query [{"from" "docs"
                              "bind" ["xt/id"]}
                             {"limit" 10}]))
        "pipeline")

  (t/is (= (Query/unify [(-> (Query/from "docs") (.binding [(OutSpec/of "xt/id" (Expr/lVar "xt/id"))]))
                         (-> (Query/from "docs") (.binding [(OutSpec/of "xt/id" (Expr/lVar "xt/id"))]))])
           (roundtrip-query {"unify" [{"from" "docs"
                                       "bind" ["xt/id"]}
                                      {"from" "docs"
                                       "bind" ["xt/id"]}]}))
        "unify")

  (t/testing "rel"
    (t/is (= (Query/relation ^List (list {"foo" (Expr/val :bar)}) ^List (list (OutSpec/of "foo" (Expr/lVar "foo"))))
             (roundtrip-query {"rel" [{"foo" :bar}]
                               "bind" ["foo"]})))
    (t/is (= (Query/relation (Expr/param "$bar") ^List (list (OutSpec/of "foo" (Expr/lVar "foo"))))
             (roundtrip-query {"rel" "$bar"
                               "bind" ["foo"]})))))

(defn roundtrip-query-tail [v]
  (.readValue jackson/query-mapper (json/write-value-as-string v jackson/json-ld-mapper) Query$QueryTail))

(deftest deserialize-query-tail-test
  (t/testing "where"
    (t/is (= (Query/where [(Expr/val {">=" ["foo" "bar"]})
                           (Expr/val {"<" ["bar" "baz"]})])
             (roundtrip-query-tail {"where" [{">=" ["foo" "bar"]}
                                             {"<" ["bar" "baz"]}]})))

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
             (roundtrip-query-tail {"orderBy" [{"val" "someField", "dir" "asc", "nulls" "first"}]})))

    (t/is (thrown-with-msg? IllegalArgumentException #"Illegal argument: ':xtql/malformed-order-by'"
                            (roundtrip-query-tail {"orderBy" [{"val" "someField", "dir" "invalid-direction"}]})))

    (t/is (thrown-with-msg? IllegalArgumentException #"Illegal argument: ':xtql/malformed-order-by'"
                            (roundtrip-query-tail {"orderBy" [{"val" "someField", "nulls" "invalid-nulls"}]}))))

  (t/testing "return"
    (t/is (= (Query/returning [(ColSpec/of "a" (Expr/lVar "a"))
                               (ColSpec/of "b" (Expr/lVar "b"))])
             (roundtrip-query-tail {"return" ["a" "b"]})))

    (t/is (= (Query/returning [(ColSpec/of "a" (Expr/lVar "a"))
                               (ColSpec/of "b" (Expr/lVar "c"))])
             (roundtrip-query-tail {"return" [{"a" "a"
                                               "b" "c"}]})))

    (t/is (thrown-with-msg? IllegalArgumentException #"Return should be a list of values"
                            (roundtrip-query-tail {"return" "a"}))))

  (t/testing "unnest"
    (t/is (= (Query/unnestCol (ColSpec/of "a" (Expr/lVar "b")))
             (roundtrip-query-tail {"unnest" {"a" "b"}})))

    (t/is (thrown-with-msg? IllegalArgumentException #"Illegal argument: ':xtql/malformed-unnest"
                            (roundtrip-query-tail {"unnest" {"a" "b"
                                                             "c" "d"}}))
          "should fail with >1 binding"))

  (t/testing "with"
    (t/is (= (Query/withCols [(ColSpec/of "a" (Expr/lVar "a"))
                              (ColSpec/of "b" (Expr/lVar "b"))])
             (roundtrip-query-tail {"with" ["a" "b"]})))

    (t/is (= (Query/withCols [(ColSpec/of "a" (Expr/lVar "b"))
                              (ColSpec/of "c" (Expr/lVar "d"))])
             (roundtrip-query-tail {"with" [{"a" "b"
                                             "c" "d"}]})))

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
    (t/is (= (Query/aggregate [(ColSpec/of "bar" (Expr/lVar "bar"))
                               (ColSpec/of "baz" (Expr/call "sum" [(Expr/lVar "foo")]))])
             (roundtrip-query-tail {"aggregate" ["bar" {"baz" {"call" "sum"
                                                               "args" ["foo"]}}]})))

    (t/is (thrown-with-msg? IllegalArgumentException #"Illegal argument: ':xtdb/malformed-spec'"
                            (roundtrip-query-tail {"aggregate" "a"}))
          "should fail when not a list")))

(defn roundtrip-unify [v]
  (.readValue jackson/query-mapper (json/write-value-as-string v jackson/json-ld-mapper) Query$Unify))


(deftest deserialize-unify-test
  (let [parsed-q (-> (Query/from "docs")
                     (.binding [(OutSpec/of "xt/id" (Expr/lVar "xt/id"))]))]
    (t/is (= (Query/unify [parsed-q])
             (roundtrip-unify {"unify" [{"from" "docs"
                                         "bind" ["xt/id"]}]})))

    (t/is (= (Query/unify [parsed-q
                           (Query/where [(Expr/val {">=" ["foo" "bar"]})])
                           (Query/unnestVar (VarSpec/of "a" (Expr/lVar "b")))
                           (Query/with [(VarSpec/of "a" (Expr/lVar "a"))
                                        (VarSpec/of "b" (Expr/lVar "b"))])
                           (-> (Query/join parsed-q [(ArgSpec/of "id" (Expr/lVar "id"))])
                               (.binding ^List (list (OutSpec/of "id" (Expr/lVar "id")))))
                           (-> (Query/leftJoin parsed-q [(ArgSpec/of "id" (Expr/lVar "id"))])
                               (.binding ^List (list (OutSpec/of "id" (Expr/lVar "id")))))
                           (Query/relation (Expr/param "$bar") ^List (list (OutSpec/of "foo" (Expr/lVar "foo"))))])
             (roundtrip-unify {"unify" [{"from" "docs"
                                         "bind" ["xt/id"]}
                                        {"where" [{">=" ["foo" "bar"]}]}
                                        {"unnest" {"a" "b"}}
                                        {"with" ["a" "b"]}
                                        {"join" {"from" "docs"
                                                 "bind" ["xt/id"]}
                                         "args" ["id"]
                                         "bind" ["id"]}
                                        {"left_join" {"from" "docs"
                                                      "bind" ["xt/id"]}
                                         "args" ["id"]
                                         "bind" ["id"]}
                                        {"rel" "$bar"
                                         "bind" ["foo"]}]}))))

  (t/is (thrown-with-msg? IllegalArgumentException #"Illegal argument: ':xtql/malformed-unify"
                          (roundtrip-unify {"unify" "foo"}))
        "unify value not an array"))

(defn roundtrip-query-map [v]
  (.readValue jackson/query-mapper (json/write-value-as-string v jackson/json-ld-mapper) QueryMap))

(deftest deserialize-query-map-test
  (let [tx-key (TransactionKey. 1 #time/instant "2023-12-06T09:31:27.570827956Z")]
    (t/is (= (QueryMap. (-> (Query/from "docs")
                            (.binding [(OutSpec/of "xt/id" (Expr/lVar "xt/id"))]))
                        {"id" :foo}
                        (Basis. tx-key Instant/EPOCH)
                        tx-key
                        #time/duration "PT3H"
                        #time/zone "America/Los_Angeles"
                        true
                        :clojure)
             (roundtrip-query-map {"query" {"from" "docs"
                                            "bind" ["xt/id"]}
                                   "args" {"id" :foo}
                                   "basis" {"at_tx" {"tx_id" 1
                                                     "system_time" #time/instant "2023-12-06T09:31:27.570827956Z"}
                                            "current_time" Instant/EPOCH}
                                   "after_tx" {"tx_id" 1
                                               "system_time" #time/instant "2023-12-06T09:31:27.570827956Z"}
                                   "tx_timeout" #time/duration "PT3H"
                                   "default_tz" #time/zone "America/Los_Angeles"
                                   "explain" true
                                   "key_fn" :clojure}))))

  (t/is (thrown-with-msg? IllegalArgumentException #"Illegal argument: ':xtql/missing-query"
                          (roundtrip-query-map {"explain" true}))
        "query map without query"))
