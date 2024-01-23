(ns xtdb.json-serde-test
  (:require [clojure.test :as t :refer [deftest]]
            [xtdb.api :as xt]
            [xtdb.error :as err]
            xtdb.serde
            [xtdb.tx-ops :as tx-ops])
  (:import (java.time Instant)
           (java.util List)
           (xtdb JsonSerde)
           (xtdb.api TransactionKey)
           (xtdb.api.query Basis Binding Expr Expr$Bool Expr$Call Expr$Null Expr$SetExpr Exprs
                           Queries Query QueryOptions QueryRequest Query$SqlQuery TemporalFilter TemporalFilter$AllTime TemporalFilters
                           XtqlQuery$Aggregate XtqlQuery$Call XtqlQuery$From XtqlQuery$Join XtqlQuery$LeftJoin XtqlQuery$OrderBy XtqlQuery$OrderDirection XtqlQuery$OrderNulls XtqlQuery$ParamRelation XtqlQuery$Pipeline XtqlQuery$QueryTail XtqlQuery$Return XtqlQuery$Unify XtqlQuery$UnnestVar XtqlQuery$Where XtqlQuery$With XtqlQuery$WithCols XtqlQuery$Without)
           (xtdb.api.tx TxOp TxOptions TxRequest)))

(defn- encode [v]
  (JsonSerde/encode v))

(defn- roundtrip-json-ld [v]
  (-> v JsonSerde/encode JsonSerde/decode))

(deftest test-json-ld-roundtripping
  (let [v {"keyword" :foo/bar
           "set-key" #{:foo :baz}
           "instant" #time/instant "2023-12-06T09:31:27.570827956Z"
           "date" #time/date "2020-01-01"
           "date-time" #time/date-time "2020-01-01T12:34:56.789"
           "zoned-date-time" #time/zoned-date-time "2020-01-01T12:34:56.789Z"
           "time-zone" #time/zone "America/Los_Angeles"
           "duration" #time/duration "PT3H1M35.23S"
           "period" #time/period "P18Y"}]
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
  (-> v
      (JsonSerde/encode TxOp)
      (JsonSerde/decode TxOp)))

(defn roundtrip-edn-tx-op [v]
  (-> v
      tx-ops/parse-tx-op
      (JsonSerde/encode TxOp)
      (JsonSerde/decode TxOp)
      tx-ops/unparse-tx-op))

(defn decode-tx-op [^String s]
  (JsonSerde/decode s TxOp))

(deftest deserialize-tx-op-test
  (t/testing "put-docs"
    (let [v #xt.tx/put-docs {:table-name :docs,
                             :docs [{"foo" :bar, "xt/id" "my-id"}],
                             :valid-from nil,
                             :valid-to nil}]

      (t/is (= v (roundtrip-tx-op v))))

    (let [v #xt.tx/put-docs {:table-name :docs,
                             :docs [{"foo" :bar, "xt/id" "my-id"}],
                             :valid-from #time/instant "2020-01-01T00:00:00Z",
                             :valid-to #time/instant "2021-01-01T00:00:00Z"}]

      (t/is (= v (roundtrip-tx-op v))))

    (t/is (thrown-with-msg? xtdb.IllegalArgumentException #"Error decoding JSON!"
                            (-> {"into" "docs"
                                 "putDocs" "blob"
                                 "validFrom" #inst "2020"
                                 "validTo" #inst "2021"}
                                encode
                                decode-tx-op))))

  (t/testing "delete-docs"
    (let [v #xt.tx/delete-docs {:table-name :docs,
                                :doc-ids ["my-id"],
                                :valid-from nil,
                                :valid-to nil}]
      (t/is (= v (roundtrip-tx-op v))))

    (let [v #xt.tx/delete-docs {:table-name :docs,
                                :doc-ids [:keyword-id],
                                :valid-from nil,
                                :valid-to nil}]

      (t/is (= v (roundtrip-tx-op v))))

    (let [v #xt.tx/delete-docs {:table-name :docs,
                                :doc-ids ["my-id"],
                                :valid-from #time/instant "2020-01-01T00:00:00Z",
                                :valid-to #time/instant "2021-01-01T00:00:00Z"}]
      (t/is (= v (roundtrip-tx-op v))))


    (t/is (thrown-with-msg? xtdb.IllegalArgumentException #"Error decoding JSON!"
                            (-> {"deleteDocs" ["my-id"]
                                 "from" "docs"
                                 "valid_from" ["not-a-date"]}
                                encode
                                decode-tx-op))))
  (t/testing "erase"
    (let [v #xt.tx/erase-docs {:table-name :docs,
                               :doc-ids ["my-id"]}]
      (t/is (= v (roundtrip-tx-op v))))

    (let [v #xt.tx/erase-docs {:table-name :docs,
                               :doc-ids [:keyword-id]}]
      (t/is (= v (roundtrip-tx-op v))))

    (t/is (thrown-with-msg? xtdb.IllegalArgumentException #"Error decoding JSON!"
                            (-> {"eraseDocs" ["my-id"]
                                 "frmo" "docs"}
                                encode
                                decode-tx-op))
          "unknown key"))

  (t/testing "call"
    (let [v #xt.tx/call {:fn-id :my-fn
                         :args ["args"]}]
      (t/is (= v (roundtrip-tx-op v))))

    (let [v #xt.tx/call {:fn-id "my-fn"
                         :args ["args"]}]
      (t/is (= v (roundtrip-tx-op v))))

    (t/is (thrown-with-msg? xtdb.IllegalArgumentException #"Error decoding JSON!"
                            (-> {"call" "my-fn"
                                 "args" {"not" "a-list"}}
                                encode
                                decode-tx-op))))

  (t/testing "sql"
    (let [v #xt.tx/sql {:sql "INSERT INTO docs (xt$id, foo) VALUES (1, \"bar\")"}]
      (t/is (= v (roundtrip-tx-op v))))

    (let [v #xt.tx/sql {:sql "INSERT INTO docs (xt$id, foo) VALUES (?, ?)", :arg-rows [[1 "bar"] [2 "toto"]]}]
      (t/is (= v (roundtrip-tx-op v))))

    (t/is (thrown-with-msg? xtdb.IllegalArgumentException #"Error decoding JSON!"
                            (-> {"sql" "INSERT INTO docs (xt$id, foo) VALUES (?, ?)"
                                 "arg_rows" [1 "bar"]}
                                encode
                                decode-tx-op))))

  ;; TODO keyword args handling when
  (t/testing "xtdml"
    (let [v [:insert-into :foo '(from :bar [xt/id])]]
      (t/is (= v (roundtrip-edn-tx-op v))))

    (let [v [:insert-into :foo '(from :bar [{:xt/id $id}])
             {"id" 1}]]
      (t/is (= v (roundtrip-edn-tx-op v))))

    (let [v [:update '{:table :users, :bind [{:xt/id $uid} version], :set {:version (inc version)}}
             {"uid" "james"}
             {"uid" "dave"}]]
      (t/is (= v (roundtrip-edn-tx-op v))))

    (let [v [:delete '{:from :users
                       :bind [{:xt/id $uid} version]
                       :unify [(from :users [version])]}]]
      (t/is (= v (roundtrip-edn-tx-op v))))

    (let [v [:delete '{:from :users
                       :bind [{:promotion-type "christmas"}]
                       :for-valid-time (in #time/instant "2023-12-26T00:00:00Z" nil)}]]
      (t/is (= v (roundtrip-edn-tx-op v))))

    (let [v [:erase '{:from :users, :bind [{:xt/id $uid} version], :unify [(from :users [version])]}]]
      (t/is (= v (roundtrip-edn-tx-op v))))

    (let [v [:assert-exists '(from :users [xt/id])]]
      (t/is (= v (roundtrip-edn-tx-op v))))

    (let [v [:assert-not-exists '(from :users [xt/id])]]
      (t/is (= v (roundtrip-edn-tx-op v))))))

(defn- roundtrip-tx [v]
  (-> v (JsonSerde/encode TxRequest) (JsonSerde/decode TxRequest)))

(defn- decode-tx [^String s]
  (JsonSerde/decode s TxRequest))

(deftest deserialize-tx-test
  (let [v (TxRequest. [#xt.tx/put-docs {:table-name :docs,
                                        :docs [{"xt/id" "my-id"}],
                                        :valid-from nil,
                                        :valid-to nil}],
                      (TxOptions.))]
    (t/is (= v (roundtrip-tx v))))

  (let [v (TxRequest. [#xt.tx/put-docs {:table-name :docs,
                                        :docs [{"xt/id" "my-id"}],
                                        :valid-from nil,
                                        :valid-to nil}],
                      (TxOptions. #time/instant "2020-01-01T12:34:56.789Z"
                                  #time/zone "America/Los_Angeles"
                                  false))]

    (t/is (= v (roundtrip-tx v))
          "transaction options"))

  (t/is (thrown-with-msg? xtdb.IllegalArgumentException #"Error decoding JSON!"
                          (-> {"tx_ops" {"put" "docs"
                                         "doc" {"xt/id" "my-id"}}}
                              encode
                              decode-tx))
        "put not wrapped throws"))

(defn- roundtrip-expr [v]
  (-> v (JsonSerde/encode Expr) (JsonSerde/decode Expr)))

(deftest deserialize-expr-test
  (t/is (= Expr$Null/INSTANCE (roundtrip-expr Expr$Null/INSTANCE))
        "null")
  (t/is (= (Exprs/val "foo") (roundtrip-expr (Exprs/val "foo")))
        "string")
  (t/is (= (Exprs/lVar "foo") (roundtrip-expr (Exprs/lVar "foo") ))
        "logic-var")
  (t/is (= (Exprs/param "foo") (roundtrip-expr (Exprs/param "foo")))
        "param")
  (t/is (= Expr$Bool/FALSE (roundtrip-expr Expr$Bool/FALSE)))
  (t/is (= (Exprs/val (long 1)) (roundtrip-expr (Exprs/val (long 1)))))
  (t/is (= (Exprs/val (double 1.2)) (roundtrip-expr (Exprs/val (double 1.2)))))

  (let [v (Exprs/exists (XtqlQuery$From. "docs" [(Binding. "xt/id" (Exprs/lVar "xt/id"))])
                       [])]
    (t/is (= v (roundtrip-expr v))
          "exists"))

  (let [v (Exprs/q (XtqlQuery$From. "docs" [(Binding. "xt/id" (Exprs/lVar "xt/id"))])
                  [])]

    (t/is (= v (roundtrip-expr v))
          "subquery"))

  (let [v (Exprs/pull (XtqlQuery$From. "docs" [(Binding. "xt/id" (Exprs/lVar "xt/id"))])
                     [])]

    (t/is (= v (roundtrip-expr v))
          "pull"))

  (let [v (Exprs/pullMany (XtqlQuery$From. "docs" [(Binding. "xt/id" (Exprs/lVar "xt/id"))])
                         [])]
    (t/is (= v (roundtrip-expr v))
          "pull-many"))

  (let [v (Expr$Call. "+" [(Exprs/val "foo") (Exprs/val "bar")])]
    (t/is (= v (roundtrip-expr v))
          "call"))

  (let [v (Exprs/list ^List (list (Exprs/val 1) (Exprs/val 2)))]
    (t/is (= v (roundtrip-expr v))
          "list"))

  (let [v (Exprs/map {"foo" (Exprs/val 1)})]
    (t/is (= v (roundtrip-expr v))
          "maps"))

  (let [v (Expr$SetExpr. [(Exprs/val 1) (Exprs/val :foo)])]
    (t/is (= v (roundtrip-expr v))
          "sets")))

(defn- roundtrip-temporal-filter [v]
  (-> v (JsonSerde/encode TemporalFilter) (JsonSerde/decode TemporalFilter)))

(defn- decode-temporal-filter [^String s]
  (JsonSerde/decode s TemporalFilter))

(deftest deserialize-temporal-filter-test
  (t/is (= TemporalFilter$AllTime/INSTANCE (roundtrip-temporal-filter TemporalFilter$AllTime/INSTANCE)) "all-time")

  (t/is (thrown-with-msg? IllegalArgumentException #"Illegal argument: 'xtql/malformed-temporal-filter'"
                          (-> "all_times" encode decode-temporal-filter))
        "all-time (wrong format)")

  (let [v (TemporalFilters/at (Exprs/val #time/instant "2020-01-01T00:00:00Z"))]
    (t/is (= v (roundtrip-temporal-filter v)) "at"))

  (let [v (TemporalFilters/from (Exprs/val #time/instant "2020-01-01T00:00:00Z"))]
    (t/is (= v (roundtrip-temporal-filter v)) "from"))

  (let [v (TemporalFilters/to (Exprs/val #time/instant "2020-01-01T00:00:00Z"))]
    (t/is (= v (roundtrip-temporal-filter v)) "to"))

  (let [v (TemporalFilters/in (Exprs/val #time/instant "2020-01-01T00:00:00Z") (Exprs/val #time/instant "2021-01-01T00:00:00Z"))]
    (t/is (= v (roundtrip-temporal-filter v)) "in"))

  (t/is (thrown-with-msg? IllegalArgumentException #"Illegal argument: 'xtql/malformed-temporal-filter'"
                          (-> {"in" [#inst "2020"]} encode decode-temporal-filter))
        "in with wrong arguments"))

(defn- roundtrip-query [v]
  (-> v (JsonSerde/encode Query) (JsonSerde/decode Query)))

(defn- decode-query [^String v]
  (JsonSerde/decode v Query))

(deftest deserialize-query-test
  (let [query (-> (Queries/from "docs")
                  (.bind (Binding. "xt/id" (Exprs/lVar "xt/id")))
                  (.bind (Binding. "a" (Exprs/lVar "b")))
                  (.build))]
    (t/is (= query (roundtrip-query query)))

    (let [v (-> (doto (Queries/from "docs")
                  (.setBindings [(Binding. "xt/id" (Exprs/lVar "xt/id"))])
                  (.forValidTime (TemporalFilters/at (Exprs/val #time/instant "2020-01-01T00:00:00Z")))
                  (.forSystemTime TemporalFilter$AllTime/INSTANCE))
                (.build))]
      (t/is (= v (roundtrip-query v)) "from with temporal bounds"))

    (t/is (thrown-with-msg? xtdb.IllegalArgumentException #"Error decoding JSON!"
                            (-> {"from" "docs" "bind" "xt/id"} encode decode-query))
          "bind not an array")

    (let [v (XtqlQuery$Pipeline. (XtqlQuery$From. "docs" [(Binding. "xt/id" (Exprs/lVar "xt/id"))])
                                 [(Queries/limit 10)])]
      (t/is (= v (roundtrip-query v)) "pipeline"))

    (let [v (XtqlQuery$Unify. [(XtqlQuery$From. "docs" [(Binding. "xt/id" (Exprs/lVar "xt/id"))])
                               (XtqlQuery$From. "docs" [(Binding. "xt/id" (Exprs/lVar "xt/id"))])])]
      (t/is (= v (roundtrip-query v)) "unify"))

    (t/testing "rel"
      (let [v (Queries/relation ^List (list {"foo" (Exprs/val :bar)}) ^List (list (Binding. "foo" (Exprs/lVar "foo"))))]
        (t/is (= v (roundtrip-query v))))

      (let [v (Queries/relation (Exprs/param "bar") ^List (list (Binding. "foo" (Exprs/lVar "foo"))))]
        (t/is (= v (roundtrip-query v)))))

    (t/testing "union-all"
      (let [v (Queries/unionAll ^List (list query query))]
        (t/is (= v (roundtrip-query v)))))

    (t/testing "sql-query"
      (let [v (Query$SqlQuery. "SELECT * FROM docs")]
        (t/is (= v (roundtrip-query v)))))))


(defn- roundtrip-query-tail [v]
  (-> v (JsonSerde/encode XtqlQuery$QueryTail) (JsonSerde/decode XtqlQuery$QueryTail)))

(defn- decode-query-tail [^String v]
  (JsonSerde/decode v XtqlQuery$QueryTail))

(deftest deserialize-query-tail-test
  (t/testing "where"
    (let [v (XtqlQuery$Where. [(Expr$Call. ">=" [(Exprs/val 1) (Exprs/val 2)])
                           (Expr$Call. "<" [(Exprs/val 1) (Exprs/val 2)])])]
      (t/is (= v (roundtrip-query-tail v))))

    (t/is (thrown-with-msg? xtdb.IllegalArgumentException #"Error decoding JSON!"
                            (-> {"where" "not-a-list"} encode decode-query-tail))
          "should fail when not a list"))

  (t/testing "limit"
    (t/is (= (Queries/limit 100) (roundtrip-query-tail (Queries/limit 100))))

    (t/is (thrown-with-msg? xtdb.IllegalArgumentException #"Error decoding JSON!"
                            (-> {"limit" "not-a-limit"} encode decode-query-tail))))
  (t/testing "offset"
    (t/is (= (Queries/offset 100)
             (roundtrip-query-tail (Queries/offset 100))))

    (t/is (thrown-with-msg? xtdb.IllegalArgumentException #"Error decoding JSON!"
                            (-> {"offset" "not-an-offset"} encode decode-query-tail))))

  (t/testing "orderBy"
    (t/is (= (XtqlQuery$OrderBy. [(Queries/orderSpec (Exprs/lVar "someField") nil nil)])
             (roundtrip-query-tail (XtqlQuery$OrderBy. [(Queries/orderSpec (Exprs/lVar "someField") nil nil)]))))

    (let [v (XtqlQuery$OrderBy. [(Queries/orderSpec (Exprs/lVar "someField") XtqlQuery$OrderDirection/ASC XtqlQuery$OrderNulls/FIRST)])]
      (t/is (= v (roundtrip-query-tail v))))

    (t/is (thrown-with-msg? xtdb.IllegalArgumentException #"Error decoding JSON!"
                            (-> {"orderBy" [{"val" {"lvar" "someField"}, "dir" "invalid-direction"}]}
                                encode
                                decode-query-tail) ))

    (t/is (thrown-with-msg? xtdb.IllegalArgumentException #"Error decoding JSON!"
                            (-> {"orderBy" [{"val" {"lvar" "someField"}, "nulls" "invalid-nulls"}]}
                                encode
                                decode-query-tail))))

  (t/testing "return"
    (let [v (XtqlQuery$Return. [(Binding. "a" (Exprs/lVar "a"))
                            (Binding. "b" (Exprs/lVar "b"))])]
      (t/is (= v (roundtrip-query-tail v))))

    (let [v (XtqlQuery$Return. [(Binding. "a" (Exprs/lVar "a"))
                            (Binding. "b" (Exprs/lVar "c"))])]
      (t/is (= v (roundtrip-query-tail v))))

    (t/is (thrown-with-msg? xtdb.IllegalArgumentException #"Error decoding JSON!"
                            (-> {"return" "a"} encode decode-query-tail))))

  (t/testing "unnest"
    (t/is (= (Queries/unnestCol (Binding. "a" (Exprs/lVar "b")))
             (roundtrip-query-tail (Queries/unnestCol (Binding. "a" (Exprs/lVar "b"))))))

    (t/is (thrown-with-msg? xtdb.IllegalArgumentException #"Illegal argument: 'xtql/malformed-binding'"
                            (-> {"unnest" {"a" {"xt:lvar" "b"} "c" {"xt:lvar" "d"}}}
                                encode
                                decode-query-tail))
          "should fail with >1 binding"))

  (t/testing "with"
    (let [v (XtqlQuery$WithCols. [(Binding. "a" (Exprs/lVar "a")) (Binding. "b" (Exprs/lVar "b"))])]
      (t/is (= v (roundtrip-query-tail v))))

    (let [v (XtqlQuery$WithCols. [(Binding. "a" (Exprs/lVar "b")) (Binding. "c" (Exprs/lVar "d"))])]
      (t/is (= v (roundtrip-query-tail v))))

    (t/is (thrown-with-msg? xtdb.IllegalArgumentException #"Error decoding JSON!"
                            (-> {"with" "a"} encode decode-query-tail))
          "should fail when not a list"))


  (t/testing "without"
    (t/is (= (XtqlQuery$Without. ["a" "b"]) (roundtrip-query-tail (XtqlQuery$Without. ["a" "b"]))))

    (t/is (thrown-with-msg? xtdb.IllegalArgumentException #"Error decoding JSON!"
                            (-> {"without" "a"} encode decode-query-tail))
          "should fail when not a list"))


  (t/testing "aggregate"
    (let [v (XtqlQuery$Aggregate. [(Binding. "bar" (Exprs/lVar "bar"))
                               (Binding. "baz" (Expr$Call. "sum" [(Exprs/val 1)]))])]
      (t/is (= v (roundtrip-query-tail v))))

    (t/is (thrown-with-msg? xtdb.IllegalArgumentException #"Error decoding JSON!"
                            (-> {"aggregate" "a"} encode decode-query-tail))
          "should fail when not a list")))

(defn- roundtrip-unify [v]
  (-> v (JsonSerde/encode XtqlQuery$Unify) (JsonSerde/decode XtqlQuery$Unify)))

(defn- decode-unify [^String v]
  (JsonSerde/decode v XtqlQuery$Unify))

(deftest deserialize-unify-test
  (let [parsed-q (XtqlQuery$From. "docs" [(Binding. "xt/id" (Exprs/lVar "xt/id"))])
        simply-unify (XtqlQuery$Unify. [parsed-q])
        complex-unify (XtqlQuery$Unify. [parsed-q
                                         (XtqlQuery$Where. [(Expr$Call. ">=" [(Exprs/val 1) (Exprs/val 2)])])
                                         (XtqlQuery$UnnestVar. (Binding. "a" (Exprs/lVar "b")))
                                         (XtqlQuery$With. [(Binding. "a" (Exprs/lVar "a"))
                                                           (Binding. "b" (Exprs/lVar "b"))])
                                         (XtqlQuery$Join. parsed-q
                                                          [(Binding. "id" (Exprs/lVar "id"))]
                                                          [(Binding. "id" (Exprs/lVar "id"))])
                                         (XtqlQuery$LeftJoin. parsed-q
                                                              [(Binding. "id" (Exprs/lVar "id"))]
                                                              [(Binding. "id" (Exprs/lVar "id"))])
                                         (XtqlQuery$ParamRelation. (Exprs/param "bar") ^List (list (Binding. "foo" (Exprs/lVar "foo"))))
                                         (XtqlQuery$Call. "my-fn" [(Exprs/val 1)] nil)])]

    (t/is (= simply-unify (roundtrip-unify simply-unify)))

    (t/is (= complex-unify (roundtrip-unify complex-unify))))

  (t/is (thrown-with-msg? xtdb.IllegalArgumentException #"Error decoding JSON!"
                          (-> {"unify" "foo"} encode decode-unify))
        "unify value not an array"))

(defn- roundtrip-query-request [v]
  (-> v (JsonSerde/encode QueryRequest) (JsonSerde/decode QueryRequest)))

(defn- decode-query-request [^String v]
  (JsonSerde/decode v QueryRequest))

(deftest deserialize-query-map-test
  (let [tx-key (TransactionKey. 1 #time/instant "2023-12-06T09:31:27.570827956Z")
        v (QueryRequest. (XtqlQuery$From. "docs" [(Binding. "xt/id" (Exprs/lVar "xt/id"))])
                         (-> (QueryOptions/queryOpts)
                             (.args {"id" :foo})
                             (.basis (Basis. tx-key Instant/EPOCH))
                             (.afterTx tx-key)
                             (.txTimeout #time/duration "PT3H")
                             (.defaultTz #time/zone "America/Los_Angeles")
                             (.defaultAllValidTime true)
                             (.explain true)
                             (.keyFn #xt/key-fn :kebab-case-keyword)
                             (.build)))]
    (t/is (= v (roundtrip-query-request v))))

  (t/is (thrown-with-msg? xtdb.IllegalArgumentException #"Error decoding JSON!"
                          (-> {"explain" true} encode decode-query-request))
        "query map without query"))
