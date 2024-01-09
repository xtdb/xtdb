(ns xtdb.java-test
  (:require [clojure.test :as t :refer [deftest]]
            [xtdb.api :as xt]
            [xtdb.protocols :as xtp]
            [xtdb.test-util :as tu]
            [xtdb.xtql.edn :as xtql.edn])
  (:import [java.util.stream Stream]
           (xtdb.api TxOptions)
           (xtdb.query Basis Binding Expr Query QueryOpts)
           (xtdb.tx TxOp)))

(t/use-fixtures :each tu/with-mock-clock tu/with-node)

(defn- stream->vec [res]
  (with-open [^Stream res res]
    (vec (.toList res))))

(deftest java-api-test
  (t/testing "transactions"
    (let [tx (.submitTx tu/*node*
                        (TxOptions. #time/instant "2020-01-01T12:34:56.000Z" nil false)
                        (into-array TxOp [(xt/put :docs {"xt/id" 1 "foo" "bar"})]))]
      (t/is (= [{:xt/id 1 :xt/system-from #time/zoned-date-time "2020-01-01T12:34:56Z[UTC]"}]
               (xt/q tu/*node*
                     '(from :docs [xt/id xt/system-from])
                     {:basis {:at-tx tx}}))))

    (let [sql-op "INSERT INTO docs (xt$id, tstz) VALUES (2, CAST(DATE '2020-08-01' AS TIMESTAMP WITH TIME ZONE))"
          tx (.submitTx tu/*node*
                        (TxOptions. nil #time/zone "America/Los_Angeles" false)
                        (into-array TxOp [(xt/sql-op sql-op)]))]
      (t/is (= [{:tstz #time/zoned-date-time "2020-08-01T00:00-07:00[America/Los_Angeles]"}]
               (xt/q tu/*node*
                     '(from :docs [{:xt/id 2} tstz])
                     {:basis {:at-tx tx}}))
            "default-tz")))

  (t/testing "queries"
    (let [tx (xt/submit-tx tu/*node* [(xt/put :docs2 {:xt/id 1 :foo "bar"})])]

      (t/is (= [{:my-foo "bar"}]
               (-> (.openQuery tu/*node* (-> (Query/from "docs2")
                                             (.binding [(Binding/bindVar "foo" "my-foo")]))
                               (-> (QueryOpts/queryOpts)
                                   (.basis (Basis. tx nil))
                                   (.build)))
                   stream->vec))
            "java ast queries")

      (t/is (= [{:my-foo "bar"}]
               (-> (.openQuery tu/*node* (-> (Query/from "docs2")
                                             (.binding [(Binding/bindVar "foo" "my-foo")]))
                               (-> (QueryOpts/queryOpts)
                                   (.basis (Basis. tx nil))
                                   (.build)))
                   stream->vec))
            "key-fn")

      (t/is (= [{:foo "bar"}]
               (-> (.openQuery tu/*node* (-> (Query/from "docs")
                                             (.binding [(Binding/bindParam "xt/id" "$id")
                                                        (Binding/bindVar "foo")]))
                               (-> (QueryOpts/queryOpts)
                                   (.args {"id" 1})
                                   (.basis (Basis. tx nil))
                                   (.build)))
                   stream->vec))
            "params")

      (t/is (= [{:current-time #time/date "2020-01-01"}]
               (-> (.openQuery tu/*node* (xtql.edn/parse-query '(-> (rel [{}] [])
                                                                    (with {:current-time (current-date)})))
                               (-> (QueryOpts/queryOpts)
                                   (.args {"id" 1})
                                   (.basis (Basis. tx #time/instant "2020-01-01T12:34:56.000Z"))
                                   (.build)))
                   stream->vec))
            "current-time")

      (t/is (= [{:timestamp #time/zoned-date-time "2020-01-01T04:34:56-08:00[America/Los_Angeles]"}]
               (-> (.openQuery tu/*node* (xtql.edn/parse-query '(-> (rel [{}] [])
                                                                    (with {:timestamp (current-timestamp 10)})))
                               (-> (QueryOpts/queryOpts)
                                   (.basis (Basis. tx #time/instant "2020-01-01T12:34:56.000Z"))
                                   (.defaultTz #time/zone "America/Los_Angeles")
                                   (.build)))
                   stream->vec))
            "default tz")

      (t/is (= '[{:plan [:scan
                         {:table docs, :for-valid-time nil, :for-system-time nil}
                         [foo]]}]
               (-> (.openQuery tu/*node* (-> (Query/from "docs")
                                             (.binding [(Binding/bindVar "foo")]))
                               (-> (QueryOpts/queryOpts)
                                   (.explain true)
                                   (.build)))
                   stream->vec))
            "default tz"))))
