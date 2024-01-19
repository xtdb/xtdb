(ns xtdb.xtql.tx-ops-test
  (:require [clojure.test :as t]
            [xtdb.tx-ops :as tx-ops]))

(defn roundtrip-tx-op [dml]
  (tx-ops/unparse-tx-op (tx-ops/parse-tx-op dml)))

(t/deftest test-parse-insert
  (t/is (= '[:insert-into :users (from :old-users [xt/id {:first-name given-name} {:last-name surname}])]

           (roundtrip-tx-op '[:insert-into :users (from :old-users [xt/id {:first-name given-name, :last-name surname}])]))))

(t/deftest test-parse-update
  (t/is (= '[:update {:table :foo
                      :for-valid-time (in #inst "2020" nil)
                      :bind [{:xt/id $uid} {:version v}]
                      :set {:version (inc v)}}]

           (roundtrip-tx-op '[:update {:table :foo
                                       :for-valid-time (from #inst "2020")
                                       :bind [{:xt/id $uid, :version v}]
                                       :set {:version (inc v)}}])))

  (t/is (= '[:update {:table :foo
                      :bind [{:xt/id foo} {:version v}]
                      :unify [(from :bar [{:xt/id $bar-id}, foo])]
                      :set {:version (inc v)}}]

           (roundtrip-tx-op '[:update {:table :foo
                                       :bind [{:xt/id foo, :version v}]
                                       :unify [(from :bar [{:xt/id $bar-id, :foo foo}])]
                                       :set {:version (inc v)}}])))

  (t/is (thrown-with-msg?
         xtdb.IllegalArgumentException #"Illegal argument: 'xtql/malformed-bind'"
         (roundtrip-tx-op '[:update {:table :foo
                                     :bind {:not-a vector}
                                     :set {:version (inc v)}}]))))

(t/deftest test-parse-delete
  (t/is (= '[:delete {:from :foo
                      :for-valid-time (in #inst "2020" nil),
                      :bind [{:xt/id $uid} {:version v}]}]

           (roundtrip-tx-op '[:delete {:from :foo
                                       :for-valid-time (in #inst "2020" nil),
                                       :bind [{:xt/id $uid} {:version v}]}])))

  (t/is (= '[:delete {:from :foo
                      :bind [{:xt/id foo} {:version v}]
                      :unify [(from :bar [{:xt/id $bar-id}, foo])]}]

           (roundtrip-tx-op '[:delete {:from :foo
                                       :bind [{:xt/id foo} {:version v}]
                                       :unify [(from :bar [{:xt/id $bar-id}, foo])]}]))))

(t/deftest test-parse-erase
  (t/is (= '[:erase {:from :foo, :bind [{:xt/id $uid} {:version v}]}]

           (roundtrip-tx-op '[:erase {:from :foo, :bind [{:xt/id $uid, :version v}]}])))

  (t/is (= '[:erase {:from :foo
                     :bind [{:xt/id foo} {:version v}]
                     :unify [(from :bar [{:xt/id $bar-id}, foo])]}]

           (roundtrip-tx-op '[:erase {:from :foo
                                      :bind [{:xt/id foo} {:version v}]
                                      :unify [(from :bar [{:xt/id $bar-id}, foo])]}]))))

(t/deftest test-parse-assert
  (t/is (= [:assert-exists '(from :users [{:email $email}])]
           (roundtrip-tx-op [:assert-exists '(from :users [{:email $email}])])))

  (t/is (= [:assert-not-exists '(from :users [{:email $email}])]
           (roundtrip-tx-op [:assert-not-exists '(from :users [{:email $email}])]))))
