(ns core2.datalog.temporal-test
  (:require [clojure.test :as t :refer [deftest]]
            [core2.test-util :as tu]
            [core2.api :as c2]))

(t/use-fixtures :each tu/with-node)

(deftest simple-temporal-tests
  (let [!tx1 (c2/submit-tx tu/*node* [[:put {:id 1 :foo "2000-4000"} {:app-time-start #inst "2000" :app-time-end #inst "4000"}]])
        !tx2 (c2/submit-tx tu/*node* [[:put {:id 1 :foo "3000-"} {:app-time-start #inst "3000"}]])]

    ;; as of tx tests
    (t/is (= [{:foo "2000-4000"}]
             (c2/datalog-query tu/*node* (assoc '{:find [foo]
                                                  :where
                                                  [[1 :foo foo]]}
                                                :basis {:tx !tx1}))))
    (t/is (= [{:foo "2000-4000" }]
             (c2/datalog-query tu/*node* (assoc '{:find [foo]
                                                  :where
                                                  [[1 :foo foo]]}
                                                :basis {:tx !tx2}))))
    ;; app-time
    (t/is (= []
             (c2/datalog-query tu/*node* (assoc '{:find [foo]
                                                  :where
                                                  [[1 :foo foo]]}
                                                :basis {:tx !tx2
                                                        :current-time (.toInstant #inst "1999")}))))
    (t/is (= [{:foo "2000-4000" }]
             (c2/datalog-query tu/*node* (assoc '{:find [foo]
                                                  :where
                                                  [[1 :foo foo]]}
                                                :basis {:tx !tx2
                                                        :current-time (.toInstant #inst "2000")}))))

    (t/is (= [{:foo "3000-" }]
             (c2/datalog-query tu/*node* (assoc '{:find [foo]
                                                  :where
                                                  [[1 :foo foo]]}
                                                :basis {:tx !tx2
                                                        :current-time (.toInstant #inst "3001")}))))

    (t/is (= []
             (c2/datalog-query tu/*node* (assoc '{:find [foo]
                                                  :where
                                                  [[1 :foo foo]]}
                                                :basis {:tx !tx1 ; <- first transaction
                                                        :current-time (.toInstant #inst "4001")}))))
    ;; sys-time
    (t/is (=  []
              (c2/datalog-query tu/*node* (assoc '{:find [foo]
                                                   :where
                                                   [[1 :foo foo]]}
                                                 :basis {:tx (c2/->TransactionInstant 0 (.toInstant #inst "2000"))}))))

    (t/is (=  [{:foo "2000-4000"}]
              (c2/datalog-query tu/*node* (assoc '{:find [foo]
                                                   :where
                                                   [[1 :foo foo]]}
                                                 :basis {:tx (c2/->TransactionInstant 0  (.toInstant (java.util.Date.)))}))))))
