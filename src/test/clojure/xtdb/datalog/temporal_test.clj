(ns xtdb.datalog.temporal-test
  (:require [clojure.test :as t :refer [deftest]]
            [xtdb.api :as xt.api]
            [xtdb.datalog :as xt]
            [xtdb.test-util :as tu]))

(t/use-fixtures :each tu/with-node)

(deftest simple-temporal-tests
  (let [tx1 (xt/submit-tx tu/*node* [[:put :xt_docs {:id 1 :foo "2000-4000"} {:app-time-start #inst "2000" :app-time-end #inst "4000"}]])
        tx2 (xt/submit-tx tu/*node* [[:put :xt_docs {:id 1 :foo "3000-"} {:app-time-start #inst "3000"}]])]

    ;; as of tx tests
    (t/is (= [{:foo "2000-4000"}]
             (xt/q tu/*node*  '{:find [foo]
                                :where [(match :xt_docs {:id 1})
                                        [1 :foo foo]]})))
    (t/is (= [{:foo "2000-4000"}]
             (xt/q tu/*node* (assoc '{:find [foo]
                                      :where [(match :xt_docs {:id 1})
                                              [1 :foo foo]]}
                                    :basis {:tx tx2}))))
    ;; app-time
    (t/is (= []
             (xt/q tu/*node* (assoc '{:find [foo]
                                      :where
                                      [(match :xt_docs {:id 1})
                                       [1 :foo foo]]}
                                    :basis {:tx tx2
                                            :current-time (.toInstant #inst "1999")}))))
    (t/is (= [{:foo "2000-4000" }]
             (xt/q tu/*node* (assoc '{:find [foo]
                                      :where [(match :xt_docs {:id 1})
                                              [1 :foo foo]]}
                                    :basis {:tx tx2
                                            :current-time (.toInstant #inst "2000")}))))

    (t/is (= [{:foo "3000-" }]
             (xt/q tu/*node* (assoc '{:find [foo]
                                      :where [(match :xt_docs {:id 1})
                                              [1 :foo foo]]}
                                    :basis {:tx tx2
                                            :current-time (.toInstant #inst "3001")}))))

    (t/is (= []
             (xt/q tu/*node* (assoc '{:find [foo]
                                      :where [(match :xt_docs {:id 1})
                                              [1 :foo foo]]}
                                    :basis {:tx tx1 ; <- first transaction
                                            :current-time (.toInstant #inst "4001")}))))

    ;; sys-time - eugh, TODO, we need to just be able to pass a sys-time to basis
    (t/is (=  []
              (xt/q tu/*node* (assoc '{:find [foo]
                                       :where [(match :xt_docs {:id 1})
                                               [1 :foo foo]]}
                                     :basis {:tx (xt.api/->TransactionInstant 0 (.toInstant #inst "2000"))}))))

    (t/is (=  [{:foo "2000-4000"}]
              (xt/q tu/*node* (assoc '{:find [foo]
                                       :where [(match :xt_docs {:id 1})
                                               [1 :foo foo]]}
                                     :basis {:tx (xt.api/->TransactionInstant 0 (.toInstant (java.util.Date.)))}))))))
