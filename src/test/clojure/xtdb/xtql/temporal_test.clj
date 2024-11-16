(ns xtdb.xtql.temporal-test
  (:require [clojure.test :as t :refer [deftest]]
            [xtdb.api :as xt]
            [xtdb.test-util :as tu]
            [xtdb.serde :as serde])
  (:import [xtdb.api TransactionKey]))

(t/use-fixtures :each tu/with-node)

#_ ; FIXME port to XTQL
(deftest simple-temporal-tests
  (xt/submit-tx tu/*node* [[:put-docs {:into :xt_docs, :valid-from #inst "2000", :valid-to #inst "4000"}
                            {:xt/id 1 :foo "2000-4000"}]])
  (xt/submit-tx tu/*node* [[:put-docs {:into :xt_docs, :valid-from #inst "3000"}
                                {:xt/id 1 :foo "3000-"}]])

  ;; as of tx tests
  (t/is (= [{:foo "2000-4000"}]
           (xt/q tu/*node* '{:find [foo]
                             :where [(match :xt_docs {:xt/id 1})
                                     [1 :foo foo]]})))
  (t/is (= [{:foo "2000-4000"}]
           (xt/q tu/*node*
                 '{:find [foo]
                   :where [(match :xt_docs {:xt/id 1})
                           [1 :foo foo]]}
                 {:snapshot-time #inst "2020-01-01"})))
  ;; app-time
  (t/is (= []
           (xt/q tu/*node* '{:find [foo]
                             :where [(match :xt_docs {:xt/id 1})
                                     [1 :foo foo]]}
                 {:current-time #inst "1999"})))
  (t/is (= [{:foo "2000-4000"}]
           (xt/q tu/*node* '{:find [foo]
                             :where [(match :xt_docs {:xt/id 1})
                                     [1 :foo foo]]}
                 {:current-time #inst "2000"})))

  (t/is (= [{:foo "3000-"}]
           (xt/q tu/*node* '{:find [foo]
                             :where [(match :xt_docs {:xt/id 1})
                                     [1 :foo foo]]}
                 {:snapshot-time #inst "2020-01-02"
                  :current-time #inst "3001"})))

  (t/is (= []
           (xt/q tu/*node* '{:find [foo]
                             :where [(match :xt_docs {:xt/id 1})
                                     [1 :foo foo]]}
                 {:snapshot-time #inst "2020", :current-time #inst "4001"})))

  ;; system-time - eugh, TODO, we need to just be able to pass a system-time to basis
  (t/is (=  []
            (xt/q tu/*node* '{:find [foo]
                              :where [(match :xt_docs {:xt/id 1})
                                      [1 :foo foo]]}
                  {:snapshot-time #inst "2000"})))

  (t/is (=  [{:foo "2000-4000"}]
            (xt/q tu/*node* '{:find [foo]
                              :where [(match :xt_docs {:xt/id 1})
                                      [1 :foo foo]]}
                  {:snapshot-time (java.util.Date.)}))))
