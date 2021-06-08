(ns core2.as-of-test
  (:require [clojure.test :as t]
            [core2.core :as c2]
            [core2.test-util :as tu]))

(t/use-fixtures :each tu/with-node)

(t/deftest test-as-of-tx
  (let [tx1 (c2/submit-tx tu/*node* [{:op :put, :doc {:_id "my-doc", :last-updated "tx1"}}])
        _ (Thread/sleep 10) ; prevent same-ms transactions
        tx2 (c2/submit-tx tu/*node* [{:op :put, :doc {:_id "my-doc", :last-updated "tx2"}}])]

    (c2/with-db [db tu/*node* {:tx tx2}]
      (t/is (= #{{:last-updated "tx2"}}
               (into #{} (c2/plan-ra '[:scan [last-updated]] db))))

      (t/is (= #{{:last-updated "tx2"}}
               (into #{} (c2/plan-q '{:find [?last-updated]
                                      :where [[?e :last-updated ?last-updated]]}
                                    db)))))

    (c2/with-db [db tu/*node* {:tx tx1}]
      (t/is (= #{{:last-updated "tx1"}}
               (into #{} (c2/plan-ra '[:scan [last-updated]] db))))

      (t/is (= #{{:last-updated "tx1"}}
               (into #{} (c2/plan-q '{:find [?last-updated]
                                      :where [[?e :last-updated ?last-updated]]}
                                    db)))))))
