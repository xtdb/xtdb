(ns core2.as-of-test
  (:require [clojure.test :as t]
            [core2.core :as c2]
            [core2.test-util :as tu]
            [core2.temporal :as temporal]))

(t/use-fixtures :each tu/with-node)

(t/deftest test-as-of-tx
  (let [!tx1 (c2/submit-tx tu/*node* [{:op :put, :doc {:_id "my-doc", :last-updated "tx1"}}])
        _ (Thread/sleep 10) ; prevent same-ms transactions
        !tx2 (c2/submit-tx tu/*node* [{:op :put, :doc {:_id "my-doc", :last-updated "tx2"}}])]

    (let [db (c2/db tu/*node* {:tx !tx2})]
      (t/is (= #{{:last-updated "tx2"}}
               (into #{} (c2/plan-ra '[:scan [last-updated]] db))))

      (t/is (= #{{:last-updated "tx2"}}
               (into #{} (c2/plan-q '{:find [?last-updated]
                                      :where [[?e :last-updated ?last-updated]]}
                                    db))))

      (t/is (= #{{:last-updated "tx1"}}
               (into #{} (c2/plan-q '{:find [?last-updated]
                                      :where [[?e :last-updated ?last-updated]]}
                                    {:default-valid-time (:tx-time @!tx1)}
                                    db)))))

    (let [db (c2/db tu/*node* {:tx !tx1})]
      (t/is (= #{{:last-updated "tx1"}}
               (into #{} (c2/plan-ra '[:scan [last-updated]] db))))

      (t/is (= #{{:last-updated "tx1"}}
               (into #{} (c2/plan-q '{:find [?last-updated]
                                      :where [[?e :last-updated ?last-updated]]}
                                    db)))))))

(t/deftest test-valid-time
  (let [{:keys [tx-time] :as tx1} @(c2/submit-tx tu/*node* [{:op :put, :doc {:_id "doc", :version 1}}
                                                            {:op :put,
                                                             :doc {:_id "doc-with-vt"}
                                                             :_valid-time-start #inst "2021"}])
        db (c2/db tu/*node* {:tx tx1})]
    (t/is (= {"doc" {:_id "doc",
                     :_valid-time-start tx-time
                     :_valid-time-end temporal/end-of-time
                     :_tx-time-start tx-time
                     :_tx-time-end temporal/end-of-time}
              "doc-with-vt" {:_id "doc-with-vt",
                             :_valid-time-start #inst "2021"
                             :_valid-time-end temporal/end-of-time
                             :_tx-time-start tx-time
                             :_tx-time-end temporal/end-of-time}}
             (->> (c2/plan-ra '[:scan [_id
                                       _valid-time-start _valid-time-end
                                       _tx-time-start _tx-time-end]]
                              db)
                  (into {} (map (juxt :_id identity))))))))
