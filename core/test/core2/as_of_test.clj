(ns core2.as-of-test
  (:require [clojure.test :as t]
            [core2.api :as c2]
            [core2.test-util :as tu]
            [core2.temporal :as temporal]
            [core2.snapshot :as snap]
            [core2.operator :as op]))

(t/use-fixtures :each tu/with-node)

(t/deftest test-as-of-tx
  (let [snapshot-factory (tu/component ::snap/snapshot-factory)

        !tx1 (c2/submit-tx tu/*node* [[:put {:_id "my-doc", :last-updated "tx1"}]])
        _ (Thread/sleep 10) ; prevent same-ms transactions
        !tx2 (c2/submit-tx tu/*node* [[:put {:_id "my-doc", :last-updated "tx2"}]])]

    (t/is (= #{{:last-updated "tx2"}}
             (into #{} (op/plan-ra '[:scan [last-updated]]
                                   (snap/snapshot snapshot-factory !tx2)))))

    (t/is (= #{{:last-updated "tx2"}}
             (->> (c2/plan-query tu/*node*
                                 (-> '{:find [?last-updated]
                                       :where [[?e :last-updated ?last-updated]]}
                                     (assoc :basis {:tx !tx2})))
                  (into #{}))))

    (t/is (= #{{:last-updated "tx1"}}
             (->> (c2/plan-query tu/*node*
                                 (-> '{:find [?last-updated]
                                       :where [[?e :last-updated ?last-updated]]}

                                     (assoc :basis {:default-valid-time (:tx-time @!tx1), :tx !tx2})))
                  (into #{}))))

    (t/testing "at tx1"
      (t/is (= #{{:last-updated "tx1"}}
               (into #{} (op/plan-ra '[:scan [last-updated]]
                                     (snap/snapshot snapshot-factory !tx1)))))

      (t/is (= #{{:last-updated "tx1"}}
               (->> (c2/plan-query tu/*node*
                                   (-> '{:find [?last-updated]
                                         :where [[?e :last-updated ?last-updated]]}
                                       (assoc :basis {:tx !tx1})))
                    (into #{})))))))

(t/deftest test-valid-time
  (let [snapshot-factory (tu/component ::snap/snapshot-factory)

        {:keys [tx-time] :as tx1} @(c2/submit-tx tu/*node* [[:put {:_id "doc", :version 1}]
                                                            [:put {:_id "doc-with-vt"}
                                                             {:_valid-time-start #inst "2021"}]])

        db (snap/snapshot snapshot-factory tx1)]

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
             (->> (op/plan-ra '[:scan [_id
                                       _valid-time-start _valid-time-end
                                       _tx-time-start _tx-time-end]]
                              db)
                  (into {} (map (juxt :_id identity))))))))
