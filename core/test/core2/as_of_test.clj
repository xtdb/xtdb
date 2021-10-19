(ns core2.as-of-test
  (:require [clojure.test :as t]
            [core2.api :as c2]
            [core2.operator :as op]
            [core2.snapshot :as snap]
            [core2.temporal :as temporal]
            [core2.test-util :as tu])
  (:import core2.api.TransactionInstant
           java.time.Instant
           java.util.Date))

(t/use-fixtures :each tu/with-node)

(t/deftest test-as-of-tx
  (let [snapshot-factory (tu/component ::snap/snapshot-factory)

        !tx1 (c2/submit-tx tu/*node* [[:put {:_id "my-doc", :last-updated "tx1"}]])
        _ (Thread/sleep 10) ; prevent same-ms transactions
        !tx2 (c2/submit-tx tu/*node* [[:put {:_id "my-doc", :last-updated "tx2"}]])]

    (t/is (= #{{:last-updated "tx2"}}
             (set (op/query-ra '[:scan [last-updated]]
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
               (set (op/query-ra '[:scan [last-updated]]
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
                                                             {:_valid-time-start #c2/instant "2021"}]])

        db (snap/snapshot snapshot-factory tx1)]

    (t/is (= {"doc" {:_id "doc",
                     :_valid-time-start tx-time
                     :_valid-time-end temporal/end-of-time
                     :_tx-time-start tx-time
                     :_tx-time-end temporal/end-of-time}
              "doc-with-vt" {:_id "doc-with-vt",
                             :_valid-time-start #c2/instant "2021"
                             :_valid-time-end temporal/end-of-time
                             :_tx-time-start tx-time
                             :_tx-time-end temporal/end-of-time}}
             (->> (op/query-ra '[:scan [_id
                                        _valid-time-start _valid-time-end
                                        _tx-time-start _tx-time-end]]
                               db)
                  (into {} (map (juxt :_id identity))))))))

(t/deftest test-tx-time
  (let [snapshot-factory (tu/component ::snap/snapshot-factory)

        {tt1 :tx-time} @(c2/submit-tx tu/*node* [[:put {:_id "doc", :version 0}]])
        _ (Thread/sleep 10) ; prevent same-ms transactions
        {tt2 :tx-time, :as tx2} @(c2/submit-tx tu/*node* [[:put {:_id "doc", :version 1}]])

        db (snap/snapshot snapshot-factory tx2)

        replaced-v0-doc {:_id "doc", :version 0
                         :_valid-time-start tt1
                         :_valid-time-end tt2
                         :_tx-time-start tt2
                         :_tx-time-end temporal/end-of-time}

        v1-doc {:_id "doc", :version 1
                :_valid-time-start tt2
                :_valid-time-end temporal/end-of-time
                :_tx-time-start tt2
                :_tx-time-end temporal/end-of-time}]

    (t/is (= [v1-doc]
             (op/query-ra '[:scan [_id version
                                   _valid-time-start _valid-time-end
                                   _tx-time-start _tx-time-end]]
                          db))
          "point in time")

    (t/is (= [replaced-v0-doc v1-doc]
             (op/query-ra '[:scan [_id version
                                   _valid-time-start {_valid-time-end (<= _valid-time-end ?eot)}
                                   _tx-time-start _tx-time-end]]
                          {'$ db, '?eot temporal/end-of-time}))
          "all vt")

    #_ ; FIXME
    (t/is (= [original-v0-doc replaced-v0-doc v1-doc]
             (op/query-ra '[:scan [_id version
                                   _valid-time-start {_valid-time-end (<= _valid-time-end ?eot)}
                                   _tx-time-start {_tx-time-end (<= _tx-time-end ?eot)}]]
                          {'$ db, '?eot temporal/end-of-time}))
          "all vt, all tt")))

(t/deftest test-evict
  (let [snapshot-factory (tu/component ::snap/snapshot-factory)]
    (letfn [(all-time-docs [db]
              (->> (op/query-ra '[:scan [_id
                                         _valid-time-start {_valid-time-end (<= _valid-time-end ?eot)}
                                         _tx-time-start {_tx-time-end (<= _tx-time-end ?eot)}]]
                                {'$ db, '?eot temporal/end-of-time})
                   (map :_id)
                   frequencies))]

      (let [_ @(c2/submit-tx tu/*node* [[:put {:_id "doc", :version 0}]
                                        [:put {:_id "other-doc", :version 0}]])
            _ (Thread/sleep 10)         ; prevent same-ms transactions
            tx2 @(c2/submit-tx tu/*node* [[:put {:_id "doc", :version 1}]])]

        (t/is (= {"doc" 3, "other-doc" 1} (all-time-docs (snap/snapshot snapshot-factory tx2)))
              "documents present before evict"))

      (let [tx3 @(c2/submit-tx tu/*node* [[:evict "doc"]])]
        (t/is (= {"other-doc" 1} (all-time-docs (snap/snapshot snapshot-factory tx3)))
              "documents removed after evict")))))
