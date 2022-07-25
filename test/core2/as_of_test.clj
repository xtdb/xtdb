(ns core2.as-of-test
  (:require [clojure.test :as t]
            [core2.api :as c2]
            [core2.operator :as op]
            [core2.snapshot :as snap]
            [core2.test-util :as tu]
            [core2.util :as util])
  (:import (java.time Clock Duration)
           (java.util List ArrayList)))

(def ^:dynamic ^List *vts*)

(t/use-fixtures :each
  (fn with-recording-clock [f]
    (binding [*vts* (ArrayList.)]
      (let [clock (-> (Clock/systemUTC)
                      (Clock/tick (Duration/ofNanos 1000)))
            clock (proxy [Clock] []
                    (instant []
                      (let [i (.instant clock)]
                        (.add *vts* i)
                        i)))]
        (tu/with-opts {:core2.tx-producer/tx-producer {:clock clock}}
          f))))

  tu/with-node)

(def end-of-time-zdt (util/->zdt util/end-of-time))

(t/deftest test-as-of-tx
  (let [snapshot-factory (tu/component ::snap/snapshot-factory)

        !tx1 (c2/submit-tx tu/*node* [[:put {:_id :my-doc, :last-updated "tx1"}]])
        _ (Thread/sleep 10) ; prevent same-ms transactions
        !tx2 (c2/submit-tx tu/*node* [[:put {:_id :my-doc, :last-updated "tx2"}]])]

    (t/is (= #{{:last-updated "tx1"} {:last-updated "tx2"}}
             (set (op/query-ra '[:scan [last-updated]]
                               (snap/snapshot snapshot-factory !tx2)))))

    (t/is (= #{{:last-updated "tx1"} {:last-updated "tx2"}}
             (->> (c2/plan-datalog tu/*node*
                                   (-> '{:find [?last-updated]
                                         :where [[?e :last-updated ?last-updated]]}
                                       (assoc :basis {:tx !tx2})))
                  (into #{}))))

    (t/testing "at tx1"
      (t/is (= #{{:last-updated "tx1"}}
               (set (op/query-ra '[:scan [last-updated]]
                                 (snap/snapshot snapshot-factory !tx1)))))

      (t/is (= #{{:last-updated "tx1"}}
               (->> (c2/plan-datalog tu/*node*
                                     (-> '{:find [?last-updated]
                                           :where [[?e :last-updated ?last-updated]]}
                                         (assoc :basis {:tx !tx1})))
                    (into #{})))))))

(t/deftest test-valid-time
  (let [snapshot-factory (tu/component ::snap/snapshot-factory)

        {:keys [tx-time] :as tx1} @(c2/submit-tx tu/*node* [[:put {:_id :doc, :version 1}]
                                                            [:put {:_id :doc-with-vt}
                                                             {:_valid-time-start #inst "2021"}]])
        tx-time (util/->zdt tx-time)

        start-vt (util/->zdt (first *vts*))

        db (snap/snapshot snapshot-factory tx1)]

    (t/is (= {:doc {:_id :doc,
                    :_valid-time-start start-vt
                    :_valid-time-end end-of-time-zdt
                    :_tx-time-start tx-time
                    :_tx-time-end end-of-time-zdt}
              :doc-with-vt {:_id :doc-with-vt,
                            :_valid-time-start (util/->zdt #inst "2021")
                            :_valid-time-end end-of-time-zdt
                            :_tx-time-start tx-time
                            :_tx-time-end end-of-time-zdt}}
             (->> (op/query-ra '[:scan [_id
                                        _valid-time-start _valid-time-end
                                        _tx-time-start _tx-time-end]]
                               db)
                  (into {} (map (juxt :_id identity))))))))

(t/deftest test-tx-time
  (let [snapshot-factory (tu/component ::snap/snapshot-factory)

        _ @(c2/submit-tx tu/*node* [[:put {:_id :doc, :version 0}]])

        _ (Thread/sleep 10) ; prevent same-ms transactions

        tx2 @(c2/submit-tx tu/*node* [[:put {:_id :doc, :version 1}]])
        tt2 (util/->zdt (:tx-time tx2))

        db (snap/snapshot snapshot-factory tx2)

        [vt1 vt2] (map util/->zdt *vts*)

        replaced-v0-doc {:_id :doc, :version 0
                         :_valid-time-start vt1
                         :_valid-time-end vt2
                         :_tx-time-start tt2
                         :_tx-time-end end-of-time-zdt}

        v1-doc {:_id :doc, :version 1
                :_valid-time-start vt2
                :_valid-time-end end-of-time-zdt
                :_tx-time-start tt2
                :_tx-time-end end-of-time-zdt}]

    (t/is (= [replaced-v0-doc v1-doc]
             (op/query-ra '[:scan [_id version
                                   _valid-time-start _valid-time-end
                                   _tx-time-start _tx-time-end]]
                          db))
          "all vt")

    #_ ; FIXME
    (t/is (= [original-v0-doc replaced-v0-doc v1-doc]
             (op/query-ra '[:scan [_id version
                                   _valid-time-start _valid-time-end
                                   _tx-time-start {_tx-time-end (<= _tx-time-end ?eot)}]]
                          {'$ db, '?eot util/end-of-time}))
          "all vt, all tt")))

(t/deftest test-evict
  (let [snapshot-factory (tu/component ::snap/snapshot-factory)]
    (letfn [(all-time-docs [db]
              (->> (op/query-ra '[:scan [_id
                                         _valid-time-start {_valid-time-end (<= _valid-time-end ?eot)}
                                         _tx-time-start {_tx-time-end (<= _tx-time-end ?eot)}]]
                                {'$ db, '?eot util/end-of-time})
                   (map :_id)
                   frequencies))]

      (let [_ @(c2/submit-tx tu/*node* [[:put {:_id :doc, :version 0}]
                                        [:put {:_id :other-doc, :version 0}]])
            _ (Thread/sleep 10)         ; prevent same-ms transactions
            tx2 @(c2/submit-tx tu/*node* [[:put {:_id :doc, :version 1}]])]

        (t/is (= {:doc 3, :other-doc 1} (all-time-docs (snap/snapshot snapshot-factory tx2)))
              "documents present before evict"))

      (let [tx3 @(c2/submit-tx tu/*node* [[:evict :doc]])]
        (t/is (= {:other-doc 1} (all-time-docs (snap/snapshot snapshot-factory tx3)))
              "documents removed after evict")))))
