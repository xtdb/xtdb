(ns core2.as-of-test
  (:require [clojure.test :as t]
            [core2.api :as c2]
            [core2.ingester :as ingest]
            [core2.test-util :as tu]
            [core2.util :as util])
  (:import core2.InstantSource
           (java.time Clock Duration)
           (java.util List ArrayList)))

(def ^:dynamic ^List *app-times*)

(t/use-fixtures :each
  (fn with-recording-clock [f]
    (binding [*app-times* (ArrayList.)]
      (let [clock (-> (Clock/systemUTC)
                      (Clock/tick (Duration/ofNanos 1000)))
            instant-src (reify InstantSource
                          (instant [_]
                            (let [i (.instant clock)]
                              (.add *app-times* i)
                              i)))]
        (tu/with-opts {:core2.tx-producer/tx-producer {:instant-src instant-src}}
          f))))

  tu/with-node)

(def end-of-time-zdt (util/->zdt util/end-of-time))

(t/deftest test-as-of-tx
  (let [ingester (tu/component :core2/ingester)

        !tx1 (c2/submit-tx tu/*node* [[:put {:id :my-doc, :last-updated "tx1"}]])
        _ (Thread/sleep 10) ; prevent same-ms transactions
        !tx2 (c2/submit-tx tu/*node* [[:put {:id :my-doc, :last-updated "tx2"}]])]

    (t/is (= #{{:last-updated "tx1"} {:last-updated "tx2"}}
             (set (tu/query-ra '[:scan [last-updated]]
                               {:srcs {'$ (ingest/snapshot ingester !tx2)}}))))

    (t/is (= #{{:last-updated "tx1"} {:last-updated "tx2"}}
             (->> (c2/plan-datalog tu/*node*
                                   (-> '{:find [?last-updated]
                                         :where [[?e :last-updated ?last-updated]]}
                                       (assoc :basis {:tx !tx2})))
                  (into #{}))))

    (t/testing "at tx1"
      (t/is (= #{{:last-updated "tx1"}}
               (set (tu/query-ra '[:scan [last-updated]]
                                 {:srcs {'$ (ingest/snapshot ingester !tx1)}}))))

      (t/is (= #{{:last-updated "tx1"}}
               (->> (c2/plan-datalog tu/*node*
                                     (-> '{:find [?last-updated]
                                           :where [[?e :last-updated ?last-updated]]}
                                         (assoc :basis {:tx !tx1})))
                    (into #{})))))))

(t/deftest test-app-time
  (let [ingester (tu/component :core2/ingester)

        {:keys [sys-time] :as tx1} @(c2/submit-tx tu/*node* [[:put {:id :doc, :version 1}]
                                                             [:put {:id :doc-with-app-time}
                                                              {:app-time-start #inst "2021"}]])
        sys-time (util/->zdt sys-time)

        start-app-time (util/->zdt (first *app-times*))]

    (t/is (= {:doc {:id :doc,
                    :application_time_start start-app-time
                    :application_time_end end-of-time-zdt
                    :system_time_start sys-time
                    :system_time_end end-of-time-zdt}
              :doc-with-app-time {:id :doc-with-app-time,
                                  :application_time_start (util/->zdt #inst "2021")
                                  :application_time_end end-of-time-zdt
                                  :system_time_start sys-time
                                  :system_time_end end-of-time-zdt}}
             (->> (tu/query-ra '[:scan [id
                                        application_time_start application_time_end
                                        system_time_start system_time_end]]
                               {:srcs {'$ (ingest/snapshot ingester tx1)}})
                  (into {} (map (juxt :id identity))))))))

(t/deftest test-sys-time
  (let [ingester (tu/component :core2/ingester)

        _ @(c2/submit-tx tu/*node* [[:put {:id :doc, :version 0}]])

        _ (Thread/sleep 10) ; prevent same-ms transactions

        tx2 @(c2/submit-tx tu/*node* [[:put {:id :doc, :version 1}]])
        tt2 (util/->zdt (:sys-time tx2))

        db (ingest/snapshot ingester tx2)

        [app-time1 app-time2] (map util/->zdt *app-times*)

        replaced-v0-doc {:id :doc, :version 0
                         :application_time_start app-time1
                         :application_time_end app-time2
                         :system_time_start tt2
                         :system_time_end end-of-time-zdt}

        v1-doc {:id :doc, :version 1
                :application_time_start app-time2
                :application_time_end end-of-time-zdt
                :system_time_start tt2
                :system_time_end end-of-time-zdt}]

    (t/is (= [replaced-v0-doc v1-doc]
             (tu/query-ra '[:scan [id version
                                   application_time_start application_time_end
                                   system_time_start system_time_end]]
                          {:srcs {'$ db}}))
          "all app-time")

    #_ ; FIXME
    (t/is (= [original-v0-doc replaced-v0-doc v1-doc]
             (tu/query-ra '[:scan [id version
                                   application_time_start application_time_end
                                   system_time_start {system_time_end (<= system_time_end ?eot)}]]
                          {:srcs {'$ db}
                           :params {'?eot util/end-of-time}}))
          "all app, all sys")))

(t/deftest test-evict
  (let [ingester (tu/component :core2/ingester)]
    (letfn [(all-time-docs [db]
              (->> (tu/query-ra '[:scan [id
                                         application_time_start {application_time_end (<= application_time_end ?eot)}
                                         system_time_start {system_time_end (<= system_time_end ?eot)}]]
                                {:srcs {'$ db}
                                 :params {'?eot util/end-of-time}})
                   (map :id)
                   frequencies))]

      (let [_ @(c2/submit-tx tu/*node* [[:put {:id :doc, :version 0}]
                                        [:put {:id :other-doc, :version 0}]])
            _ (Thread/sleep 10)         ; prevent same-ms transactions
            tx2 @(c2/submit-tx tu/*node* [[:put {:id :doc, :version 1}]])]

        (t/is (= {:doc 3, :other-doc 1} (all-time-docs (ingest/snapshot ingester tx2)))
              "documents present before evict"))

      (let [tx3 @(c2/submit-tx tu/*node* [[:evict :doc]])]
        (t/is (= {:other-doc 1} (all-time-docs (ingest/snapshot ingester tx3)))
              "documents removed after evict")))))
