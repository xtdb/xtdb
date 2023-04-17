(ns xtdb.as-of-test
  (:require [clojure.test :as t]
            [xtdb.datalog :as xt]
            [xtdb.test-util :as tu]
            [xtdb.util :as util]))

(t/use-fixtures :once tu/with-allocator)
(t/use-fixtures :each tu/with-node)

(def end-of-time-zdt (util/->zdt util/end-of-time))

(t/deftest test-as-of-tx
  (let [tx1 (xt/submit-tx tu/*node* [[:put :xt_docs {:xt/id :my-doc, :last-updated "tx1"}]])
        tx2 (xt/submit-tx tu/*node* [[:put :xt_docs {:xt/id :my-doc, :last-updated "tx2"}]])]

    (t/is (= #{{:last-updated "tx1"} {:last-updated "tx2"}}
             (set (tu/query-ra '[:scan {:table xt_docs} [last-updated]]
                               {:node tu/*node* :default-all-app-time? true}))))

    (t/is (= #{{:last-updated "tx2"}}
             (set (xt/q tu/*node*
                        (-> '{:find [last-updated]
                              :where [(match :xt_docs {:xt/id e})
                                      [e :last-updated last-updated]]}
                            (assoc :basis {:tx tx2}))))))

    (t/testing "at tx1"
      (t/is (= #{{:last-updated "tx1"}}
               (set (tu/query-ra '[:scan {:table xt_docs} [last-updated]]
                                 {:node tu/*node*, :basis {:tx tx1}}))))

      (t/is (= #{{:last-updated "tx1"}}
               (set (xt/q tu/*node*
                          (-> '{:find [last-updated]
                                :where [(match :xt_docs {:xt/id e})
                                        [e :last-updated last-updated]]}
                              (assoc :basis {:tx tx1})))))))))

(t/deftest test-app-time
  (let [{:keys [sys-time]} (xt/submit-tx tu/*node* [[:put :xt_docs {:xt/id :doc, :version 1}]
                                                    [:put :xt_docs {:xt/id :doc-with-app-time}
                                                     {:for-app-time [:in #inst "2021"]}]])
        sys-time (util/->zdt sys-time)]

    (t/is (= {:doc {:xt/id :doc,
                    :application_time_start sys-time
                    :application_time_end end-of-time-zdt
                    :system_time_start sys-time
                    :system_time_end end-of-time-zdt}
              :doc-with-app-time {:xt/id :doc-with-app-time,
                                  :application_time_start (util/->zdt #inst "2021")
                                  :application_time_end end-of-time-zdt
                                  :system_time_start sys-time
                                  :system_time_end end-of-time-zdt}}
             (->> (tu/query-ra '[:scan {:table xt_docs}
                                 [xt/id
                                  application_time_start application_time_end
                                  system_time_start system_time_end]]
                               {:node tu/*node*})
                  (into {} (map (juxt :xt/id identity))))))))

(t/deftest test-sys-time
  (let [tx1 (xt/submit-tx tu/*node* [[:put :xt_docs {:xt/id :doc, :version 0}]])
        tt1 (util/->zdt (:sys-time tx1))

        tx2 (xt/submit-tx tu/*node* [[:put :xt_docs {:xt/id :doc, :version 1}]])

        tt2 (util/->zdt (:sys-time tx2))

        original-v0-doc {:xt/id :doc, :version 0
                         :application_time_start tt1
                         :application_time_end end-of-time-zdt
                         :system_time_start tt1
                         :system_time_end tt2}

        replaced-v0-doc {:xt/id :doc, :version 0
                         :application_time_start tt1
                         :application_time_end tt2
                         :system_time_start tt2
                         :system_time_end end-of-time-zdt}

        v1-doc {:xt/id :doc, :version 1
                :application_time_start tt2
                :application_time_end end-of-time-zdt
                :system_time_start tt2
                :system_time_end end-of-time-zdt}]

    (t/is (= [replaced-v0-doc v1-doc]
             (tu/query-ra '[:scan {:table xt_docs}
                            [xt/id version
                             application_time_start application_time_end
                             system_time_start system_time_end]]
                          {:node tu/*node* :default-all-app-time? true}))
          "all app-time")

    (t/is (= [original-v0-doc replaced-v0-doc v1-doc]
             (tu/query-ra '[:scan {:table xt_docs, :for-sys-time :all-time}
                            [xt/id version
                             application_time_start application_time_end
                             system_time_start system_time_end]]
                          {:node tu/*node*
                           :params {'eot util/end-of-time}
                           :default-all-app-time? true}))
          "all app, all sys")))

(t/deftest test-evict
  (letfn [(all-time-docs []
            (->> (tu/query-ra '[:scan {:table xt_docs, :for-sys-time :all-time}
                                [xt/id
                                 application_time_start {application_time_end (<= application_time_end eot)}
                                 system_time_start system_time_end]]
                              {:node tu/*node*, :params {'eot util/end-of-time}
                               :default-all-app-time? true})
                 (map :xt/id)
                 frequencies))]

    (xt/submit-tx tu/*node* [[:put :xt_docs {:xt/id :doc, :version 0}]
                             [:put :xt_docs {:xt/id :other-doc, :version 0}]])

    (xt/submit-tx tu/*node* [[:put :xt_docs {:xt/id :doc, :version 1}]])

    (t/is (= {:doc 3, :other-doc 1} (all-time-docs))
          "documents present before evict")

    (xt/submit-tx tu/*node* [[:evict :xt_docs :doc]])

    (t/is (= {:other-doc 1} (all-time-docs))
          "documents removed after evict")))
