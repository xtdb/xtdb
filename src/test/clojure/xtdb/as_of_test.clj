(ns xtdb.as-of-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.test-util :as tu]
            [xtdb.util :as util]))

(t/use-fixtures :once tu/with-allocator)
(t/use-fixtures :each tu/with-node)

(t/deftest test-as-of-tx
  (let [tx1 (xt/submit-tx tu/*node* [[:put :xt_docs {:xt/id :my-doc, :last-updated "tx1"}]])]
    (xt/submit-tx tu/*node* [[:put :xt_docs {:xt/id :my-doc, :last-updated "tx2"}]])

    (t/is (= #{{:last-updated "tx1"} {:last-updated "tx2"}}
             (set (tu/query-ra '[:scan {:table xt_docs} [last-updated]]
                               {:node tu/*node* :default-all-valid-time? true}))))

    (t/is (= #{{:last-updated "tx2"}}
             (set (xt/q tu/*node*
                        '{:find [last-updated]
                          :where [(match :xt_docs {:xt/id e})
                                  [e :last-updated last-updated]]}))))

    (t/testing "at tx1"
      (t/is (= #{{:last-updated "tx1"}}
               (set (tu/query-ra '[:scan {:table xt_docs} [last-updated]]
                                 {:node tu/*node*, :basis {:tx tx1}}))))

      (t/is (= #{{:last-updated "tx1"}}
               (set (xt/q tu/*node*
                          '{:find [last-updated]
                            :where [(match :xt_docs {:xt/id e})
                                    [e :last-updated last-updated]]}
                          {:basis {:tx tx1}})))))))

(t/deftest test-app-time
  (let [{:keys [system-time]} (xt/submit-tx tu/*node* [[:put :xt_docs {:xt/id :doc, :version 1}]
                                                       [:put :xt_docs {:xt/id :doc-with-app-time}
                                                        {:for-valid-time [:in #inst "2021"]}]])
        system-time (util/->zdt system-time)]

    (t/is (= {:doc {:xt/id :doc,
                    :xt/valid-from system-time
                    :xt/valid-to nil
                    :xt/system-from system-time
                    :xt/system-to nil}
              :doc-with-app-time {:xt/id :doc-with-app-time,
                                  :xt/valid-from (util/->zdt #inst "2021")
                                  :xt/valid-to nil
                                  :xt/system-from system-time
                                  :xt/system-to nil}}
             (->> (tu/query-ra '[:scan {:table xt_docs}
                                 [xt/id
                                  xt/valid-from xt/valid-to
                                  xt/system-from xt/system-to]]
                               {:node tu/*node*})
                  (into {} (map (juxt :xt/id identity))))))))

(t/deftest test-system-time
  (let [tx1 (xt/submit-tx tu/*node* [[:put :xt_docs {:xt/id :doc, :version 0}]])
        tt1 (util/->zdt (:system-time tx1))

        tx2 (xt/submit-tx tu/*node* [[:put :xt_docs {:xt/id :doc, :version 1}]])
        tt2 (util/->zdt (:system-time tx2))

        original-v0-doc {:xt/id :doc, :version 0
                         :xt/valid-from tt1
                         :xt/valid-to tt2
                         :xt/system-from tt1
                         :xt/system-to nil}

        replaced-v0-doc {:xt/id :doc, :version 0
                         :xt/valid-from tt2
                         :xt/valid-to nil
                         :xt/system-from tt1
                         :xt/system-to tt2}

        v1-doc {:xt/id :doc, :version 1
                :xt/valid-from tt2
                :xt/valid-to nil
                :xt/system-from tt2
                :xt/system-to nil}]

    (t/is (= #{original-v0-doc v1-doc}
             (set (tu/query-ra '[:scan {:table xt_docs}
                                 [xt/id version
                                  xt/valid-from xt/valid-to
                                  xt/system-from xt/system-to]]
                               {:node tu/*node*, :default-all-valid-time? true})))
          "all app-time")

    (t/is (= #{original-v0-doc replaced-v0-doc v1-doc}
             (set (tu/query-ra '[:scan {:table xt_docs, :for-system-time :all-time}
                                 [xt/id version
                                  xt/valid-from xt/valid-to
                                  xt/system-from xt/system-to]]
                               {:node tu/*node*
                                :default-all-valid-time? true})))
          "all app, all sys")))

(t/deftest test-evict
  (letfn [(all-time-docs []
            (->> (tu/query-ra '[:scan {:table xt_docs, :for-valid-time :all-time, :for-system-time :all-time}
                                [xt/id xt/valid-from xt/valid-to xt/system-from xt/system-to]]
                              {:node tu/*node*
                               :default-all-valid-time? true})
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
