(ns xtdb.as-of-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.test-util :as tu]
            [xtdb.time :as time]))

(t/use-fixtures :once tu/with-allocator)
(t/use-fixtures :each tu/with-mock-clock tu/with-node)

(t/deftest test-as-of-tx
  (xt/submit-tx tu/*node* [[:put-docs :docs {:xt/id :my-doc, :last-updated "tx1"}]])
  (xt/submit-tx tu/*node* [[:put-docs :docs {:xt/id :my-doc, :last-updated "tx2"}]])

  (t/is (= #{{:last-updated "tx1"} {:last-updated "tx2"}}
           (set (tu/query-ra '[:scan {:table #xt/table docs, :for-valid-time :all-time} [last_updated]]
                             {:node tu/*node*}))))

  (t/is (= #{{:last-updated "tx2"}}
           (set (xt/q tu/*node* '(from :docs [last-updated])))))

  (t/testing "at tx1"
    (t/is (= #{{:last-updated "tx1"}}
             (set (tu/query-ra '[:scan {:table #xt/table docs} [last_updated]]
                               {:node tu/*node*, :snapshot-time (time/->instant #inst "2020")}))))

    (t/is (= #{{:last-updated "tx1"}}
             (set (xt/q tu/*node* '(from :docs [last-updated])
                        {:snapshot-time #inst "2020"}))))))

(t/deftest test-app-time
  (let [tx (xt/execute-tx tu/*node* [[:put-docs :docs {:xt/id :doc, :version 1}]
                                     [:put-docs {:into :docs, :valid-from #inst "2021"}
                                      {:xt/id :doc-with-app-time}]])
        system-time (time/->zdt (.getSystemTime tx))]

    (t/is (= {:doc {:xt/id :doc,
                    :xt/valid-from system-time
                    :xt/system-from system-time}
              :doc-with-app-time {:xt/id :doc-with-app-time,
                                  :xt/valid-from (time/->zdt #inst "2021")
                                  :xt/system-from system-time}}
             (->> (tu/query-ra '[:scan {:table #xt/table docs}
                                 [_id
                                  _valid_from _valid_to
                                  _system_from _system_to]]
                               {:node tu/*node*})
                  (into {} (map (juxt :xt/id identity))))))))

(t/deftest test-system-time
  (let [tx1 (xt/execute-tx tu/*node* [[:put-docs :docs {:xt/id :doc, :version 0}]])
        tt1 (time/->zdt (.getSystemTime tx1))

        tx2 (xt/execute-tx tu/*node* [[:put-docs :docs {:xt/id :doc, :version 1}]])
        tt2 (time/->zdt (.getSystemTime tx2))

        original-v0-doc {:xt/id :doc, :version 0
                         :xt/valid-from tt1
                         :xt/valid-to tt2
                         :xt/system-from tt1}

        replaced-v0-doc {:xt/id :doc, :version 0
                         :xt/valid-from tt2
                         :xt/system-from tt1
                         :xt/system-to tt2}

        v1-doc {:xt/id :doc, :version 1
                :xt/valid-from tt2
                :xt/system-from tt2}]

    (t/is (= #{original-v0-doc v1-doc}
             (set (xt/q tu/*node*
                        '(from :docs {:bind [xt/id version
                                             xt/valid-from xt/valid-to xt/system-from xt/system-to]
                                      :for-valid-time :all-time}))))
          "all app-time")

    (t/is (= #{original-v0-doc replaced-v0-doc v1-doc}
             (set (xt/q tu/*node*
                        '(from :docs {:bind [xt/id version
                                             xt/valid-from xt/valid-to xt/system-from xt/system-to]
                                      :for-valid-time :all-time
                                      :for-system-time :all-time}))))
          "all app, all sys")))

(t/deftest test-erase
  (letfn [(all-time-docs []
            (->> (xt/q tu/*node*
                       '(from :docs {:bind [xt/id xt/valid-from xt/valid-to xt/system-from xt/system-to]
                                     :for-valid-time :all-time
                                     :for-system-time :all-time}))
                 (map :xt/id)
                 frequencies))]

    (xt/submit-tx tu/*node* [[:put-docs :docs {:xt/id :doc, :version 0}]
                             [:put-docs :docs {:xt/id :other-doc, :version 0}]])

    (xt/submit-tx tu/*node* [[:put-docs :docs {:xt/id :doc, :version 1}]])

    (t/is (= {:doc 3, :other-doc 1} (all-time-docs))
          "documents present before erase")

    (xt/submit-tx tu/*node* [[:erase-docs :docs :doc]])

    (t/is (= {:other-doc 1} (all-time-docs))
          "documents removed after erase")))
