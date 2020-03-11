(ns crux.compaction2-test
  (:require [clojure.test :as t]
            [crux.codec :as c]
            [crux.compaction :as cc]
            [crux.fixtures.api :as fapi :refer [*api*]]
            [crux.fixtures.kv :as kvf]
            [crux.fixtures.standalone :as fs]
            [crux.node :as n]))

(defn with-compaction [f]
  (fapi/with-opts (-> fapi/*opts*
                      (update ::n/topology conj cc/module))
    f))

(t/use-fixtures :each fs/with-standalone-node with-compaction kvf/with-kv-dir fapi/with-node)

(t/deftest test-compact-below-watermark
  (with-redefs [cc/valid-time-watermark (fn [_] #inst "2017")]
    (t/testing "preserve initial document below watermark"
      (let [{:crux.tx/keys [tx-time]} (fapi/submit+await-tx [[:crux.tx/put {:crux.db/id :ivan :name "Ivan-2015"} #inst "2015"]])]
        (t/is (.document *api* (c/new-id {:crux.db/id :ivan :name "Ivan-2015"})))))

    (t/testing "compact away below valid-time watermark"
      (let [{:crux.tx/keys [tx-time]} (fapi/submit+await-tx [[:crux.tx/put {:crux.db/id :ivan :name "Ivan-2016"} #inst "2016"]
                                                             [:crux.tx/put {:crux.db/id :ivan :name "Ivan-2020"} #inst "2020"]])]
        (t/is (not (.document *api* (c/new-id {:crux.db/id :ivan :name "Ivan-2015"}))))
        (t/is (.document *api* (c/new-id {:crux.db/id :ivan :name "Ivan-2016"})))
        (t/is (.document *api* (c/new-id {:crux.db/id :ivan :name "Ivan-2020"})))))

    (t/testing "do not compact away document that exists above and below watermark"
      (let [{:crux.tx/keys [tx-time]} (fapi/submit+await-tx [[:crux.tx/put {:crux.db/id :ivan :name "Ivan-2015"} #inst "2015"]
                                                             [:crux.tx/put {:crux.db/id :ivan :name "Ivan-2015"} #inst "2021"]])]
        (t/is (.document *api* (c/new-id {:crux.db/id :ivan :name "Ivan-2015"})))
        (t/is (.document *api* (c/new-id {:crux.db/id :ivan :name "Ivan-2016"})))
        (t/is (.document *api* (c/new-id {:crux.db/id :ivan :name "Ivan-2020"})))))))
