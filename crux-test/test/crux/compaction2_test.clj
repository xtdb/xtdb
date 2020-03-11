(ns crux.compaction2-test
  (:require [clojure.test :as t]
            [crux.codec :as c]
            [crux.compaction :as cc]
            [crux.fixtures.api :as fapi :refer [*api*]]
            [crux.fixtures.kv :as kvf]
            [crux.fixtures.standalone :as fs]))

(t/use-fixtures :each fs/with-standalone-node kvf/with-kv-dir fapi/with-node)

(defn- assert-present [& docs]
  (doseq [doc docs]
    (assert (.document *api* (c/new-id doc)) doc)))

(t/deftest test-compact-below-watermark
  (t/testing "preserve initial document below watermark"
    (let [{:crux.tx/keys [tx-time]} (fapi/submit+await-tx [[:crux.tx/put {:crux.db/id :ivan :name "Ivan-2015"} #inst "2015"]])]
      (assert-present {:crux.db/id :ivan :name "Ivan-2015"})
      (let [db (.db *api*)]
        (with-open [snapshot (.newSnapshot db)]
          (cc/compact (:object-store *api*) snapshot :ivan #inst "2017" tx-time)))
      (t/is (.document *api* (c/new-id {:crux.db/id :ivan :name "Ivan-2015"})))))

  (t/testing "compact away below valid-time watermark"
    (let [{:crux.tx/keys [tx-time]} (fapi/submit+await-tx [[:crux.tx/put {:crux.db/id :ivan :name "Ivan-2016"} #inst "2016"]
                                                           [:crux.tx/put {:crux.db/id :ivan :name "Ivan-2020"} #inst "2020"]])]
      (assert-present {:crux.db/id :ivan :name "Ivan-2015"}
                      {:crux.db/id :ivan :name "Ivan-2016"}
                      {:crux.db/id :ivan :name "Ivan-2020"})
      (let [db (.db *api*)]
        (with-open [snapshot (.newSnapshot db)]
          (cc/compact (:object-store *api*) snapshot :ivan #inst "2017" tx-time)))
      (t/is (not (.document *api* (c/new-id {:crux.db/id :ivan :name "Ivan-2015"}))))
      (t/is (.document *api* (c/new-id {:crux.db/id :ivan :name "Ivan-2016"})))
      (t/is (.document *api* (c/new-id {:crux.db/id :ivan :name "Ivan-2020"})))))

  (t/testing "do not compact away document that exists above and below watermark"
    (let [{:crux.tx/keys [tx-time]} (fapi/submit+await-tx [[:crux.tx/put {:crux.db/id :ivan :name "Ivan-2015"} #inst "2015"]
                                                           [:crux.tx/put {:crux.db/id :ivan :name "Ivan-2015"} #inst "2021"]])]
      (assert-present {:crux.db/id :ivan :name "Ivan-2015"}
                      {:crux.db/id :ivan :name "Ivan-2016"}
                      {:crux.db/id :ivan :name "Ivan-2020"})
      (let [db (.db *api*)]
        (with-open [snapshot (.newSnapshot db)]
          (cc/compact (:object-store *api*) snapshot :ivan #inst "2017" tx-time)))
      (t/is (.document *api* (c/new-id {:crux.db/id :ivan :name "Ivan-2015"})))
      (t/is (.document *api* (c/new-id {:crux.db/id :ivan :name "Ivan-2016"})))
      (t/is (.document *api* (c/new-id {:crux.db/id :ivan :name "Ivan-2020"})))))

  (t/testing "handle no data present below watermark"
    (assert-present {:crux.db/id :ivan :name "Ivan-2015"})
    (let [db (.db *api*)]
      (with-open [snapshot (.newSnapshot db)]
        (cc/compact (:object-store *api*) snapshot :ivan #inst "2000" (:crux.tx/tx-time (.latestCompletedTx *api*)))))))
