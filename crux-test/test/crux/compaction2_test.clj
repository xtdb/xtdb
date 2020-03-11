(ns crux.compaction2-test
  (:require [clojure.test :as t]
            [crux.codec :as c]
            [crux.node :as n]
            [crux.compaction :as cc]
            [crux.fixtures.api :as fapi :refer [*api*]]
            [crux.fixtures.kv :as kvf]
            [crux.fixtures.standalone :as fs]))

(t/use-fixtures :each fs/with-standalone-node kvf/with-kv-dir fapi/with-node)

(t/deftest test-can-use-api-to-access-crux
  (t/testing "compact away before valid-time watermark"
    (let [{:crux.tx/keys [tx-time]} (apif/submit+await-tx [[:crux.tx/put {:crux.db/id :ivan :name "Ivan-2015"} #inst "2015"]
                                                           [:crux.tx/put {:crux.db/id :ivan :name "Ivan-2016"} #inst "2016"]
                                                           [:crux.tx/put {:crux.db/id :ivan :name "Ivan-2020"} #inst "2020"]])]
      (assert (and (.document *api* (c/new-id {:crux.db/id :ivan :name "Ivan-2015"}))
                   (.document *api* (c/new-id {:crux.db/id :ivan :name "Ivan-2016"}))
                   (.document *api* (c/new-id {:crux.db/id :ivan :name "Ivan-2020"}))))
      (let [db (.db *api*)]
        (with-open [snapshot (.newSnapshot db)]
          (cc/compact (:object-store *api*) snapshot :ivan #inst "2017" tx-time)))
      (t/is (not (.document *api* (c/new-id {:crux.db/id :ivan :name "Ivan-2015"}))))
      (t/is (.document *api* (c/new-id {:crux.db/id :ivan :name "Ivan-2016"})))
      (t/is (.document *api* (c/new-id {:crux.db/id :ivan :name "Ivan-2020"}))))))
