(ns crux.compaction-test
  (:require [clojure.test :as t]
            [crux.codec :as c]
            [crux.compaction :as cc]
            [crux.fixtures.api :as fapi :refer [*api*]]
            [crux.fixtures.compaction :as cf]
            [crux.fixtures.kv :as kvf]
            [crux.fixtures.standalone :as fs]
            [crux.kv :as kv]))

(t/use-fixtures :each fs/with-standalone-node cf/with-compaction kvf/with-kv-dir fapi/with-node)

(defn- av-exists? [k v doc]
  (with-open [snapshot (kv/new-snapshot (:kv-store *api*))]
    (let [avec-k (c/encode-avec-key-to nil
                                       (c/->id-buffer k)
                                       (c/->value-buffer v)
                                       (c/->id-buffer (:crux.db/id doc))
                                       (c/->id-buffer (c/new-id doc)))]
      (boolean (kv/get-value snapshot avec-k)))))

(t/deftest test-compact-below-watermark
  (with-redefs [cc/valid-time-watermark (fn [_ _] #inst "2017")]
    (t/testing "preserve initial document below watermark"
      (let [{:crux.tx/keys [tx-time]} (fapi/submit+await-tx [[:crux.tx/put {:crux.db/id :ivan :name "Ivan-2015"} #inst "2015"]])]
        (t/is (.document *api* (c/new-id {:crux.db/id :ivan :name "Ivan-2015"})))))

    (t/testing "compact away below valid-time watermark"
      (let [{:crux.tx/keys [tx-time]} (fapi/submit+await-tx [[:crux.tx/put {:crux.db/id :ivan :name "Ivan-2016"} #inst "2016"]
                                                             [:crux.tx/put {:crux.db/id :ivan :name "Ivan-2020"} #inst "2020"]])]
        ;; TODO intermittent, as compaction occurs AFTER last-submitted-tx committed
        (t/is (not (.document *api* (c/new-id {:crux.db/id :ivan :name "Ivan-2015"}))))
        (t/is (.document *api* (c/new-id {:crux.db/id :ivan :name "Ivan-2016"})))
        (t/is (.document *api* (c/new-id {:crux.db/id :ivan :name "Ivan-2020"})))

        (t/testing "prune from the AV indices"
          (t/is (av-exists? :crux.db/id :ivan {:crux.db/id :ivan :name "Ivan-2016"}))
          (t/is (not (av-exists? :name "Ivan-2015" {:crux.db/id :ivan :name "Ivan-2015"}))))))

    (t/testing "do not compact away document that exists above and below watermark"
      (let [{:crux.tx/keys [tx-time]} (fapi/submit+await-tx [[:crux.tx/put {:crux.db/id :ivan :name "Ivan-2015"} #inst "2015"]
                                                             [:crux.tx/put {:crux.db/id :ivan :name "Ivan-2015"} #inst "2021"]])]
        (t/is (.document *api* (c/new-id {:crux.db/id :ivan :name "Ivan-2015"})))
        (t/is (.document *api* (c/new-id {:crux.db/id :ivan :name "Ivan-2016"})))
        (t/is (.document *api* (c/new-id {:crux.db/id :ivan :name "Ivan-2020"})))))))

(t/deftest test-should-compact-using-window-against-tx-time
  (assert (:tt-vt-interval-s (:indexer *api*)))
  (t/testing "compact away doc below the watermark"
    (let [{:crux.tx/keys [tx-time]} (fapi/submit+await-tx [[:crux.tx/put {:crux.db/id :ivan :name "Ivan-2015"} #inst "2015"]
                                                           [:crux.tx/put {:crux.db/id :ivan :name "Ivan-2016"} #inst "2016"]])]
      (fapi/submit+await-tx [[:crux.tx/put {:crux.db/id :ivan :name "Ivan-Now"}]])
      (fapi/submit+await-tx [[:crux.tx/put {:crux.db/id :ivan :name "Ivan-Now1"}]])
      (t/is (not (.document *api* (c/new-id {:crux.db/id :ivan :name "Ivan-2015"}))))
      (t/is (.document *api* (c/new-id {:crux.db/id :ivan :name "Ivan-2016"})))
      (t/is (.document *api* (c/new-id {:crux.db/id :ivan :name "Ivan-Now"})))
      (t/is (.document *api* (c/new-id {:crux.db/id :ivan :name "Ivan-Now1"}))))))
