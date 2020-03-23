(ns crux.compaction-test
  (:require [clojure.test :as t]
            [crux.codec :as c]
            [crux.api :as api]
            [crux.compaction :as cc]
            [crux.fixtures.api :as fapi :refer [*api*]]
            [crux.fixtures.bus :as bf]
            [crux.fixtures.compaction :as cf]
            [crux.fixtures.kv :as kvf]
            [crux.fixtures.standalone :as fs]
            [crux.kv :as kv])
  (:import java.util.Calendar))

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
    (let [compacted-events (bf/listen! *api* :crux.compaction/compacted)]
      (t/testing "preserve initial document below watermark"
        (let [{:crux.tx/keys [tx-time]} (fapi/submit+await-tx [[:crux.tx/put {:crux.db/id :ivan :name "Ivan-2015"} #inst "2015"]])]
          (t/is (.document *api* (c/new-id {:crux.db/id :ivan :name "Ivan-2015"})))
          (bf/wait-for-bus-event! compacted-events 4000)
          (t/is (= 0 (:crux.compaction/compacted-count (first @compacted-events))))))

      (t/testing "compact away below valid-time watermark"
        (reset! compacted-events [])
        (let [{:crux.tx/keys [tx-time]} (fapi/submit+await-tx [[:crux.tx/put {:crux.db/id :ivan :name "Ivan-2016"} #inst "2016"]
                                                               [:crux.tx/put {:crux.db/id :ivan :name "Ivan-2020"} #inst "2020"]])]
          (bf/wait-for-bus-event! compacted-events 4000)
          (t/is (= 1 (:crux.compaction/compacted-count (first @compacted-events))))
          (t/is (not (.document *api* (c/new-id {:crux.db/id :ivan :name "Ivan-2015"}))))
          (t/is (.document *api* (c/new-id {:crux.db/id :ivan :name "Ivan-2016"})))
          (t/is (.document *api* (c/new-id {:crux.db/id :ivan :name "Ivan-2020"})))

          (t/testing "prune from the AV indices"
            (t/is (av-exists? :crux.db/id :ivan {:crux.db/id :ivan :name "Ivan-2016"}))
            (t/is (not (av-exists? :name "Ivan-2015" {:crux.db/id :ivan :name "Ivan-2015"}))))))

      (t/testing "do not compact away document that exists above and below watermark"
        (reset! compacted-events [])
        (let [{:crux.tx/keys [tx-time]} (fapi/submit+await-tx [[:crux.tx/put {:crux.db/id :ivan :name "Ivan-2015"} #inst "2015"]
                                                               [:crux.tx/put {:crux.db/id :ivan :name "Ivan-2015"} #inst "2021"]])]
          (t/is (= 0 (:crux.compaction/compacted-count (first @compacted-events))))
          (t/is (.document *api* (c/new-id {:crux.db/id :ivan :name "Ivan-2015"})))
          (t/is (.document *api* (c/new-id {:crux.db/id :ivan :name "Ivan-2016"})))
          (t/is (.document *api* (c/new-id {:crux.db/id :ivan :name "Ivan-2020"}))))))))

(t/deftest test-should-compact-using-window-against-tx-time
  (assert (:tt-vt-interval-s (:indexer *api*)))
  (t/testing "compact away doc below the watermark"
    (let [compacted-events (bf/listen! *api* :crux.compaction/compacted)
          {:crux.tx/keys [tx-time]} (fapi/submit+await-tx [[:crux.tx/put {:crux.db/id :ivan :name "Ivan-2015"} #inst "2015"]
                                                           [:crux.tx/put {:crux.db/id :ivan :name "Ivan-2016"} #inst "2016"]])]
      (bf/wait-for-bus-event! compacted-events 4000)
      (t/is (= 1 (:crux.compaction/compacted-count (first @compacted-events))))
      (t/is (not (.document *api* (c/new-id {:crux.db/id :ivan :name "Ivan-2015"}))))

      (reset! compacted-events [])
      (fapi/submit+await-tx [[:crux.tx/put {:crux.db/id :ivan :name "Ivan-Now"}]])
      (bf/wait-for-bus-event! compacted-events 4000)
      (t/is (= 0 (:crux.compaction/compacted-count (first @compacted-events))))

      (reset! compacted-events [])
      (fapi/submit+await-tx [[:crux.tx/put {:crux.db/id :ivan :name "Ivan-Now1"}]])
      (bf/wait-for-bus-event! compacted-events 4000)
      (t/is (= 0 (:crux.compaction/compacted-count (first @compacted-events)))))))

(t/deftest test-compact-valid-time-chunks
  (let [compaction-fn cc/txes-to-compact]
    (with-redefs [cc/valid-time-watermark (fn [_ _] #inst "2017")
                  cc/txes-to-compact (fn [_ txes] (compaction-fn 86400 txes))]
      (let [compacted-events (bf/listen! *api* :crux.compaction/compacted)
            txes (for [each-day (range 7)
                       each-hour (range 24)
                       :let [vt (.getTime
                                 (doto (Calendar/getInstance)
                                   (.set Calendar/YEAR 2000)
                                   (.set Calendar/MONTH 0)
                                   (.set Calendar/DAY_OF_MONTH (inc each-day))
                                   (.set Calendar/HOUR_OF_DAY each-hour)))]]
                   [:crux.tx/put {:crux.db/id :ivan :v {:crux.db/id :ivan :d each-day :h each-hour}} vt])]
        (fapi/submit+await-tx (vec txes))
        (bf/wait-for-bus-event! compacted-events 4000)
        (t/is (= 160 (:crux.compaction/compacted-count (first @compacted-events))))
        (t/is (= 8 (count (keep #(api/document *api* (:crux.db/content-hash %))
                                (api/history *api* :ivan)))))
        (t/is (= [{:crux.db/id :ivan, :v {:crux.db/id :ivan, :d 6, :h 23}}
                  {:crux.db/id :ivan, :v {:crux.db/id :ivan, :d 6, :h 22}}
                  {:crux.db/id :ivan, :v {:crux.db/id :ivan, :d 5, :h 23}}
                  {:crux.db/id :ivan, :v {:crux.db/id :ivan, :d 4, :h 23}}
                  {:crux.db/id :ivan, :v {:crux.db/id :ivan, :d 3, :h 23}}
                  {:crux.db/id :ivan, :v {:crux.db/id :ivan, :d 2, :h 23}}
                  {:crux.db/id :ivan, :v {:crux.db/id :ivan, :d 1, :h 23}}
                  {:crux.db/id :ivan, :v {:crux.db/id :ivan, :d 0, :h 23}}]
                 (keep #(api/document *api* (:crux.db/content-hash %))
                       (api/history *api* :ivan))))))))
