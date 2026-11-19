(ns xtdb.bench-test
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.test :as t :refer [deftest]]
            [xtdb.bench :as b])
  (:import (java.io File)))

(deftest summary-keys-report-the-total-time-of-their-tagged-stages
  (let [log-file (File/createTempFile "bench" ".log")]
    (try
      ((b/compile-benchmark {:title "test", :benchmark-type :test
                             :bench-log-file (str log-file)
                             :tasks [{:t :call, :stage :cold-queries-q1, :f (fn [_] (Thread/sleep 5))}
                                     {:t :do, :stage :hot-queries
                                      :tasks [{:t :call, :stage :hot-queries-q1, :summary-key :hot-query-time-ms
                                               :f (fn [_] (Thread/sleep 10))}
                                              {:t :call, :stage :hot-queries-q2, :summary-key :hot-query-time-ms
                                               :f (fn [_] (Thread/sleep 10))
                                               :verify (fn [_] (Thread/sleep 200))}]}]})
       nil)
      (let [records (with-open [rdr (io/reader log-file)]
                      (mapv #(json/read-str % :key-fn keyword) (line-seq rdr)))
            stage-ms (fn [stage] (:time-taken-ms (first (filter #(= stage (:stage %)) records))))
            summary (first (filter #(= "summary" (:stage %)) records))]
        (t/is (= (+ (stage-ms "hot-queries-q1") (stage-ms "hot-queries-q2"))
                 (get-in summary [:stage-totals :hot-query-time-ms])))
        (t/is (< (get-in summary [:stage-totals :hot-query-time-ms]) 200) "a stage's :verify isn't counted")
        (t/is (= #{:hot-query-time-ms} (set (keys (:stage-totals summary))))
              "untagged stages report nothing"))
      (finally
        (.delete log-file)))))
