(ns xtdb.bench.test-benchmark
  (:require [xtdb.api :as xt]
            [xtdb.bench :as bench])
  (:import (java.time Duration)))

(defn benchmark [{:keys [seed duration] :or {seed 0}}]
  (let [duration (Duration/parse duration)]
    {:title "A simple benchmark for testing transactions and queries"
     :seed seed
     :tasks
     [{:t :concurrently
       :duration duration
       :join-wait (Duration/ofSeconds 5)
       :thread-tasks [{:t :freq-job
                       :duration duration
                       :freq (Duration/ofMillis 100)
                       :job-task {:t :call,
                                  :transaction :insert-documents,
                                  :f (bench/wrap-in-catch
                                      (fn [{:keys [node] :as worker}]
                                        (xt/submit-tx node [(->>
                                                             (fn [] {:xt/id (rand-int 1000000)
                                                                     :foo (bench/random-str worker)})
                                                             (repeatedly 10)
                                                             (into [:put-docs :docs]))])))}}

                      {:t :freq-job
                       :duration duration
                       :freq (Duration/ofMillis 200)
                       :job-task {:t :call,
                                  :transaction :query-documents,
                                  :f (bench/wrap-in-catch
                                      (fn [{:keys [node]}]
                                        (xt/q node '(from :docs [xt/id foo]))))}}]}]}))
