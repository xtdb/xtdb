(ns xtdb.cache.second-chance-test
  (:require [clojure.test :as t]
            [xtdb.cache.second-chance :as scc])
  (:import (java.time Duration)))

(set! *warn-on-reflection* false)

;; https://github.com/xtdb/xtdb/pull/1821
(t/deftest cache-size-back-pressure-test
  (let [stop (atom false)]
    (try
      (let [sample-for (Duration/ofSeconds 1)
            sample-frequency (Duration/ofMillis 10)
            cache-size 1024
            n-elements (* 1024 1024)
            n-threads 42

            elements (vec (repeatedly n-elements #(rand-int Integer/MAX_VALUE)))
            scc (scc/->second-chance-cache
                  {:cache-size cache-size
                   :adaptive-sizing? false
                   :cooling-factor 0.1
                   :adaptive-break-even-level 0.8
                   :downsize-load-factor 0.01})
            mutate (fn [] (let [k (rand-nth elements)] (.computeIfAbsent scc k identity (constantly k))))
            sizes-ref (atom [])]

        (dorun (repeatedly n-threads #(future (while (not @stop) (mutate)))))

        (loop [sample-until (+ (System/currentTimeMillis) (.toMillis sample-for))]
          (swap! sizes-ref conj (.size (.getHot scc)))
          (Thread/sleep (max 1 (.toMillis sample-frequency)))
          (when (< (System/currentTimeMillis) sample-until)
            (recur sample-until)))

        (let [sizes @sizes-ref
              max-size (reduce max 0 sizes)]
          (t/is (<= max-size (+ cache-size 2048)))))

      (finally (reset! stop true)))))
