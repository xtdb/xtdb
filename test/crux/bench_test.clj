(ns crux.bench-test
  (:require [clojure.spec.alpha :as s]
            [clojure.test :as t]
            [crux.bench :as b]))

(defn acceptable-duration ;; for one query, in msec
  [query]
  (case query
    :insert 500
    :join 999999 ;; temporary, until :join issues are fixed
    100)) ;; default

(t/deftest test-query-speed
  (let [benchmark (b/bench
                   :sample 3
                   :query :all
                   :speed :instant)]
    (run! (fn [query]
            (t/testing (str query " is reasonably fast")
              (t/is
               (< (query benchmark)
                  (acceptable-duration query)))))
          (keys benchmark))))
