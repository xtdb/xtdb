(ns crux.bench-test
  (:require [clojure.spec.alpha :as s]
            [clojure.test :as t]
            [crux.bench :as b]))

(defn acceptable-duration ;; for one query, in msec
  [query]
  (case query
    :insert 500
    :range 200
    100)) ;; default

(t/deftest test-query-speed
  (let [benchmark (b/bench
                   :sample 3
                   :query [:name :multiple-clauses :range] ;; excluding :join until it's fixed
                   :speed :instant)]
    (run! (fn [query]
            (t/testing (str query " is reasonably fast")
              (t/is
               (< (query benchmark)
                  (acceptable-duration query)))))
          (keys benchmark))))
