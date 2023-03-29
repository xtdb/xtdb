(ns xtdb.temporal.histogram-test
  (:require [clojure.test :as t]
            [xtdb.temporal.histogram :as hist])
  (:import [xtdb.temporal.histogram IBin IHistogram]))

(defn- bins->seq [^IHistogram h]
  (for [^IBin b (.getBins h)]
    [(.getValue b) (.getCount b)]))

;; See Appendix A.
;; "A Streaming Parallel Decision Tree Algorithm"
;; https://www.jmlr.org/papers/volume11/ben-haim10a/ben-haim10a.pdf

(t/deftest example-histogram
  (let [^IHistogram h (hist/->histogram 5)]
    (doseq [v [23 19 10 16 36]]
      (.update h v))

    (t/is (= [[10.0 1] [16.0 1] [19.0 1] [23.0 1] [36.0 1]]
             (bins->seq h)))
    (t/is (= 5 (.getTotal h)))
    (t/is (= 10.0 (.getMin h)))
    (t/is (= 36.0 (.getMax h)))

    (.update h 2)

    (t/is (= [[2.0 1] [10.0 1] [17.5 2] [23.0 1] [36.0 1]]
             (bins->seq h)))
    (t/is (= 6 (.getTotal h)))

    (.update h 9)

    (t/is (= [[2.0 1] [9.5 2] [17.5 2] [23.0 1] [36.0 1]]
             (bins->seq h)))

    (doseq [v [32 30 45]]
      (.update h v))

    (t/is (= [[2.0 1] [9.5 2] [19.333333333333332 3] [32.666666666666664 3] [45.0 1]]
             (bins->seq h)))

    (t/is (= 3.275064636598679 (.sum h 15)))
    (t/is (= 0.0 (.sum h 1)))
    (t/is (= 10.0 (.sum h 45)))

    ;; NOTE: this is a bit off from paper.
    (t/is (= [15.222890825137508 28.96296296296297] (vec (.uniform h 3))))

    (t/is (= 10 (.getTotal h)))
    (t/is (= 2.0 (.getMin h)))
    (t/is (= 45.0 (.getMax h)))))
