(ns xtdb.bitemporal-test
  (:require [clojure.test :as t]
            [xtdb.bitemporal :as bitemp])
  (:import [java.util LinkedList]
           xtdb.bitemporal.RowConsumer))

(t/deftest test-correct-rectangle-cutting
  (letfn [(test-er [& events]
            (let [!state (atom [])
                  rc (reify RowConsumer
                       (accept [_ idx valid-from valid-to sys-from sys-to]
                         (swap! !state conj [idx valid-from valid-to sys-from sys-to])))
                  er (bitemp/event-resolver (LinkedList.))]
              (doseq [[idx valid-from valid-to sys-from] events]
                (.resolveEvent er idx valid-from valid-to sys-from rc))
              @!state))]

    (t/is (= [[1 2005 2009 1 Long/MAX_VALUE] [0 2010 2020 0 Long/MAX_VALUE]]
             (test-er [1 2005 2009 1]
                      [0 2010 2020 0]))
          "period starts before and does NOT overlap")

    (t/is (= [[1 2010 2020 1 Long/MAX_VALUE]
              [0 2015 2020 0 1]
              [0 2020 2025 0 Long/MAX_VALUE]]
             (test-er [1 2010 2020 1]
                      [0 2015 2025 0]))
          "period starts before and overlaps")

    (t/is (= [[1 2010 2020 1 Long/MAX_VALUE]
              [0 2010 2020 0 1]
              [0 2020 2025 0 Long/MAX_VALUE]]
             (test-er [1 2010 2020 1]
                      [0 2010 2025 0]))
          "period starts equally and overlaps")

    (t/is (= [[1 2015 2020 1 Long/MAX_VALUE]
              [0 2010 2015 0 Long/MAX_VALUE]
              [0 2015 2020 0 1]
              [0 2020 2025 0 Long/MAX_VALUE]]
             (test-er [1 2015 2020 1]
                      [0 2010 2025 0]))
          "newer period completely covered")

    (t/is (= [[1 2010 2025 1 Long/MAX_VALUE]
              [0 2010 2020 0 1]]
             (test-er [1 2010 2025 1]
                      [0 2010 2020 0]))
          "older period completely covered")

    (t/is (= [[1 2015 2025 1 Long/MAX_VALUE]
              [0 2010 2015 0 Long/MAX_VALUE]
              [0 2015 2025 0 1]]
             (test-er [1 2015 2025 1]
                      [0 2010 2025 0]))
          "period end equally and overlaps")

    (t/is (= [[1 2015 2025 1 Long/MAX_VALUE]
              [0 2010 2015 0 Long/MAX_VALUE]
              [0 2015 2020 0 1]]
             (test-er [1 2015 2025 1]
                      [0 2010 2020 0]))
          "period ends after and overlaps")

    (t/is (= [[1 2005 2010 1 Long/MAX_VALUE]
              [0 2010 2020 0 Long/MAX_VALUE]]
             (test-er [1 2005 2010 1]
                      [0 2010 2020 0]))
          "period starts before and touches")

    (t/is (= [[1 2010 2020 1 Long/MAX_VALUE]
              [0 2005 2010 0 Long/MAX_VALUE]]
             (test-er [1 2010 2020 1]
                      [0 2005 2010 0]))
          "period starts after and touches")

    (t/is (= [[1 2010 2020 1 Long/MAX_VALUE]
              [0 2005 2009 0 Long/MAX_VALUE]]
             (test-er [1 2010 2020 1]
                      [0 2005 2009 0]))
          "period starts after and does NOT overlap")))
